# spark_to_iceberg.py
# Chạy với pyspark (spark-submit) có thêm các jars/packages của kafka và iceberg
from pyspark.sql import SparkSession, functions as F, types as T
import logging
import sys

# Python logging (để log từ driver)
logger = logging.getLogger("spark-iceberg-ingest")
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s")
handler.setFormatter(formatter)
logger.setLevel(logging.INFO)
logger.addHandler(handler)

KAFKA_BOOTSTRAP = "kafka1:9092"
KAFKA_TOPIC = "events.topic"
ICEBERG_TABLE = "iceberg.test.test_spark_kafka"  # ví dụ: catalog.namespace.table

def create_spark():
    spark = SparkSession.builder \
    .appName("spark_kafka") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7,"
            "org.apache.kafka:kafka-clients:3.5.7") \
    .config("spark.cores.max", "1") \
    .config("spark.executor.memory", "2g") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "hive") \
    .config("spark.sql.catalog.iceberg.uri", "thrift://hive-metastore:9083") \
    .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.iceberg.warehouse", "s3a://warehouse/") \
    .config("spark.sql.catalog.iceberg.s3.endpoint", "http://minio:9000") \
    .getOrCreate()
    # set log level to WARN for spark internals to reduce noise (tùy bạn)
    spark.sparkContext.setLogLevel("WARN")
    return spark

def parse_kafka_value(df):
    """
    Kafka value schema: giả sử JSON string, ta parse
    """
    schema = T.StructType([
        T.StructField("id", T.StringType()),
        T.StructField("created_at", T.StringType()),
        T.StructField("user_id", T.LongType()),
        T.StructField("amount", T.DoubleType()),
        T.StructField("category", T.StringType())
    ])
    # value là binary -> string
    df2 = df.selectExpr("CAST(key AS STRING) as kafka_key", "CAST(value AS STRING) as json_str", "topic", "partition", "offset", "timestamp")
    parsed = df2.withColumn("data", F.from_json(F.col("json_str"), schema)).select(
        "kafka_key", "topic", "partition", "offset", "timestamp",
        F.col("data.*")
    )
    # tùy chỉnh: convert kiểu, timestamp parsing
    parsed = parsed.withColumn("created_at_ts", F.to_timestamp("created_at"))
    return parsed

def foreach_batch_function(batch_df, batch_id):
    """
    batch_df is a DataFrame for this micro-batch (batch mode).
    Trong đây bạn dùng write API tương thích Iceberg.
    """
    logger.info("Processing batch_id=%s, rows=%s", batch_id, batch_df.count())
    if batch_df.rdd.isEmpty():
        logger.info("Empty batch %s - skip", batch_id)
        return

    # Ví dụ: chuẩn hóa column, chọn cột cần lưu
    to_write = batch_df.select(
        F.col("id"),
        F.col("created_at_ts").alias("created_at"),
        F.col("user_id"),
        F.col("amount"),
        F.col("category")
    )

    # Một cách viết (DataFrameWriter V1 style với format iceberg)
    # Bạn có thể dùng .save("catalog.db.table") hoặc .saveAsTable nếu muốn
    try:
        # Cách 1: dùng format("iceberg").mode("append").save(tableIdentifier)
        # Khi dùng catalog namespaced identifier, save lại bằng catalog.table
        to_write.write.format("iceberg").mode("append").save(ICEBERG_TABLE)
        logger.info("Batch %s written to Iceberg table %s (append)", batch_id, ICEBERG_TABLE)
    except Exception as e:
        logger.exception("Failed to write batch %s to Iceberg: %s", batch_id, e)
        # Tùy chính sách: có thể raise để job restart, hoặc gửi alert. Mình raise để chuyển sự cố lên hệ thống.
        raise

def main():
    spark = create_spark()
    logger.info("Spark session created.")
    # đọc từ Kafka (structured streaming)
    df_kafka = (spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
                .option("subscribe", KAFKA_TOPIC)
                .option("startingOffsets", "earliest")  # hoặc "latest"
                .option("failOnDataLoss", "false")
                .load())

    parsed = parse_kafka_value(df_kafka)

    # hiển thị schema, sample log
    logger.info("Parsed schema: %s", parsed.schema.simpleString())
    parsed.printSchema()

    # dùng foreachBatch để ghi vào Iceberg (sử dụng batch API cho stability & exactly-once semantics)
    query = (parsed.writeStream
             .foreachBatch(foreach_batch_function)
             .option("checkpointLocation", "./tmp/spark_checkpoints/kafka_to_iceberg")  # đặt path checkpoint hợp lý
             .outputMode("append")
             .trigger(processingTime="10 seconds")  # micro-batch mỗi 10s, tùy chỉnh
             .start())

    logger.info("Stream started, awaiting termination...")
    query.awaitTermination()

if __name__ == "__main__":
    main()
