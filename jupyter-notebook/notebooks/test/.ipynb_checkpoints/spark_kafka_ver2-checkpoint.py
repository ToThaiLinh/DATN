# spark_to_iceberg_stream.py
from pyspark.sql import SparkSession, functions as F, types as T
import logging
import sys

# Logging
logger = logging.getLogger("spark-iceberg-ingest")
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s")
handler.setFormatter(formatter)
logger.setLevel(logging.INFO)
logger.addHandler(handler)

KAFKA_BOOTSTRAP = "kafka1:9092"
KAFKA_TOPIC = "events.topic"
ICEBERG_TABLE = "iceberg.test.test_spark_kafka"

def create_spark():
    spark = (
        SparkSession.builder
        .appName("spark_kafka_iceberg_stream")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7,"
            "org.apache.kafka:kafka-clients:3.5.7"
        )
        .config("spark.cores.max", "1")
        .config("spark.executor.memory", "2g")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.iceberg",
                "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "hive")
        .config("spark.sql.catalog.iceberg.uri",
                "thrift://hive-metastore:9083")
        .config("spark.sql.catalog.iceberg.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.iceberg.warehouse",
                "s3a://warehouse/")
        .config("spark.sql.catalog.iceberg.s3.endpoint",
                "http://minio:9000")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def parse_kafka_value(df):
    schema = T.StructType([
        T.StructField("id", T.StringType()),
        T.StructField("created_at", T.StringType()),
        T.StructField("user_id", T.LongType()),
        T.StructField("amount", T.DoubleType()),
        T.StructField("category", T.StringType())
    ])

    parsed = (
        df.selectExpr(
            "CAST(key AS STRING) as kafka_key",
            "CAST(value AS STRING) as json_str",
            "topic",
            "partition",
            "offset",
            "timestamp"
        )
        .withColumn("data", F.from_json("json_str", schema))
        .select(
            "topic",
            "partition",
            "offset",
            "timestamp",
            F.col("data.*")
        )
        .withColumn("created_at", F.to_timestamp("created_at"))
    )

    return parsed


def main():
    spark = create_spark()
    logger.info("Spark session created")

    df_kafka = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = parse_kafka_value(df_kafka)

    # chọn các cột cần ghi vào Iceberg
    final_df = parsed.select(
        "id",
        "created_at",
        "user_id",
        "amount",
        "category"
    )

    query = (
        final_df.writeStream
        .format("iceberg")
        .outputMode("append")
        .option("checkpointLocation",
                "./tmp/spark_checkpoints/kafka_to_iceberg_native")
        .trigger(processingTime="10 seconds")
        .toTable(ICEBERG_TABLE)
    )

    logger.info("Streaming write to Iceberg started")
    query.awaitTermination()


if __name__ == "__main__":
    main()
