from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

# Initialize logging
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

spark = SparkSession.builder \
    .appName("spark_kafka") \
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

spark.sparkContext.setLogLevel("ERROR")

# Kafka
kafka_bootstrap_servers = "kafka1:9092"

transactionSchema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("date_time", TimestampType(), True),
    StructField("status", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("event_time", TimestampType(), True),   # MUST exist in Kafka
])

transactionDF = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", "ecommerce_transactions")
        .option("startingOffsets", "earliest")
        .load()
        .selectExpr("CAST(value AS STRING)")
        .select(from_json("value", transactionSchema).alias("data"))
        .select("data.*")
)

# If generator does not send event_time → fallback
transactionDF = transactionDF.withColumn(
    "event_time",
    when(col("event_time").isNull(), col("date_time")).otherwise(col("event_time"))
)

transactionDF = transactionDF.withWatermark("event_time", "2 minutes")

def foreach_batch_function(batchDF, batch_id):

    salesAnalysisDF = (
        batchDF.groupBy(
            window(col("event_time"), "1 minute"),
            col("product_id")
        )
        .agg(
            count("transaction_id").alias("number_of_sales"),
            sum("quantity").alias("total_quantity_sold"),
            approx_count_distinct("customer_id").alias("unique_customers")
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "product_id",
            "number_of_sales",
            "total_quantity_sold",
            "unique_customers"
        )
    )

    salesAnalysisDF.writeTo("iceberg.bronze.sales_per_minute").append()


query = (
    transactionDF.writeStream
        .foreachBatch(foreach_batch_function)
        .option("checkpointLocation", "./tmp/spark_checkpoints/kafka_to_iceberg")
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
)

query.awaitTermination()
