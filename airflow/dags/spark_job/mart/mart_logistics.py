from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("mart_logistics") \
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

spark.sparkContext.setLogLevel('ERROR')

fact = spark.table("iceberg.gold.fact_order_item")
dim_date = spark.table("iceberg.gold.dim_date")
dim_seller = spark.table("iceberg.gold.dim_seller")
dim_customer = spark.table("iceberg.gold.dim_customer")

fact_enriched = (
    fact
    .join(
        dim_date.alias("purchase_date"),
        col("order_purchase_timestamp_sk") == col("purchase_date.date_sk"),
        "left"
    )
    .join(
        dim_date.alias("delivery_date"),
        col("order_delivery_customer_date_sk") == col("delivery_date.date_sk"),
        "left"
    )
    .join(
        dim_date.alias("estimated_date"),
        col("order_estimated_date_delivery_date_sk") == col("estimated_date.date_sk"),
        "left"
    )
    .join(dim_seller, "seller_sk", "left")
    .join(dim_customer, "customer_sk", "left")
)

fact_with_metrics = (
    fact_enriched
    .withColumn(
        "delivery_days",
        datediff(col("delivery_date.full_date"), col("purchase_date.full_date"))
    )
    .withColumn(
        "delay_days",
        datediff(col("delivery_date.full_date"), col("estimated_date.full_date"))
    )
    .withColumn(
        "late_flag",
        when(col("delay_days") > 0, 1).otherwise(0)
    )
)

mart_logistics = (
    fact_with_metrics
    .groupBy(
        col("delivery_date.date_sk").alias("delivery_date_sk"),
        col("delivery_date.year"),
        col("delivery_date.month"),
        col("seller_sk"),
        col("customer_sk")
    )
    .agg(
        sum("freight_value").alias("shipping_amount"),
        avg("delivery_days").alias("avg_delivery_days"),
        avg("delay_days").alias("avg_delay_days"),
        sum("late_flag").alias("late_items"),
        count("order_item_id").alias("items_delivered")
    )
)

mart_logistics.write \
    .format('iceberg') \
    .mode('overwrite') \
    .saveAsTable('iceberg.gold.mart_logistics')

spark.stop()
