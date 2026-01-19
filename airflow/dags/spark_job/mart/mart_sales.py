from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, countDistinct, count, lit
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("mart_sales") \
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

fact_order_item = spark.table("iceberg.gold.fact_order_item")

# Dimensions
dim_date = spark.table("iceberg.gold.dim_date")
dim_product = spark.table("iceberg.gold.dim_product")
dim_customer = spark.table("iceberg.gold.dim_customer")
dim_seller = spark.table("iceberg.gold.dim_seller")

fact_enriched = (
    fact_order_item
    .join(dim_date,
          fact_order_item.order_purchase_timestamp_sk == dim_date.date_sk,
          "left")
    .join(dim_product, "product_sk", "left")
    .join(dim_customer, "customer_sk", "left")
    .join(dim_seller, "seller_sk", "left")
)

mart_sales = (
    fact_enriched
    .groupBy(
        col("date_sk"),
        col("full_date"),
        col("year"),
        col("month"),
        col("product_sk"),
        col("seller_sk"),
        col("customer_sk"),
        col("order_status")
    )
    .agg(
        _sum("price").alias("gross_sales"),
        _sum("freight_value").alias("shipping_amount"),
        _sum("total_amount").alias("net_sales"),
        count("order_item_id").alias("items_sold"),
        countDistinct("order_id").alias("orders_count")
    )
)

mart_sales.write \
    .format('iceberg') \
    .mode('overwrite') \
    .saveAsTable('iceberg.gold.mart_sales')

spark.stop()