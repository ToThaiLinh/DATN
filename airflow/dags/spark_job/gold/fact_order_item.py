from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("fact_order_item") \
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

orders = spark.table("iceberg.silver.order_clean")
order_items = spark.table("iceberg.silver.order_item_clean")

fact_base = (
    order_items.alias("oi")
    .join(
        orders.alias("o"),
        col("oi.order_id") == col("o.order_id"),
        "inner"
    )
    .select(
        col("oi.order_id"),
        col("oi.order_item_id"),
        col("o.customer_id"),
        col("oi.product_id"),
        col("oi.seller_id"),

        col("o.order_purchase_timestamp"),
        col("o.order_approved_at"),
        col("o.order_delivered_carrier_date"),
        col("o.order_delivered_customer_date"),
        col("o.order_estimated_delivery_date"),
        col("oi.shipping_limit_date"),

        col("oi.price"),
        col("oi.freight_value"),
        col("o.order_status"),
        col("oi.ingestion_time")
    )
)

dim_customer = (
    spark.table("iceberg.gold.dim_customer")
    .filter(col("is_current") == True)
    .select("customer_id", "customer_sk")
)

dim_product = (
    spark.table("iceberg.gold.dim_product")
    .filter(col("is_current") == True)
    .select("product_id", "product_sk")
)

dim_seller = (
    spark.table("iceberg.gold.dim_seller")
    .filter(col("is_current") == True)
    .select("seller_id", "seller_sk")
)

dim_date = spark.table("iceberg.gold.dim_date")

fact_joined = (
    fact_base
    .join(dim_customer, "customer_id", "left")
    .join(dim_product, "product_id", "left")
    .join(dim_seller, "seller_id", "left")

    .join(
        dim_date.alias("d_order"),
        to_date(col("order_purchase_timestamp")) == col("d_order.full_date"),
        "left"
    )
    .join(
        dim_date.alias("d_approved"),
        to_date(col("order_approved_at")) == col("d_approved.full_date"),
        "left"
    )
    .join(
        dim_date.alias("d_carrier"),
        to_date(col("order_delivered_carrier_date")) == col("d_carrier.full_date"),
        "left"
    )
    .join(
        dim_date.alias("d_customer"),
        to_date(col("order_delivered_customer_date")) == col("d_customer.full_date"),
        "left"
    )
    .join(
        dim_date.alias("d_estimated"),
        to_date(col("order_estimated_delivery_date")) == col("d_estimated.full_date"),
        "left"
    )
    .join(
        dim_date.alias("d_shipping"),
        to_date(col("shipping_limit_date")) == col("d_shipping.full_date"),
        "left"
    )
)

fact_order_item = (
    fact_joined
    .select(
        monotonically_increasing_id().alias("order_item_sk"),
        col("order_id"),
        col("order_item_id"),

        col("customer_sk"),
        col("product_sk"),
        col("seller_sk"),

        col("d_order.date_sk").alias("order_purchase_date_sk"),
        col("d_approved.date_sk").alias("order_approved_at_date_sk"),
        col("d_carrier.date_sk").alias("order_delivery_carrier_date_sk"),
        col("d_customer.date_sk").alias("order_delivery_customer_date_sk"),
        col("d_estimated.date_sk").alias("order_estimated_date_delivery_date_sk"),
        col("d_shipping.date_sk").alias("shipping_limit_date_sk"),

        col("price"),
        col("freight_value"),
        (col("price") + col("freight_value")).alias("total_amount"),

        col("order_status"),
        current_timestamp().alias("ingestion_time")
    )
)

fact_order_item.write.mode("overwrite").saveAsTable("iceberg.gold.fact_order_item")

spark.stop()
