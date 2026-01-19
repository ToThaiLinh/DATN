from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("dm_product") \
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

df = spark.read \
    .format('iceberg') \
    .load('iceberg.bronze.product_details')
# df.printSchema()

df = df.select(
    col('id').alias('product_id'),
    col('categories_id').alias('category_id'),
    'brand_id',
    col('current_seller_id').alias('seller_id'),
    'sku', 
    'name',
    'type',
    'short_description',
    'favourite_count',
    'review_count',
    'has_ebook',
    'is_fresh',
    'is_flower',
    'is_gift_card',
    'is_baby_milk',
    'is_acoholic_drink',
    'has_buynow',
    'price', 
    'original_price',
    'discount',
    'discount_rate',
    'inventory_status',
    'inventory_type',
    col("stock_item_qty").alias("stock_qty"),
    col("stock_item_min_sale_qty").alias("min_sales_qty"),
    col("stock_item_max_sale_qty").alias("max_sales_qty"),
    col('quantity_sold_value').alias("quantity_sold"),
    'data_version',
    'day_ago_created',
   'ngay_cap_nhat'
)

df = df.withColumn("day_ago_created", col("day_ago_created").cast("int"))
df = df.withColumn("created_date", F.date_sub(F.col("ngay_cap_nhat"), F.col("day_ago_created")))

w = (
    Window
    .partitionBy("product_id")
    .orderBy(F.col("ngay_cap_nhat").desc())
)

df = (
    df
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn", "ngay_cap_nhat")
)

df = df.withColumn("favourite_count", col("favourite_count").cast("int")) \
    .withColumn("review_count", col("review_count").cast("int")) \
    .withColumn("has_ebook", when(col("has_ebook") == "False", False).otherwise(True)) \
    .withColumn("is_fresh", when(col("is_fresh") == "False", False).otherwise(True)) \
    .withColumn("is_flower", when(col("is_flower") == "False", False).otherwise(True)) \
    .withColumn("has_buynow", when(col("has_buynow") == "False", False).otherwise(True)) \
    .withColumn("is_gift_card", when(col("is_gift_card") == "False", False).otherwise(True)) \
    .withColumn("is_baby_milk", when(col("is_baby_milk") == "False", False).otherwise(True)) \
    .withColumn("is_acoholic_drink", when(col("is_acoholic_drink") == "False", False).otherwise(True)) \
    .withColumn("stock_qty", col("stock_qty").cast("long")) \
    .withColumn("min_sales_qty", col("min_sales_qty").cast("long")) \
    .withColumn("max_sales_qty", col("max_sales_qty").cast("long")) \
    .withColumn("price", col("price").cast("long")) \
    .withColumn("original_price", col("original_price").cast("long")) \
    .withColumn("discount", col("discount").cast("long")) \
    .withColumn("discount_rate", col("discount_rate").cast("float")) \
    .withColumn("quantity_sold", col("quantity_sold").cast("long"))

df = df.withColumn("ngay_cap_nhat", current_timestamp())

df.write \
    .format('iceberg') \
    .mode('overwrite') \
    .saveAsTable('iceberg.silver.dm_product')

spark.stop()