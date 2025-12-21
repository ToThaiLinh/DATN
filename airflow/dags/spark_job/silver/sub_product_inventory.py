from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("sub_product_inventory") \
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

df = df.select(
    col('id').alias('product_id'),
    'inventory_status',
    'inventory_type',
    col("stock_item_qty").alias("stock_qty"),
    col("stock_item_min_sale_qty").alias("min_sales_qty"),
    col("stock_item_max_sale_qty").alias("max_sales_qty"),
    'ngay_cap_nhat'
).withColumn("ngay_cap_nhat_date", F.to_date('ngay_cap_nhat'))
df.printSchema()


max_date = df.agg(F.max("ngay_cap_nhat_date")).first()[0]

df = df.filter(col('ngay_cap_nhat_date') == max_date)
df = df.drop('ngay_cap_nhat_date', 'ngay_cap_nhat')

df = df.dropDuplicates()

df = df.withColumn("stock_qty", col("stock_qty").cast("long")) \
    .withColumn("min_sales_qty", col("min_sales_qty").cast("long")) \
    .withColumn("max_sales_qty", col("max_sales_qty").cast("long"))

df = df.filter(col("product_id").isNotNull())

df = df.withColumn("ngay_cap_nhat", current_timestamp())

df.write \
    .format('iceberg') \
    .mode('overwrite') \
    .saveAsTable('iceberg.silver.sub_product_inventory')

spark.stop()