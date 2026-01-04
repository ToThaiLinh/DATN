from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("dm_seller") \
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
    'current_seller_id',
    'current_seller_sku',
    'current_seller_name',
    'current_seller_store_id',
    'ngay_cap_nhat'
)

w = (
    Window
    .partitionBy("current_seller_id")
    .orderBy(F.col("ngay_cap_nhat").desc())
)

df = (
    df
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn", "ngay_cap_nhat")
)

df = df.withColumnRenamed("current_seller_id", "seller_id") \
    .withColumnRenamed("current_seller_sku", "seller_sku") \
    .withColumnRenamed("current_seller_name", "seller_name") \
    .withColumnRenamed("current_seller_store_id", "seller_store_id")

df = df.filter(col("seller_id").isNotNull())

df = df.withColumn("ngay_cap_nhat", current_timestamp())

df.write \
    .format('iceberg') \
    .mode('overwrite') \
    .saveAsTable('iceberg.silver.dm_seller')

spark.stop()