from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *

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
    'id',
    'source_sub_category_id',
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
    'has_buynow',
    'is_gift_card',
    'is_baby_milk',
    'is_acoholic_drink',
    'data_version',
    'day_ago_created',
   'ngay_cap_nhat'
).withColumn("ngay_cap_nhat_date", F.to_date('ngay_cap_nhat'))
df.printSchema()

max_date = df.agg(F.max("ngay_cap_nhat_date")).first()[0]

df = df.filter(col('ngay_cap_nhat_date') == max_date)

df = df.withColumn("day_ago_created", col("day_ago_created").cast("int"))
df = df.withColumn("created_date", F.date_sub(F.col("ngay_cap_nhat"), F.col("day_ago_created")))
df = df.drop('ngay_cap_nhat_date', 'ngay_cap_nhat')

df = df.dropDuplicates()

df = df.withColumn("favourite_count", col("favourite_count").cast("int")) \
    .withColumn("review_count", col("review_count").cast("int")) \
    .withColumn("has_ebook", when(col("has_ebook") == "False", False).otherwise(True)) \
    .withColumn("is_fresh", when(col("is_fresh") == "False", False).otherwise(True)) \
    .withColumn("is_flower", when(col("is_flower") == "False", False).otherwise(True)) \
    .withColumn("has_buynow", when(col("has_buynow") == "False", False).otherwise(True)) \
    .withColumn("is_gift_card", when(col("is_gift_card") == "False", False).otherwise(True)) \
    .withColumn("is_baby_milk", when(col("is_baby_milk") == "False", False).otherwise(True)) \
    .withColumn("is_acoholic_drink", when(col("is_acoholic_drink") == "False", False).otherwise(True))

df = df.withColumnRenamed("id", "product_id") \
    .withColumnRenamed("source_sub_category_id", "category_id")

df = df.withColumn("ngay_cap_nhat", current_timestamp())

df.write \
    .format('iceberg') \
    .mode('overwrite') \
    .saveAsTable('iceberg.silver.dm_product')

spark.stop()