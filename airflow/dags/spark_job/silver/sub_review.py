from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("sub_review") \
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
    .load('iceberg.bronze.reviews')

df = df.select(
    'review_id',
    'product_id',
    'customer_id',
    'seller_id',
    'rating',
    'score',
    'new_score',
    'title',
    'content',
    'status',
    'thank_count',
    'comment_count',
    'delivery_rating',
    'timeline_review_created_date',
    'current_date',
    'timeline_content',
    'ngay_cap_nhat'
).withColumn("ngay_cap_nhat_date", F.to_date('ngay_cap_nhat'))

max_date = df.agg(F.max("ngay_cap_nhat_date")).first()[0]

df = df.filter(col('ngay_cap_nhat_date') == max_date)
df = df.drop('ngay_cap_nhat_date', 'ngay_cap_nhat')

df = df.dropDuplicates()

df = df.withColumnRenamed("timeline_review_created_date", "created_date")

df = df.withColumn("rating", col("rating").cast("integer")) \
    .withColumn("score", col("score").cast("float")) \
    .withColumn("new_score", col("new_score").cast("float")) \
    .withColumn("thank_count", col("thank_count").cast("integer")) \
    .withColumn("comment_count", col("comment_count").cast("integer")) \
    .withColumn("created_date", F.to_timestamp(col("created_date"))) \
    .withColumn("current_date", F.to_timestamp(col("current_date")))

df = df.filter(col("review_id").isNotNull())

df = df.withColumn("ngay_cap_nhat", current_timestamp())

df.write \
    .format('iceberg') \
    .mode('overwrite') \
    .saveAsTable('iceberg.silver.sub_review')

spark.stop()
