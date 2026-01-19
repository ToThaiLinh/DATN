from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("fact_review_product") \
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

df_review = spark.read \
    .format('iceberg') \
    .load('iceberg.silver.sub_review') 

df_review = df_review.withColumnRenamed('created_date', 'review_date')

df_product = spark.read \
    .format('iceberg') \
    .load('iceberg.silver.dm_product') \
    .select(
        'product_id',
        'name'
    )
# df_product.printSchema()

df = df_review.join(df_product, df_review['product_id'] == df_product['product_id'], 'inner').drop(df_product['product_id'])
df = df.select(
    df_review['*'],
    df_product['name'].alias('product_name')
)

group_column = ['product_id', 'product_name']
df = df.groupBy(*group_column) \
    .agg(
        F.count(col('review_id')).alias('total_review'),
        F.avg(col('rating')).alias('avg_rating'),
        F.sum(when(col("rating") <= 2, 1).otherwise(0)).alias('negative_review_count'),
        (F.sum(when(col("rating") <= 2, 1).otherwise(0)) / F.count(col('review_id')) * 100).alias('negative_review_rate'),
         F.sum(when(col("rating") > 2, 1).otherwise(0)).alias('positive_review_count'),
        (F.sum(when(col("rating") > 2, 1).otherwise(0)) / F.count(col('review_id')) * 100).alias('positive_review_rate')
    )

df = df.withColumn("ngay_cap_nhat", current_timestamp())

df.write \
    .format('iceberg') \
    .mode('overwrite') \
    .saveAsTable('iceberg.gold.fact_review_product')

spark.stop()
