from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
# import boto3

spark = SparkSession.builder \
    .appName("bronze_categories") \
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

product_category = 's3a://warehouse/data/all_product_ids.csv'
df = spark.read \
    .format('csv') \
    .option('header', 'true') \
    .option('inferSchema', 'false') \
    .option("escape", '"') \
    .option("quote", '"') \
    .option("multiLine", "true") \
    .option("ignoreLeadingWhiteSpace", "true") \
    .option("ignoreTrailingWhiteSpace", "true") \
    .load(product_category)
# df.printSchema()
# df.show(5)

df = df.withColumn('ngay_cap_nhat', current_timestamp())
df.write \
    .format('iceberg') \
    .mode('overwrite') \
    .saveAsTable('iceberg.bronze.product_category')

spark.stop()