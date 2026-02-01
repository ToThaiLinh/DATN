from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("olist_orders_dataset") \
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

jdbc_url = "jdbc:mysql://mariadb:3306/database_raw?useSSL=false&serverTimezone=UTC"

connection_properties = {
    "user": "admin",
    "password": "admin",
    "driver": "com.mysql.cj.jdbc.Driver"
}

watermark_df = spark.sql("""
    SELECT max(updated_at) AS max_updated_at
    FROM iceberg.bronze.olist_orders_dataset
""")

last_updated_at = watermark_df.collect()[0]["max_updated_at"]
if last_updated_at is None:
    last_updated_at = "1970-01-01 00:00:00"

query = f"""
(
    SELECT *
    FROM olist_orders_dataset
    WHERE updated_at > '{last_updated_at}'
) AS src
"""

df = spark.read.jdbc(
    url=jdbc_url,
    table=query,
    properties=connection_properties
)


df = df_orders.withColumn('ingestion_time', current_timestamp())

df.write \
    .format('iceberg') \
    .mode('append') \
    .saveAsTable('iceberg.bronze.olist_orders_dataset')

spark.stop()