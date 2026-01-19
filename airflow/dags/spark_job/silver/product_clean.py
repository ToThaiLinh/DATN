from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("product_clean") \
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
    .load('iceberg.bronze.olist_products_dataset')
df.printSchema()
# df.show()

w = Window.partitionBy("product_id").orderBy(col("ingestion_time").desc())

df = (
    df.withColumn("rn", row_number().over(w))
    .filter(col("rn") == 1)
    .drop("rn")
)

df = df.withColumn("product_name_lenght", col("product_name_lenght").cast("long")) \
    .withColumn("product_description_lenght", col("product_description_lenght").cast("long")) \
    .withColumn("product_photos_qty", col("product_photos_qty").cast("long")) \
    .withColumn("product_weight_g", col("product_weight_g").cast("long")) \
    .withColumn("product_length_cm", col("product_length_cm").cast("long")) \
    .withColumn("product_height_cm", col("product_height_cm").cast("long")) \
    .withColumn("product_width_cm", col("product_width_cm").cast("long"))

df = df.withColumn("ingestion_time", current_timestamp())

df.write \
    .format('iceberg') \
    .mode('overwrite') \
    .saveAsTable('iceberg.silver.product_clean')

spark.stop()