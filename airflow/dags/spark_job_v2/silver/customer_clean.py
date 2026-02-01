from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from gx_validations.base import run_validation

spark = SparkSession.builder \
    .appName("customer_clean") \
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

try:
    last_processed_time = spark.sql("""
        SELECT COALESCE(MAX(ingestion_time), TIMESTAMP '1970-01-01 00:00:00')
        FROM iceberg.silver.customer_clean
    """).collect()[0][0]
except:
    last_processed_time = "1970-01-01 00:00:00"

bronze_inc_df = spark.read \
    .format("iceberg") \
    .load("iceberg.bronze.olist_customers_dataset") \
    .filter(col("ingestion_time") > last_processed_time)

if bronze_inc_df.rdd.isEmpty():
    print("No new bronze records. Skip silver load.")
    spark.stop()
    exit(0)

w = Window.partitionBy("customer_id").orderBy(col("ingestion_time").desc())

staging_df = (
    bronze_inc_df
    .withColumn("rn", row_number().over(w))
    .filter(col("rn") == 1)
    .drop("rn")
    .withColumn("ingestion_time", current_timestamp())
)

staging_df.createOrReplaceTempView("stg_customer_clean")

# table_full_name = 'iceberg.silver.customer_clean'
# run_validation(
#     dataframe=staging_df,
#     table_name=table_full_name,
#     validate_fn=validation_module.validate
# )

spark.sql("""
MERGE INTO iceberg.silver.customer_clean t
USING stg_customer_clean s
ON t.customer_id = s.customer_id
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *
""")

spark.stop()