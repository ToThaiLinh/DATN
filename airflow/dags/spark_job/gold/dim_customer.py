from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("dim_customer") \
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

customer_src = spark.read \
    .format('iceberg') \
    .load('iceberg.silver.customer_clean')
customer_src.printSchema()
# df.show()

customer_stg = (
    customer_src
    .select(
        "customer_id",
        "customer_unique_id",
        col("customer_zip_code_prefix").alias("customer_zip_code"),
        "customer_city",
        "customer_state"
    )
    .withColumn(
        "hash_diff",
        sha2(
            concat_ws(
                "||",
                col("customer_zip_code"),
                col("customer_city"),
                col("customer_state")
            ),
            256
        )
    )
)

try:
    dim_customer = spark.table("iceberg.gold.dim_customer")
except:
    dim_customer = None

if dim_customer is None:
    dim_customer_init = (
        customer_stg
        .withColumn("customer_sk", monotonically_increasing_id())
        .withColumn("effective_from", current_timestamp())
        .withColumn("effective_to", lit("9999-12-31").cast("timestamp"))
        .withColumn("is_current", lit(True))
        .drop("hash_diff")
    )

    dim_customer_init.write.mode("overwrite").saveAsTable("iceberg.gold.dim_customer")
    
else:
    dim_current = dim_customer.filter(col("is_current") == True)

    dim_current_hash = (
        dim_current
        .withColumn(
            "hash_diff",
            sha2(
                concat_ws(
                    "||",
                    col("customer_zip_code"),
                    col("customer_city"),
                    col("customer_state")
                ),
                256
            )
        )
    )

    # JOIN để phát hiện thay đổi
    joined = (
        customer_stg.alias("src")
        .join(
            dim_current_hash.alias("dim"),
            col("src.customer_id") == col("dim.customer_id"),
            "left"
        )
    )

    # Record mới hoặc changed
    changed = joined.filter(
        (col("dim.customer_id").isNull()) |
        (col("src.hash_diff") != col("dim.hash_diff"))
    )

    # Đóng record cũ
    expired = (
        dim_current
        .join(
            changed.select(col("src.customer_id").alias("customer_id")).distinct(),
            "customer_id",
            "inner"
        )
        .withColumn("effective_to", current_timestamp())
        .withColumn("is_current", lit(False))
    )

    # Record mới
    new_records = (
        changed
        .select(
            col("src.customer_id"),
            col("src.customer_unique_id"),
            col("src.customer_zip_code"),
            col("src.customer_city"),
            col("src.customer_state")
        )
        .withColumn("customer_sk", monotonically_increasing_id())
        .withColumn("effective_from", current_timestamp())
        .withColumn("effective_to", lit("9999-12-31").cast("timestamp"))
        .withColumn("is_current", lit(True))
    )

    if not changed.rdd.isEmpty():

        final_dim = (
            dim_customer
            .filter(col("is_current") == False)
            .unionByName(expired)
            .unionByName(new_records)
        )
    
        final_dim.write.mode("overwrite").saveAsTable("iceberg.gold.dim_customer")
    
    else:
        print("No changes detected for dim_customer. Skip overwrite.")

spark.stop()
