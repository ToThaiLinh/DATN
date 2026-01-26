from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("dim_seller") \
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

seller_src = spark.table("iceberg.silver.seller_clean")

seller_stg = (
    seller_src
    .select(
        "seller_id",
        col("seller_zip_code_prefix"),
        "seller_city",
        "seller_state"
    )
    .withColumn(
        "hash_diff",
        sha2(
            concat_ws(
                "||",
                col("seller_zip_code_prefix"),
                col("seller_city"),
                col("seller_state")
            ),
            256
        )
    )
)

try:
    dim_seller = spark.table("iceberg.gold.dim_seller")
except:
    dim_seller = None

if dim_seller is None:
    dim_seller_init = (
        seller_stg
        .withColumn("seller_sk", monotonically_increasing_id())
        .withColumn("effective_from", current_timestamp())
        .withColumn("effective_to", lit("9999-12-31").cast("timestamp"))
        .withColumn("is_current", lit(True))
        .drop("hash_diff")
    )

    dim_seller_init.write.mode("overwrite").saveAsTable("iceberg.gold.dim_seller")

else:
    dim_current = dim_seller.filter(col("is_current") == True)

    dim_current_hash = (
        dim_current
        .withColumn(
            "hash_diff",
            sha2(
                concat_ws(
                    "||",
                    col("seller_zip_code_prefix"),
                    col("seller_city"),
                    col("seller_state")
                ),
                256
            )
        )
    )

    joined = (
        seller_stg.alias("src")
        .join(
            dim_current_hash.alias("dim"),
            col("src.seller_id") == col("dim.seller_id"),
            "left"
        )
    )

    changed = joined.filter(
        (col("dim.seller_id").isNull()) |
        (col("src.hash_diff") != col("dim.hash_diff"))
    )

    expired = (
        dim_current
        .join(
            changed.select(col("src.seller_id").alias("seller_id")).distinct(),
            "seller_id",
            "inner"
        )
        .withColumn("effective_to", current_timestamp())
        .withColumn("is_current", lit(False))
    )

    new_records = (
        changed
        .select(
            col("src.seller_id"),
            col("src.seller_zip_code_prefix"),
            col("src.seller_city"),
            col("src.seller_state")
        )
        .withColumn("seller_sk", monotonically_increasing_id())
        .withColumn("effective_from", current_timestamp())
        .withColumn("effective_to", lit("9999-12-31").cast("timestamp"))
        .withColumn("is_current", lit(True))
    )

    final_dim = (
        dim_seller
        .filter(col("is_current") == False)
        .unionByName(expired)
        .unionByName(new_records)
    )

    final_dim.write.mode("overwrite").saveAsTable("iceberg.gold.dim_seller")

spark.stop()