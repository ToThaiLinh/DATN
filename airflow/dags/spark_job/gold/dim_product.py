from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("dim_product") \
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

product_src = spark.table("iceberg.silver.product_clean")

product_stg = (
    product_src
    .select(
        "product_id",
        "product_category_name",
        "product_name",
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_height_cm",
        "product_length_cm",
        "product_width_cm"
    )
    .withColumn(
        "hash_diff",
        sha2(
            concat_ws(
                "||",
                col("product_category_name"),
                col("product_name"),
                col("product_name_lenght"),
                col("product_description_lenght"),
                col("product_photos_qty"),
                col("product_weight_g"),
                col("product_height_cm"),
                col("product_length_cm"),
                col("product_width_cm")
            ),
            256
        )
    )
)


try:
    dim_product = spark.table("iceberg.gold.dim_product")
except:
    dim_product = None

if dim_product is None:
    dim_product_init = (
        product_stg
        .withColumn("product_sk", monotonically_increasing_id())
        .withColumn("effective_from", current_timestamp())
        .withColumn("effective_to", lit("9999-12-31").cast("timestamp"))
        .withColumn("is_current", lit(True))
        .drop("hash_diff")
    )

    dim_product_init.write.mode("overwrite").saveAsTable("iceberg.gold.dim_product")

else:
    dim_current = dim_product.filter(col("is_current") == True)

    dim_current_hash = (
        dim_current
        .withColumn(
            "hash_diff",
            sha2(
                concat_ws(
                    "||",
                    col("product_category_name"),
                    col("product_name"),
                    col("product_name_lenght"),
                    col("product_description_lenght"),
                    col("product_photos_qty"),
                    col("product_weight_g"),
                    col("product_height_cm"),
                    col("product_length_cm"),
                    col("product_width_cm")
                ),
                256
            )
        )
    )

    joined = (
        product_stg.alias("src")
        .join(
            dim_current_hash.alias("dim"),
            col("src.product_id") == col("dim.product_id"),
            "left"
        )
    )

    changed = joined.filter(
        (col("dim.product_id").isNull()) |
        (col("src.hash_diff") != col("dim.hash_diff"))
    )

    expired = (
        dim_current
        .join(
            changed.select(col("src.product_id").alias("product_id")).distinct(),
            "product_id",
            "inner"
        )
        .withColumn("effective_to", current_timestamp())
        .withColumn("is_current", lit(False))
    )

    new_records = (
        changed
        .select(
            col("src.product_id"),
            col("src.product_category_name"),
            col("src.product_name"),
            col("src.product_name_lenght"),
            col("src.product_description_lenght"),
            col("src.product_photos_qty"),
            col("src.product_weight_g"),
            col("src.product_height_cm"),
            col("src.product_length_cm"),
            col("src.product_width_cm")
        )
        .withColumn("product_sk", monotonically_increasing_id())
        .withColumn("effective_from", current_timestamp())
        .withColumn("effective_to", lit("9999-12-31").cast("timestamp"))
        .withColumn("is_current", lit(True))
    )

    if changed.rdd.isEmpty():
        print("No changes detected. Skip SCD2 update.")
    else:
        final_dim = (
            dim_product
            .filter(col("is_current") == False)
            .unionByName(expired)
            .unionByName(new_records)
        )
    
        final_dim.write.mode("overwrite").saveAsTable("iceberg.gold.dim_product")

    spark.stop()