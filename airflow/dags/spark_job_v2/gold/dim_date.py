from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("dim_date") \
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

# 1. Tạo range ngày
start_date = "2016-01-01"
end_date   = "2018-12-31"

df_date = (
    spark.sql(f"""
        SELECT explode(
            sequence(
                to_date('{start_date}'),
                to_date('{end_date}'),
                interval 1 day
            )
        ) AS full_date
    """)
)

# 2. Sinh các thuộc tính thời gian
dim_date = (
    df_date
    .withColumn("date_sk", date_format(col("full_date"), "yyyyMMdd").cast("int"))
    .withColumn("day", dayofmonth(col("full_date")))
    .withColumn("month", month(col("full_date")))
    .withColumn("month_name", date_format(col("full_date"), "MMMM"))
    .withColumn("quarter", quarter(col("full_date")))
    .withColumn("year", year(col("full_date")))
    .withColumn("week_of_year", weekofyear(col("full_date")))
    .withColumn("day_of_week", dayofweek(col("full_date")))
    .withColumn(
        "day_name",
        date_format(col("full_date"), "EEEE")
    )
    .withColumn(
        "is_weekend",
        when(col("day_of_week").isin(1, 7), True).otherwise(False)
    )
)

# dim_date.printSchema()
# dim_date.show(5)

dim_date.write \
    .format('iceberg') \
    .mode('overwrite') \
    .saveAsTable('iceberg.gold.dim_date')

spark.stop()
