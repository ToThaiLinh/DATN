from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("dm_category") \
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
    .load('iceberg.bronze.sub_categories')
# df.printSchema()

df = df.select(
    'id', 
    'parent_id',
    'name', 
    'type',
    'url_key',
    'url_path',
    'level',
    'status',
    'include_in_menu',
    'is_leaf',
    'meta_title',
    'meta_description',
    'meta_keywords',
    'thumbnail_url',
    'ngay_cap_nhat'
)

w = (
    Window
    .partitionBy("id")
    .orderBy(F.col("ngay_cap_nhat").desc())
)

df = (
    df
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn", "ngay_cap_nhat")
)

df = df.withColumnRenamed('id', 'category_id')
df = df.withColumnRenamed('name', 'category_name')

df = df.withColumn('level', col('level').cast('int')) \
    .withColumn('include_in_menu', 
        when(lower(trim(col('include_in_menu'))) == 'true', True)
        .when(lower(trim(col('include_in_menu'))) == 'false', False)
        .otherwise(None)
    ) \
    .withColumn('is_leaf', 
        when(lower(trim(col('is_leaf'))) == 'true', True)
        .when(lower(trim(col('is_leaf'))) == 'false', False)
        .otherwise(None)
    )

df = df.withColumn("ngay_cap_nhat", current_timestamp())

df.write \
    .format('iceberg') \
    .mode('overwrite') \
    .saveAsTable('iceberg.silver.dm_category')

spark.stop()