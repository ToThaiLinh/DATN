from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("fact_category_product") \
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

df_product = spark.read \
    .format('iceberg') \
    .load('iceberg.silver.dm_product') \
    .select(
        'product_id',
        'category_id',
        'name',
        'created_date'
    )

df_category = spark.read \
    .format('iceberg') \
    .load('iceberg.silver.dm_category') \
    .select(
        'category_id',
        'category_name'
    )
df_product_price = spark.read \
    .format('iceberg') \
    .load('iceberg.silver.sub_product_price') \
    .select(
        'product_id',
        'price',
        'original_price',
        'discount',
        'discount_rate'
    )

df_product_sale = spark.read \
    .format('iceberg') \
    .load('iceberg.silver.sub_product_sale') \
    .select(
        'product_id',
        'quantity_sold'
    )

df_product_inventory = spark.read \
    .format('iceberg') \
    .load('iceberg.silver.sub_product_inventory') \
    .select (
        'product_id',
        'stock_qty'
    )

df = df_product.join(df_category, df_product['category_id'] == df_category['category_id'], 'inner') \
    .drop(df_category['category_id']) \
    .join(df_product_price, df_product['product_id'] == df_product_price['product_id'], 'inner') \
    .drop(df_product_price['product_id']) \
    .join(df_product_sale, df_product['product_id'] == df_product_sale['product_id'], 'inner') \
    .drop(df_product_sale['product_id']) \
    .join(df_product_inventory, df_product['product_id'] == df_product_inventory['product_id'], 'inner') \
    .drop(df_product_inventory['product_id'])

group_column = ['category_id', 'category_name']
df = df.groupBy(*group_column) \
    .agg(
        F.count(col('product_id')).alias('total_product'),
        F.sum(col('quantity_sold')).alias('total_quantity_sold'),
        (F.sum(col('price') * col('quantity_sold'))).alias('revenue'),
        F.round(F.avg(col('price')), 2).alias('avg_price'),
        F.round(F.sum(col('quantity_sold')) / F.count(col('product_id')), 2).alias('avg_units_sold_per_product')
    )
df.printSchema()

df = df.withColumn("ngay_cap_nhat", current_timestamp())

df.write \
    .format('iceberg') \
    .mode('overwrite') \
    .saveAsTable('iceberg.gold.fact_category_product')

spark.stop()

