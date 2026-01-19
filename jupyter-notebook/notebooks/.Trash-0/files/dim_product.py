#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window


# In[2]:


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


# In[3]:


bronze_product = spark.read.format("iceberg") \
    .load("bronze.olist_products_dataset") \
    .select(
        "product_id",
        "product_category_name",
        "product_name",
        F.col("product_name_lenght").cast("long"),
        F.col("product_description_lenght").cast("long"),
        F.col("product_photos_qty").cast("long"),
        F.col("product_weight_g").cast("long"),
        F.col("product_height_cm").cast("long"),
        F.col("product_length_cm").cast("long"),
        F.col("product_width_cm").cast("long"),
        "ingestion_time"
    )


# In[4]:


dim_product_current = spark.read.format("iceberg") \
    .load("iceberg.silver.dim_product") \
    .filter("is_current = true")


# In[5]:


join_cond = "product_id"

joined = bronze_product.alias("b") \
    .join(dim_product_current.alias("d"), join_cond, "left")


# In[6]:


change_cond = (
    (F.col("b.product_category_name") != F.col("d.product_category_name")) |
    (F.col("b.product_name") != F.col("d.product_name")) |
    (F.col("b.product_weight_g") != F.col("d.product_weight_g")) |
    (F.col("b.product_height_cm") != F.col("d.product_height_cm")) |
    (F.col("b.product_length_cm") != F.col("d.product_length_cm")) |
    (F.col("b.product_width_cm") != F.col("d.product_width_cm"))
)


# In[7]:


changed_or_new = joined.filter(
    F.col("d.product_id").isNull() | change_cond
).select("b.*")


# In[8]:


max_sk = dim_product_current.agg(
    F.max("product_sk")
).collect()[0][0]

max_sk = max_sk or 0


# In[9]:


# Thêm SCD mới
w = Window.orderBy("product_id")

new_dim_product = changed_or_new \
    .withColumn(
        "product_sk",
        F.row_number().over(w) + F.lit(max_sk)
    ) \
    .withColumn("effective_from", F.current_timestamp()) \
    .withColumn("effective_to", F.lit(None).cast("timestamp")) \
    .withColumn("is_current", F.lit(True)) \
    .withColumn("ingestion_time", F.current_timestamp())


# In[10]:


# Đóng SCD cũ
expired_product = dim_product_current.alias("d") \
    .join(
        changed_or_new.select("product_id").alias("c"),
        "product_id",
        "inner"
    ) \
    .withColumn("effective_to", F.current_timestamp()) \
    .withColumn("is_current", F.lit(False))


# In[11]:


expired_product.write.format("iceberg") \
    .mode("append") \
    .saveAsTable("iceberg.silver.dim_product")


# In[12]:


new_dim_product.write.format("iceberg") \
    .mode("append") \
    .saveAsTable("iceberg.silver.dim_product")


# In[13]:


spark.stop()


# In[ ]:




