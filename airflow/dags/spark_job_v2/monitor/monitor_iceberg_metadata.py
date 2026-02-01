import os
os.system('pip install boto3')

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import boto3
import re
from botocore.client import Config
from urllib.parse import urlparse
from datetime import datetime

spark = SparkSession.builder \
    .appName("monitor_iceberg_data") \
    .master("spark://spark-master:7077") \
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

# khai báo ngày và file ghi các table lỗi
date = datetime.now()
date = str(date.strftime('%Y-%m-%d %H:%M:%S'))

# csv_error = '/opt/airflow/spark_job/monitor_iceberg_data_table_error.csv'
catalog = "iceberg"

a = []
df = spark.sql(f"SHOW SCHEMAS FROM {catalog}")

for row in df.collect(): 
    namespace = row['namespace']  
    df1 = spark.sql(f"SHOW TABLES FROM {catalog}.{namespace}")
    for i in df1.collect():
        table = catalog + '.' +  i['namespace'] + '.' + i['tableName'] 
        a.append(table)

def convert_bytes_to_mb(size_in_bytes):
    if size_in_bytes is None:
        return None
    
    size_in_mb = size_in_bytes / (1024 * 1024)
    return size_in_mb

# Hàm lấy size 1 thư mục trong minio
def get_size_folder(s3a_path):
    s3 = boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin',
        config=Config(signature_version='s3v4')
    )

    # Parse s3a path
    parsed_url = urlparse(s3a_path)
    bucket = parsed_url.netloc
    prefix = parsed_url.path.lstrip('/')

    total_size = 0
    total_files = 0
    continuation_token = None
   
    while True:
        if continuation_token:
            response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, ContinuationToken=continuation_token)
        else:
            response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
            
        total_files += len(response['Contents'])
        if 'Contents' in response:
            for obj in response['Contents']:
                total_size += obj['Size']
                #total_files += 1
                
        if response.get('IsTruncated'):  # If truncated, it means there are more objects to fetch
            continuation_token = response.get('NextContinuationToken')
        else:
            break
            
    total_size = convert_bytes_to_mb(total_size)
    return total_files, total_size

schema = StructType([
    StructField('catalog_name', StringType(), True),
    StructField('schema_name', StringType(), True),
    StructField('table_name', StringType(), True),
    StructField('total_metadata_size', FloatType(), True),
    StructField('total_metadata_file', LongType(), True) 
])

result = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)

for table_old in a:
    try:
        # lấy metadata
        print(f"Đang xử lí {table_old}")
        sc = table_old.split('.')[1]
        tb = table_old.split('.')[2]
        
         # Lấy lệnh tạo bảng
        location = spark.sql(f"SHOW CREATE TABLE {table_old};").tail(1)[0]["createtab_stmt"]
        # Trích xuất location từ metadata
        match = re.search(r"LOCATION\s+'([^']*)'", location)
        if match:
            location = match.group(1) + '/metadata'
        # Tính toán dung lượng và số lượng file metadata
        total_metadata_file, total_metadata_size = get_size_folder(location)
        # Tạo DataFrame chứa kết quả
        data = [Row(catalog_name=catalog, schema_name=sc, table_name=tb, total_metadata_size=total_metadata_size, total_metadata_file=total_metadata_file)]
        new_df = spark.createDataFrame(data, schema)
        
        result = result.union(new_df)
    except Exception as e:
        print(f"Đã có lỗi xảy ra khi xử lý {table_old}: {e}")


result = result.withColumn("total_metadata_size", round(result["total_metadata_size"], 3).cast('string'))
result = result.withColumn("total_metadata_size", round(result["total_metadata_size"], 3))

result = result.withColumn('ngay_cap_nhat', current_timestamp())

table = f'{catalog}.monitor.monitor_iceberg_metadata'
result.write \
  .format("iceberg") \
  .mode("append") \
  .saveAsTable(table) 

spark.stop()
