import os
os.system('pip install boto3')

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import boto3
from datetime import datetime

spark = SparkSession.builder \
    .appName("monitor_iceberg_data") \
    .master('spark://spark-master:7077') \
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

schema = StructType([
    StructField('catalog_name', StringType(), True),
    StructField('schema_name', StringType(), True),
    StructField('table_name', StringType(), True),
    StructField('create_time', StringType(), True),
    StructField('operation', StringType(), True),
    StructField('added_data_files', LongType(), True),
    StructField('added_data_size', FloatType(), True), # added_files_size
    StructField('added_records', LongType(), True),  
    StructField('total_data_size', FloatType(), True), # total_files_size
    StructField('total_records', LongType(), True),
    StructField('total_data_files', LongType(), True),
    StructField('avg_size_record', FloatType(), True)
])

result = spark.createDataFrame(
    data=[], 
    schema=schema
)

for table_old in a:
    try:
        # lấy data
        print(f"Đang xử lí {table_old}")
        sc = table_old.split('.')[1]
        tb = table_old.split('.')[2]
        table = table_old + '.snapshots'
        df = spark.read \
            .format("iceberg") \
            .load(table)
        x = df.select("committed_at", "operation", "summary").tail(1)[0]
        create_time = str(x["committed_at"])
        operation = x["operation"]
        added_data_files = int(x["summary"].get('added-data-files')) if x["summary"].get('added-data-files') is not None else None

        added_files_size = float(x["summary"].get('added-files-size')) if x["summary"].get('added-files-size') is not None else None

        added_records = int(x["summary"].get('added-records')) if x["summary"].get('added-records') is not None else None

        total_files_size = float(x["summary"].get('total-files-size')) if x["summary"].get('total-files-size') is not None else None
        
        total_records = int(x["summary"].get('total-records')) if x["summary"].get('total-records') is not None else None  
        total_data_files = int(x["summary"].get('total-data-files')) if x["summary"].get('total-data-files') is not None else None
        
        if total_records is not None and total_records > 0:
            avg_size_record = total_files_size / total_records
        else:
            avg_size_record = None  

        added_files_size = convert_bytes_to_mb(added_files_size)
        total_files_size = convert_bytes_to_mb(total_files_size)
        data = [Row(catalog_name=catalog, schema_name=sc, table_name=tb, create_time=create_time, operation=operation, added_data_files=added_data_files, \
                    added_data_size=added_files_size, added_records=added_records, total_data_size=total_files_size, \
                    total_records=total_records, total_data_files=total_data_files, avg_size_record=avg_size_record)]
        new_df = spark.createDataFrame(data, schema)
        
        result = result.union(new_df)
    except Exception as e:
        print(f"Đã có lỗi xảy ra khi xử lý {table_old}: {e}")
        # data = {'table': [table_old], 'date': [date], 'error': [e]}
        # df = pd.DataFrame(data)
        # df.to_csv(csv_error, mode='a', header=False, index=False) 

result = result.withColumn("added_data_size", round(result["added_data_size"], 3).cast('string'))
result = result.withColumn("added_data_size", round(result["added_data_size"], 3))

result = result.withColumn("total_data_size", round(result["total_data_size"], 3).cast('string'))
result = result.withColumn("total_data_size", round(result["total_data_size"], 3))

result = result.withColumn("avg_size_record", round(result["avg_size_record"], 3).cast('string'))
result = result.withColumn("avg_size_record", round(result["avg_size_record"], 3))

result = result.withColumn('ngay_cap_nhat', current_timestamp())
result = result.withColumn('create_time', col('create_time').cast('timestamp'))
result = result.coalesce(10)

table = f'{catalog}.monitor.monitor_iceberg_data'
result.write \
  .format("iceberg") \
  .mode("append") \
  .saveAsTable(table) 

spark.stop()