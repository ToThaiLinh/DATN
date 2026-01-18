# from airflow import DAG
# from airflow.decorators import task
# import pendulum

# import great_expectations as gx
# from pyspark.sql import SparkSession

# from gx_validations.base import run_validation
# from gx_validations import validate_dm_brand


# def get_spark(app_name: str):
#     return (
#         SparkSession.builder \
#         .appName("gx") \
#         .master('spark://spark-master:7077') \
#         .config("spark.cores.max", "1") \
#         .config("spark.executor.memory", "1g") \
#         .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
#         .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
#         .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
#         .config("spark.hadoop.fs.s3a.path.style.access", "true") \
#         .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
#         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
#         .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
#         .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
#         .config("spark.sql.catalog.iceberg.type", "hive") \
#         .config("spark.sql.catalog.iceberg.uri", "thrift://hive-metastore:9083") \
#         .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
#         .config("spark.sql.catalog.iceberg.warehouse", "s3a://warehouse/") \
#         .config("spark.sql.catalog.iceberg.s3.endpoint", "http://minio:9000") \
#         .config("spark.hadoop.fs.s3a.region", "us-east-1") \
#         .getOrCreate()
#     )


# with DAG(
#     dag_id="ge_validate_dm_brand_test",
#     start_date=pendulum.datetime(2025, 12, 20, tz="UTC"),
#     schedule=None,
#     catchup=False,
#     tags=["great_expectations", "test", "dm_brand"],
# ) as dag:

#     @task
#     def validate_dm_brand_task():
#         # ---------- Spark ----------
#         spark = get_spark("gx_validate_dm_brand")

#         df = (
#             spark.read
#             .format("iceberg")
#             .load("iceberg.silver.dm_brand")
#         )

#         print(f"📊 dm_brand rows: {df.count()}")

#         # ---------- GX ----------
#         context = gx.get_context(
#             project_root_dir="/opt/airflow/great_expectations"
#         )

#         run_validation(
#             context=context,
#             dataframe=df,
#             table_name="dm_brand",
#             validate_fn=validate_dm_brand.validate
#         )

#         print("✅ dm_brand validation PASSED")

#     validate_dm_brand_task()
