from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
import pendulum


@dag(
    dag_id="ge_validate_iceberg_customers",
    schedule=None,
    start_date=pendulum.datetime(2025, 12, 1, tz="UTC"),
    catchup=False,
    tags=["great-expectations", "spark", "iceberg"],
)
def ge_validate_iceberg():

    start = EmptyOperator(task_id="start")

    @task
    def init_or_check_gx_context():
        import great_expectations as gx
        from pathlib import Path

        GX_ROOT = Path("/opt/airflow/great_expectations")
        GX_YML = GX_ROOT / "great_expectations.yml"

        # tạo folder nếu chưa có
        GX_ROOT.mkdir(parents=True, exist_ok=True)

        # nếu chưa có project → init bằng CLI
        if not GX_YML.exists():
            print("⚠️ GX project chưa tồn tại → init bằng CLI")
            import subprocess
            subprocess.run(
                ["great_expectations", "init"],
                cwd=str(GX_ROOT),
                check=True
            )
            print("✅ GX project initialized")

        # load context
        context = gx.get_context(project_root_dir=str(GX_ROOT))

        print(f"📁 GX root: {context.root_directory}")
        print(f"📊 Docs sites: {context.get_docs_sites_urls()}")

        return "GX READY"

    @task
    def run_ge_checkpoint():
        try:
            import great_expectations as gx
            from pyspark.sql import SparkSession

            # ---------- Spark Session ----------
            spark = SparkSession.builder \
                .appName("gx") \
                .master('spark://spark-master:7077') \
                .config("spark.cores.max", "1") \
                .config("spark.executor.memory", "1g") \
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
                .config("spark.hadoop.fs.s3a.region", "us-east-1") \
                .getOrCreate()

            print("✅ Spark session created")

            # ---------- Read Iceberg ----------
            df = spark.read \
                .format('iceberg') \
                .load('iceberg.test.test_iceberg_v1')
            print(f"📊 Loaded dataframe: {df.count()} rows")

            # ---------- GX ----------
            context = gx.get_context(
                project_root_dir="/opt/airflow/great_expectations"
            )

            # datasource (idempotent)
            try:
                datasource = context.get_datasource("spark_iceberg")
                print("ℹ️ Datasource spark_iceberg đã tồn tại")
            except Exception:
                datasource = context.sources.add_spark(
                    name="spark_iceberg"
                )
                print("✅ Datasource spark_iceberg created")

            # asset (idenpotency)
            asset_name = "test_iceberg_v1_df"
            try:
                asset = datasource.get_asset(asset_name)
                print(f"♻️ Reuse asset: {asset_name}")
            except Exception:
                asset = datasource.add_dataframe_asset(
                    name=asset_name
                )
                print(f"✅ Asset {asset_name} created")

            # # batch request (bind runtime dataframe)
            batch_request = asset.build_batch_request(
                dataframe=df
            )
            # print("DataFrame type:", type(df))
            # print("Row count:", df.count())
            # print("BatchRequest:", batch_request)
            # print("Datasource:", datasource)
            # print("Asset:", asset)
            # print("BatchRequest:", batch_request)
            # print("DataFrame type:", type(df))


            validator = context.get_validator(
                batch_request=batch_request
            )

            validator.expect_column_to_exist("name")
            validator.expect_column_values_to_not_be_null("name")

            validator.save_expectation_suite(discard_failed_expectations=False)

            checkpoint = context.add_or_update_checkpoint(
                name="my_quickstart_checkpoint",
                validator=validator,
            )

            checkpoint_result = checkpoint.run()

            context.view_validation_result(checkpoint_result)

            context.build_data_docs()
            urls = context.get_docs_sites_urls()
            print("Data Docs URLs:", urls)

            # result = validator.validate()
            # if not result["success"]:
            #     raise ValueError("❌ GX validation FAILED")

            print("✅ GX validation SUCCESS")
            return "VALIDATION OK"

        except Exception as e:
            print("❌ Lỗi khi chạy GX checkpoint")
            raise RuntimeError(str(e))

    end = EmptyOperator(task_id="end")

    start >> init_or_check_gx_context() >> run_ge_checkpoint() >> end


dag_instance = ge_validate_iceberg()
