from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
import pendulum
from datetime import datetime, timezone
from utils.audit_metrics import persist_run_metrics, persist_expectation_metrics, persist_error_records


@dag(
    dag_id="test_gx_spark_ver2",
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
        GX_ROOT.mkdir(parents=True, exist_ok=True)

        context = gx.get_context(
            project_root_dir=str(GX_ROOT),
            mode="file"
        )

        print("✅ GX context ready")
        print(context.root_directory)

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
                .format("iceberg") \
                .load("iceberg.test.test_iceberg_v1")

            print(f"📊 Loaded dataframe: {df.count()} rows")

            # ---------- GX Context ----------
            import great_expectations as gx

            context = gx.get_context(
                project_root_dir="/opt/airflow/great_expectations"
            )

            # ---------- Datasource (idempotent) ----------
            datasource_name = "spark_iceberg"

            try:
                datasource = context.get_datasource(datasource_name)
                print(f"ℹ️ Datasource {datasource_name} đã tồn tại")
            except Exception:
                datasource = context.sources.add_spark(
                    name=datasource_name
                )
                print(f"✅ Datasource {datasource_name} created")

            # ---------- Asset (idempotent) ----------
            asset_name = "test_iceberg_v1_df"

            try:
                asset = datasource.get_asset(asset_name)
                print(f"♻️ Reuse asset: {asset_name}")
            except Exception:
                asset = datasource.add_dataframe_asset(
                    name=asset_name
                )
                print(f"✅ Asset {asset_name} created")

            # ---------- Batch Request ----------
            batch_request = asset.build_batch_request(
                dataframe=df
            )

            suite_name = "test_iceberg_v1_suite"

            # ---- Get or create expectation suite ----
            try:
                context.get_expectation_suite(suite_name)
                print(f"♻️ Reuse expectation suite: {suite_name}")
            except Exception:
                context.add_expectation_suite(expectation_suite_name=suite_name)
                print(f"✅ Created expectation suite: {suite_name}")


            # ---------- Expectation Suite ----------
            suite_name = "test_iceberg_v1_suite"

            validator = context.get_validator(
                batch_request=batch_request,
                expectation_suite_name=suite_name
            )
            validator.expectation_suite.expectations = []

            # ---------- Expectations ----------
            validator.expect_column_to_exist("name")
            validator.expect_column_values_to_not_be_null("name")

            validator.save_expectation_suite(discard_failed_expectations=False)

            # ---------- Checkpoint (CHUẨN) ----------
            checkpoint_name = "test_iceberg_v1_checkpoint"

            checkpoint = context.add_or_update_checkpoint(
                name=checkpoint_name,
                validations=[
                    {
                        "batch_request": batch_request,
                        "expectation_suite_name": suite_name,
                    }
                ],
            )

            # ---------- Run Checkpoint ----------
            run_name = f"gx_test_iceberg_v1_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

            checkpoint_result = checkpoint.run(
                run_name=run_name
            )

            print(checkpoint_result)

            # ---------- Persist DQ Metrics ----------
            persist_run_metrics(
                spark,
                checkpoint_result,
                table_name="iceberg.test.dq_run_metrics"
            )
            persist_expectation_metrics(
                spark,
                checkpoint_result,
                table_name="iceberg.test.dq_expectation_metrics"
            )

            persist_error_records(
                df, 
                checkpoint_result, 
                table_name="iceberg.test.test_iceberg_v1_error"
            )
            # ---------- Data Docs ----------
            # context.build_data_docs()

            # docs_urls = context.get_docs_sites_urls()
            # print("📘 Data Docs URLs:", docs_urls)

            # ---------- Fail DAG nếu validation fail ----------
            if not checkpoint_result["success"]:
                raise RuntimeError("❌ GX validation FAILED")

            print("✅ GX validation SUCCESS")
            return "VALIDATION OK"

        except Exception as e:
            print("❌ Lỗi khi chạy GX checkpoint")
            raise RuntimeError(str(e))

    end = EmptyOperator(task_id="end")

    start >> init_or_check_gx_context() >> run_ge_checkpoint() >> end


dag_instance = ge_validate_iceberg()
