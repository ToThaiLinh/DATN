from airflow.decorators import task
import great_expectations as gx
from utils.spark import get_spark
from gx_validations.base import run_validation
from helper.telegram_notification import notify_telegram

def gx_validate_task(catalog_name = 'iceberg', schema_name = 'silver', table_name = None, validation_module = None):

    @task(
        task_id=f"gx_validate_{table_name}",
        on_failure_callback=lambda context: notify_telegram(
            telegram_conn_id='telegram_defaul',
            context=context,
            title="Great Expectations Validation Failed",
            message=str(context.get("exception"))
        )
    )
    def _validate():
        spark = get_spark(f"gx_{table_name}")

        df = spark.read.format("iceberg").load(
            f"{catalog_name}.{schema_name}.{table_name}"
        )

        context = gx.get_context(
            project_root_dir="/opt/airflow/great_expectations"
        )

        table_full_name = f"{catalog_name}.{schema_name}.{table_name}"
        run_validation(
            context=context,
            dataframe=df,
            table_name=table_full_name,
            validate_fn=validation_module.validate
        )

    return _validate()
