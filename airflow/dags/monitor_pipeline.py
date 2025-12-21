from airflow import DAG
from airflow.models.baseoperator import chain
from helper.telegram_notification import notify_telegram
from helper.SparkOperator import SparkOperator
import pendulum

default_args = {
    "owner": "airflow",
    "retries": 0
}

with DAG(
    dag_id="monitor_pipeline",
    start_date=pendulum.datetime(2025, 12, 15, tz='UTC'),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["spark", "telegram"],
) as dag:

    monitor_iceberg_data = SparkOperator(
        task_id="monitor_iceberg_data",
        name="smonitor_iceberg_data",
        application="/opt/airflow/dags/spark_job/monitor/monitor_iceberg_data.py"
    ).build()

    monitor_iceberg_metadata = SparkOperator(
        task_id="monitor_iceberg_metadata",
        name="monitor_iceberg_metadata",
        application="/opt/airflow/dags/spark_job/monitor/monitor_iceberg_metadata.py"
    ).build()

    monitor_iceberg_data
    monitor_iceberg_metadata