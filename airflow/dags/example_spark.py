from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
import pendulum
from helper.telegram_notification import notify_telegram

with DAG(
    dag_id="spark_submit_job_cluster",
    start_date=pendulum.datetime(2025, 12, 1, tz = 'UTC'),
    schedule_interval=None,
    catchup=False,
    tags=["spark"],
) as dag:

    submit_job = SparkSubmitOperator(
        task_id="submit_job",
        application="/opt/airflow/dags/spark_job/test_spark_submit.py",
        conn_id="spark_default",
        verbose=True,
        application_args=[
        "--biz_date", "{{ ds }}",
        ],

        on_failure_callback=lambda context: notify_telegram(
            telegram_conn_id="telegram_default",
            context=context,
            title="🔥 Spark Job Failed",
            message=str(context.get("exception"))
        ),


    )
