from airflow import DAG
from datetime import datetime
from helper.SparkOperator import SparkOperator

with DAG(
    dag_id="spark_class_based_dag",
    start_date=datetime(2025, 12, 15),
    schedule=None,
    catchup=False,
    tags=["spark", "telegram"],
) as dag:

    spark_job = SparkOperator(
        task_id="spark_etl_product",
        name="etl_product_job",
        application="/opt/airflow/dags/spark_job/test_spark_submit.py",
        application_args=[
            "--biz_date", "{{ ds }}"
        ],
    ).build()
