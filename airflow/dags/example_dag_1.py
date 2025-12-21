from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from helper.telegram_notification import notify_telegram

def fail_task():
    raise ValueError("Test lỗi PythonOperator để gửi Telegram alert")

default_args = {
    "owner": "airflow",
    "retries": 0,
    "on_failure_callback": lambda context: notify_telegram(
        telegram_conn_id="telegram_default",
        context=context,
        title="🔥 PythonOperator Failed",
        message=str(context.get("exception"))
    ),
}

with DAG(
    dag_id="test_telegram_python_operator",
    start_date=datetime(2025, 12, 15),
    schedule=None,          # manual trigger
    catchup=False,
    default_args=default_args,
    tags=["test", "telegram"],
) as dag:

    test_fail = PythonOperator(
        task_id="fail_me",
        python_callable=fail_task,
    )
