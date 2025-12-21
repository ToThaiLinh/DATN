import pytz
import requests
from airflow.hooks.base import BaseHook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


class SparkOperator:
    def __init__(
        self,
        task_id: str,
        name: str,
        application: str,
        telegram_conn_id: str = "telegram_default",
        spark_conn_id: str = "spark_default",
        application_args: list | None = None,
        verbose: bool = True,
    ):
        self.task_id = task_id
        self.name = name
        self.application = application
        self.telegram_conn_id = telegram_conn_id
        self.spark_conn_id = spark_conn_id
        self.application_args = application_args or []
        self.verbose = verbose

    # ======================
    # Telegram callback
    # ======================
    def _notify_telegram(self, context):
        conn = BaseHook.get_connection(self.telegram_conn_id)
        token = conn.password
        chat_id = conn.host

        ti = context["task_instance"]
        logical_date = context["logical_date"].astimezone(
            pytz.timezone("Asia/Ho_Chi_Minh")
        )

        error = str(context.get("exception"))[:300]

        text = f"""
            🔥 Spark Job Failed

            📊 DAG: {ti.dag_id}
            ⚡ Task: {ti.task_id}
            🆔 Run: {context["dag_run"].run_id}
            📅 Logical date: {logical_date:%d-%m-%Y}

            🧠 App Name: {self.name}
            📦 Application: {self.application}

            🔄 Try: {ti.try_number}
            ⏱️ Duration: {ti.duration}s

            🔗 Airflow Log:
            {ti.log_url}

            🧨 Error:
            {error}
        """

        url = f"https://api.telegram.org/bot{token}/sendMessage"
        requests.post(url, data={"chat_id": chat_id, "text": text})

    # ======================
    # Build Spark operator
    # ======================
    def build(self) -> SparkSubmitOperator:
        return SparkSubmitOperator(
            task_id=self.task_id,
            name=self.name,
            application=self.application,
            application_args=self.application_args,
            conn_id=self.spark_conn_id,
            verbose=self.verbose,
            on_failure_callback=self._notify_telegram,
        )
