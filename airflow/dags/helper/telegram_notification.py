import requests
import pytz
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowNotFoundException

# 🔒 Giá trị mặc định
DEFAULT_TELEGRAM_TOKEN = "8221848443:AAEhn-_x_-6zkjBrLa7E673ifCCBbZdrpG0"
DEFAULT_TELEGRAM_CHAT_ID = "-4992683729"

def notify_telegram(
    telegram_conn_id=None,
    context=None,
    title="Airflow Alert",
    message=""
):
    try:
        if telegram_conn_id:
            conn = BaseHook.get_connection(telegram_conn_id)
            token = conn.password or DEFAULT_TELEGRAM_TOKEN
            chat_id = conn.host or DEFAULT_TELEGRAM_CHAT_ID
        else:
            token = DEFAULT_TELEGRAM_TOKEN
            chat_id = DEFAULT_TELEGRAM_CHAT_ID

    except AirflowNotFoundException:
        token = DEFAULT_TELEGRAM_TOKEN
        chat_id = DEFAULT_TELEGRAM_CHAT_ID

    ti = context["task_instance"]
    logical_date = context["logical_date"].astimezone(
        pytz.timezone("Asia/Ho_Chi_Minh")
    )

    text = f"""
        🔥 {title}

        🗂️ DAG: {ti.dag_id}
        ⚙️ Task: {ti.task_id}
        🆔 Run: {context["dag_run"].run_id}
        📅 Logical date: {logical_date:%d-%m-%Y}

        🔄 Try: {ti.try_number}
        ⏱️ Duration: {ti.duration}s
        👤 Owner: {ti.task.owner}

        🚦 Queue: {ti.queue}
        🏊 Pool: {ti.pool}

        🔗 Log: {ti.log_url}

        🧨 Error: {message[:300]}
    """

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    requests.post(url, data={"chat_id": chat_id, "text": text})
