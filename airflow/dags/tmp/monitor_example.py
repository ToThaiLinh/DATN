import os
import json
import logging
import requests
import pytz
import re
import boto3
from urllib.parse import urlparse
from botocore.client import Config
from datetime import datetime
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.apache.spark.hooks.spark_submit import SparkSubmitHook
from airflow.hooks.base import BaseHook
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.exceptions import AirflowException, AirflowTaskTimeout
from pyspark.sql import Row
from pyspark.sql.types import *

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class IcebergMonitorHook(SparkSubmitHook, LoggingMixin):
    """Hook mở rộng SparkSubmitHook để kiểm tra tài nguyên Spark và gửi cảnh báo Telegram."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def check_resource(self):
        try:
            conn_ = self.get_connection(self._conn_id)
            master_host = f"{conn_.host}"
            spark_host = master_host.replace("spark://", "http://")
            response = requests.get(f"{spark_host}:8080/json", timeout=30)
            data = response.json()
            cores = sum(worker['coresfree'] for worker in data['workers'])
            memory = sum(worker['memoryfree'] for worker in data['workers'])
            return {'cores': cores, 'memory': memory}
        except Exception as e:
            self.log.error(f"Không thể lấy thông tin tài nguyên Spark: {e}")
            return {'cores': 0, 'memory': 0}

    def determine_resources(self, required_cores, required_memory):
        resources = self.check_resource()
        available_cores = resources.get('cores', 0)
        available_memory = resources.get('memory', 0)
        if required_cores > available_cores or required_memory > available_memory:
            self.log.warning("Không đủ tài nguyên để chạy job Spark monitor.")
            return False
        return True

    def notify_telegram(self, telegram_conn_id, context, message):
        telegram_conn = BaseHook.get_connection(telegram_conn_id)
        telegram_token = telegram_conn.password
        telegram_chat_id = telegram_conn.host
        if not telegram_token or not telegram_chat_id:
            self.log.error("Thiếu thông tin telegram token/chat_id.")
            return

        dag_id = context['task_instance'].dag_id
        task_id = context['task_instance'].task_id
        try_number = context['task_instance'].try_number
        execution_date = context['execution_date']
        local_tz = pytz.timezone('Asia/Ho_Chi_Minh')
        local_execution_date = execution_date.astimezone(local_tz)
        formatted_execution_date = local_execution_date.strftime("%d-%m-%Y %H:%M:%S")

        formatted_message = (
            f"⚠️ Iceberg Monitor Job Failed ⚠️\n\n"
            f"❌ DAG: {dag_id}\n"
            f"🧩 Task: {task_id}\n"
            f"🔁 Lần thử: {try_number}\n"
            f"🕐 Thời gian: {formatted_execution_date}\n"
            f"📜 Lỗi: {message[:300]}"
        )
        url = f"https://api.telegram.org/bot{telegram_token}/sendMessage"
        payload = {'chat_id': telegram_chat_id, 'text': formatted_message}
        response = requests.post(url, data=payload)
        if response.status_code != 200:
            self.log.error(f"Gửi cảnh báo Telegram thất bại: {response.text}")
        else:
            self.log.info("Đã gửi cảnh báo Telegram thành công.")


class IcebergMonitorOperator(SparkSubmitOperator):
    """
    Operator để monitor dữ liệu và metadata của bảng Iceberg.
    Kết quả monitor sẽ ghi vào bảng monitor:
      - Catalog: truyền động
      - Schema: cố định "monitor"
      - Table: monitor_iceberg_data / monitor_iceberg_metadata
    """

    def __init__(
        self,
        catalogs,
        monitor_catalog,
        monitor_data_table="monitor.monitor_iceberg_data",
        monitor_metadata_table="monitor.monitor_iceberg_metadata",
        s3_endpoint=None,
        s3_access_key=None,
        s3_secret_key=None,
        monitor_type="all",       # 'data' | 'metadata' | 'all'
        spark_conf=None,
        spark_conn_id='spark_default',
        total_executor_cores='4',
        executor_memory='4g',
        telegram_conn=None,
        alert=False,
        *args,
        **kwargs,
    ):
        super().__init__(
            *args,
            **kwargs,
            total_executor_cores=total_executor_cores,
            executor_memory=executor_memory,
            conf=spark_conf,
        )
        self.catalogs = catalogs
        self.monitor_catalog = monitor_catalog
        self.monitor_data_table = monitor_data_table
        self.monitor_metadata_table = monitor_metadata_table
        self.monitor_type = monitor_type
        self.spark_conf = spark_conf or {}
        self.spark_conn_id = spark_conn_id
        self.total_executor_cores = total_executor_cores
        self.executor_memory = executor_memory
        self.telegram_conn = telegram_conn
        self.alert = alert
        self.s3_endpoint = s3_endpoint
        self.s3_access_key = s3_access_key
        self.s3_secret_key = s3_secret_key

    def _generate_monitor_script(self):
        catalogs = self.catalogs
        monitor_data_table = self.monitor_data_table
        monitor_metadata_table = self.monitor_metadata_table
        s3_endpoint = self.s3_endpoint
        s3_access_key = self.s3_access_key
        s3_secret_key = self.s3_secret_key

        script = f"""
import os
import re
import boto3
from urllib.parse import urlparse
from botocore.client import Config
from datetime import datetime
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import *

spark = SparkSession.builder.appName("IcebergMonitorJob").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

catalogs = {catalogs}

def convert_bytes_to_mb(x):
    return None if x is None else x / (1024*1024)

# ----------------- MONITOR DATA -----------------
data_schema = StructType([
    StructField('catalog', StringType(), True),
    StructField('schema', StringType(), True),
    StructField('table', StringType(), True),
    StructField('record_count', LongType(), True),
    StructField('total_data_files', LongType(), True),
    StructField('total_data_size', FloatType(), True),
    StructField('avg_size_record', FloatType(), True),
    StructField('checked_at', TimestampType(), True)
])

data_rows = []

for catalog in catalogs:
    try:
        schemas_df = spark.sql(f"SHOW SCHEMAS IN {{catalog}}")
        schemas = [row['namespace'] for row in schemas_df.collect()]
        for schema in schemas:
            tables_df = spark.sql(f"SHOW TABLES IN {{catalog}}.{{schema}}")
            tables = [row['tableName'] for row in tables_df.collect()]
            for table in tables:
                full_table = f"{{catalog}}.{{schema}}.{{table}}"
                try:
                    count = spark.sql(f"SELECT COUNT(*) as cnt FROM {{full_table}}").collect()[0]['cnt']
                    snapshot_df = spark.read.format("iceberg").load(full_table + ".snapshots")
                    if snapshot_df.count() > 0:
                        last_snapshot = snapshot_df.select("committed_at","operation","summary").tail(1)[0]
                        summary = last_snapshot["summary"]
                        total_files = int(summary.get("total-data-files",0))
                        total_size = convert_bytes_to_mb(float(summary.get("total-files-size",0)))
                        avg_size = total_size / count if count > 0 else None
                    else:
                        total_files, total_size, avg_size = None, None, None

                    data_rows.append(Row(
                        catalog=catalog,
                        schema=schema,
                        table=table,
                        record_count=count,
                        total_data_files=total_files,
                        total_data_size=total_size,
                        avg_size_record=avg_size,
                        checked_at=datetime.now()
                    ))
                except Exception as e:
                    print(f"⚠️ Lỗi khi đọc bảng {{full_table}}: {{e}}")
    except Exception as e:
        print(f"❌ Lỗi khi lấy schema trong catalog {{catalog}}: {{e}}")

data_result = spark.createDataFrame(data_rows, data_schema)
data_result.show(truncate=False)
data_result.write.format("iceberg").mode("append").saveAsTable("{monitor_data_table}")

# ----------------- MONITOR METADATA -----------------
metadata_schema = StructType([
    StructField('catalog', StringType(), True),
    StructField('schema', StringType(), True),
    StructField('table', StringType(), True),
    StructField('total_metadata_files', LongType(), True),
    StructField('total_metadata_size', FloatType(), True),
    StructField('checked_at', TimestampType(), True)
])

s3 = boto3.client(
    's3',
    endpoint_url="{s3_endpoint}",
    aws_access_key_id="{s3_access_key}",
    aws_secret_access_key="{s3_secret_key}",
    config=Config(signature_version='s3v4')
)

def get_size_folder(s3_path):
    parsed = urlparse(s3_path)
    bucket, prefix = parsed.netloc, parsed.path.lstrip('/')
    total_size, total_files, token = 0, 0, None
    while True:
        if token:
            resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, ContinuationToken=token)
        else:
            resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        if 'Contents' in resp:
            for obj in resp['Contents']:
                total_size += obj['Size']
                total_files += 1
        if resp.get('IsTruncated'):
            token = resp.get('NextContinuationToken')
        else:
            break
    return total_files, convert_bytes_to_mb(total_size)

metadata_rows = []

for catalog in catalogs:
    try:
        schemas_df = spark.sql(f"SHOW SCHEMAS IN {{catalog}}")
        schemas = [row['namespace'] for row in schemas_df.collect()]
        for schema in schemas:
            tables_df = spark.sql(f"SHOW TABLES IN {{catalog}}.{{schema}}")
            tables = [row['tableName'] for row in tables_df.collect()]
            for table in tables:
                full_table = f"{{catalog}}.{{schema}}.{{table}}"
                try:
                    stmt = spark.sql(f"SHOW CREATE TABLE {{full_table}}").collect()[0]['createtab_stmt']
                    loc_match = re.search(r"LOCATION\\s+'([^']+)'", stmt)
                    if loc_match:
                        loc = loc_match.group(1) + "/metadata"
                        total_files, total_size = get_size_folder(loc)
                        metadata_rows.append(Row(
                            catalog=catalog,
                            schema=schema,
                            table=table,
                            total_metadata_files=total_files,
                            total_metadata_size=total_size,
                            checked_at=datetime.now()
                        ))
                except Exception as e:
                    print(f"⚠️ Lỗi metadata {{full_table}}: {{e}}")
    except Exception as e:
        print(f"❌ Lỗi khi lấy schema trong catalog {{catalog}}: {{e}}")

metadata_result = spark.createDataFrame(metadata_rows, metadata_schema)
metadata_result.show(truncate=False)
metadata_result.write.format("iceberg").mode("append").saveAsTable("{monitor_metadata_table}")

spark.stop()
"""
        return script

    def execute(self, context):
        hook = IcebergMonitorHook(conn_id=self.spark_conn_id)

        # Kiểm tra tài nguyên Spark
        required_cores = int(self.total_executor_cores)
        required_memory = (
            int(self.executor_memory.replace('g', '')) * 1024
            if self.executor_memory.endswith('g')
            else int(self.executor_memory.replace('m', ''))
        )
        if not hook.determine_resources(required_cores, required_memory):
            msg = "Không đủ tài nguyên để chạy monitor job."
            self.log.error(msg)
            if self.alert:
                hook.notify_telegram(self.telegram_conn, context, msg)
            raise AirflowException(msg)

        # Sinh script động
        local_script = "/opt/airflow/spark_job/iceberg_monitor.py"
        os.makedirs(os.path.dirname(local_script), exist_ok=True)
        content = self._generate_monitor_script()
        self.log.info("=== Generated Iceberg Monitor Script ===\n" + content)
        with open(local_script, "w") as f:
            f.write(content)

        self.application = local_script

        try:
            super().execute(context)
        except (AirflowTaskTimeout, Exception) as e:
            err = str(e)
            self.log.error(err)
            if self.alert:
                hook.notify_telegram(self.telegram_conn, context, err)
            raise
        finally:
            if os.path.exists(local_script):
                os.remove(local_script)
                self.log.info(f"Đã xóa file tạm: {local_script}")
