from airflow import DAG
from airflow.models.baseoperator import chain
from helper.telegram_notification import notify_telegram
from helper.SparkOperator import SparkOperator
import pendulum
from datetime import timedelta

from gx_validations import (
    validate_dm_brand,
    validate_dm_category,
    validate_dm_product,
    validate_dm_seller,
    validate_sub_review,
)
from helper.GXValidateOperator import gx_validate_task

default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "email_on_failure": False,
    "email_on_retry": False,
}


with DAG(
    dag_id="etl_pipeline",
    start_date=pendulum.datetime(2025, 12, 15, tz='UTC'),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["spark", "telegram", "etl_pipeline"],
) as dag:

    silver_dm_brand = SparkOperator(
        task_id="silver_dm_brand",
        name="silver_dm_brand",
        application="/opt/airflow/dags/spark_job/silver/dm_brand.py"
    ).build()

    silver_dm_category = SparkOperator(
        task_id="silver_dm_category",
        name="silver_dm_category",
        application="/opt/airflow/dags/spark_job/silver/dm_category.py"
    ).build()

    silver_dm_product = SparkOperator(
        task_id="silver_dm_product",
        name="silver_dm_product",
        application="/opt/airflow/dags/spark_job/silver/dm_product.py",
    ).build()

    silver_dm_seller = SparkOperator(
        task_id="sillver_dm_seller",
        name="silver_dm_seller",
        application="/opt/airflow/dags/spark_job/silver/dm_seller.py",
    ).build()

    silver_sub_review = SparkOperator(
        task_id="silver_sub_review",
        name="silver_sub_review",
        application="/opt/airflow/dags/spark_job/silver/sub_review.py",
    ).build()

    gold_dim_brand = SparkOperator(
        task_id="gold_dim_brand",
        name="gold_dim_brand",
        application="/opt/airflow/dags/spark_job/gold/dim_brand.py",
    ).build()

    gold_dim_category = SparkOperator(
        task_id="gold_dim_category",
        name="gold_dim_category",
        application="/opt/airflow/dags/spark_job/gold/dim_category.py",
    ).build()

    gold_dim_product = SparkOperator(
        task_id="gold_dim_product",
        name="gold_dim_product",
        application="/opt/airflow/dags/spark_job/gold/dim_product.py",
    ).build()

    gold_dim_seller = SparkOperator(
        task_id="gold_dim_seller",
        name="gold_dim_seller",
        application="/opt/airflow/dags/spark_job/gold/dim_seller.py",
    ).build()

    gold_fact_category_product = SparkOperator(
        task_id="gold_fact_category_product",
        name="gold_fact_category_product",
        application="/opt/airflow/dags/spark_job/gold/fact_category_product.py",
    ).build()

    gold_fact_review_product = SparkOperator(
        task_id="gold_fact_review_product",
        name="gold_fact_review_product ",
        application="/opt/airflow/dags/spark_job/gold/fact_review_product.py",
    ).build()

    gold_fact_review = SparkOperator(
        task_id="gold_fact_review",
        name="gold_fact_review",
        application="/opt/airflow/dags/spark_job/gold/fact_review.py",
    ).build()


    silver = [
        silver_dm_brand,
        silver_dm_category,
        silver_dm_product,
        silver_dm_seller
    ]

    gx_dm_brand = gx_validate_task("iceberg", "silver", "dm_brand", validate_dm_brand)
    gx_dm_category = gx_validate_task("iceberg", "silver", "dm_category", validate_dm_category)
    gx_dm_product = gx_validate_task("iceberg", "silver", "dm_product", validate_dm_product)
    gx_dm_seller = gx_validate_task("iceberg", "silver", "dm_seller", validate_dm_seller)
    gx_sub_review = gx_validate_task("iceberg", "silver", "sub_review", validate_sub_review)

    gold = [
        gold_dim_brand,
        gold_dim_category,
        gold_dim_product,
        gold_dim_seller,
        gold_fact_category_product,
        gold_fact_review_product,
        gold_fact_review,
    ]



    silver_dm_brand >> gx_dm_brand >> gold_dim_brand
    silver_dm_category >> gx_dm_category >> gold_dim_category
    silver_dm_product >> gx_dm_product >> gold_dim_product
    silver_dm_seller >> gx_dm_seller >> gold_dim_seller
    silver_sub_review >> gx_sub_review >> gold_fact_review
    [gx_dm_product, gx_dm_category] >> gold_fact_category_product
    [gx_dm_product, gx_sub_review] >> gold_fact_review_product



