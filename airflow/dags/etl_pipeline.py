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
    dag_id="etl_pipeline",
    start_date=pendulum.datetime(2025, 12, 15, tz='UTC'),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["spark", "telegram"],
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

    silver_sub_product_inventory = SparkOperator(
        task_id="silver_sub_product_inventory",
        name="silver_sub_product_inventory",
        application="/opt/airflow/dags/spark_job/silver/sub_product_inventory.py",
    ).build()

    silver_sub_product_price = SparkOperator(
        task_id="silver_sub_product_price",
        name="silver_sub_product_price",
        application="/opt/airflow/dags/spark_job/silver/sub_product_price.py",
    ).build()

    silver_sub_product_sale = SparkOperator(
        task_id="silver_sub_product_sale",
        name="silver_sub_product_sale",
        application="/opt/airflow/dags/spark_job/silver/sub_product_sale.py",
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
        silver_dm_seller,
        silver_sub_product_inventory,
        silver_sub_product_price,
        silver_sub_product_sale,
    ]

    gold = [
        gold_dim_brand,
        gold_dim_category,
        gold_dim_product,
        gold_dim_seller,
        gold_fact_category_product,
        gold_fact_review_product,
        gold_fact_review,
    ]

    silver_dm_product >> gold_dim_product
    silver_dm_category >> gold_dim_category
    silver_dm_seller >> gold_dim_seller
    silver_sub_review >> gold_fact_review
    [gold_dim_product, gold_dim_category, gold_dim_brand, gold_dim_seller] >> gold_fact_category_product
    [gold_dim_product, gold_fact_review] >> gold_fact_review_product



