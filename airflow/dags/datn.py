from airflow import DAG
from helper.SparkOperator import SparkOperator
import pendulum
from datetime import timedelta

from gx_validations import (
    validate_customer_clean,
    validate_product_clean,
    validate_seller_clean,
    validate_order_clean,
    validate_order_item_clean,
    validate_dim_customer,
    validate_dim_product,
    validate_dim_seller,
    validate_fact_order_item,
    validate_mart_sales,
    validate_mart_logistics
)
from helper.GXValidateOperator import gx_validate_task

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "email_on_failure": False,
    "email_on_retry": False,
}


with DAG(
    dag_id="datn",
    start_date=pendulum.datetime(2025, 12, 15, tz='UTC'),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["spark", "telegram", "etl_pipeline", "great_expecatation"],
) as dag:

    olist_customers_dataset = SparkOperator(
        task_id="olist_customers_dataset",
        name="olist_customers_dataset",
        application="/opt/airflow/dags/spark_job/bronze/olist_customers_dataset.py"
    ).build()

    olist_products_dataset = SparkOperator(
        task_id="olist_products_dataset",
        name="olist_products_dataset",
        application="/opt/airflow/dags/spark_job/bronze/olist_products_dataset.py"
    ).build()

    olist_sellers_dataset = SparkOperator(
        task_id="olist_sellers_dataset",
        name="olist_sellers_dataset",
        application="/opt/airflow/dags/spark_job/bronze/olist_sellers_dataset.py"
    ).build()

    olist_orders_dataset = SparkOperator(
        task_id="olist_orders_dataset",
        name="olist_orders_dataset",
        application="/opt/airflow/dags/spark_job/bronze/olist_orders_dataset.py"
    ).build()

    olist_order_items_dataset = SparkOperator(
        task_id="olist_order_items_dataset",
        name="olist_order_items_dataset",
        application="/opt/airflow/dags/spark_job/bronze/olist_order_items_dataset.py"
    ).build()

    customer_clean = SparkOperator(
        task_id="customer_clean",
        name="customer_clean",
        application="/opt/airflow/dags/spark_job/silver/customer_clean.py"
    ).build()

    product_clean = SparkOperator(
        task_id="product_clean",
        name="product_clean",
        application="/opt/airflow/dags/spark_job/silver/product_clean.py"
    ).build()

    seller_clean = SparkOperator(
        task_id="seller_clean",
        name="seller_clean",
        application="/opt/airflow/dags/spark_job/silver/seller_clean.py",
    ).build()

    order_clean = SparkOperator(
        task_id="order_clean",
        name="order_clean",
        application="/opt/airflow/dags/spark_job/silver/order_clean.py",
    ).build()

    order_item_clean = SparkOperator(
        task_id="order_item_clean",
        name="order_item_clean",
        application="/opt/airflow/dags/spark_job/silver/order_item_clean.py",
    ).build()

    dim_customer = SparkOperator(
        task_id="dim_customer",
        name="dim_customer",
        application="/opt/airflow/dags/spark_job/gold/dim_customer.py",
    ).build()

    dim_product = SparkOperator(
        task_id="dim_product",
        name="dim_product",
        application="/opt/airflow/dags/spark_job/gold/dim_product.py",
    ).build()

    dim_seller = SparkOperator(
        task_id="dim_seller",
        name="dim_seller",
        application="/opt/airflow/dags/spark_job/gold/dim_seller.py",
    ).build()

    fact_order_item = SparkOperator(
        task_id="fact_order_item",
        name="fact_order_item",
        application="/opt/airflow/dags/spark_job/gold/fact_order_item.py",
    ).build()

    mart_sales = SparkOperator(
        task_id="mart_sales",
        name="mart_sales",
        application="/opt/airflow/dags/spark_job/gold/mart_sales.py",
    ).build()

    mart_logistics = SparkOperator(
        task_id="mart_logistics",
        name="mart_logistics",
        application="/opt/airflow/dags/spark_job/gold/mart_logistics.py",
    ).build()

    bronze = [
        olist_products_dataset,
        olist_sellers_dataset,
        olist_customers_dataset,
        olist_orders_dataset,
        olist_order_items_dataset
    ]

    olist_products_dataset >> product_clean
    olist_sellers_dataset >> seller_clean
    olist_customers_dataset >> customer_clean
    olist_orders_dataset >> order_clean
    olist_order_items_dataset >> order_item_clean

    silver = [
        customer_clean,
        order_clean,
        order_item_clean,
        product_clean,
        seller_clean
    ]

    gx_customer_clean = gx_validate_task("iceberg", "silver", "customer_clean", validate_customer_clean)
    gx_product_clean = gx_validate_task("iceberg", "silver", "product_clean", validate_product_clean)
    gx_seller_clean = gx_validate_task("iceberg", "silver", "seller_clean", validate_seller_clean)
    gx_order_clean = gx_validate_task("iceberg", "silver", "order_clean", validate_order_clean)
    gx_order_item_clean = gx_validate_task("iceberg", "silver", "order_item_clean", validate_order_item_clean)

    gold = [
        dim_customer,
        dim_product,
        dim_seller,
        fact_order_item
    ]

    gx_dim_customer = gx_validate_task('iceberg', 'gold', 'dim_customer', validate_dim_customer)
    gx_dim_product = gx_validate_task('iceberg', 'gold', 'dim_product', validate_dim_product)
    gx_dim_seller = gx_validate_task('iceberg', 'gold', 'dim_seller', validate_dim_seller)
    gx_fact_order_item = gx_validate_task('iceberg', 'gold', 'fact_order_item', validate_fact_order_item)

    customer_clean >> gx_customer_clean >> dim_customer >> gx_dim_customer
    product_clean >> gx_product_clean >> dim_product >> gx_dim_product
    seller_clean >> gx_seller_clean >> dim_seller >> gx_dim_seller
    order_clean  >> gx_order_clean >> fact_order_item >> gx_fact_order_item
    order_item_clean >> gx_order_item_clean >> fact_order_item

    gx_mart_sales = gx_validate_task('iceberg', 'gold', 'mart_sales', validate_mart_sales)
    gx_mart_logistics = gx_validate_task('iceberg', 'gold', 'mart_logistics', validate_mart_logistics)

    [gx_dim_customer, gx_dim_product, gx_dim_seller, gx_fact_order_item] >> mart_sales >> gx_mart_sales
    [gx_dim_customer, gx_dim_product, gx_dim_seller, gx_fact_order_item] >> mart_logistics >> gx_mart_logistics



