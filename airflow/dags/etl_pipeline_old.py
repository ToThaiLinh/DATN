# from airflow import DAG
# from airflow.models.baseoperator import chain
# from helper.telegram_notification import notify_telegram
# from helper.SparkOperator import SparkOperator
# import pendulum
# from datetime import timedelta

# from gx_validations import (
#     validate_dm_brand,
#     validate_dm_category,
#     validate_dm_product,
#     validate_dm_seller,
#     validate_sub_review,
#     validate_dim_brand,
#     validate_dim_category,
#     validate_dim_product,
#     validate_dim_seller,
#     validate_fact_category_product,
#     validate_fact_review,
#     validate_fact_review_product
# )
# from helper.GXValidateOperator import gx_validate_task

# default_args = {
#     "owner": "airflow",
#     "depends_on_past": False,
#     "retries": 0,
#     "retry_delay": timedelta(minutes=1),
#     "email_on_failure": False,
#     "email_on_retry": False,
# }


# with DAG(
#     dag_id="etl_pipeline",
#     start_date=pendulum.datetime(2025, 12, 15, tz='UTC'),
#     schedule=None,
#     catchup=False,
#     default_args=default_args,
#     tags=["spark", "telegram", "etl_pipeline", "great_expecatation"],
# ) as dag:

#     bronze_sub_categories = SparkOperator(
#         task_id="bronze_sub_categories",
#         name="bronze_sub_categories",
#         application="/opt/airflow/dags/spark_job/bronze/bronze_sub_categories.py"
#     ).build()

#     bronze_product_details = SparkOperator(
#         task_id="bronze_product_details",
#         name="bronze_product_details",
#         application="/opt/airflow/dags/spark_job/bronze/bronze_product_details.py"
#     ).build()

#     bronze_reviews = SparkOperator(
#         task_id="bronze_reviews",
#         name="bronze_reviews",
#         application="/opt/airflow/dags/spark_job/bronze/bronze_reviews.py"
#     ).build()

#     silver_dm_brand = SparkOperator(
#         task_id="silver_dm_brand",
#         name="silver_dm_brand",
#         application="/opt/airflow/dags/spark_job/silver/dm_brand.py"
#     ).build()

#     silver_dm_category = SparkOperator(
#         task_id="silver_dm_category",
#         name="silver_dm_category",
#         application="/opt/airflow/dags/spark_job/silver/dm_category.py"
#     ).build()

#     silver_dm_product = SparkOperator(
#         task_id="silver_dm_product",
#         name="silver_dm_product",
#         application="/opt/airflow/dags/spark_job/silver/dm_product.py",
#     ).build()

#     silver_dm_seller = SparkOperator(
#         task_id="sillver_dm_seller",
#         name="silver_dm_seller",
#         application="/opt/airflow/dags/spark_job/silver/dm_seller.py",
#     ).build()

#     silver_sub_review = SparkOperator(
#         task_id="silver_sub_review",
#         name="silver_sub_review",
#         application="/opt/airflow/dags/spark_job/silver/sub_review.py",
#     ).build()

#     gold_dim_brand = SparkOperator(
#         task_id="gold_dim_brand",
#         name="gold_dim_brand",
#         application="/opt/airflow/dags/spark_job/gold/dim_brand.py",
#     ).build()

#     gold_dim_category = SparkOperator(
#         task_id="gold_dim_category",
#         name="gold_dim_category",
#         application="/opt/airflow/dags/spark_job/gold/dim_category.py",
#     ).build()

#     gold_dim_product = SparkOperator(
#         task_id="gold_dim_product",
#         name="gold_dim_product",
#         application="/opt/airflow/dags/spark_job/gold/dim_product.py",
#     ).build()

#     gold_dim_seller = SparkOperator(
#         task_id="gold_dim_seller",
#         name="gold_dim_seller",
#         application="/opt/airflow/dags/spark_job/gold/dim_seller.py",
#     ).build()

#     gold_fact_category_product = SparkOperator(
#         task_id="gold_fact_category_product",
#         name="gold_fact_category_product",
#         application="/opt/airflow/dags/spark_job/gold/fact_category_product.py",
#     ).build()

#     gold_fact_review_product = SparkOperator(
#         task_id="gold_fact_review_product",
#         name="gold_fact_review_product ",
#         application="/opt/airflow/dags/spark_job/gold/fact_review_product.py",
#     ).build()

#     gold_fact_review = SparkOperator(
#         task_id="gold_fact_review",
#         name="gold_fact_review",
#         application="/opt/airflow/dags/spark_job/gold/fact_review.py",
#     ).build()

#     bronze = [
#         bronze_sub_categories,
#         bronze_product_details,
#         bronze_reviews
#     ]

#     bronze_sub_categories >> silver_dm_category
#     bronze_product_details >> [silver_dm_product, silver_dm_brand, silver_dm_seller]
#     bronze_reviews >> [silver_sub_review]

#     silver = [
#         silver_dm_brand,
#         silver_dm_category,
#         silver_dm_product,
#         silver_dm_seller,
#         silver_sub_review
#     ]

#     gx_dm_brand = gx_validate_task("iceberg", "silver", "dm_brand", validate_dm_brand)
#     gx_dm_category = gx_validate_task("iceberg", "silver", "dm_category", validate_dm_category)
#     gx_dm_product = gx_validate_task("iceberg", "silver", "dm_product", validate_dm_product)
#     gx_dm_seller = gx_validate_task("iceberg", "silver", "dm_seller", validate_dm_seller)
#     gx_sub_review = gx_validate_task("iceberg", "silver", "sub_review", validate_sub_review)

#     gold = [
#         gold_dim_brand,
#         gold_dim_category,
#         gold_dim_product,
#         gold_dim_seller,
#         gold_fact_category_product,
#         gold_fact_review_product,
#         gold_fact_review,
#     ]

#     gx_dim_brand = gx_validate_task('iceberg', 'gold', 'dim_brand', validate_dim_brand)
#     gx_dim_category = gx_validate_task('iceberg', 'gold', 'dim_category', validate_dim_category)
#     gx_dim_product = gx_validate_task('iceberg', 'gold', 'dim_product', validate_dim_product)
#     gx_dim_seller = gx_validate_task('iceberg', 'gold', 'dim_seller', validate_dim_seller)
#     gx_fact_category_product = gx_validate_task('iceberg', 'gold', 'fact_category_product', validate_fact_category_product)
#     gx_fact_review = gx_validate_task('iceberg', 'gold', 'fact_review', validate_fact_review)
#     gx_fact_review_product = gx_validate_task('iceberg', 'gold', 'fact_review_product', validate_fact_review_product)

#     silver_dm_brand >> gx_dm_brand >> gold_dim_brand
#     silver_dm_category >> gx_dm_category >> gold_dim_category
#     silver_dm_product >> gx_dm_product >> gold_dim_product
#     silver_dm_seller >> gx_dm_seller >> gold_dim_seller
#     silver_sub_review >> gx_sub_review >> gold_fact_review
#     [gx_dm_product, gx_dm_category] >> gold_fact_category_product
#     [gx_dm_product, gx_sub_review] >> gold_fact_review_product

#     gold_dim_category >> gx_dim_category
#     gold_dim_brand >> gx_dim_brand
#     gold_dim_seller >> gx_dim_seller
#     gold_dim_product >> gx_dim_product
#     gold_fact_category_product >> gx_fact_category_product
#     gold_fact_review >> gx_fact_review
#     gold_fact_review_product >> gx_fact_review_product




