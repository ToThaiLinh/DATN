def validate(validator):
    # Schema
    validator.expect_table_columns_to_match_set([
        "order_item_sk",
        "order_id",
        "order_item_id",
        "customer_sk",
        "product_sk",
        "seller_sk",
        "order_purchase_timestamp_sk",
        "order_approved_at_date_sk",
        "order_delivery_carrier_date_sk",
        "order_delivery_customer_date_sk",
        "order_estimated_date_delivery_date_sk",
        "shipping_limit_date_sk",
        "price",
        "freight_value",
        "total_amount",
        "order_status",
        "ingestion_time"
    ])

    # PK
    validator.expect_column_values_to_not_be_null("order_item_sk")
    validator.expect_column_values_to_be_unique("order_item_sk")

    # Business rule
    validator.expect_column_values_to_not_be_null("order_id")
    validator.expect_column_values_to_not_be_null("order_item_id")

    validator.expect_column_values_to_not_be_null("customer_sk")
    validator.expect_column_values_to_not_be_null("product_sk")
    validator.expect_column_values_to_not_be_null("seller_sk")

    validator.expect_column_values_to_not_be_null("order_purchase_timestamp_sk")
    validator.expect_column_values_to_not_be_null("shipping_limit_date_sk")

    validator.expect_column_values_to_not_be_null("price")
    validator.expect_column_values_to_be_between(
        "price",
        min_value=0
    )

    validator.expect_column_values_to_not_be_null("freight_value")
    validator.expect_column_values_to_be_between(
        "freight_value",
        min_value=0
    )

    validator.expect_column_values_to_not_be_null("total_amount")
    validator.expect_column_values_to_be_between(
        "total_amount",
        min_value=0
    )

    validator.expect_column_values_to_not_be_null("order_status")
    validator.expect_column_values_to_be_in_set(
        "order_status",
        [
            "created",
            "approved",
            "invoiced",
            "processing",
            "shipped",
            "delivered",
            "canceled",
            "unavailable"
        ]
    )

    validator.expect_column_values_to_not_be_null("ingestion_time")
