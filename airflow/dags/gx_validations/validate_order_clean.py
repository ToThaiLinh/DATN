def validate(validator):
    # Schema
    validator.expect_table_columns_to_match_set([
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
        "ingestion_time"
    ])

    # PK
    validator.expect_column_values_to_not_be_null("order_id")
    validator.expect_column_values_to_be_unique("order_id")

    # Business rule
    validator.expect_column_values_to_not_be_null("customer_id")

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

    validator.expect_column_values_to_not_be_null("order_purchase_timestamp")

    validator.expect_column_values_to_not_be_null("order_estimated_delivery_date")

    validator.expect_column_values_to_not_be_null("ingestion_time")
