def validate(validator):
    # Schema
    validator.expect_table_columns_to_match_set([
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        "shipping_limit_date",
        "price",
        "freight_value",
        "ingestion_time"
    ])

    # PK
    validator.expect_column_values_to_not_be_null("order_id")
    validator.expect_column_values_to_not_be_null("order_item_id")
    validator.expect_compound_columns_to_be_unique(
        ["order_id", "order_item_id"]
    )

    # Business rule
    validator.expect_column_values_to_not_be_null("product_id")
    validator.expect_column_values_to_not_be_null("seller_id")

    validator.expect_column_values_to_not_be_null("shipping_limit_date")

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

    validator.expect_column_values_to_not_be_null("ingestion_time")
