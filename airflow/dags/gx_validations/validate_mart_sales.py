def validate(validator):
    # Schema
    validator.expect_table_columns_to_match_set([
        "date_sk",
        "full_date",
        "year",
        "month",
        "product_sk",
        "seller_sk",
        "customer_sk",
        "order_status",
        "gross_sales",
        "shipping_amount",
        "net_sales",
        "items_sold",
        "orders_count"
    ])

    # PK
    validator.expect_column_values_to_not_be_null("date_sk")
    validator.expect_column_values_to_not_be_null("product_sk")
    validator.expect_column_values_to_not_be_null("seller_sk")
    validator.expect_column_values_to_not_be_null("customer_sk")
    validator.expect_column_values_to_not_be_null("order_status")
    validator.expect_compound_columns_to_be_unique(
        ["date_sk", "product_sk", "seller_sk", "customer_sk", "order_status"]
    )

    # Business rule
    validator.expect_column_values_to_not_be_null("full_date")

    validator.expect_column_values_to_not_be_null("year")
    validator.expect_column_values_to_be_between(
        "year",
        min_value=2016
    )

    validator.expect_column_values_to_not_be_null("month")
    validator.expect_column_values_to_be_between(
        "month",
        min_value=1,
        max_value=12
    )

    validator.expect_column_values_to_not_be_null("gross_sales")
    validator.expect_column_values_to_be_between(
        "gross_sales",
        min_value=0
    )

    validator.expect_column_values_to_not_be_null("shipping_amount")
    validator.expect_column_values_to_be_between(
        "shipping_amount",
        min_value=0
    )

    validator.expect_column_values_to_not_be_null("net_sales")
    validator.expect_column_values_to_be_between(
        "net_sales",
        min_value=0
    )

    validator.expect_column_values_to_not_be_null("items_sold")
    validator.expect_column_values_to_be_between(
        "items_sold",
        min_value=0
    )

    validator.expect_column_values_to_not_be_null("orders_count")
    validator.expect_column_values_to_be_between(
        "orders_count",
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
