def validate(validator):

    # Schema
    validator.expect_table_columns_to_match_set([
        "delivery_date_sk",
        "year",
        "month",
        "seller_sk",
        "customer_sk",
        "shipping_amount",
        "avg_delivery_days",
        "avg_delay_days",
        "late_items",
        "items_delivery"
    ])

    # PK
    validator.expect_column_values_to_not_be_null("delivery_date_sk")
    validator.expect_column_values_to_not_be_null("seller_sk")
    validator.expect_column_values_to_not_be_null("customer_sk")

    validator.expect_compound_columns_to_be_unique([
        "delivery_date_sk",
        "seller_sk",
        "customer_sk"
    ])

    # Business rule
    validator.expect_column_values_to_not_be_null("year")
    validator.expect_column_values_to_be_between(
        "year", min_value=2000, max_value=2100
    )

    validator.expect_column_values_to_not_be_null("month")
    validator.expect_column_values_to_be_between(
        "month", min_value=1, max_value=12
    )

    validator.expect_column_values_to_not_be_null("shipping_amount")
    validator.expect_column_values_to_be_between(
        "shipping_amount", min_value=0
    )

    validator.expect_column_values_to_not_be_null("avg_delivery_days")
    validator.expect_column_values_to_be_between(
        "avg_delivery_days", min_value=0
    )

    validator.expect_column_values_to_not_be_null("avg_delay_days")
    validator.expect_column_values_to_be_between(
        "avg_delay_days", min_value=0
    )

    validator.expect_column_values_to_not_be_null("late_items")
    validator.expect_column_values_to_be_between(
        "late_items", min_value=0
    )

    validator.expect_column_values_to_not_be_null("items_delivery")
    validator.expect_column_values_to_be_between(
        "items_delivery", min_value=0
    )

    validator.expect_column_pair_values_A_to_be_less_than_or_equal_to_B(
        "late_items",
        "items_delivery"
    )
