def validate(validator):
    # Schema
    validator.expect_table_columns_to_match_set([
        "customer_id", "customer_name", "customer_region",
        "customer_created_time", "customer_total_review", 
        "customer_total_thank", "ngay_cap_nhat"
    ])

    # PK
    validator.expect_column_values_to_be_unique("customer_id")
    validator.expect_column_values_to_not_be_null("customer_id")

    validator.expect_column_values_to_be_between(
        "customer_total_review", min_value=0
    )
    validator.expect_column_values_to_be_between(
        "customer_total_thank", min_value=0
    )
