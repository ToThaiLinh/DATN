def validate(validator):
    # Schema
    validator.expect_table_columns_to_match_set([
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
        "ingestion_time"
    ])

    # PK
    validator.expect_column_values_to_not_be_null("customer_id")
    validator.expect_column_values_to_be_unique("customer_id")

    # Business rule
    validator.expect_column_values_to_not_be_null("customer_unique_id")

    validator.expect_column_values_to_not_be_null("customer_zip_code_prefix")

    validator.expect_column_values_to_not_be_null("customer_city")
    
    validator.expect_column_values_to_not_be_null("customer_state")
    validator.expect_column_value_lengths_to_be_between(
        "customer_state",
        min_value=2,
        max_value=20
    )

    validator.expect_column_values_to_not_be_null("ingestion_time")
