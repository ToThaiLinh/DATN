def validate(validator):
    # Schema
    validator.expect_table_columns_to_match_set([
        "seller_id",
        "seller_zip_code_prefix",
        "seller_city",
        "seller_state",
        "ingestion_time"
    ])

    # PK
    validator.expect_column_values_to_not_be_null("seller_id")
    validator.expect_column_values_to_be_unique("seller_id")

    # Business rule
    validator.expect_column_values_to_not_be_null("seller_zip_code_prefix")

    validator.expect_column_values_to_not_be_null("seller_city")

    validator.expect_column_values_to_not_be_null("seller_state")
    validator.expect_column_value_lengths_to_be_between(
        "seller_state",
        min_value=2,
        max_value=20
    )

    validator.expect_column_values_to_not_be_null("ingestion_time")
