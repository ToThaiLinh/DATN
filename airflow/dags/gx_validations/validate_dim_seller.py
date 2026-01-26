def validate(validator):
    # Schema
    validator.expect_table_columns_to_match_set([
        "seller_sk",
        "seller_id",
        "seller_zip_code_prefix",
        "seller_city",
        "seller_state",
        "effective_from",
        "effective_to",
        "is_current"
    ])

    # PK
    validator.expect_column_values_to_not_be_null("seller_sk")
    validator.expect_column_values_to_be_unique("seller_sk")

    # Business rule
    validator.expect_column_values_to_not_be_null("seller_id")

    validator.expect_column_values_to_not_be_null("seller_zip_code_prefix")

    validator.expect_column_values_to_not_be_null("seller_city")

    validator.expect_column_values_to_not_be_null("seller_state")
    validator.expect_column_value_lengths_to_be_between(
        "seller_state",
        min_value=2,
        max_value=20
    )

    validator.expect_column_values_to_not_be_null("effective_from")

    validator.expect_column_values_to_not_be_null("is_current")
    validator.expect_column_values_to_be_in_set(
        "is_current",
        [True, False]
    )
