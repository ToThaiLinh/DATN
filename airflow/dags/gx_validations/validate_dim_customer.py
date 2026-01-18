def validate(validator):
    # Schema
    validator.expect_table_columns_to_match_set([
        "customer_sk",
        "customer_id",
        "customer_unique_id",
        "customer_city",
        "customer_state",
        "effectivate_from",
        "effectivate_to",
        "is_current",
        "ingestion_time"
    ])

    # PK
    validator.expect_column_values_to_not_be_null("customer_sk")
    validator.expect_column_values_to_be_unique("customer_sk")

    # Business rule
    validator.expect_column_values_to_not_be_null("customer_id")
    validator.expect_column_values_to_not_be_null("customer_unique_id")

    validator.expect_column_values_to_not_be_null("customer_city")
    
    validator.expect_column_values_to_not_be_null("customer_state")
    validator.expect_column_value_lengths_to_be_between(
        "customer_state",
        min_value=2,
        max_value=20
    )

    validator.expect_column_values_to_not_be_null("effectivate_from")

    validator.expect_column_values_to_not_be_null("is_current")
    validator.expect_column_values_to_be_in_set(
        "is_current",
        [True, False]
    )

    validator.expect_column_values_to_not_be_null("ingestion_time")
