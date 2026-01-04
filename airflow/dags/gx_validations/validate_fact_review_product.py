def validate(validator):
    # Schema
    validator.expect_table_columns_to_match_set([
        "product_id",
        "product_name",
        "total_review",
        "avg_rating",
        "negative_review_count",
        "negative_review_rate",
        "positive_review_count",
        "positive_review_rate",
        "ngay_cap_nhat"
    ])

    # Grain
    # validator.expect_compound_columns_to_be_unique(
    #     ["review_date", "product_id"]
    # )

    # Measures
    validator.expect_column_values_to_be_between(
        "total_review", min_value=0
    )

    validator.expect_column_values_to_be_between(
        "avg_rating", min_value=1, max_value=5
    )

    validator.expect_column_values_to_be_between(
        "negative_review_rate", min_value=0.0, max_value=100
    )

    validator.expect_column_values_to_be_between(
        "positive_review_rate", min_value=0.0, max_value=100
    )
