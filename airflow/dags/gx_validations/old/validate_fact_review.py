def validate(validator):
    # Schema
    validator.expect_table_columns_to_match_set([
        "review_id",
        "product_id",
        "customer_id",
        "seller_id",
        "rating",
        "score",
        "new_score",
        "title",
        "content",
        "status",
        "thank_count",
        "comment_count",
        "delivery_rating",
        "created_date",
        "delivery_date",
        "current_date",
        "timeline_content",
        "ngay_cap_nhat"
    ])

    # PK
    validator.expect_column_values_to_not_be_null("review_id")
    validator.expect_column_values_to_be_unique("review_id")

    # FK
    validator.expect_column_values_to_not_be_null("product_id")
    # validator.expect_column_values_to_not_be_null("customer_id")

    # Business rules
    validator.expect_column_values_to_be_between(
        "rating", min_value=1, max_value=5
    )

    validator.expect_column_values_to_be_between(
        "thank_count", min_value=0
    )

    validator.expect_column_values_to_be_between(
        "comment_count", min_value=0
    )
