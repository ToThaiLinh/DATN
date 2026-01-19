def validate(validator):
    # Schema
    validator.expect_table_columns_to_match_set([
        "review_id", "product_id", "customer_id", "seller_id",
        "rating", "score", "new_score", "title", "content",
        "status", "thank_count", "comment_count", "delivery_rating",
        "created_date", "delivery_date", "current_date",
        "timeline_content", "ngay_cap_nhat"
    ]),

    # PK
    validator.expect_column_values_to_be_unique("review_id")
    validator.expect_column_values_to_not_be_null("review_id")

    # FK
    validator.expect_column_values_to_not_be_null("product_id")
    # validator.expect_column_values_to_not_be_null("customer_id")

    # Rating logic
    validator.expect_column_values_to_be_between("rating", min_value=1, max_value=5)

    # Score
    # validator.expect_column_values_to_be_between("score", min_value=0)
    validator.expect_column_values_to_be_between("new_score", min_value=0)

    # Engagement
    validator.expect_column_values_to_be_between("thank_count", min_value=0)
    validator.expect_column_values_to_be_between("comment_count", min_value=0)

    # Date logic
    # validator.expect_column_values_to_not_be_null("created_date")
    # validator.expect_column_values_to_not_be_null("current_date")
    validator.expect_column_values_to_not_be_null("ngay_cap_nhat")
