def validate(validator):
    # Schema
    validator.expect_table_columns_to_match_set([
        "category_id", "parent_id", "category_name", "type",
        "url_key", "url_path", "level", "status",
        "include_in_menu", "is_leaf",
        "meta_title", "meta_description", "meta_keywords",
        "thumbnail_url", "ngay_cap_nhat"
    ])

    # Primary key
    validator.expect_column_values_to_be_unique("category_id")
    validator.expect_column_values_to_not_be_null("category_id")

    # Business
    validator.expect_column_values_to_not_be_null("category_name")
    validator.expect_column_values_to_be_between("level", min_value=0)

    # Flag logic
    validator.expect_column_values_to_be_in_set(
        "is_leaf", [True, False]
    )
