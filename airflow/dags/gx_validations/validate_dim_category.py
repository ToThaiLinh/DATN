def validate(validator):
    # Schema
    validator.expect_table_columns_to_match_set([
        "category_id",
        "parent_id",
        "category_name",
        "type",
        "level",
        "status",
        "is_leaf",
        "ngay_cap_nhat"
    ])

    # PK
    validator.expect_column_values_to_not_be_null("category_id")
    validator.expect_column_values_to_be_unique("category_id")

    # Business rule
    validator.expect_column_values_to_not_be_null("category_name")

    validator.expect_column_values_to_be_between(
        "level", min_value=0
    )

    # validator.expect_column_values_to_be_in_set(
    #     "status", ["active", "inactive"]
    # )

    validator.expect_column_values_to_be_in_set(
        "is_leaf", [True, False]
    )
