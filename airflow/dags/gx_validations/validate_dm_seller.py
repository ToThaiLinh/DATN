def validate(validator):
    # Schema
    validator.expect_table_columns_to_match_set([
        "seller_id", "seller_sku", "seller_name",
        "seller_store_id", "ngay_cap_nhat"
    ])

    # PK
    validator.expect_column_values_to_be_unique("seller_id")
    validator.expect_column_values_to_not_be_null("seller_id")

    validator.expect_column_values_to_not_be_null("seller_name")

    # FK
    validator.expect_column_values_to_not_be_null("seller_store_id")
