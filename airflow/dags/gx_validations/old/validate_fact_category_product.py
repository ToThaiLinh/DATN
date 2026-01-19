def validate(validator):
    # Schema
    validator.expect_table_columns_to_match_set([
        "category_id",
        "category_name",
        "total_product",
        "total_quantity_sold",
        "avg_price",
        "revenue",
        "avg_units_sold_per_product",
        "ngay_cap_nhat"
    ])

    # Grain
    # validator.expect_compound_columns_to_be_unique(
    #     ["snapshot_date", "category_id"]
    # )

    # Measures
    validator.expect_column_values_to_be_between(
        "total_product", min_value=0
    )
    validator.expect_column_values_to_be_between(
        "total_quantity_sold", min_value=0
    )
    validator.expect_column_values_to_be_between(
        "revenue", min_value=0
    )
    validator.expect_column_values_to_be_between(
        "avg_price", min_value=0
    )
