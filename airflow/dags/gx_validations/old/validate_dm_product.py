def validate(validator):
    # Schema
    validator.expect_table_columns_to_match_set([
        "product_id", "category_id", "brand_id", "seller_id",
        "sku", "name", "type", "short_description", "favourite_count",
        "review_count", "has_ebook", "is_fresh", "is_flower", "is_gift_card",
        "is_baby_milk", "is_acoholic_drink", "has_buynow", 
        "price", "original_price", "discount", "discount_rate",
        "inventory_status", "inventory_type",
        "stock_qty", "min_sales_qty", "max_sales_qty", "quantity_sold",
        "data_version", "day_ago_created", "created_date", "ngay_cap_nhat"
    ])

    # PK
    validator.expect_column_values_to_not_be_null("product_id")
    validator.expect_column_values_to_be_unique("product_id")

    # FK (ở mức basic)
    validator.expect_column_values_to_not_be_null("category_id")
    # validator.expect_column_values_to_not_be_null("brand_id")
    validator.expect_column_values_to_not_be_null("seller_id")

    # Numeric sanity
    validator.expect_column_values_to_be_between("price", min_value=0)
    validator.expect_column_values_to_be_between("original_price", min_value=0)
    validator.expect_column_values_to_be_between("discount_rate", min_value=0, max_value=100)

    # Inventory
    validator.expect_column_values_to_be_between("stock_qty", min_value=0)
    validator.expect_column_values_to_be_between("quantity_sold", min_value=0)
    validator.expect_column_values_to_be_in_set(
        "inventory_status",
        ["available", "instock", "out_of_stock", "backorder"]
    )

    # Business logic
    validator.expect_column_pair_values_A_to_be_greater_than_B(
        column_A="original_price",
        column_B="price",
        or_equal=True
    )

    # Snapshot
    validator.expect_column_values_to_not_be_null("ngay_cap_nhat")
