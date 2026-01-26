def validate(validator):
    # Schema
    validator.expect_table_columns_to_match_set([
        "product_sk",
        "product_id",
        "product_category_name",
        "product_name",
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_height_cm",
        "product_length_cm",
        "product_width_cm",
        "effective_from",
        "effective_to",
        "is_current"
    ])

    # PK
    validator.expect_column_values_to_not_be_null("product_sk")
    validator.expect_column_values_to_be_unique("product_sk")

    # Business rule
    validator.expect_column_values_to_not_be_null("product_id")
    # validator.expect_column_values_to_not_be_null("product_category_name")
    # validator.expect_column_values_to_not_be_null("product_name")

    # validator.expect_column_values_to_not_be_null("product_name_lenght")
    validator.expect_column_values_to_be_between(
        "product_name_lenght",
        min_value=1
    )

    # validator.expect_column_values_to_not_be_null("product_description_lenght")
    validator.expect_column_values_to_be_between(
        "product_description_lenght",
        min_value=0
    )

    # validator.expect_column_values_to_not_be_null("product_photos_qty")
    validator.expect_column_values_to_be_between(
        "product_photos_qty",
        min_value=0
    )

    # validator.expect_column_values_to_not_be_null("product_weight_g")
    validator.expect_column_values_to_be_between(
        "product_weight_g",
        min_value=0
    )

    # validator.expect_column_values_to_not_be_null("product_height_cm")
    validator.expect_column_values_to_be_between(
        "product_height_cm",
        min_value=0
    )

    # validator.expect_column_values_to_not_be_null("product_length_cm")
    validator.expect_column_values_to_be_between(
        "product_length_cm",
        min_value=0
    )

    # validator.expect_column_values_to_not_be_null("product_width_cm")
    validator.expect_column_values_to_be_between(
        "product_width_cm",
        min_value=0
    )

    validator.expect_column_values_to_not_be_null("effective_from")

    validator.expect_column_values_to_not_be_null("is_current")
    validator.expect_column_values_to_be_in_set(
        "is_current",
        [True, False]
    )