def validate(validator):
   # Schema
   validator.expect_table_columns_to_match_set([
      "brand_id", "brand_name", "brand_slug", "ngay_cap_nhat"
   ])

   validator.expect_column_values_to_be_unique("brand_id")
   validator.expect_column_values_to_not_be_null("brand_id")

   # validator.expect_column_values_to_not_be_null("brand_name")

   # validator.expect_column_values_to_match_regex(
   #     "brand_slug", r"^[a-z0-9\-]+$"
   # )

