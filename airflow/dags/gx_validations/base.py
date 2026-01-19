def run_validation(context, dataframe, table_name, validate_fn):
    datasource_name = "spark_iceberg"
    suite_name = f"{table_name}.expectations"

    try:
        datasource = context.get_datasource(datasource_name)
    except Exception:
        datasource = context.sources.add_spark(name=datasource_name)

    try:
        asset = datasource.get_asset(table_name)
    except Exception:
        asset = datasource.add_dataframe_asset(name=table_name)

    batch_request = asset.build_batch_request(dataframe=dataframe)

    # ---------- Ensure expectation suite ----------
    try:
        context.get_expectation_suite(suite_name)
        print(f"♻️ Reuse expectation suite: {suite_name}")
    except Exception:
        context.add_expectation_suite(expectation_suite_name=suite_name)
        print(f"✅ Created expectation suite: {suite_name}")

    # ---------- Validator ----------
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name
    )

    # RESET expectations cũ
    validator.expectation_suite.expectations = []

    validate_fn(validator)

    validator.save_expectation_suite(discard_failed_expectations=False)

    checkpoint = context.add_or_update_checkpoint(
        name=f"checkpoint_{table_name}",
        validations=[
            {
                "batch_request": batch_request,
                "expectation_suite_name": suite_name,
            }
        ],
    )

    result = checkpoint.run()
    context.build_data_docs()

    # context.view_validation_result(result)

    if not result.success:
        raise ValueError(f"❌ Validation FAILED for {table_name}")

    return True
