from datetime import datetime
from pyspark.sql.functions import *

def persist_run_metrics(spark, checkpoint_result, table_name):
    validation_results = checkpoint_result.list_validation_results()
    vr = validation_results[0]
    stats = vr["statistics"]

    row = [{
        "run_id": checkpoint_result.run_id.run_name,
        "table_name": table_name,
        "success": checkpoint_result.success,
        "evaluated_expectations": stats["evaluated_expectations"],
        "successful_expectations": stats["successful_expectations"],
        "unsuccessful_expectations": stats["unsuccessful_expectations"],
        "success_percent": stats["success_percent"],
        "created_at": datetime.now(),
    }]

    print(row)

    spark.createDataFrame(row) \
        .write.format("iceberg") \
        .mode("append") \
        .saveAsTable(f"{table_name}")

def persist_expectation_metrics(spark, checkpoint_result, table_name):
    rows = []
    now = datetime.now()

    for vr in checkpoint_result["_validation_results"]:
        for r in vr["results"]:
            rows.append({
                "run_id": checkpoint_result.run_id.run_name,
                "table_name": table_name,
                "expectation_type": r["expectation_config"]["expectation_type"],
                "column_name": r["expectation_config"]["kwargs"].get("column"),
                "success": r["success"],
                "unexpected_count": r["result"].get("unexpected_count"),
                "unexpected_percent": r["result"].get("unexpected_percent"),
                "created_at": now,
            })
    print(rows)
    spark.createDataFrame(rows) \
        .write.format("iceberg") \
        .mode("append") \
        .saveAsTable(f"{table_name}")


def persist_error_records(df, checkpoint_result, table_name: str):
    """
    Persist row-level data quality errors into an Iceberg error table.
    Assumption:
      - 1 source table <-> 1 error table
      - Error table schema = source schema + metadata columns
    """

    run_id = checkpoint_result.run_id.run_name
    error_dfs = []

    validation_results = checkpoint_result["_validation_results"]

    for vr in validation_results:
        for r in vr["results"]:
            if r["success"]:
                continue

            exp_cfg = r["expectation_config"]
            exp_type = exp_cfg["expectation_type"]
            kwargs = exp_cfg.get("kwargs", {})
            column = kwargs.get("column")

            # ---------- Build row-level failed condition ----------
            failed_condition = None

            if exp_type == "expect_column_values_to_not_be_null":
                failed_condition = col(column).isNull()

            elif exp_type == "expect_column_values_to_be_between":
                failed_condition = ~col(column).between(
                    kwargs.get("min_value"),
                    kwargs.get("max_value"),
                )

            elif exp_type == "expect_column_values_to_be_in_set":
                failed_condition = ~col(column).isin(
                    kwargs.get("value_set", [])
                )

            elif exp_type == "expect_column_values_to_match_regex":
                failed_condition = ~col(column).rlike(
                    kwargs.get("regex")
                )

            # ---------- Skip unsupported expectations ----------
            if failed_condition is None:
                continue

            # ---------- Build error dataframe ----------
            err_df = (
                df.filter(failed_condition)
                  .withColumn("run_id", lit(run_id))
                  .withColumn("error_type", lit("DATA_QUALITY"))
                  .withColumn("expectation_type", lit(exp_type))
                  .withColumn("column_name", lit(column))
                  .withColumn("run_time", current_timestamp())
            )

            error_dfs.append(err_df)

    if not error_dfs:
        return

    # ---------- Union all error dataframes ----------
    final_df = error_dfs[0]
    for d in error_dfs[1:]:
        final_df = final_df.unionByName(d)

    (
        final_df.write
        .format("iceberg")
        .mode("append")
        .saveAsTable(table_name)
    )
