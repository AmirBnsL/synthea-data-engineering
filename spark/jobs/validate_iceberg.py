import great_expectations as gx
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.expectations import (
    ExpectColumnValuesToNotBeNull,
    ExpectColumnValuesToBeInSet,
    ExpectColumnValueLengthsToBeBetween,
)
from pyspark.sql import SparkSession


TABLE_NAMES = {
    "raw.raw_landing.patients": "patients",
    "raw.raw_landing.encounters": "encounters",
    "raw.raw_landing.conditions": "conditions",
    "raw.raw_landing.observations": "observations",
    "raw.raw_landing.procedures": "procedures",
}


def init_spark():
    return (
        SparkSession.builder.appName("GX-Validation")
        .config("spark.sql.catalog.raw", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.raw.type", "rest")
        .config("spark.sql.catalog.raw.uri", "http://iceberg-rest:8181")
        .config("spark.sql.catalog.raw.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.raw.s3.endpoint", "http://minio:9000")
        .config("spark.sql.catalog.raw.s3.path-style-access", "true")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate()
    )


def build_suite(table_name):
    suite = ExpectationSuite(name=table_name)

    if table_name == "patients":
        suite.add_expectation(ExpectColumnValuesToNotBeNull(column="patient_id"))
        suite.add_expectation(ExpectColumnValuesToNotBeNull(column="birth_date"))
        suite.add_expectation(
            ExpectColumnValuesToBeInSet(
                column="gender", value_set=["male", "female", "other", "unknown"]
            )
        )
        suite.add_expectation(
            ExpectColumnValueLengthsToBeBetween(column="patient_id", min_value=36, max_value=36)
        )

    elif table_name == "encounters":
        suite.add_expectation(ExpectColumnValuesToNotBeNull(column="encounter_id"))
        suite.add_expectation(ExpectColumnValuesToNotBeNull(column="patient_id"))
        suite.add_expectation(ExpectColumnValuesToNotBeNull(column="period_start"))

    elif table_name == "conditions":
        suite.add_expectation(ExpectColumnValuesToNotBeNull(column="condition_id"))
        suite.add_expectation(ExpectColumnValuesToNotBeNull(column="patient_id"))
        suite.add_expectation(ExpectColumnValuesToNotBeNull(column="condition_text"))

    elif table_name == "observations":
        suite.add_expectation(ExpectColumnValuesToNotBeNull(column="observation_id"))
        suite.add_expectation(ExpectColumnValuesToNotBeNull(column="patient_id"))
        suite.add_expectation(ExpectColumnValuesToNotBeNull(column="observation_text"))

    elif table_name == "procedures":
        suite.add_expectation(ExpectColumnValuesToNotBeNull(column="procedure_id"))
        suite.add_expectation(ExpectColumnValuesToNotBeNull(column="patient_id"))
        suite.add_expectation(ExpectColumnValuesToNotBeNull(column="procedure_text"))

    return suite


def validate_table(context, spark, short_name, df):
    ds_name = f"{short_name}_ds"
    asset_name = f"{short_name}_asset"
    try:
        context.data_sources.delete(ds_name)
    except Exception:
        pass

    ds = context.data_sources.add_spark(name=ds_name)
    asset = ds.add_dataframe_asset(name=asset_name)
    batch_request = asset.build_batch_request(options={"dataframe": df})

    suite = build_suite(short_name)
    try:
        context.suites.delete(short_name)
    except Exception:
        pass
    context.suites.add(suite)

    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=short_name,
    )
    result = validator.validate()
    return result


def main():
    spark = init_spark()
    context = gx.get_context(mode="ephemeral")

    all_passed = True
    results_summary = []

    for full_name, short_name in TABLE_NAMES.items():
        print(f"\n{'='*50}")
        print(f"Validating {short_name} ({full_name})")
        print(f"{'='*50}")

        df = spark.table(full_name)
        row_count = df.count()
        print(f"  Rows: {row_count}")

        result = validate_table(context, spark, short_name, df)

        stats = result.statistics
        success = result.success
        all_passed = all_passed and success

        status = "PASSED" if success else "FAILED"
        print(f"  {status}")
        print(f"  Expectations: {stats['evaluated_expectations']} evaluated, "
              f"{stats['successful_expectations']} passed, "
              f"{stats['unsuccessful_expectations']} failed "
              f"({stats['success_percent']}%)")

        if not success:
            for r in result.results:
                if not r.success:
                    print(f"    FAILED: {r.expectation_config.type} on column "
                          f"{r.expectation_config.kwargs.get('column', '?')}")

        results_summary.append((short_name, status, row_count))

    print(f"\n{'='*50}")
    print("SUMMARY")
    print(f"{'='*50}")
    for name, status, count in results_summary:
        print(f"  {name:15s} {status:6s} ({count} rows)")

    spark.stop()

    if not all_passed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
