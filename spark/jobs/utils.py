from pyspark.sql import SparkSession
import great_expectations as gx
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.expectations import (
    ExpectColumnValuesToNotBeNull,
    ExpectColumnValuesToBeInSet,
    ExpectColumnValueLengthsToBeBetween,
)

CATALOG = "raw"
NAMESPACE = "raw_landing"
TABLE_NAMES = {
    f"{CATALOG}.{NAMESPACE}.patients": "patients",
    f"{CATALOG}.{NAMESPACE}.encounters": "encounters",
    f"{CATALOG}.{NAMESPACE}.conditions": "conditions",
    f"{CATALOG}.{NAMESPACE}.observations": "observations",
    f"{CATALOG}.{NAMESPACE}.procedures": "procedures",
}


def init_spark(app_name="FHIR-to-Iceberg"):
    return SparkSession.builder.appName(app_name).getOrCreate()


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
    return validator.validate()


def get_current_snapshot_id(spark, table_name):
    full_name = f"{CATALOG}.{NAMESPACE}.{table_name}"
    try:
        row = spark.sql(
            f"SELECT snapshot_id FROM {full_name}.snapshots ORDER BY committed_at DESC LIMIT 1"
        ).first()
        return row[0] if row else None
    except Exception:
        return None


def rollback_to_snapshot(spark, table_name, snapshot_id):
    full_name = f"{CATALOG}.{NAMESPACE}.{table_name}"
    spark.sql(
        f"CALL {CATALOG}.system.rollback_to_snapshot('{full_name}', {snapshot_id})"
    )
    print(f"  Rolled back {table_name} to snapshot {snapshot_id}")
