import json
import os
import glob as glob_mod
import sys

from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import StringType

import great_expectations as gx
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.expectations import (
    ExpectColumnValuesToNotBeNull,
    ExpectColumnValuesToBeInSet,
    ExpectColumnValueLengthsToBeBetween,
)


def init_spark():
    return (
        SparkSession.builder.appName("FHIR-to-Iceberg")
        .config("spark.sql.catalog.raw", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.raw.type", "rest")
        .config("spark.sql.catalog.raw.uri", "http://iceberg-rest:8181")
        .config("spark.sql.catalog.raw.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.raw.s3.endpoint", "http://minio:9000")
        .config("spark.sql.catalog.raw.s3.path-style-access", "true")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def get_patient_files(path: str) -> list:
    all_files = glob_mod.glob(os.path.join(path, "*.json"))
    return sorted(f for f in all_files
                  if not os.path.basename(f).startswith("hospital")
                  and not os.path.basename(f).startswith("practitioner"))


def drop_tables(spark):
    for table in ["patients", "encounters", "conditions", "observations", "procedures"]:
        spark.sql(f"DROP TABLE IF EXISTS raw.raw_landing.{table}")


def create_tables(spark):
    spark.sql("CREATE DATABASE IF NOT EXISTS raw.raw_landing")

    spark.sql("""
        CREATE TABLE raw.raw_landing.patients (
            patient_id     STRING,
            birth_date     DATE,
            death_date     DATE,
            first_name     STRING,
            last_name      STRING,
            gender         STRING,
            address_city   STRING,
            address_state  STRING,
            address_zip    STRING,
            marital_status STRING,
            mrn            STRING,
            file_source    STRING
        ) USING iceberg PARTITIONED BY (months(birth_date))
    """)

    spark.sql("""
        CREATE TABLE raw.raw_landing.encounters (
            encounter_id  STRING,
            patient_id    STRING,
            status        STRING,
            class_code    STRING,
            class_display STRING,
            period_start  TIMESTAMP,
            period_end    TIMESTAMP,
            file_source   STRING
        ) USING iceberg PARTITIONED BY (days(period_start))
    """)

    spark.sql("""
        CREATE TABLE raw.raw_landing.conditions (
            condition_id      STRING,
            patient_id        STRING,
            encounter_id      STRING,
            clinical_status   STRING,
            condition_text    STRING,
            file_source       STRING
        ) USING iceberg
    """)

    spark.sql("""
        CREATE TABLE raw.raw_landing.observations (
            observation_id   STRING,
            patient_id       STRING,
            encounter_id     STRING,
            observation_text STRING,
            value            STRING,
            value_unit       STRING,
            effective_date   TIMESTAMP,
            file_source      STRING
        ) USING iceberg PARTITIONED BY (days(effective_date))
    """)

    spark.sql("""
        CREATE TABLE raw.raw_landing.procedures (
            procedure_id   STRING,
            patient_id     STRING,
            procedure_text STRING,
            procedure_date TIMESTAMP,
            file_source    STRING
        ) USING iceberg PARTITIONED BY (days(procedure_date))
    """)


def extract_and_flatten(spark, path, resource_type, flatten_fn, table_name):
    files = get_patient_files(path)
    bundles = spark.read.option("multiLine", "true").json(files)
    entries = bundles.select(F.explode(F.col("entry")).alias("entry"))
    resources = entries.select(F.col("entry.resource").alias("resource"))
    filtered = resources.filter(F.col("resource.resourceType") == resource_type)
    flat = flatten_fn(filtered)
    flat.writeTo(f"raw.raw_landing.{table_name}").append()


def flatten_patients(df):
    return df.select(
        F.col("resource.id").alias("patient_id"),
        F.to_date(F.col("resource.birthDate")).alias("birth_date"),
        F.lit(None).cast("date").alias("death_date"),
        F.when(F.size(F.col("resource.name")) > 0,
               F.when(F.size(F.col("resource.name").getItem(0).getField("given")) > 0,
                      F.col("resource.name").getItem(0).getField("given").getItem(0))
               ).alias("first_name"),
        F.when(F.size(F.col("resource.name")) > 0,
               F.col("resource.name").getItem(0).getField("family")).alias("last_name"),
        F.col("resource.gender").alias("gender"),
        F.when(F.size(F.col("resource.address")) > 0,
               F.col("resource.address").getItem(0).getField("city")).alias("address_city"),
        F.when(F.size(F.col("resource.address")) > 0,
               F.col("resource.address").getItem(0).getField("state")).alias("address_state"),
        F.when(F.size(F.col("resource.address")) > 0,
               F.col("resource.address").getItem(0).getField("postalCode")).alias("address_zip"),
        F.col("resource.maritalStatus.text").alias("marital_status"),
        F.when(F.size(F.col("resource.identifier")) > 0,
               F.col("resource.identifier").getItem(0).getField("value")).alias("mrn"),
        F.input_file_name().alias("file_source"),
    )


def flatten_encounters(df):
    return df.select(
        F.col("resource.id").alias("encounter_id"),
        F.regexp_extract(F.col("resource.subject.reference"), r"urn:uuid:(.*)", 1).alias("patient_id"),
        F.col("resource.status").alias("status"),
        F.col("resource.class.code").alias("class_code"),
        F.lit(None).cast("string").alias("class_display"),
        F.to_timestamp(F.col("resource.period.start")).alias("period_start"),
        F.to_timestamp(F.col("resource.period.end")).alias("period_end"),
        F.input_file_name().alias("file_source"),
    )


def flatten_conditions(df):
    return df.select(
        F.col("resource.id").alias("condition_id"),
        F.regexp_extract(F.col("resource.subject.reference"), r"urn:uuid:(.*)", 1).alias("patient_id"),
        F.regexp_extract(F.col("resource.encounter.reference"), r"urn:uuid:(.*)", 1).alias("encounter_id"),
        F.when(F.size(F.col("resource.clinicalStatus.coding")) > 0,
               F.col("resource.clinicalStatus.coding").getItem(0).getField("code")).alias("clinical_status"),
        F.col("resource.code.text").alias("condition_text"),
        F.input_file_name().alias("file_source"),
    )


def flatten_observations(df):
    value_col = F.coalesce(
        F.col("resource.valueQuantity.value"),
        F.col("resource.valueCodeableConcept.coding").getItem(0).getField("code"),
    )
    return df.select(
        F.col("resource.id").alias("observation_id"),
        F.regexp_extract(F.col("resource.subject.reference"), r"urn:uuid:(.*)", 1).alias("patient_id"),
        F.regexp_extract(F.col("resource.encounter.reference"), r"urn:uuid:(.*)", 1).alias("encounter_id"),
        F.col("resource.code.text").alias("observation_text"),
        value_col.alias("value"),
        F.col("resource.valueQuantity.unit").alias("value_unit"),
        F.to_timestamp(F.col("resource.effectiveDateTime")).alias("effective_date"),
        F.input_file_name().alias("file_source"),
    )


def flatten_procedures(df):
    return df.select(
        F.col("resource.id").alias("procedure_id"),
        F.regexp_extract(F.col("resource.subject.reference"), r"urn:uuid:(.*)", 1).alias("patient_id"),
        F.col("resource.code.text").alias("procedure_text"),
        F.to_timestamp(F.col("resource.performedPeriod.start")).alias("procedure_date"),
        F.input_file_name().alias("file_source"),
    )


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


def main():
    spark = init_spark()

    # --- Full refresh ---
    drop_tables(spark)
    create_tables(spark)

    # --- Ingest FHIR data ---
    path = "/home/iceberg/data/output/fhir/"
    for resource_type, flatten_fn, table_name in [
        ("Patient", flatten_patients, "patients"),
        ("Encounter", flatten_encounters, "encounters"),
        ("Condition", flatten_conditions, "conditions"),
        ("Observation", flatten_observations, "observations"),
        ("Procedure", flatten_procedures, "procedures"),
    ]:
        extract_and_flatten(spark, path, resource_type, flatten_fn, table_name)

    # --- Validate with Great Expectations ---
    context = gx.get_context(mode="ephemeral")
    table_names = {
        "raw.raw_landing.patients": "patients",
        "raw.raw_landing.encounters": "encounters",
        "raw.raw_landing.conditions": "conditions",
        "raw.raw_landing.observations": "observations",
        "raw.raw_landing.procedures": "procedures",
    }

    all_passed = True
    for full_name, short_name in table_names.items():
        df = spark.table(full_name)
        row_count = df.count()
        result = validate_table(context, spark, short_name, df)
        success = result.success
        all_passed = all_passed and success

        status = "PASSED" if success else "FAILED"
        print(f"[GX] {short_name:15s} {status:6s} ({row_count} rows)")

        if not success:
            for r in result.results:
                if not r.success:
                    print(f"      FAILED: {r.expectation_config.type} on column "
                          f"{r.expectation_config.kwargs.get('column', '?')}")

    if not all_passed:
        print("[GX] Some validations FAILED. Exiting with code 1.")
        spark.stop()
        sys.exit(1)

    print("[GX] All validations PASSED.")
    spark.stop()


if __name__ == "__main__":
    main()
