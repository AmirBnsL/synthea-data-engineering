import argparse
import sys

import great_expectations as gx
from pyspark.sql import functions as F

from utils import (
    init_spark, validate_table, get_current_snapshot_id, rollback_to_snapshot,
    CATALOG, NAMESPACE,
)


NAME_SCHEMA = "array<struct<use:string,family:string,given:array<string>,prefix:array<string>>>"
ADDRESS_SCHEMA = "array<struct<city:string,state:string,postalCode:string>>"
CODE_SCHEMA = "struct<coding:array<struct<code:string,system:string>>,text:string>"


def parse_json_str(column, schema):
    return F.from_json(F.col(column).cast("string"), schema)


def extract_and_flatten(spark, s3_path):
    bundles = spark.read.option("multiLine", "true").json(f"{s3_path}/*.json")
    entries = bundles.select(F.explode(F.col("entry")).alias("entry"))
    resources = entries.select(F.col("entry.resource").alias("resource"))
    resources.cache()
    return resources


def flatten_patients(df):
    parsed = df.withColumn("name", parse_json_str("resource.name", NAME_SCHEMA)) \
               .withColumn("address", parse_json_str("resource.address", ADDRESS_SCHEMA))
    return parsed.select(
        F.col("resource.id").alias("patient_id"),
        F.to_date(F.col("resource.birthDate")).alias("birth_date"),
        F.to_date(F.col("resource.deceasedDateTime")).alias("death_date"),
        F.when(F.size(F.col("name")) > 0,
               F.when(F.size(F.col("name").getItem(0).getField("given")) > 0,
                       F.col("name").getItem(0).getField("given").getItem(0))
               ).alias("first_name"),
        F.when(F.size(F.col("name")) > 0,
               F.col("name").getItem(0).getField("family")).alias("last_name"),
        F.col("resource.gender").alias("gender"),
        F.when(F.size(F.col("address")) > 0,
               F.col("address").getItem(0).getField("city")).alias("address_city"),
        F.when(F.size(F.col("address")) > 0,
               F.col("address").getItem(0).getField("state")).alias("address_state"),
        F.when(F.size(F.col("address")) > 0,
               F.col("address").getItem(0).getField("postalCode")).alias("address_zip"),
        F.col("resource.maritalStatus.text").alias("marital_status"),
        F.col("resource.identifier").getItem(0).getField("value").alias("mrn"),
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
    parsed = df.withColumn("code", parse_json_str("resource.code", CODE_SCHEMA))
    return parsed.select(
        F.col("resource.id").alias("condition_id"),
        F.regexp_extract(F.col("resource.subject.reference"), r"urn:uuid:(.*)", 1).alias("patient_id"),
        F.regexp_extract(F.col("resource.encounter.reference"), r"urn:uuid:(.*)", 1).alias("encounter_id"),
        F.col("resource.clinicalStatus.coding").getItem(0).getField("code").alias("clinical_status"),
        F.col("code.text").alias("condition_text"),
        F.input_file_name().alias("file_source"),
    )


def flatten_observations(df):
    parsed = df.withColumn("code", parse_json_str("resource.code", CODE_SCHEMA))
    value_col = F.coalesce(
        F.col("resource.valueQuantity.value"),
        F.col("resource.valueCodeableConcept.coding").getItem(0).getField("code"),
    )
    return parsed.select(
        F.col("resource.id").alias("observation_id"),
        F.regexp_extract(F.col("resource.subject.reference"), r"urn:uuid:(.*)", 1).alias("patient_id"),
        F.regexp_extract(F.col("resource.encounter.reference"), r"urn:uuid:(.*)", 1).alias("encounter_id"),
        F.col("code.text").alias("observation_text"),
        value_col.alias("value"),
        F.col("resource.valueQuantity.unit").alias("value_unit"),
        F.to_timestamp(F.col("resource.effectiveDateTime")).alias("effective_date"),
        F.input_file_name().alias("file_source"),
    )


def flatten_procedures(df):
    parsed = df.withColumn("code", parse_json_str("resource.code", CODE_SCHEMA))
    return parsed.select(
        F.col("resource.id").alias("procedure_id"),
        F.regexp_extract(F.col("resource.subject.reference"), r"urn:uuid:(.*)", 1).alias("patient_id"),
        F.col("code.text").alias("procedure_text"),
        F.to_timestamp(F.col("resource.performedPeriod.start")).alias("procedure_date"),
        F.input_file_name().alias("file_source"),
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Execution date (YYYY-MM-DD)")
    args = parser.parse_args()

    spark = init_spark()

    s3_path = f"s3a://raw-landing/landing/{args.date}"
    print(f"Reading FHIR data from: {s3_path}")

    resources = extract_and_flatten(spark, s3_path)
    context = gx.get_context(mode="ephemeral")

    resource_mapping = [
        ("Patient", flatten_patients, "patients"),
        ("Encounter", flatten_encounters, "encounters"),
        ("Condition", flatten_conditions, "conditions"),
        ("Observation", flatten_observations, "observations"),
        ("Procedure", flatten_procedures, "procedures"),
    ]

    for resource_type, flatten_fn, table_name in resource_mapping:
        print(f"\nProcessing {resource_type} -> {table_name}")
        filtered = resources.filter(F.col("resource.resourceType") == resource_type)
        flat = flatten_fn(filtered)
        full_name = f"{CATALOG}.{NAMESPACE}.{table_name}"

        sample = flat.head(1)
        if not sample:
            print(f"  Skipping (no records)")
            continue
        print(f"  Records: inferring from write...")

        snapshot_before = get_current_snapshot_id(spark, table_name)
        print(f"  Snapshot before: {snapshot_before}")

        flat.writeTo(full_name).append()
        print(f"  Appended to {table_name}")

        result = validate_table(context, spark, table_name, flat)

        if result.success:
            print(f"  GX: PASSED")
        else:
            print(f"  GX: FAILED — rolling back")
            for r in result.results:
                if not r.success:
                    print(f"    Failed: {r.expectation_config.type} on "
                          f"{r.expectation_config.kwargs.get('column', '?')}")
            if snapshot_before is not None:
                rollback_to_snapshot(spark, table_name, snapshot_before)
            else:
                print(f"  No prior snapshot — removing appended data")
                spark.sql(
                    f"DELETE FROM {full_name} WHERE file_source LIKE '%{args.date}%'"
                )
            resources.unpersist()
            spark.stop()
            sys.exit(1)

    resources.unpersist()
    print("\nAll tables ingested and validated successfully.")
    spark.stop()


if __name__ == "__main__":
    main()
