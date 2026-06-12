import sys

from utils import init_spark, CATALOG, NAMESPACE


def init_tables(spark):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG}.{NAMESPACE}")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.{NAMESPACE}.patients (
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

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.{NAMESPACE}.encounters (
            encounter_id  STRING,
            patient_id    STRING,
            status        STRING,
            class_code    STRING,
            class_display STRING,
            period_start  TIMESTAMP,
            period_end    TIMESTAMP,
            file_source   STRING
        ) USING iceberg PARTITIONED BY (months(period_start))
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.{NAMESPACE}.conditions (
            condition_id      STRING,
            patient_id        STRING,
            encounter_id      STRING,
            clinical_status   STRING,
            condition_text    STRING,
            file_source       STRING
        ) USING iceberg
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.{NAMESPACE}.observations (
            observation_id   STRING,
            patient_id       STRING,
            encounter_id     STRING,
            observation_text STRING,
            value            STRING,
            value_unit       STRING,
            effective_date   TIMESTAMP,
            file_source      STRING
        ) USING iceberg PARTITIONED BY (months(effective_date))
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.{NAMESPACE}.procedures (
            procedure_id   STRING,
            patient_id     STRING,
            procedure_text STRING,
            procedure_date TIMESTAMP,
            file_source    STRING
        ) USING iceberg PARTITIONED BY (months(procedure_date))
    """)

    print("Tables initialized successfully.")


def main():
    spark = init_spark(app_name="Init-Iceberg-Tables")
    init_tables(spark)
    spark.stop()


if __name__ == "__main__":
    main()
