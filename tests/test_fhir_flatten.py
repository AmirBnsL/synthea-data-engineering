from datetime import date
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

import sys
sys.path.insert(0, "/home/iceberg/spark/jobs")

from fhir_to_iceberg import (
    flatten_patients,
    flatten_encounters,
    flatten_conditions,
    flatten_observations,
    flatten_procedures,
)


def create_spark():
    return (
        SparkSession.builder.appName("test")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )


def make_resource(schema, data):
    """Create a DataFrame with resource as JSON strings for fields
    that Spark merges to STRING (name, address, code, etc.)."""
    return Row(resource=data)


PATIENT_NAME_JSON = (
    '[{"family":"Smith","given":["John","M"],"use":"official"}]'
)
PATIENT_ADDR_JSON = (
    '[{"city":"Boston","state":"MA","postalCode":"02108"}]'
)


def test_flatten_patients(spark):
    schema = StructType([
        StructField("resource", StructType([
            StructField("resourceType", StringType()),
            StructField("id", StringType()),
            StructField("birthDate", StringType()),
            StructField("deceasedDateTime", StringType()),
            StructField("gender", StringType()),
            StructField("maritalStatus", StructType([
                StructField("text", StringType()),
            ])),
            StructField("name", StringType()),
            StructField("address", StringType()),
            StructField("identifier", ArrayType(StructType([
                StructField("value", StringType()),
            ]))),
        ])),
    ])

    data = [(
        Row(
            resourceType="Patient",
            id="p123",
            birthDate="1985-03-15",
            deceasedDateTime=None,
            gender="male",
            maritalStatus=Row(text="married"),
            name=PATIENT_NAME_JSON,
            address=PATIENT_ADDR_JSON,
            identifier=[Row(value="MRN-001")],
        ),
    )]

    df = spark.createDataFrame(data, schema)
    result = flatten_patients(df)

    row = result.first()
    assert row.patient_id == "p123"
    assert row.birth_date == date(1985, 3, 15)
    assert row.death_date is None
    assert row.first_name == "John"
    assert row.last_name == "Smith"
    assert row.gender == "male"
    assert row.address_city == "Boston"
    assert row.address_state == "MA"
    assert row.address_zip == "02108"
    assert row.marital_status == "married"
    assert row.mrn == "MRN-001"
    assert row.file_source is not None


def test_flatten_patients_deceased(spark):
    schema = StructType([
        StructField("resource", StructType([
            StructField("resourceType", StringType()),
            StructField("id", StringType()),
            StructField("birthDate", StringType()),
            StructField("deceasedDateTime", StringType()),
            StructField("gender", StringType()),
            StructField("maritalStatus", StructType([
                StructField("text", StringType()),
            ])),
            StructField("name", StringType()),
            StructField("address", StringType()),
            StructField("identifier", ArrayType(StructType([
                StructField("value", StringType()),
            ]))),
        ])),
    ])

    data = [(
        Row(
            resourceType="Patient",
            id="p456",
            birthDate="1920-07-01",
            deceasedDateTime="2020-12-31",
            gender="female",
            maritalStatus=Row(text="widowed"),
            name=('[{"family":"Doe","given":["Jane"],"use":"official"}]'),
            address=('[{"city":"NYC","state":"NY","postalCode":"10001"}]'),
            identifier=[Row(value="MRN-002")],
        ),
    )]

    df = spark.createDataFrame(data, schema)
    result = flatten_patients(df)

    row = result.first()
    assert row.patient_id == "p456"
    assert row.death_date == date(2020, 12, 31), f"Expected 2020-12-31, got {row.death_date}"


def test_flatten_encounters(spark):
    schema = StructType([
        StructField("resource", StructType([
            StructField("resourceType", StringType()),
            StructField("id", StringType()),
            StructField("status", StringType()),
            StructField("class", StructType([
                StructField("code", StringType()),
            ])),
            StructField("subject", StructType([
                StructField("reference", StringType()),
            ])),
            StructField("period", StructType([
                StructField("start", StringType()),
                StructField("end", StringType()),
            ])),
        ])),
    ])

    data = [(
        Row(**{
            "resourceType": "Encounter",
            "id": "e001",
            "status": "finished",
            "class": Row(code="AMB"),
            "subject": Row(reference="urn:uuid:p123"),
            "period": Row(start="2026-01-15T10:00:00Z", end="2026-01-15T10:30:00Z"),
        }),
    )]

    df = spark.createDataFrame(data, schema)
    result = flatten_encounters(df)

    row = result.first()
    assert row.encounter_id == "e001"
    assert row.patient_id == "p123"
    assert row.status == "finished"
    assert row.class_code == "AMB"


def test_flatten_conditions(spark):
    schema = StructType([
        StructField("resource", StructType([
            StructField("resourceType", StringType()),
            StructField("id", StringType()),
            StructField("subject", StructType([
                StructField("reference", StringType()),
            ])),
            StructField("encounter", StructType([
                StructField("reference", StringType()),
            ])),
            StructField("clinicalStatus", StructType([
                StructField("coding", ArrayType(StructType([
                    StructField("code", StringType()),
                ]))),
            ])),
            StructField("code", StringType()),
        ])),
    ])

    data = [(
        Row(
            resourceType="Condition",
            id="c001",
            subject=Row(reference="urn:uuid:p123"),
            encounter=Row(reference="urn:uuid:e001"),
            clinicalStatus=Row(coding=[Row(code="active")]),
            code='{"text":"Diabetes Type 2","coding":[{"code":"44054006","system":"http://snomed.info/sct"}]}',
        ),
    )]

    df = spark.createDataFrame(data, schema)
    result = flatten_conditions(df)

    row = result.first()
    assert row.condition_id == "c001"
    assert row.patient_id == "p123"
    assert row.encounter_id == "e001"
    assert row.clinical_status == "active"
    assert row.condition_text == "Diabetes Type 2"


def test_flatten_observations(spark):
    schema = StructType([
        StructField("resource", StructType([
            StructField("resourceType", StringType()),
            StructField("id", StringType()),
            StructField("subject", StructType([
                StructField("reference", StringType()),
            ])),
            StructField("encounter", StructType([
                StructField("reference", StringType()),
            ])),
            StructField("code", StringType()),
            StructField("valueQuantity", StructType([
                StructField("value", StringType()),
                StructField("unit", StringType()),
            ])),
            StructField("valueCodeableConcept", StructType([
                StructField("coding", ArrayType(StructType([
                    StructField("code", StringType()),
                ]))),
            ])),
            StructField("effectiveDateTime", StringType()),
        ])),
    ])

    data = [(
        Row(
            resourceType="Observation",
            id="obs001",
            subject=Row(reference="urn:uuid:p123"),
            encounter=Row(reference="urn:uuid:e001"),
            code='{"text":"Blood Pressure"}',
            valueQuantity=Row(value="120", unit="mmHg"),
            valueCodeableConcept=Row(coding=[]),
            effectiveDateTime="2026-01-15T10:05:00Z",
        ),
    )]

    df = spark.createDataFrame(data, schema)
    result = flatten_observations(df)

    row = result.first()
    assert row.observation_id == "obs001"
    assert row.patient_id == "p123"
    assert row.encounter_id == "e001"
    assert row.observation_text == "Blood Pressure"
    assert row.value == "120"
    assert row.value_unit == "mmHg"
    assert row.effective_date is not None


def test_flatten_observations_codeable_concept(spark):
    schema = StructType([
        StructField("resource", StructType([
            StructField("resourceType", StringType()),
            StructField("id", StringType()),
            StructField("subject", StructType([
                StructField("reference", StringType()),
            ])),
            StructField("encounter", StructType([
                StructField("reference", StringType()),
            ])),
            StructField("code", StringType()),
            StructField("valueQuantity", StructType([
                StructField("value", StringType()),
                StructField("unit", StringType()),
            ])),
            StructField("valueCodeableConcept", StructType([
                StructField("coding", ArrayType(StructType([
                    StructField("code", StringType()),
                ]))),
            ])),
            StructField("effectiveDateTime", StringType()),
        ])),
    ])

    data = [(
        Row(
            resourceType="Observation",
            id="obs002",
            subject=Row(reference="urn:uuid:p123"),
            encounter=Row(reference="urn:uuid:e001"),
            code='{"text":"Smoking Status"}',
            valueQuantity=Row(value=None, unit=None),
            valueCodeableConcept=Row(coding=[Row(code="LA15920-4")]),
            effectiveDateTime="2026-01-15T10:05:00Z",
        ),
    )]

    df = spark.createDataFrame(data, schema)
    result = flatten_observations(df)

    row = result.first()
    assert row.observation_id == "obs002"
    assert row.value == "LA15920-4"


def test_flatten_procedures(spark):
    schema = StructType([
        StructField("resource", StructType([
            StructField("resourceType", StringType()),
            StructField("id", StringType()),
            StructField("subject", StructType([
                StructField("reference", StringType()),
            ])),
            StructField("code", StringType()),
            StructField("performedPeriod", StructType([
                StructField("start", StringType()),
            ])),
        ])),
    ])

    data = [(
        Row(
            resourceType="Procedure",
            id="pr001",
            subject=Row(reference="urn:uuid:p123"),
            code='{"text":"Appendectomy"}',
            performedPeriod=Row(start="2026-01-15T11:00:00Z"),
        ),
    )]

    df = spark.createDataFrame(data, schema)
    result = flatten_procedures(df)

    row = result.first()
    assert row.procedure_id == "pr001"
    assert row.patient_id == "p123"
    assert row.procedure_text == "Appendectomy"
    assert row.procedure_date is not None
