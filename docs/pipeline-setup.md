# Pipeline Setup Guide (Phase 1.1–1.6)

## Architecture

```
┌──────────────┐    ┌──────────────┐    ┌──────────────────┐
│   Synthea    │───▶│    MinIO     │───▶│  Spark + Iceberg │
│  (Java JAR)  │    │  (S3-compat) │    │  (PySpark job)   │
│  -p 5 pats   │    │  raw-landing │    │  fhir_to_iceberg │
│  ONC/PCOR    │    │   bucket     │    │  + GX validation │
└──────────────┘    └──────────────┘    └────────┬─────────┘
                                                 │
                    ┌────────────────────────────┘
                    ▼
         ┌────────────────────┐
         │  Iceberg REST Cat  │
         │  raw.raw_landing   │
         │  5 tables          │
         └────────────────────┘

         ┌────────────────────┐
         │  Marquez UI        │
         │  (localhost:3000)  │
         │  Lineage viz       │
         └────────────────────┘
```

## Services

| Service | Container | Image | Ports |
|---------|-----------|-------|-------|
| Iceberg REST Catalog | `iceberg-rest` | `apache/iceberg-rest-fixture` | 8181 |
| MinIO | `minio` | `minio/minio` | 9000, 9001 |
| Spark + Iceberg | `spark-iceberg` | Custom (`spark/Dockerfile`) | 8888, 8080, 10000-10001 |
| MinIO Client | `mc` | `minio/mc` | - |
| Postgres (Airflow) | `postgres` | `postgres:latest` | 5432 |
| Postgres (Marquez) | `postgres-marquez` | `postgres:14` | 5433 |
| Marquez API | `marquez` | `marquezproject/marquez:0.50.0` | 5000 |
| Marquez Web UI | `marquez-web` | `marquezproject/marquez-web:latest` | 3000 |
| Airflow Init | `airflow-init` | Custom (`Dockerfile.airflow`) | - |
| Airflow API Server | `airflow-webserver` | Custom (`Dockerfile.airflow`) | 8089 |
| Airflow Scheduler | `airflow-scheduler` | Custom (`Dockerfile.airflow`) | - |
| Airflow DAG Processor | `airflow-dag-processor` | Custom (`Dockerfile.airflow`) | - |

## Iceberg Tables (raw.raw_landing)

| Table | Partitioning | Key Columns |
|-------|-------------|-------------|
| `patients` | `months(birth_date)` | patient_id, gender, birth_date |
| `encounters` | `days(period_start)` | encounter_id, patient_id, class_code |
| `conditions` | none | condition_id, patient_id, condition_text |
| `observations` | `days(effective_date)` | observation_id, patient_id, value |
| `procedures` | `days(procedure_date)` | procedure_id, patient_id, procedure_text |

## Great Expectations Validation

All 5 tables validated with 3–4 expectations each:
- **patients**: patient_id not null, birth_date not null, gender in [male,female,other,unknown], patient_id length = 36
- **encounters**: encounter_id not null, patient_id not null, period_start not null
- **conditions**: condition_id not null, patient_id not null, condition_text not null
- **observations**: observation_id not null, patient_id not null, observation_text not null
- **procedures**: procedure_id not null, patient_id not null, procedure_text not null

## DAG: `synthea_to_minio`

```
generate_fhir  →  upload_fhir_to_minio  →  fhir_to_iceberg
(-p 5 pats)     (S3Hook, raw-landing)    (docker exec spark-submit)
```

## Pipeline Commands

### Build custom images
```bash
docker compose build airflow-webserver
```

### Start all services
```bash
docker compose up -d
```

### Run pipeline manually (for testing)
```bash
# Step 1: Generate FHIR data (passes ONC/PCOR modules embedded in JAR)
docker exec airflow-scheduler java -jar /opt/airflow/bin/synthea-with-dependencies.jar \
  -p 5 --exporter.baseDirectory=/opt/airflow/data/output

# Step 2: Upload FHIR files to MinIO
docker exec airflow-scheduler python3 -c "
import os, boto3
s3 = boto3.client('s3', endpoint_url='http://minio:9000',
  aws_access_key_id='admin', aws_secret_access_key='password')
for f in os.listdir('/opt/airflow/data/output/fhir'):
  if f.endswith('.json'):
    s3.upload_file(f'/opt/airflow/data/output/fhir/{f}', 'raw-landing', f'landing/{f}')
"

# Step 3: Ingest to Iceberg + GX Validate
docker exec spark-iceberg spark-submit --driver-memory 4g \
  /home/iceberg/spark/jobs/fhir_to_iceberg.py
```

### Trigger DAG run (direct database insert)
```bash
docker exec postgres psql -U airflow -d airflow -c "
INSERT INTO dag_run (dag_id, run_id, logical_date, data_interval_start,
  data_interval_end, run_after, queued_at, state, run_type)
SELECT 'synthea_to_minio',
  'manual_' || to_char(now(), 'YYYYMMDD_HH24MISS'),
  now(), now(), now(), now(), now(), 'queued', 'manual'
WHERE NOT EXISTS (
  SELECT 1 FROM dag_run WHERE dag_id = 'synthea_to_minio'
  AND state IN ('queued', 'running')
);"
```

### Query Lineage Graph (Marquez)

```bash
# Via GraphQL — shows lineage graph with jobs, datasets, and edges
curl -s "http://localhost:5000/api/v1-beta/graphql" -X POST \
  -H "Content-Type: application/json" \
  -d '{"query":"{ lineageFromJob(namespace: \"synthea\", name: \"fhir_to_iceberg\", depth: 1) { graph { __typename ... on JobLineageEntry { name namespace type inEdges { name namespace type } outEdges { name namespace type } } ... on DatasetLineageEntry { name namespace type inEdges { name namespace type } outEdges { name namespace type } } } } }"}'
```

Or open Marquez Web UI at `http://localhost:3000`.

### Verify Iceberg data
```bash
docker exec spark-iceberg spark-submit --driver-memory 2g \
  --conf spark.sql.catalog.raw.type=rest \
  --conf spark.sql.catalog.raw.uri=http://iceberg-rest:8181 \
  --conf spark.sql.catalog.raw.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.catalog.raw.s3.endpoint=http://minio:9000 \
  --conf spark.sql.catalog.raw.s3.path-style-access=true \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  -e "
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
for t in ['patients','encounters','conditions','observations','procedures']:
    print(f'{t}: {spark.table(f\"raw.raw_landing.{t}\").count()}')
"
```

## Key Configuration Details

### Iceberg Catalog
- Type: REST (not Hive)
- I/O: S3FileIO (avoids 280MB hadoop-aws dependency)
- Endpoint: `http://minio:9000` with path-style access
- Namespace: `raw.raw_landing`

### Airflow OpenLineage
- Provider: `apache-airflow-providers-openlineage==2.18.0`
- Env vars: `OPENLINEAGE_URL=http://marquez:5000`, `OPENLINEAGE_NAMESPACE=synthea`
- Marquez DB: Isolated `postgres:14` on port 5433

### Marquez
- API server: `marquezproject/marquez:0.50.0` (port 5000, admin port 5001)
- Web UI: `marquezproject/marquez-web:latest` (port 3000)
- Web required env vars: `MARQUEZ_HOST=marquez`, `MARQUEZ_PORT=5000`, `WEB_PORT=3000`
- Lineage API: `POST /api/v1/lineage` (OpenLineage RunEvent submission)
- Lineage query: GraphQL at `/api/v1-beta/graphql` with `lineageFromJob(namespace, name, depth)`

### FHIR Data Format
- All `subject.reference` values use `urn:uuid:<uuid>` (not `Patient/<id>`)
- Regex for patient_id extraction: `r"urn:uuid:(.*)"`

## Known Issues

| Issue | Impact | Workaround |
|-------|--------|------------|
| Airflow 3 CLI missing `psycopg2` | Cannot run `airflow dags trigger` from CLI | Use direct DB insert (see commands above) |
| Simple Auth Manager generates random password | Admin password not set by env var | Check startup logs for generated password |
| LocalExecutor task auth failure with OpenLineage | Manual DAG runs via API fail | Run pipeline manually or wait for scheduled runs |
| Spark OOM on partitioned writes | Encounters table with 6+ patients | Use `--driver-memory 4g` |
| Marquez 0.50.0 has no REST lineage GET | Lineage graph only available via GraphQL | Use `/api/v1-beta/graphql` with `lineageFromJob` query |
| Marquez GraphQL edge direction reversed | `inEdges` shows outputs, `outEdges` shows inputs | Read edges semantically from each node's perspective |

## ONC/PCOR Modules

Embedded in `synthea-with-dependencies.jar` — no download needed:

| Module | Condition |
|--------|-----------|
| `acute_myeloid_leukemia` | AML diagnoses, chemo, BMT |
| `diabetic_retinopathy_treatment` | Diabetes with retinopathy |
| `congestive_heart_failure` | CHF with exacerbations |
| `myocardial_infarction` | Heart attacks with post-MI care |
| `sepsis` | Sepsis with ICU encounters |
| `cerebral_palsy` | CP with mobility devices |
