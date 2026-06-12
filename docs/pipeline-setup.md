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
```

## Services

| Service | Container | Image | Ports |
|---------|-----------|-------|-------|
| Iceberg REST Catalog | `iceberg-rest` | `apache/iceberg-rest-fixture` | 8181 |
| MinIO | `minio` | `minio/minio` | 9000, 9001 |
| Spark + Iceberg | `spark-iceberg` | Custom (`spark/Dockerfile`) | 8888, 8080, 10000-10001 |
| MinIO Client | `mc` | `minio/mc` | - |
| Postgres | `postgres` | `postgres:latest` | 5432 |
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

### Run pipeline via Airflow tasks (recommended)
```bash
# Run each task sequentially (avoids Airflow 3 LocalExecutor fork issue)
docker exec airflow-scheduler airflow tasks test synthea_to_minio generate_fhir 2026-06-12
docker exec airflow-scheduler airflow tasks test synthea_to_minio upload_fhir_to_minio 2026-06-12
docker exec airflow-scheduler airflow tasks test synthea_to_minio fhir_to_iceberg 2026-06-12
```

### Run pipeline manually
```bash
# Step 1: Generate FHIR data
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
docker exec spark-iceberg spark-submit --driver-memory 6g \
  --conf spark.driver.extraJavaOptions="-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35" \
  /home/iceberg/spark/jobs/fhir_to_iceberg.py
```

### Trigger DAG via CLI
```bash
docker exec airflow-scheduler airflow dags trigger synthea_to_minio
```

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

### Docker-outside-of-Docker (DooD) Pattern

The `fhir_to_iceberg` task uses a `BashOperator` that calls `docker exec spark-iceberg spark-submit ...` from inside the `airflow-scheduler` container. Here's how it works:

**Mechanism**: `BashOperator` runs bash commands as a subprocess inside the Airflow worker that picked up the task. In our LocalExecutor setup, that worker lives in the `airflow-scheduler` container. The `docker exec` command reaches the Docker daemon on the host through the mounted socket (`/var/run/docker.sock`), which then executes the command inside the `spark-iceberg` container. stdout/stderr flows back through the daemon → socket → docker CLI → Airflow task log.

**Why**: Spark + Iceberg + GX have heavy Java/Python dependencies built into a separate image (`spark/Dockerfile`). Running the PySpark job from a single monolithic Airflow image would bloat it unnecessarily. The DooD pattern keeps images single-responsibility: Airflow orchestrates, Spark computes — no Kubernetes or Celery needed just to run one cross-container job.

**Socket mount** in `docker-compose.yml`:
```yaml
volumes:
  - /var/run/docker.sock:/var/run/docker.sock
```

This gives the `airflow-scheduler` container the same Docker API access as the host. It's the standard pattern used by CI runners (GitLab, Jenkins) and Airflow's own `DockerOperator`.

**Security note**: Mounting the Docker socket is equivalent to granting root access on the host. Acceptable for local dev; for production, prefer Airflow's `DockerOperator` (which handles the exec natively without requiring a socket mount) or `KubernetesPodOperator`.

### Spark Memory
- Driver/Executor: 6g each with G1GC garbage collector
- Config: `spark.driver.extraJavaOptions=-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35`

### Airflow JWT Authentication
- All services (webserver, scheduler, dag-processor) must share the same `AIRFLOW__API_AUTH__JWT_SECRET`
- Without this, the LocalExecutor worker processes cannot authenticate with the API server, causing `Signature verification failed` errors
- Also requires `AIRFLOW__CORE__SECRET_KEY` to be consistent across all services

### Airflow Login
- `admin:admin` works via a pre-populated password file mounted at `/opt/airflow/simple_auth_manager_passwords.json.generated`
- Airflow 3's Simple Auth Manager ignores the `password` field in the env var — passwords are always auto-generated
- File format: `{"admin": "admin"}` (JSON object mapping username → password)

### FHIR Data Format
- All `subject.reference` values use `urn:uuid:<uuid>` (not `Patient/<id>`)
- Regex for patient_id extraction: `r"urn:uuid:(.*)"`

## Known Issues

| Issue | Impact | Workaround |
|-------|--------|------------|
| mc container resets warehouse on restart | Iceberg tables lose metadata | Re-run pipeline after any `docker compose restart` |
| Spark OOM on encounters ingestion | 6+ patients with default settings | Use `--driver-memory 6g` + G1GC (already configured in DAG) |
| Env var changes require `docker compose up -d` | `docker compose restart` doesn't apply new env vars | Use `docker compose up -d` to recreate containers |

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
