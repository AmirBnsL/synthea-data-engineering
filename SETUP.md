# Synthea + Iceberg Pipeline Setup

## Prerequisites

- Docker & Docker Compose
- 16 GB RAM recommended for Spark (10g driver + 10g executor)

## Quick Start

```bash
# Build and start all services
docker compose build spark-iceberg
docker compose up -d

# Verify services
docker ps

# Check Spark SQL works (no --conf flags needed)
docker exec spark-iceberg spark-sql -e "SHOW TABLES IN raw.raw_landing;" 2>/dev/null

# Trigger a full pipeline run
docker exec airflow-webserver airflow dags trigger synthea_to_minio
```

## What Each Container Does

| Service | Purpose |
|---------|---------|
| `spark-iceberg` | PySpark with Iceberg, reads FHIR from MinIO → writes to Iceberg tables |
| `iceberg-rest` | Iceberg REST catalog (metadata server) |
| `minio` | S3-compatible storage for FHIR bundles + Iceberg data files |
| `mc` | MinIO client — creates buckets on startup |
| `postgres` | Airflow metadata database |
| `airflow-*` | Airflow webserver, scheduler, dag-processor |

## Determinism: What Survives a Restart

After `docker compose down && docker compose up -d`:

| Component | Persists | Why |
|-----------|----------|-----|
| **Iceberg data** (Parquet files) | ✅ | MinIO has `./minio_data:/data` bind mount |
| **Iceberg tables** (catalog metadata) | ✅ | REST catalog persists to warehouse; `recover_tables.py` registra metadados existentes |
| **Airflow DB** (DAG runs, variables) | ✅ | PostgreSQL has `postgres_data` named volume |
| **FHIR bundles** in MinIO | ✅ | Under `raw-landing/` bucket |
| Spark containers | ❌ | Recreated fresh from image |

*On restart, the spark-iceberg container's custom entrypoint auto-runs:*
1. `recover_tables.py` — finds existing metadata files in MinIO and registers them
2. `init_tables.py` — creates the `raw_landing` namespace + any missing tables

## Key Files

| File | Purpose |
|------|---------|
| `docker-compose.yml` | All service definitions |
| `spark/conf/spark-defaults.conf` | Spark session defaults (Iceberg catalog, S3, memory) |
| `spark/jobs/utils.py` | `init_spark()`, catalog constants, validation helpers |
| `spark/jobs/fhir_to_iceberg.py` | Flattens FHIR bundles into Iceberg tables + GX validation |
| `spark/jobs/init_tables.py` | Creates tables with schemas |
| `spark/jobs/recover_tables.py` | Registers existing metadata from MinIO on restart |
| `spark/scripts/entrypoint.sh` | Custom container entrypoint (recovery → init → services) |
| `dags/synthea-to-minio.py` | Airflow DAG: generate FHIR → upload → fhir_to_iceberg |
| `spark/Dockerfile` | Builds `synthea-spark:latest` image |

## Customizing

### Change number of patients per run

```bash
docker exec airflow-webserver airflow variables set synthea_patient_count 500
```

Or set it in the Airflow UI → Admin → Variables.

### Add a new table

1. Add the schema to `init_tables.py` (use `PARTITIONED BY` only for small cardinality columns — avoids FanoutDataWriter OOM)
2. Add a flatten function in `fhir_to_iceberg.py`
3. Add it to `resource_mapping` list and `TABLE_NAMES` in `utils.py`
4. Add an expectation suite in `build_suite()` in `utils.py`
5. Rebuild: `docker compose build spark-iceberg && docker compose up -d`

### Run pipeline manually (without Airflow)

```bash
# 1. Generate FHIR
docker exec airflow-webserver java -jar /opt/airflow/bin/synthea-with-dependencies.jar \
  -p 10 --exporter.baseDirectory=/opt/airflow/data/output

# 2. Upload to MinIO
TODAY=$(date +%Y-%m-%d)
docker exec airflow-webserver python3 -c "
import os, boto3
s3 = boto3.client('s3', endpoint_url='http://minio:9000',
  aws_access_key_id='admin', aws_secret_access_key='password')
for f in os.listdir('/opt/airflow/data/output/fhir'):
    if f.endswith('.json'):
        s3.upload_file(f'/opt/airflow/data/output/fhir/{f}', 'raw-landing', f'landing/$TODAY/{f}')
"

# 3. Run Spark job
docker exec spark-iceberg spark-submit /home/iceberg/spark/jobs/fhir_to_iceberg.py \
  --date "$TODAY"
```

### Check data in Iceberg

```bash
docker exec spark-iceberg spark-sql -e "
SELECT 'patients', COUNT(*) FROM raw.raw_landing.patients UNION ALL
SELECT 'encounters', COUNT(*) FROM raw.raw_landing.encounters UNION ALL
SELECT 'conditions', COUNT(*) FROM raw.raw_landing.conditions UNION ALL
SELECT 'observations', COUNT(*) FROM raw.raw_landing.observations UNION ALL
SELECT 'procedures', COUNT(*) FROM raw.raw_landing.procedures;
" 2>/dev/null
```

### Filter DR patients

```sql
SELECT patient_id, condition_text
FROM raw.raw_landing.conditions
WHERE condition_text LIKE '%retinopathy%';
```

## Troubleshooting

### Container exits immediately after start

Check logs:
```bash
docker logs spark-iceberg
```
The most common cause: the custom entrypoint script has an error. Ensure `spark/scripts/entrypoint.sh` ends with `tail -f /dev/null` to keep the container alive.

### `OutOfMemoryError: Java heap space` during Iceberg writes

The FanoutDataWriter opens one Parquet writer per partition. Tables partitioned by `months(period_start)` can produce many partitions.

**Solution:** Remove partitioning from that table (already done for encounters, observations, procedures). See `init_tables.py`.

### spark-sql returns no output or "catalog does not exist"

Check that the REST catalog is running and the `spark-defaults.conf` points to `http://iceberg-rest:8181`:

```bash
docker logs iceberg-rest
```

### Data missing after restart (tables exist but empty)

The recovery script failed. Check:
```bash
docker logs spark-iceberg | grep -E "Registering|Recovered|Failed"
```

Common causes:
- Namespace wasn't created before registering tables (fixed — `recover_tables.py` now creates it)
- Metadata files were cleaned from MinIO (re-run the pipeline)

### Airflow DAG task `fhir_to_iceberg` fails

The DockerOperator launches a new container from `synthea-spark:latest`. If you changed the image, rebuild first:

```bash
docker compose build spark-iceberg
```

Then check the task logs in Airflow UI at `localhost:8089`.

## Architecture Overview

```
Synthea (Java)
  │  generates FHIR JSON bundles
  ▼
Airflow: upload_fhir_to_minio
  │  s3://raw-landing/landing/{date}/*.json
  ▼
Airflow: fhir_to_iceberg (DockerOperator → synthea-spark:latest)
  │  spark-submit fhir_to_iceberg.py --date {date}
  │
  ├──► Reads FHIR from MinIO (S3A)
  ├──► Flattens into 5 tables (Patient, Encounter, Condition, Observation, Procedure)
  ├──► Writes to Iceberg (raw.raw_landing.{table})
  ├──► Validates with Great Expectations
  └──► Rolls back on validation failure
```

## Files Changed for Determinism

| Date | File | Change |
|------|------|--------|
| 2026-06-14 | `docker-compose.yml` | Added `./minio_data:/data` volume to MinIO |
| 2026-06-14 | `spark/conf/spark-defaults.conf` | Full Iceberg + S3 + memory config |
| 2026-06-14 | `spark/jobs/init_tables.py` | Removed partitioning on encounters/observations/procedures |
| 2026-06-14 | `spark/jobs/recover_tables.py` | New — registers existing metadata from MinIO on restart |
| 2026-06-14 | `spark/scripts/entrypoint.sh` | New — runs recovery → init → Spark services |
| 2026-06-14 | `spark/Dockerfile` | Copies entrypoint + config, installs boto3 |
| 2026-06-14 | `dags/synthea-to-minio.py` | Simplified spark-submit command |
| 2026-06-14 | `.gitignore` | Added `minio_data/` |
