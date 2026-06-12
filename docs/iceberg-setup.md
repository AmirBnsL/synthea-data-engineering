## Iceberg Setup Guide

### Table of Contents
1. [What is Apache Iceberg?](#what-is-apache-iceberg)
2. [Architecture in this Project](#architecture-in-this-project)
3. [Prerequisites](#prerequisites)
4. [Docker Compose Configuration](#docker-compose-configuration)
5. [Connecting via spark-sql](#connecting-via-spark-sql)
6. [Creating a Namespace & Table](#creating-a-namespace--table)
7. [Verifying Table Metadata in MinIO](#verifying-table-metadata-in-minio)
8. [Common Pitfalls](#common-pitfalls)

### What is Apache Iceberg?

Iceberg is an **open table format** for analytic datasets stored in object stores (S3, MinIO, GCS). It adds a metadata layer on top of Parquet/ORC/Avro files that provides:

- **ACID transactions** — concurrent writers see consistent snapshots
- **Schema evolution** — add, drop, rename, reorder columns without rewriting data
- **Time travel** — query the table as of any previous snapshot
- **Hidden partitioning** — tables are partitioned by a transform (e.g. `months(birth_date)`) and Iceberg automatically routes data to the correct partition; no need to manage partition directories
- **Partition evolution** — you can change a table's partition spec without rewriting existing data
- **Manifest-based metadata** — table state is tracked as immutable JSON metadata files; no metastore bottlenecks

**Metadata file hierarchy:**

```
warehouse/<namespace>/<table>/
  metadata/
    00000-<uuid>.metadata.json   ← root pointer (current version)
    00001-<uuid>.metadata.json   ← updated on each commit
    snap-<snapshot-id>-<uuid>.avro  ← manifest list (links to manifests)
    manifest-<uuid>.avro            ← manifest (links to data files)
  data/
    ...parquet files...
```

### Architecture in this Project

```
┌─────────────────────────────────────────────────────┐
│                    Spark Session                     │
│  spark-sql / PySpark / spark-submit                 │
│  ┌──────────────────────────────────────────────┐   │
│  │  SparkCatalog (type=rest)                    │   │
│  │  raw.raw_landing.patients                    │   │
│  └──────────┬───────────────────────────────────┘   │
└─────────────┼───────────────────────────────────────┘
              │ Iceberg REST API (port 8181)
┌─────────────▼───────────────────────────────────────┐
│                 iceberg-rest (server)                │
│  Apache Iceberg REST Catalog fixture                 │
│  ┌──────────────────────────────────────────────┐   │
│  │  JdbcCatalog (SQLite)                        │   │
│  │  └── stores: namespaces, table metadata       │   │
│  │  S3FileIO (writes metadata to MinIO)          │   │
│  └──────────┬───────────────────────────────────┘   │
└─────────────┼───────────────────────────────────────┘
              │ S3 API (port 9000)
┌─────────────▼───────────────────────────────────────┐
│                     MinIO                            │
│  Bucket: warehouse                                   │
│  └── raw_landing/patients/metadata/*.json           │
│  Bucket: raw-landing                                 │
│  └── landing/*.json  (FHIR source data)             │
└─────────────────────────────────────────────────────┘
```

### Prerequisites

- Docker Compose with four core services running:
  - `minio` (MinIO S3, port 9000)
  - `rest` (Apache Iceberg REST catalog, port 8181)
  - `spark-iceberg` (Spark 3.5.5 + Iceberg, no exposed port)
  - All on `iceberg_net` network (DNS resolution between containers)

- MinIO buckets pre-created: `warehouse` and `raw-landing`

- AWS credentials set in `docker-compose.yml`:
  ```yaml
  AWS_ACCESS_KEY_ID=admin
  AWS_SECRET_ACCESS_KEY=password
  AWS_REGION=us-east-1
  ```

### Docker Compose Configuration

**Minimal setup for S3-based Iceberg:**

```yaml
rest:
  image: apache/iceberg-rest-fixture
  container_name: iceberg-rest
  environment:
    - AWS_ACCESS_KEY_ID=admin
    - AWS_SECRET_ACCESS_KEY=password
    - AWS_REGION=us-east-1
    - CATALOG_WAREHOUSE=s3://warehouse/
    - CATALOG_IO__IMPL=org.apache.iceberg.aws.s3.S3FileIO
    - CATALOG_S3_ENDPOINT=http://minio:9000
    - CATALOG_S3_PATH__STYLE__ACCESS=true
```

**Key configs explained:**

| Environment Variable | Property it sets | Purpose |
|---|---|---|
| `CATALOG_WAREHOUSE` | `warehouse` | Root S3 path for all tables |
| `CATALOG_IO__IMPL` | `io-impl` | File I/O implementation (S3FileIO for S3/MinIO) |
| `CATALOG_S3_ENDPOINT` | `s3.endpoint` | Custom S3 endpoint (MinIO) |
| `CATALOG_S3_PATH__STYLE__ACCESS` | `s3.path-style-access` | Required for MinIO (path-style URLs) |

**Spark session config** (used in every `spark-sql` or `spark-submit` command):

```bash
--conf "spark.sql.catalog.raw=org.apache.iceberg.spark.SparkCatalog"
--conf "spark.sql.catalog.raw.type=rest"
--conf "spark.sql.catalog.raw.uri=http://iceberg-rest:8181"
--conf "spark.sql.catalog.raw.io-impl=org.apache.iceberg.aws.s3.S3FileIO"
--conf "spark.sql.catalog.raw.s3.endpoint=http://minio:9000"
--conf "spark.sql.catalog.raw.s3.path-style-access=true"
```

### Connecting via spark-sql

```bash
docker exec spark-iceberg spark-sql \
  --conf "spark.sql.catalog.raw=org.apache.iceberg.spark.SparkCatalog" \
  --conf "spark.sql.catalog.raw.type=rest" \
  --conf "spark.sql.catalog.raw.uri=http://iceberg-rest:8181" \
  --conf "spark.sql.catalog.raw.io-impl=org.apache.iceberg.aws.s3.S3FileIO" \
  --conf "spark.sql.catalog.raw.s3.endpoint=http://minio:9000" \
  --conf "spark.sql.catalog.raw.s3.path-style-access=true"
```

All subsequent SQL examples assume these `--conf` flags are passed.

### Creating a Namespace & Table

**Step 1:** Create the database (namespace):

```sql
CREATE DATABASE IF NOT EXISTS raw.raw_landing;
```

**Step 2:** Create a partitioned Iceberg table:

```sql
CREATE TABLE IF NOT EXISTS raw.raw_landing.patients (
  patient_id     STRING,
  birth_date     DATE,
  death_date     DATE,
  ssn            STRING,
  first_name     STRING,
  last_name      STRING,
  gender         STRING,
  race           STRING,
  ethnicity      STRING,
  address_city   STRING,
  address_state  STRING,
  address_zip    STRING,
  marital_status STRING,
  mrn            STRING,
  file_source    STRING
)
USING iceberg
PARTITIONED BY (months(birth_date));
```

The `PARTITIONED BY (months(birth_date))` creates a **hidden partition**. Iceberg automatically computes the partition value from `birth_date` on write; the partition column doesn't appear in the table schema directly but is visible in `_partition` metadata.

**Step 3:** Verify the table:

```sql
SHOW TABLES IN raw.raw_landing;

DESCRIBE EXTENDED raw.raw_landing.patients;
```

Expected output includes:
- `Type: MANAGED` — Iceberg manages the data lifecycle
- `Location: s3://warehouse/raw_landing/patients/` — where table data and metadata live
- `Provider: iceberg`
- `Partition spec: [months(birth_date)]` — the partition transform

### Verifying Table Metadata in MinIO

```bash
# Recursively list the table directory in MinIO
docker exec minio mc ls --recursive local/warehouse/raw_landing/patients/
```

Expected output:
```
[date]  1.6KiB STANDARD metadata/00000-<uuid>.metadata.json
[date]  1.6KiB STANDARD metadata/00001-<uuid>.metadata.json
```

Each `.metadata.json` file corresponds to a table version snapshot. Iceberg creates one metadata file on `CREATE TABLE` and a new one on every write / schema change.

### Common Pitfalls

#### 1. `warehouse.minio: Name or service not known`

**Cause:** The S3 SDK defaults to **virtual-hosted-style** URLs (e.g. `warehouse.minio:9000`), but MinIO requires **path-style** (e.g. `http://minio:9000/warehouse/`).

**Fix:** Set `s3.path-style-access=true` in both:
- REST catalog env: `CATALOG_S3_PATH__STYLE__ACCESS=true`
- Spark session: `--conf "spark.sql.catalog.raw.s3.path-style-access=true"`

The double underscore `__` in the env var is required — Iceberg's REST fixture converts `__` to `-` and `_` to `.`:
- `CATALOG_S3_PATH__STYLE__ACCESS` → `s3.path-style-access` ✓
- `CATALOG_S3_PATH_STYLE_ACCESS` → `s3.path.style.access` ✗ (dots won't match the S3FileIO property key)

#### 2. `NoSuchNamespaceException: Namespace raw_landing does not exist`

**Cause:** The SQLite-backed catalog was reset (container restart) and the namespace wasn't recreated.

**Fix:** Run `CREATE DATABASE IF NOT EXISTS raw.raw_landing;` before creating tables.

#### 3. `Class org.apache.hadoop.fs.s3a.S3AFileSystem not found`

**Cause:** Using `type=hadoop` with `s3a://` URIs, but the `hadoop-aws` JAR isn't in the `spark-iceberg` container.

**Fix:** Use `type=rest` with `S3FileIO` (provided by `iceberg-aws-bundle-1.8.1.jar`, which is included in the tabulario/spark-iceberg image).
