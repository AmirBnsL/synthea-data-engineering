import os
import boto3
from utils import init_spark, CATALOG, NAMESPACE

S3_ENDPOINT = "http://minio:9000"
S3_ACCESS_KEY = "admin"
S3_SECRET_KEY = "password"
BUCKET = "warehouse"
BASE_PREFIX = "raw_landing"

TABLES = ["patients", "encounters", "conditions", "observations", "procedures"]


def get_latest_metadata(s3, table_name):
    prefix = f"{BASE_PREFIX}/{table_name}/metadata/"
    try:
        resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
        if "Contents" not in resp:
            return None
        files = [
            obj["Key"]
            for obj in resp["Contents"]
            if obj["Key"].endswith(".metadata.json")
        ]
        if not files:
            return None
        latest = sorted(files)[-1]
        return f"s3a://{BUCKET}/{latest}"
    except Exception as e:
        print(f"  MinIO error: {e}")
        return None


def main():
    spark = init_spark(app_name="Recover-Tables")

    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
    )

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG}.{NAMESPACE}")

    any_recovered = False
    for table in TABLES:
        path = get_latest_metadata(s3, table)
        if not path:
            print(f"No existing data to recover for {table}")
            continue

        full = f"{CATALOG}.{NAMESPACE}.{table}"
        print(f"Registering {full} from {path}")
        try:
            spark.sql(f"DROP TABLE IF EXISTS {full}")
            spark.sql(
                f"CALL {CATALOG}.system.register_table('{NAMESPACE}.{table}', '{path}')"
            )
            count = spark.sql(f"SELECT COUNT(*) FROM {full}").collect()[0][0]
            print(f"  Recovered {count} rows")
            any_recovered = True
        except Exception as e:
            print(f"  Failed to recover: {e}")

    if not any_recovered:
        print("Nothing to recover — starting fresh")

    spark.stop()


if __name__ == "__main__":
    main()
