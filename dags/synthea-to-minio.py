import os
from datetime import datetime

from airflow import DAG
from airflow.sdk import task
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

with DAG(
    dag_id="synthea_to_minio",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    generate_fhir = BashOperator(
        task_id="generate_fhir",
        bash_command=(
            "java -jar /opt/airflow/bin/synthea-with-dependencies.jar "
            "-p 5 "
            "--exporter.baseDirectory=/opt/airflow/data/output"
        ),
    )

    @task
    def upload_fhir_to_minio():
        local_dir = "/opt/airflow/data/output/fhir"
        bucket_name = "raw-landing"
        s3_hook = S3Hook(aws_conn_id="minio_conn_id")

        if not os.path.exists(local_dir):
            raise FileNotFoundError(
                f"Synthea output directory not found at: {local_dir}"
            )

        files = [f for f in os.listdir(local_dir) if f.endswith(".json")]

        if not files:
            print("No JSON files found to upload.")
            return

        for file_name in files:
            local_file_path = os.path.join(local_dir, file_name)
            dest_key = f"landing/{file_name}"

            print(f"Uploading {file_name} to s3://{bucket_name}/{dest_key}")
            s3_hook.load_file(
                filename=local_file_path,
                key=dest_key,
                bucket_name=bucket_name,
                replace=True,
            )

    fhir_to_iceberg = BashOperator(
        task_id="fhir_to_iceberg",
        bash_command=(
            "docker exec spark-iceberg spark-submit "
            "--driver-memory 4g "
            "/home/iceberg/spark/jobs/fhir_to_iceberg.py"
        ),
    )

    generate_fhir >> upload_fhir_to_minio() >> fhir_to_iceberg
