import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.sdk import task
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.docker.operators.docker import DockerOperator, Mount
from airflow.models import Variable

PROJECT_ROOT = os.environ.get("PROJECT_ROOT", "/home/amirbnsl/Projects/synthea")

default_args = {
    "retries": 2,
    "retry_delay": timedelta(seconds=300),
    "execution_timeout": timedelta(seconds=1800),
}

with DAG(
    dag_id="synthea_to_minio",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
) as dag:
    patient_count = Variable.get("synthea_patient_count", default_var="100")

    generate_fhir = BashOperator(
        task_id="generate_fhir",
        bash_command=(
            "java -jar /opt/airflow/bin/synthea-with-dependencies.jar "
            f"-p {patient_count} "
            "--exporter.baseDirectory=/opt/airflow/data/output"
        ),
    )

    @task
    def upload_fhir_to_minio(ds=None):
        if ds is None:
            from datetime import date
            ds = date.today().isoformat()
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

        prefix = f"landing/{ds}"
        for file_name in files:
            local_file_path = os.path.join(local_dir, file_name)
            dest_key = f"{prefix}/{file_name}"

            print(f"Uploading {file_name} to s3://{bucket_name}/{dest_key}")
            s3_hook.load_file(
                filename=local_file_path,
                key=dest_key,
                bucket_name=bucket_name,
                replace=True,
            )

    fhir_to_iceberg = DockerOperator(
        task_id="fhir_to_iceberg",
        image="synthea-spark:latest",
        entrypoint=["/bin/sh"],
        command=[
            "-c",
            "spark-submit "
            "/home/iceberg/spark/jobs/fhir_to_iceberg.py "
            '--date "$(date +%Y-%m-%d)"',
        ],
        network_mode="container:spark-iceberg",
        environment={
            "AWS_ACCESS_KEY_ID": "admin",
            "AWS_SECRET_ACCESS_KEY": "password",
            "AWS_REGION": "us-east-1",
        },
        mounts=[
            Mount(
                source=f"{PROJECT_ROOT}/spark",
                target="/home/iceberg/spark",
                type="bind",
            ),
        ],
        auto_remove="force",
        mount_tmp_dir=False,
        force_pull=False,
        docker_url="unix://var/run/docker.sock",
    )

    generate_fhir >> upload_fhir_to_minio() >> fhir_to_iceberg
