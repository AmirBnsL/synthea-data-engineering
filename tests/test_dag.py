import pytest
from pathlib import Path

_candidates = [
    Path(__file__).parent.parent / "dags" / "synthea-to-minio.py",
    Path("/opt/airflow/dags/synthea-to-minio.py"),
    Path("/home/iceberg/dags/synthea-to-minio.py"),
]
DAG_PATH = next((p for p in _candidates if p.exists()), _candidates[0])


def test_dag_imports():
    """Verify the DAG file parses as valid Python."""
    with open(DAG_PATH) as f:
        code = compile(f.read(), str(DAG_PATH), "exec")
    assert code is not None


def test_dag_ast_has_expected_tasks():
    """Check DAG structure via AST without importing Airflow."""
    import ast

    with open(DAG_PATH) as f:
        tree = ast.parse(f.read())

    task_ids = []
    func_names = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            for kw in node.keywords:
                if kw.arg == "task_id" and isinstance(kw.value, ast.Constant):
                    task_ids.append(kw.value.value)
        elif isinstance(node, ast.FunctionDef):
            func_names.append(node.name)

    assert "generate_fhir" in task_ids, "Missing generate_fhir task"
    assert "fhir_to_iceberg" in task_ids, "Missing fhir_to_iceberg task"
    assert "upload_fhir_to_minio" in func_names, "Missing upload_fhir_to_minio task function"


AIRFLOW_AVAILABLE = False
try:
    import airflow  # noqa: F401
except Exception:
    pass
else:
    try:
        from airflow.models import DagBag
        from airflow.providers.docker.operators.docker import DockerOperator
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        from airflow.sdk import task  # noqa: F401

        AIRFLOW_AVAILABLE = True
    except Exception as e:
        print(f"  Airflow module import error: {e}")


@pytest.mark.skipif(not AIRFLOW_AVAILABLE, reason="Airflow not installed")
def test_dag_bag_loads():
    """Verify the DAG loads without errors in Airflow's DagBag."""
    from airflow.models import DagBag

    dag_bag = DagBag(
        dag_folder=str(DAG_PATH.parent),
        include_examples=False,
    )
    assert len(dag_bag.import_errors) == 0, f"DAG import errors: {dag_bag.import_errors}"
    assert "synthea_to_minio" in dag_bag.dags, "DAG synthea_to_minio not found"


@pytest.mark.skipif(not AIRFLOW_AVAILABLE, reason="Airflow not installed")
def test_dag_structure():
    """Verify task dependencies in the DAG."""
    from airflow.models import DagBag

    dag_bag = DagBag(
        dag_folder=str(DAG_PATH.parent),
        include_examples=False,
    )
    dag = dag_bag.dags["synthea_to_minio"]

    task_ids = {t.task_id for t in dag.tasks}
    assert "generate_fhir" in task_ids
    assert "upload_fhir_to_minio" in task_ids
    assert "fhir_to_iceberg" in task_ids

    upstream = dag.get_task("upload_fhir_to_minio").upstream_task_ids
    downstream = dag.get_task("upload_fhir_to_minio").downstream_task_ids

    assert "generate_fhir" in upstream
    assert "fhir_to_iceberg" in downstream


@pytest.mark.skipif(not AIRFLOW_AVAILABLE, reason="Airflow not installed")
def test_dag_default_args():
    """Verify DAG has proper default_args."""
    from airflow.models import DagBag

    dag_bag = DagBag(
        dag_folder=str(DAG_PATH.parent),
        include_examples=False,
    )
    dag = dag_bag.dags["synthea_to_minio"]

    assert dag.max_active_runs == 1
    assert dag.default_args.get("retries") is not None
    assert dag.default_args.get("retry_delay") is not None
    assert dag.default_args.get("execution_timeout") is not None


@pytest.mark.skipif(not AIRFLOW_AVAILABLE, reason="Airflow not installed")
def test_docker_operator_config():
    """Verify DockerOperator has correct image and network_mode."""
    from airflow.models import DagBag

    dag_bag = DagBag(
        dag_folder=str(DAG_PATH.parent),
        include_examples=False,
    )
    dag = dag_bag.dags["synthea_to_minio"]
    task = dag.get_task("fhir_to_iceberg")

    assert task.image == "synthea-spark:latest"
    assert task.network_mode == "container:spark-iceberg"
    assert task.auto_remove == "force"
    assert task.force_pull is False
    assert task.mount_tmp_dir is False
    assert "driver-memory 10g" in task.command
    assert "executor.memory=10g" in task.command
