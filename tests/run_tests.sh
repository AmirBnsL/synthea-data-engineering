#!/bin/bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

echo "========================================"
echo " Synthea Pipeline Test Suite"
echo "========================================"

echo ""
echo "--- 1. Python syntax check ---"
python3 -m py_compile "$PROJECT_ROOT/dags/synthea-to-minio.py" && echo -e "${GREEN}PASS${NC}" || echo -e "${RED}FAIL${NC}"
python3 -m py_compile "$PROJECT_ROOT/spark/jobs/utils.py" && echo -e "${GREEN}PASS${NC}" || echo -e "${RED}FAIL${NC}"
python3 -m py_compile "$PROJECT_ROOT/spark/jobs/fhir_to_iceberg.py" && echo -e "${GREEN}PASS${NC}" || echo -e "${RED}FAIL${NC}"
python3 -m py_compile "$PROJECT_ROOT/spark/jobs/init_tables.py" && echo -e "${GREEN}PASS${NC}" || echo -e "${RED}FAIL${NC}"
python3 -m py_compile "$PROJECT_ROOT/spark/jobs/validate_iceberg.py" && echo -e "${GREEN}PASS${NC}" || echo -e "${RED}FAIL${NC}"
python3 -m py_compile "$PROJECT_ROOT/spark/jobs/table_history.py" && echo -e "${GREEN}PASS${NC}" || echo -e "${RED}FAIL${NC}"

echo ""
echo "--- 2. Flatten unit tests (runs in spark-iceberg container) ---"
echo "Run: docker exec spark-iceberg pytest -v /home/iceberg/spark/jobs/../tests/test_fhir_flatten.py"
echo ""

echo "--- 3. DAG structure tests (runs in Airflow container) ---"
echo "Run: docker exec airflow-scheduler pytest -v /opt/airflow/dags/../tests/test_dag.py"
echo ""

echo "--- 4. End-to-end test ---"
echo "Run: docker exec airflow-webserver python -m airflow dags trigger synthea_to_minio"
echo ""
