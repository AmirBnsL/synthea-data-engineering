#!/bin/bash

echo "=== Checking for existing data ==="
spark-submit /home/iceberg/spark/jobs/recover_tables.py 2>&1 | grep -v "^26/"

echo ""
echo "=== Initializing missing Iceberg tables ==="
spark-submit /home/iceberg/spark/jobs/init_tables.py 2>&1 | grep -v "^26/"

echo ""
echo "=== Starting Spark services ==="
start-master.sh -p 7077
start-worker.sh spark://spark-iceberg:7077
start-history-server.sh
start-thriftserver.sh --driver-java-options "-Dderby.system.home=/tmp/derby"

tail -f /dev/null
