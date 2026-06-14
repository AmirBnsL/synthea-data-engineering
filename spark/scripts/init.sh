#!/bin/bash
set -e

echo "=== Initializing Iceberg tables ==="
spark-submit /home/iceberg/spark/jobs/init_tables.py 2>&1 | grep -v "^26/"

echo ""
echo "=== Recovering existing data if any ==="
spark-submit /home/iceberg/spark/jobs/recover_tables.py 2>&1 | grep -v "^26/"

echo ""
echo "=== Startup complete ==="
exec /opt/spark/sbin/start-thriftserver.sh "$@"
