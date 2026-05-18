#!/bin/bash

SPARK_SBIN="/opt/spark/sbin"
# MASTER_HOSTNAME="vbox"
MASTER_URL="spark://$(hostname):7077"

echo "=== starting master ==="
$SPARK_SBIN/start-master.sh
echo
sleep 2

echo "=== starting connect server ==="
$SPARK_SBIN/start-connect-server.sh --packages org.postgresql:postgresql:42.7.11
echo
sleep 2

echo "=== starting 3 workers ==="
for i in {1..3}; do
    echo "Starting worker $i..."
    # -c = cores, -m = memory, --webui-port = port shift for UI (8081, 8082, 8083)
    $SPARK_SBIN/start-worker.sh "$MASTER_URL" -c 1 -m 2G --webui-port 808$i
done