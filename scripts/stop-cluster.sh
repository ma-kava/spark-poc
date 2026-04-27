#!/bin/bash

SPARK_DIR="/opt/spark"

"$SPARK_DIR/sbin/stop-master.sh"
"$SPARK_DIR/sbin/stop-connect-server.sh"
"$SPARK_DIR/sbin/stop-worker.sh"
