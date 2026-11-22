#!/bin/bash
# Runs the ERP activity simulator in a loop.
#   INTERVAL: seconds between calls (default 5)
#   ORDERS:   new orders per call (default 3)
set -euo pipefail
INTERVAL="${INTERVAL:-5}"
ORDERS="${ORDERS:-3}"
echo "simulating: $ORDERS orders every ${INTERVAL}s (Ctrl+C to stop)"
n=0
while true; do
  /opt/mssql-tools18/bin/sqlcmd -S sqlserver -U sa -P "$MSSQL_SA_PASSWORD" -C -b \
    -d "$SQLSERVER_DB" -Q "EXEC dbo.SimulateActivity @NewOrders = $ORDERS"
  n=$((n + 1))
  if (( n % 12 == 0 )); then echo "$n rounds"; fi
  sleep "$INTERVAL"
done
