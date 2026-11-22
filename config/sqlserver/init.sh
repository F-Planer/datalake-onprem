#!/bin/bash
# Init job for the simulated SQL Server: database, data, simulator, CDC, logins and DW.
set -euo pipefail
SQLCMD=(/opt/mssql-tools18/bin/sqlcmd -S sqlserver -U sa -P "$MSSQL_SA_PASSWORD" -C -b
        -v DB="$SQLSERVER_DB"
           CDC_USER="$SQLSERVER_CDC_USER" CDC_PASSWORD="$SQLSERVER_CDC_PASSWORD"
           BATCH_USER="$SQLSERVER_BATCH_USER" BATCH_PASSWORD="$SQLSERVER_BATCH_PASSWORD"
           DW_DB="$SQLSERVER_DW_DB" DW_USER="$SQLSERVER_DW_USER" DW_PASSWORD="$SQLSERVER_DW_PASSWORD")

for i in $(seq 1 60); do
  "${SQLCMD[@]}" -Q "SELECT 1" >/dev/null 2>&1 && break
  echo "waiting for SQL Server ($i)..."; sleep 2
done

for script in 01-database.sql 02-simulator.sql 03-cdc-and-access.sql 04-dw.sql; do
  echo "== $script"
  "${SQLCMD[@]}" -i "/config/$script"
done
echo "simulated SQL Server ready"
