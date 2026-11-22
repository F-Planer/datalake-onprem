#!/bin/bash
# Quick health check of the data lake. Changes nothing; does not need sudo.
#
#   ./scripts/check.sh
#
# Shows: stopped or unhealthy containers, the Debezium connector, Spark jobs,
# latest Airflow runs, the most recent data in the lake and the DW, and disk
# space. Each item is explained in docs/OPERATIONS.md.
cd "$(dirname "$0")/.." || exit 1

title() { printf '\n== %s\n' "$1"; }
ok()    { printf '  [ok]   %s\n' "$1"; }
warn()  { printf '  [!!]   %s\n' "$1"; }

title "Containers"
problems=0
while IFS=$'\t' read -r service state health; do
  case "$state" in
    running)
      if [ "$health" = "unhealthy" ]; then warn "$service: running, but unhealthy"; problems=1; fi ;;
    exited)
      # Init jobs are supposed to exit; it is only a problem if the exit code is not 0
      code=$(docker inspect -f '{{.State.ExitCode}}' "datalake-$service-1" 2>/dev/null)
      [ "$code" != "0" ] && { warn "$service: exited with an error (code $code)"; problems=1; } ;;
    *) warn "$service: $state"; problems=1 ;;
  esac
done < <(docker compose ps -a --format '{{.Service}}\t{{.State}}\t{{.Health}}' 2>/dev/null)
running=$(docker compose ps --format '{{.Service}}' 2>/dev/null | wc -l)
[ "$problems" = 0 ] && ok "$running containers running, none with problems"

title "CDC (Debezium)"
if status=$(curl -sf --max-time 5 localhost:8083/connectors/erp-sqlserver/status); then
  states=$(echo "$status" | grep -o '"state":"[A-Z]*"' | cut -d'"' -f4 | sort -u | tr '\n' ' ')
  if [ "$states" = "RUNNING " ]; then ok "connector erp-sqlserver: RUNNING"
  else warn "connector erp-sqlserver: $states(details: curl localhost:8083/connectors/erp-sqlserver/status)"; fi
else
  warn "Kafka Connect did not respond (cdc profile down?)"
fi

title "Spark"
if spark=$(curl -sf --max-time 5 localhost:8090/json/); then
  echo "$spark" | python3 -c '
import json, sys
d = json.load(sys.stdin)
alive, used, total = d["aliveworkers"], d["coresused"], d["cores"]
print(f"  workers alive: {alive}   cores in use: {used} of {total}")
names = [a["name"] for a in d["activeapps"]]
for name in names:
    print(f"  running: {name}")
expected = {"bronze_orders", "erp_cdc"}
for missing in sorted(expected - set(names)):
    print(f"  [!!]   continuous job {missing} is not running")
'
else
  warn "Spark master did not respond"
fi

title "Airflow (latest runs)"
for dag in erp_sales_hourly erp_sales_daily orders_events_hourly lakehouse_maintenance; do
  line=$(docker exec datalake-airflow-scheduler-1 airflow dags list-runs "$dag" -o plain 2>/dev/null | sed -n 2p)
  if [ -z "$line" ]; then warn "$dag: no runs (or Airflow is down)"; continue; fi
  state=$(echo "$line" | awk '{print $3}')
  start=$(echo "$line" | awk '{print $6}' | cut -c1-16)
  if [ "$state" = "success" ] || [ "$state" = "running" ]; then ok "$dag: $state (started $start UTC)"
  else warn "$dag: $state (started $start UTC) — see Airflow, http://localhost:8095"; fi
done

title "Most recent data"
docker exec datalake-trino-1 trino --output-format TSV --execute "
  SELECT 'lake, CDC (silver.erp_orders)', CAST(max(_source_changed_at) AS varchar) FROM lakehouse.silver.erp_orders
  UNION ALL
  SELECT 'lake, gold (fact_sales_items)', CAST(max(created_at) AS varchar) FROM lakehouse.gold.fact_sales_items
  UNION ALL
  SELECT 'lake, Kafka events (gold.orders_daily)', CAST(max(_computed_at) AS varchar) FROM lakehouse.gold.orders_daily
" 2>/dev/null | sed 's/^/  /;s/\t/: /' || warn "Trino did not respond"
if [ -f .env ]; then
  sa_password=$(grep '^MSSQL_SA_PASSWORD=' .env | cut -d= -f2-)
  dw_db=$(grep '^SQLSERVER_DW_DB=' .env | cut -d= -f2-)
  docker exec datalake-sqlserver-1 /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$sa_password" -C -h -1 -W \
    -Q "SET NOCOUNT ON; SELECT '  DW, latest delivery: ' + CONVERT(varchar, max(_loaded_to_dw_at), 120) + ' UTC' FROM ${dw_db:-AnalyticsDW}.dbo.daily_sales" \
    2>/dev/null || warn "simulated SQL Server did not respond"
fi

title "Disk"
df -h / | awk 'NR==2 {print "  disk: " $3 " used of " $2 " (" $5 ")"}'
docker system df --format '  docker {{.Type}}: {{.Size}}' 2>/dev/null
echo
