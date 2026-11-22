#!/bin/sh
# Init job: creates or updates the Debezium connectors in Kafka Connect.
# Each config/debezium/<name>.json becomes connector <name> (PUT is idempotent).
set -eu
CONNECT="http://kafka-connect:8083"

for i in $(seq 1 60); do
  curl -sf "$CONNECT/connectors" >/dev/null && break
  echo "waiting for Kafka Connect ($i)..."; sleep 3
done

for file in /config/*.json; do
  name=$(basename "$file" .json)
  code=$(curl -s -o /tmp/resp -w '%{http_code}' -X PUT -H 'Content-Type: application/json' \
    --data @"$file" "$CONNECT/connectors/$name/config")
  case "$code" in
    200) echo "connector '$name' updated" ;;
    201) echo "connector '$name' created" ;;
    *)   echo "connector '$name' failed (HTTP $code):"; cat /tmp/resp; echo; exit 1 ;;
  esac
done
