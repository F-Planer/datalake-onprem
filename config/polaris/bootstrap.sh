#!/bin/sh
# Init job: creates the Polaris tables in Postgres and the root principal.
# Idempotent: if the realm is already bootstrapped, it exits successfully.
set -u
cd /deployments

output=$(java -XX:MaxRAMPercentage=70 -jar polaris-admin-tool.jar bootstrap \
  -r POLARIS -c "POLARIS,${POLARIS_ROOT_CLIENT_ID},${POLARIS_ROOT_CLIENT_SECRET}" 2>&1)
rc=$?
echo "$output" | grep -vE '^\s*at ' | tail -20

if [ $rc -eq 0 ]; then exit 0; fi
if echo "$output" | grep -qiE 'already (been )?bootstrapped'; then
  echo "realm POLARIS already bootstrapped"
  exit 0
fi
exit $rc
