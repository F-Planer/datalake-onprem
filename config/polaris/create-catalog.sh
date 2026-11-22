#!/bin/sh
# Init job: creates the Iceberg catalog "lakehouse" in Polaris, pointing to the
# SeaweedFS bucket, plus the namespaces of the bronze/silver/gold layers.
# Idempotent: whatever already exists is kept.
set -eu
POLARIS="http://polaris:8181"
CATALOG="lakehouse"

token=$(curl -sf "$POLARIS/api/catalog/v1/oauth/tokens" \
  -d grant_type=client_credentials \
  -d client_id="$POLARIS_ROOT_CLIENT_ID" \
  -d client_secret="$POLARIS_ROOT_CLIENT_SECRET" \
  -d scope=PRINCIPAL_ROLE:ALL | sed -E 's/.*"access_token":"([^"]+)".*/\1/')
auth="Authorization: Bearer $token"
json="Content-Type: application/json"

# 1. Catalog. stsUnavailable: SeaweedFS has no STS for Polaris to issue
#    temporary credentials; Spark and Trino use their own S3 credential.
code=$(curl -s -o /tmp/resp -w '%{http_code}' -X POST "$POLARIS/api/management/v1/catalogs" \
  -H "$auth" -H "$json" -d @- <<JSON
{
  "catalog": {
    "name": "$CATALOG",
    "type": "INTERNAL",
    "properties": {
      "default-base-location": "s3://lakehouse/warehouse",
      "polaris.config.drop-with-purge.enabled": "true"
    },
    "storageConfigInfo": {
      "storageType": "S3",
      "allowedLocations": ["s3://lakehouse/warehouse"],
      "endpoint": "http://seaweedfs:8333",
      "endpointInternal": "http://seaweedfs:8333",
      "pathStyleAccess": true,
      "stsUnavailable": true,
      "region": "us-east-1"
    }
  }
}
JSON
)
case "$code" in
  201) echo "catalog '$CATALOG' created" ;;
  409) echo "catalog '$CATALOG' already exists" ;;
  *)   echo "failed to create catalog (HTTP $code):"; cat /tmp/resp; exit 1 ;;
esac

# 2. Lets the catalog_admin role (which root already has) manage the content.
curl -sf -X PUT "$POLARIS/api/management/v1/catalogs/$CATALOG/catalog-roles/catalog_admin/grants" \
  -H "$auth" -H "$json" -d '{"grant":{"type":"catalog","privilege":"CATALOG_MANAGE_CONTENT"}}'
echo "privilege CATALOG_MANAGE_CONTENT granted to catalog_admin"

# 3. Layer namespaces.
for ns in bronze silver gold; do
  code=$(curl -s -o /tmp/resp -w '%{http_code}' -X POST "$POLARIS/api/catalog/v1/$CATALOG/namespaces" \
    -H "$auth" -H "$json" -d "{\"namespace\":[\"$ns\"]}")
  case "$code" in
    200) echo "namespace '$ns' created" ;;
    409) echo "namespace '$ns' already exists" ;;
    *)   echo "failed to create namespace '$ns' (HTTP $code):"; cat /tmp/resp; exit 1 ;;
  esac
done
