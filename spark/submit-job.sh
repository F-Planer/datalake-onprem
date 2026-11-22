#!/bin/sh
# Submits a PySpark job to the cluster, passing the secrets from .env as --conf.
# Usage (inside a container built from the datalake/spark image):
#   /opt/spark/submit-job.sh /opt/spark/jobs/streaming/bronze_orders.py [args...]
set -eu
: "${POLARIS_ROOT_CLIENT_ID:?}" "${POLARIS_ROOT_CLIENT_SECRET:?}" "${S3_ACCESS_KEY:?}" "${S3_SECRET_KEY:?}"

exec /opt/spark/bin/spark-submit \
  --conf "spark.sql.catalog.lakehouse.credential=${POLARIS_ROOT_CLIENT_ID}:${POLARIS_ROOT_CLIENT_SECRET}" \
  --conf "spark.sql.catalog.lakehouse.s3.access-key-id=${S3_ACCESS_KEY}" \
  --conf "spark.sql.catalog.lakehouse.s3.secret-access-key=${S3_SECRET_KEY}" \
  ${SPARK_SUBMIT_EXTRA:-} \
  "$@"
