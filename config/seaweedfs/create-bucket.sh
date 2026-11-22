#!/bin/sh
# Init job: creates the lakehouse bucket in SeaweedFS (idempotent).
set -eu
BUCKET="${BUCKET:-lakehouse}"
MASTER="seaweedfs:9333"

for i in $(seq 1 30); do
  if echo "s3.bucket.list" | weed shell -master="$MASTER" >/tmp/list 2>&1; then break; fi
  echo "waiting for SeaweedFS ($i)..."; sleep 2
done

if grep -qE "^\s*${BUCKET}(\s|$)" /tmp/list; then
  echo "bucket '${BUCKET}' already exists"
else
  echo "s3.bucket.create -name ${BUCKET}" | weed shell -master="$MASTER"
  echo "bucket '${BUCKET}' created"
fi
