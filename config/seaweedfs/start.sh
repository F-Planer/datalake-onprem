#!/bin/sh
# Builds the S3 identity config from the environment variables and starts
# SeaweedFS in "all in one" mode (master + volume + filer + S3).
set -eu

cat > /tmp/s3.json <<JSON
{
  "identities": [
    {
      "name": "${S3_ACCESS_KEY}",
      "credentials": [{ "accessKey": "${S3_ACCESS_KEY}", "secretKey": "${S3_SECRET_KEY}" }],
      "actions": ["Admin", "Read", "Write", "List", "Tagging"]
    }
  ]
}
JSON

exec weed server \
  -dir=/data \
  -ip=seaweedfs \
  -ip.bind=0.0.0.0 \
  -master.volumeSizeLimitMB=1024 \
  -volume.max=0 \
  -filer \
  -s3 \
  -s3.port=8333 \
  -s3.config=/tmp/s3.json
