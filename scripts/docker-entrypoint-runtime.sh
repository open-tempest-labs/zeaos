#!/bin/sh
# Runtime entrypoint for the ZeaOS container.
# Writes a ZeaDrive S3 config from environment variables so the REPL can reach
# the MinIO service (or any S3-compatible endpoint) without a FUSE mount.
set -e

if [ -n "$ZEA_TEST_S3_ENDPOINT" ]; then
  mkdir -p "$HOME/.zeaos"
  cat > "$HOME/.zeaos/volumez.json" <<EOF
{
  "mounts": [
    {
      "path": "/s3-data",
      "backend": "s3",
      "config": {
        "bucket": "${ZEA_TEST_S3_BUCKET:-zeaos-data}",
        "region": "us-east-1",
        "endpoint": "${ZEA_TEST_S3_ENDPOINT}"
      }
    }
  ],
  "cache": {
    "enabled": true,
    "max_size": 1073741824,
    "ttl": 300,
    "metadata_ttl": 60
  }
}
EOF
fi

exec zeaos "$@"
