#!/bin/sh
# Writes a ZeaDrive S3 config pointing at the MinIO test service, then runs
# the test suite. Only runs when ZEA_TEST_S3_ENDPOINT is set (i.e. in CI /
# docker-compose). No-op config write if the env var is absent.
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
        "bucket": "${ZEA_TEST_S3_BUCKET:-zeaos-test}",
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
  echo "zeaos-test: wrote ZeaDrive S3 config → $HOME/.zeaos/volumez.json"
fi

exec go test -tags duckdb_arrow -v -count=1 ./... ./zeaberg/...
