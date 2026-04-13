#!/usr/bin/env bash
set -euo pipefail

S3_BUCKET="s3://cafecito-archives/processingcache"
S5CMD_BIN=/home/soumitsr/go/bin/s5cmd
S5CMD_ARGS=(--credentials-file /home/soumitsr/.aws/credentials --endpoint-url https://t3.storage.dev)
DUMP_FILE="/home/soumitsr/pycoffeemaker/.cache/statestore.dump"
PG_DSN="postgresql://postgres:local@localhost:5432/statestore"
ROOT_DIR="/home/soumitsr/pycoffeemaker/.cache"

pg_dump -Fc "$PG_DSN" -f "$DUMP_FILE"
"$S5CMD_BIN" "${S5CMD_ARGS[@]}" cp "$DUMP_FILE" "$S3_BUCKET/$(basename "$DUMP_FILE")"
rm -f "$DUMP_FILE"

for dir in beans.lance categories.lance sentiments.lance; do
  dir_path="$ROOT_DIR/$dir"
  if [[ -d "$dir_path" ]]; then
    "$S5CMD_BIN" "${S5CMD_ARGS[@]}" cp --no-clobber "$dir_path" "$S3_BUCKET/"
  fi
done
