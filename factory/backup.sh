#!/usr/bin/env bash
set -euo pipefail

S3_BUCKET="s3://cafecito-archives/processingcache"
S5CMD_BIN=/home/soumitsr/go/bin/s5cmd
S5CMD_ARGS=(--credentials-file /home/soumitsr/.aws/credentials --endpoint-url https://t3.storage.dev)
DUMP_FILE="/home/soumitsr/pycoffeemaker/.cache"
PG_DSN="postgresql://postgres:local@localhost:5432"
ROOT_DIR="/home/soumitsr/pycoffeemaker/.cache"

pg_dump -Fc "$PG_DSN/statestore" -f "$DUMP_FILE/statestore.dump"
"$S5CMD_BIN" "${S5CMD_ARGS[@]}" cp "$DUMP_FILE/statestore.dump" "$S3_BUCKET/$(basename "$DUMP_FILE/statestore.dump")"
rm -f "$DUMP_FILE/statestore.dump"

pg_dump -Fc "$PG_DSN/clsstore" -f "$DUMP_FILE/clsstore.dump"
"$S5CMD_BIN" "${S5CMD_ARGS[@]}" cp "$DUMP_FILE/clsstore.dump" "$S3_BUCKET/$(basename "$DUMP_FILE/clsstore.dump")"
rm -f "$DUMP_FILE/clsstore.dump"

