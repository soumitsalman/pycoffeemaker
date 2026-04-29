#!/usr/bin/env bash
set -euo pipefail

PG_DSN="postgresql://postgres:local@localhost:5432"
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
DUMP_DIR="$SCRIPT_DIR/../.cache"
S3_BUCKET="s3://cafecito-archives/processingcache"
S5CMD_BIN="$HOME/go/bin/s5cmd"
S5CMD_ARGS=(--credentials-file "$HOME/.aws/credentials" --endpoint-url https://t3.storage.dev)

if [[ ! -d "$DUMP_DIR/clsstore" ]]; then
  echo "Expected directory not found: $DUMP_DIR/clsstore" >&2
  exit 1
fi
# Upload all files under $DUMP_DIR/clsstore while preserving directory structure.
$S5CMD_BIN "${S5CMD_ARGS[@]}" cp "$DUMP_DIR/clsstore/*" "$S3_BUCKET/clsstore/"

pg_dump --data-only --no-owner --no-privileges --format=t --file="$DUMP_DIR/statestore.dump" "$PG_DSN/statestore"
$S5CMD_BIN "${S5CMD_ARGS[@]}" cp "$DUMP_DIR/statestore.dump" "$S3_BUCKET/$(basename "$DUMP_DIR/statestore.dump")"
rm -f "$DUMP_DIR/statestore.dump"



