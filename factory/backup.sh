#!/usr/bin/env bash
set -euo pipefail

PG_DSN="postgresql://postgres:local@localhost:5432"
DUMP_DIR="$HOME/codes/pycoffeemaker/.cache"
S3_BUCKET="s3://cafecito-archives/processingcache"
S5CMD_BIN="$HOME/go/bin/s5cmd"
S5CMD_ARGS=(--credentials-file "$HOME/.aws/credentials" --endpoint-url https://t3.storage.dev)

pg_dump --format=t --file=$DUMP_DIR/statestore.dump $PG_DSN/statestore
$S5CMD_BIN "${S5CMD_ARGS[@]}" cp $DUMP_DIR/statestore.dump "$S3_BUCKET/$(basename "$DUMP_DIR/statestore.dump")"
rm -f $DUMP_DIR/statestore.dump

pg_dump --format=t --file=$DUMP_DIR/clsstore.dump $PG_DSN/clsstore
$S5CMD_BIN "${S5CMD_ARGS[@]}" cp $DUMP_DIR/clsstore.dump "$S3_BUCKET/$(basename "$DUMP_DIR/clsstore.dump")"
rm -f $DUMP_DIR/clsstore.dump

