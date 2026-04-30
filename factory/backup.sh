#!/bin/bash
set -euo pipefail

# --- ARGS / DEFAULTS ---
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

PG_DSN="postgresql://postgres:local@localhost:5432"
CLSSTORE_DIR=""

usage() {
  cat <<'EOF'
Usage: backup.sh [--pg-dsn <dsn>] [--clsstore-dir <path>]

Options:
  --pg-dsn          Postgres DSN (default: postgresql://postgres:local@localhost:5432)
  --clsstore-dir    clsstore directory (default: <repo>/.cache/clsstore)
  -h, --help        Show this help text
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --pg-dsn)
      PG_DSN="${2:-}"
      shift 2
      ;;
    --clsstore-dir)
      CLSSTORE_DIR="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -z "$PG_DSN" ]]; then
  echo "Error: --pg-dsn requires a value" >&2
  exit 2
fi

if [[ -z "$CLSSTORE_DIR" ]]; then
  CLSSTORE_DIR="$SCRIPT_DIR/../.cache/clsstore"
fi

# --- BACKUP BUKET ---
S5CMD_BIN="$HOME/go/bin/s5cmd"
S5CMD_ARGS=(--credentials-file "$HOME/.aws/credentials" --endpoint-url https://t3.storage.dev)
BACKUP_BUCKET="cafecito-archives-new/processingcache"
WORK_DIR="/tmp/processingcache_backup"
mkdir -p "${WORK_DIR}"

# alt approach using pgdump and restore
echo "=== Starting PG State Cache Backup ==="

PG_VERSION="17"
PG_DATA_DIR="/var/lib/postgresql/${PG_VERSION}/main"
PG_SERVICE="postgresql@${PG_VERSION}-main"
BACKUP_NAME="statestore_backup.tar.gz"

sudo systemctl stop "${PG_SERVICE}"

sudo tar -czf "${WORK_DIR}/${BACKUP_NAME}" "${PG_DATA_DIR}"
$S5CMD_BIN "${S5CMD_ARGS[@]}" cp "${WORK_DIR}/${BACKUP_NAME}" "s3://$BACKUP_BUCKET/statestore/$BACKUP_NAME"
rm -f "$WORK_DIR/$BACKUP_NAME"

sudo systemctl start "${PG_SERVICE}"

# echo "=== Starting PG State Cache Backup ==="
# pg_dump --data-only --no-owner --no-privileges --format=t --file="$WORK_DIR/statestore.dump" "$PG_DSN/statestore"
# $S5CMD_BIN "${S5CMD_ARGS[@]}" cp "$WORK_DIR/statestore.dump" "s3://$BACKUP_BUCKET/statestore/statestore.dump"
# rm -f "$WORK_DIR/statestore.dump"

echo "=== Starting ZVEC Classification Cache Backup ==="
BACKUP_NAME="clsstore_backup.tar.gz"
tar -czf "$WORK_DIR/$BACKUP_NAME" "$CLSSTORE_DIR"
$S5CMD_BIN "${S5CMD_ARGS[@]}" cp "$WORK_DIR/$BACKUP_NAME" "s3://$BACKUP_BUCKET/clsstore/"
rm -f "$WORK_DIR/$BACKUP_NAME"





