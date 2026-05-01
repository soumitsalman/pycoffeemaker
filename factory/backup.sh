#!/bin/bash
set -euo pipefail

# --- ARGS / DEFAULTS ---
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

PG_PCACHE="postgresql://postgres:local@localhost:5432/statestore"
LOCAL_CLSCACHE=""

usage() {
  cat <<'EOF'
Usage: backup.sh [--pg-pcache <dsn>] [--local-clscache <path>]

Options:
  --pg-pcache          Postgres DSN (default: postgresql://postgres:local@localhost:5432)
  --local-clscache    clsstore directory (default: <repo>/.cache/clsstore)
  -h, --help        Show this help text
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --pg-pcache)
      PG_PCACHE="${2:-}"
      shift 2
      ;;
    --local-clscache)
      LOCAL_CLSCACHE="${2:-}"
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

if [[ -z "$PG_PCACHE" ]]; then
  echo "Error: --pg-pcache requires a value" >&2
  exit 2
fi

if [[ -z "$LOCAL_CLSCACHE" ]]; then
  LOCAL_CLSCACHE="$SCRIPT_DIR/../.cache/clsstore"
fi

# --- BACKUP BUKET ---
S5CMD_BIN="$HOME/go/bin/s5cmd"
S5CMD_ARGS=(--credentials-file "$HOME/.aws/credentials" --endpoint-url https://t3.storage.dev)
BACKUP_BUCKET="s3://cafecito-archives-new/processingcache"
WORK_DIR="$HOME/.cache/pycoffeemaker"
mkdir -p "${WORK_DIR}"

echo "=== Starting ZVEC Classification Cache Backup ==="
DUMP_FILE="$WORK_DIR/clscache.tar.gz"
tar -czf "$DUMP_FILE" "$LOCAL_CLSCACHE"
$S5CMD_BIN "${S5CMD_ARGS[@]}" cp "$DUMP_FILE" "$BACKUP_BUCKET/"
rm -f "$DUMP_FILE"

echo "=== Starting PG State Cache Backup ==="
DUMP_FILE="$WORK_DIR/statecache.dump"
pg_dump --no-owner --no-privileges --format=custom --file="$DUMP_FILE" "$PG_PCACHE"
$S5CMD_BIN "${S5CMD_ARGS[@]}" cp "$DUMP_FILE" "$BACKUP_BUCKET/"
rm -f "$DUMP_FILE"







