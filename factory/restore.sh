set -euo pipefail

if [[ -n "${BASH_SOURCE[0]:-}" && -f "${BASH_SOURCE[0]}" ]]; then
    WORKING_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
else
    WORKING_DIR="$(pwd -P)"
fi

# --- ARGS / DEFAULTS ---
PG_PCACHE="postgresql://postgres:local@localhost:5432/statestore"

usage() {
  cat <<'EOF'
Usage: backup.sh [--pg-pcache <dsn>] 

Options:
  --pg-pcache          Postgres DSN (default: postgresql://postgres:local@localhost:5432/statestore)
  -h, --help        Show this help text
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --pg-pcache)
      PG_PCACHE="${2:-}"
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


S5CMD_BIN="$HOME/go/bin/s5cmd"
S5CMD_ARGS=(--credentials-file "$HOME/.aws/credentials" --endpoint-url https://t3.storage.dev)
BACKUP_BUCKET="s3://cafecito-archives-new/processingcache"


restore_zvec() {
  echo "=== [STARTING] ZVEC Classification Cache Restore ==="
  "$S5CMD_BIN" "${S5CMD_ARGS[@]}" cp "$BACKUP_BUCKET/clscache.tar.gz" "$WORKING_DIR/.cache/"
  tar -xzf "$WORKING_DIR/.cache/clscache.tar.gz" -C "$WORKING_DIR/.cache/"
  rm "$WORKING_DIR/.cache/clscache.tar.gz"
  echo "=== [FINISHED] ZVEC Classification Cache Restore ==="
}

restore_pg() {
  echo "=== [STARTING] PG State Cache Restore ==="
  "$S5CMD_BIN" "${S5CMD_ARGS[@]}" cp "$BACKUP_BUCKET/statecache.dump" "$WORK_DIR/"
  pg_restore --clean --if-exists --no-owner --no-privileges --schema=public --dbname="$PG_PCACHE" "$WORK_DIR/statecache.dump"
  rm "$WORK_DIR/statecache.dump"
  echo "=== [FINISHED] PG State Cache Restore ==="
}

# disabling pg restore - its unimportant
# restore_zvec &
# restore_pg &
# wait
restore_zvec