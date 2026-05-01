set -euo pipefail

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
WORK_DIR="$HOME/.cache/pycoffeemaker"
mkdir -p "${WORK_DIR}"

echo "=== Restoring ZVEC Classification Cache ==="
$S5CMD_BIN "${S5CMD_ARGS[@]}" cp "$BACKUP_BUCKET/clscache.tar.gz" $WORK_DIR/
tar -xzf "$WORK_DIR/clscache.tar.gz"
rm "$WORK_DIR/clscache.tar.gz"

echo "=== Restoring PG State Cache ==="
$S5CMD_BIN "${S5CMD_ARGS[@]}" cp "$BACKUP_BUCKET/statecache.dump" $WORK_DIR/
pg_restore --clean --if-exists --no-owner --no-privileges --schema=public --dbname="$PG_PCACHE" "$WORK_DIR/statecache.dump"
rm "$WORK_DIR/statecache.dump"

# sudo systemctl stop postgresql@17-main
# sudo rm -rf /var/lib/postgresql/17/main
# sudo mkdir -p /var/lib/postgresql/17
# sudo tar -xzf .cache/statestore_backup.tar.gz -C /
# sudo chown -R postgres:postgres /var/lib/postgresql/17/main
# sudo chmod 700 /var/lib/postgresql/17/main
# sudo systemctl start postgresql@17-main

# rm .cache/statestore_backup.tar.gz
