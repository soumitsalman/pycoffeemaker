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

set -a
# shellcheck disable=SC1091
[[ -f "$WORKING_DIR/../.env" ]] && source "$WORKING_DIR/../.env"
set +a

S5CMD_BIN="${S5CMD_BIN:-$HOME/go/bin/s5cmd}"
S3_ENDPOINT="${S3_ENDPOINT:-https://t3.storage.dev}"
BACKUP_BUCKET="${BACKUP_BUCKET:-s3://cafecito-archives-new/processingcache}"
export AWS_ACCESS_KEY_ID="${S3_ACCESS_KEY_ID:-}"
export AWS_SECRET_ACCESS_KEY="${S3_SECRET_ACCESS_KEY:-}"
export AWS_DEFAULT_REGION="${S3_REGION:-auto}"

require_s3_environment() {
  if [[ -z "${S3_ACCESS_KEY_ID:-}" ]]; then
    echo "Error: S3_ACCESS_KEY_ID is required" >&2
    return 1
  fi
  if [[ -z "${S3_SECRET_ACCESS_KEY:-}" ]]; then
    echo "Error: S3_SECRET_ACCESS_KEY is required" >&2
    return 1
  fi
  if [[ ! -x "$S5CMD_BIN" ]]; then
    echo "Error: s5cmd not found or not executable at $S5CMD_BIN" >&2
    return 1
  fi
}

S5CMD_ARGS=(--endpoint-url "$S3_ENDPOINT")
require_s3_environment


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
