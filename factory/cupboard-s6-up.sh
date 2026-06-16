#!/bin/bash
# s6 oneshot entry: start run_cupboard.sh in the background at container boot.
set -euo pipefail

LOG="/home/ubuntu/pycoffeemaker/.logs/cupboard.log"
WORKDIR="/home/ubuntu/pycoffeemaker"
SCRIPT="$WORKDIR/run_cupboard.sh"
ARGS=(--consolidator-batch-size 64 --digestor-batch-size 64)

mkdir -p "$WORKDIR/.logs"

if pgrep -f "$SCRIPT" >/dev/null 2>&1; then
    echo "=== [S6 BOOT $(date -u +%Y-%m-%dT%H:%M:%SZ)] cupboard already running, skipping ===" >>"$LOG"
    exit 0
fi

if command -v /command/s6-setuidgid >/dev/null 2>&1; then
    RUNAS=(/command/s6-setuidgid ubuntu)
elif command -v s6-setuidgid >/dev/null 2>&1; then
    RUNAS=(s6-setuidgid ubuntu)
else
    RUNAS=(runuser -u ubuntu --)
fi

(
    sleep 10
    "${RUNAS[@]}" bash -lc "
        cd '$WORKDIR'
        echo '=== [S6 BOOT $(date -u +%Y-%m-%dT%H:%M:%SZ)] ==='
        exec bash '$SCRIPT' ${ARGS[*]}
    "
) >>"$LOG" 2>&1 &

exit 0
