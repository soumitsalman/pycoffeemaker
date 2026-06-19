#!/bin/bash
# s6 oneshot entry: start run_pipeline.sh in the background at container boot.
set -euo pipefail

LOG="/home/ubuntu/pycoffeemaker/.logs/thundercompute-tasks.log"
WORKDIR="/home/ubuntu/pycoffeemaker"
SCRIPT="$WORKDIR/run_pipeline.sh"
ARGS=(--digestor 128 --consolidator 128)

mkdir -p "$WORKDIR/.logs"

if pgrep -f "$SCRIPT" >/dev/null 2>&1; then
    echo "=== [S6 BOOT $(date -u +%Y-%m-%dT%H:%M:%SZ)] thundercompute-tasks already running, skipping ===" >>"$LOG"
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
        export HOME=/home/ubuntu
        cd '$WORKDIR'
        echo '=== [S6 BOOT $(date -u +%Y-%m-%dT%H:%M:%SZ)] ==='
        exec bash '$SCRIPT' ${ARGS[*]}
    "
) >>"$LOG" 2>&1 &

exit 0
