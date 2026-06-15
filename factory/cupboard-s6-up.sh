#!/bin/bash
# s6 oneshot entry: start run_cupboard.sh in the background at container boot.
set -euo pipefail

LOG="/home/ubuntu/pycoffeemaker/.logs/cupboard.log"
WORKDIR="/home/ubuntu/pycoffeemaker"
SCRIPT="$WORKDIR/run_cupboard.sh"

mkdir -p "$WORKDIR/.logs"

(
    sleep 10
    if command -v s6-setuidgid >/dev/null 2>&1; then
        RUNAS=(s6-setuidgid ubuntu)
    else
        RUNAS=(runuser -u ubuntu --)
    fi
    "${RUNAS[@]}" bash -lc "
        cd '$WORKDIR'
        echo '=== [S6 BOOT $(date -u +%Y-%m-%dT%H:%M:%SZ)] ==='
        exec '$SCRIPT' --consolidator-batch-size 64 --digestor-batch-size 128
    "
) >>"$LOG" 2>&1 &

exit 0
