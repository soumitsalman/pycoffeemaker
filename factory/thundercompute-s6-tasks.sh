#!/bin/bash
# s6 oneshot entry: start run_pipeline.sh in the background at container boot.
set -euo pipefail

LOG="/home/ubuntu/pycoffeemaker/.logs/pipeline.log"
WORKDIR="/home/ubuntu/pycoffeemaker"
SCRIPT="$WORKDIR/run_pipeline.sh"
ARGS=(--embedder 512 --clustering 512 --digestor 64 --consolidator 64)

mkdir -p "$WORKDIR/.logs"

if pgrep -f "$SCRIPT" >/dev/null 2>&1; then
    echo "=== [S6 BOOT $(date -u +%Y-%m-%dT%H:%M:%SZ)] pipeline already running, skipping ===" >>"$LOG"
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
        export LOG_DIR="/home/ubuntu/pycoffeemaker/.logs/$(date +%y-%m-%d-%H-%M-%S).log"
        export VLLM_MAX_NUM_BATCHED_TOKENS=98304
        export VLLM_MAX_NUM_SEQS=256
        export VLLM_GPU_MEMORY_UTILIZATION=0.99
        cd '$WORKDIR'
        echo '=== [S6 BOOT $(date -u +%Y-%m-%dT%H:%M:%SZ)] ==='
        exec bash '$SCRIPT' ${ARGS[*]}
    "
) >>"$LOG" 2>&1 &

exit 0
