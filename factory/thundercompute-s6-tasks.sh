#!/bin/bash
# s6 oneshot entry: start run_pipeline.sh in the background at container boot.
set -euo pipefail

# Runtime fallback defaults (manual runs without install).
EMBEDDER_BATCH=128
CLUSTERING_BATCH=256
EXTRACTOR_BATCH=40
DIGESTOR_BATCH=128
CONSOLIDATOR_BATCH=128

mkdir -p /home/ubuntu/.logs
chown -R ubuntu:ubuntu /home/ubuntu/.logs
LOG="/home/ubuntu/.logs/pipeline.log"
WORKDIR="/home/ubuntu/pycoffeemaker"
SCRIPT="$WORKDIR/run_pipeline.sh"

ARGS=(
    --embedder "$EMBEDDER_BATCH"
    --extractor "$EXTRACTOR_BATCH"
    --clustering "$CLUSTERING_BATCH"
    --digestor "$DIGESTOR_BATCH"
    --consolidator "$CONSOLIDATOR_BATCH"
)

ARGS_QUOTED="$(printf '%q ' "${ARGS[@]}")"

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
        export LOG_DIR=/home/ubuntu/.logs
        export PROCESSING_WINDOW=2
        cd '$WORKDIR'
        echo '=== [S6 BOOT $(date -u +%Y-%m-%dT%H:%M:%SZ)] ==='
        exec bash '$SCRIPT' $ARGS_QUOTED
    "
) >>"$LOG" 2>&1 &

exit 0
