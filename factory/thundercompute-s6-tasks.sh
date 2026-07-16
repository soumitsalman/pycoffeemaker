#!/bin/bash
# s6 oneshot entry: start run_pipeline.sh in the background at container boot.
set -euo pipefail

# Runtime fallback defaults (manual runs without install).
EMBEDDER_BATCH=448
CLUSTERING_BATCH=448
DIGESTOR_BATCH=128
CONSOLIDATOR_BATCH=128
ARGS_FILE="/etc/thundercompute/pipeline.args"

LOG="/home/ubuntu/pycoffeemaker/.logs/pipeline.log"
WORKDIR="/home/ubuntu/pycoffeemaker"
SCRIPT="$WORKDIR/run_pipeline.sh"

resolve_preset() {
    case "$1" in
        embedder)
            ARGS=(--embedder "$EMBEDDER_BATCH")
            ;;
        digestor)
            ARGS=(--clustering "$CLUSTERING_BATCH" --digestor "$DIGESTOR_BATCH")
            ;;
        embedder_digestor)
            ARGS=(
                --embedder "$EMBEDDER_BATCH"
                --clustering "$CLUSTERING_BATCH"
                --digestor "$DIGESTOR_BATCH"
            )
            ;;
        consolidator)
            ARGS=(--consolidator "$CONSOLIDATOR_BATCH")
            ;;
        all)
            ARGS=(
                --embedder "$EMBEDDER_BATCH"
                --clustering "$CLUSTERING_BATCH"
                --digestor "$DIGESTOR_BATCH"
                --consolidator "$CONSOLIDATOR_BATCH"
            )
            ;;
        *)
            echo "unknown preset: $1 (expected embedder, digestor, embedder_digestor, consolidator, all)" >&2
            return 1
            ;;
    esac
}

if [[ $# -gt 0 && "$1" == --* ]]; then
    ARGS=("$@")
elif [[ $# -gt 0 ]]; then
    resolve_preset "$1"
elif [[ -f "$ARGS_FILE" ]]; then
    mapfile -t ARGS < "$ARGS_FILE"
elif [[ -f /etc/thundercompute/pipeline.mode ]]; then
    MODE="$(cat /etc/thundercompute/pipeline.mode)"
    resolve_preset "$MODE"
else
    resolve_preset all
fi

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
        export PROCESSING_WINDOW=7
        export VLLM_MAX_NUM_BATCHED_TOKENS=98304
        export VLLM_MAX_NUM_SEQS=256
        export VLLM_GPU_MEMORY_UTILIZATION=0.95
        cd '$WORKDIR'
        echo '=== [S6 BOOT $(date -u +%Y-%m-%dT%H:%M:%SZ)] ==='
        exec bash '$SCRIPT' $ARGS_QUOTED
    "
) >>"$LOG" 2>&1 &

exit 0
