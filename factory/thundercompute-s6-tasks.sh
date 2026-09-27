#!/bin/bash
# s6 oneshot entry: start run_pipeline.sh in the background at container boot.
set -euo pipefail

# Batch sizes are a linear fit to GPU VRAM (GiB), exact at ~48 and ~80:
#   embedder     = round(1.5*V + 8)    # 80 @ 48GB, 128 @ 80GB
#   extractor    = round(0.75*V + 4)   # 40 @ 48GB,  64 @ 80GB
#   clustering   = 512                  # CPU stage; same at both sizes
#   digestor     = round(8*V - 192)     # 192 @ 48GB, 448 @ 80GB
#   consolidator = round(4*V - 64)      # 128 @ 48GB, 256 @ 80GB
# V is total memory of the first GPU in GiB (nvidia-smi MiB / 1024).
# Below ~24GB, digestor hits the floor of 1.

gpu_vram_mib() {
    local raw line
    raw="$(nvidia-smi --query-gpu=memory.total --format=csv,noheader,nounits 2>/dev/null)" || return 1
    line="${raw%%$'\n'*}"
    line="${line//[[:space:]]/}"
    [[ "$line" =~ ^[0-9]+$ ]] || return 1
    printf '%s\n' "$line"
}

# Integer rounding of the fit above. mib is nvidia-smi memory.total.
set_batches_from_vram_mib() {
    local mib="$1" value
    EMBEDDER_BATCH=$(( (3 * mib + 1024) / 2048 + 8 ))
    EXTRACTOR_BATCH=$(( (3 * mib + 2048) / 4096 + 4 ))
    CLUSTERING_BATCH=512
    DIGESTOR_BATCH=$(( (mib + 64) / 128 - 192 ))
    CONSOLIDATOR_BATCH=$(( (mib + 128) / 256 - 64 ))
    for value in EMBEDDER_BATCH EXTRACTOR_BATCH DIGESTOR_BATCH CONSOLIDATOR_BATCH; do
        if (( ${!value} < 1 )); then
            printf -v "$value" '%s' 1
        fi
    done
}

if [[ "${1:-}" == "--emit-batch-env" ]]; then
    mib="$(gpu_vram_mib)"
    set_batches_from_vram_mib "$mib"
    printf 'EMBEDDER_BATCH=%s\n' "$EMBEDDER_BATCH"
    printf 'EXTRACTOR_BATCH=%s\n' "$EXTRACTOR_BATCH"
    printf 'CLUSTERING_BATCH=%s\n' "$CLUSTERING_BATCH"
    printf 'DIGESTOR_BATCH=%s\n' "$DIGESTOR_BATCH"
    printf 'CONSOLIDATOR_BATCH=%s\n' "$CONSOLIDATOR_BATCH"
    echo "GPU VRAM ${mib} MiB -> embedder=${EMBEDDER_BATCH} extractor=${EXTRACTOR_BATCH} clustering=${CLUSTERING_BATCH} digestor=${DIGESTOR_BATCH} consolidator=${CONSOLIDATOR_BATCH}" >&2
    exit 0
fi

# Install writes these before boot. Manual runs size from the live GPU.
# No-GPU fallback is the 48GB row.
if [[ -z "${EMBEDDER_BATCH:-}" ]]; then
    if mib="$(gpu_vram_mib)"; then
        set_batches_from_vram_mib "$mib"
    else
        EMBEDDER_BATCH=80
        EXTRACTOR_BATCH=40
        CLUSTERING_BATCH=512
        DIGESTOR_BATCH=192
        CONSOLIDATOR_BATCH=128
    fi
fi

mkdir -p /home/ubuntu/.logs
chown -R ubuntu:ubuntu /home/ubuntu/.logs
LOG="/home/ubuntu/.logs/pipeline-$(date +%Y-%m-%d-%H-%M-%S).log"
WORKDIR="/home/ubuntu/pycoffeemaker"
SCRIPT="$WORKDIR/run_pipeline.sh"

ARGS=(
    # --collector "$COLLECTOR_BATCH"
    --embedder "$EMBEDDER_BATCH"
    --extractor "$EXTRACTOR_BATCH"
    --clustering "$CLUSTERING_BATCH"
    --digestor "$DIGESTOR_BATCH"
    --consolidator "$CONSOLIDATOR_BATCH"
)

ARGS_QUOTED="$(printf '%q ' "${ARGS[@]}")"

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
        export PROCESSING_WINDOW=3
        cd '$WORKDIR'
        echo '=== [S6 BOOT $(date -u +%Y-%m-%dT%H:%M:%SZ)] embedder=$EMBEDDER_BATCH extractor=$EXTRACTOR_BATCH clustering=$CLUSTERING_BATCH digestor=$DIGESTOR_BATCH consolidator=$CONSOLIDATOR_BATCH ==='
        exec bash '$SCRIPT' $ARGS_QUOTED
    "
) >>"$LOG" 2>&1 &

exit 0
