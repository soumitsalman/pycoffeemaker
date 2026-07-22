#!/bin/bash
set -euo pipefail

if [[ -n "${BASH_SOURCE[0]:-}" && -f "${BASH_SOURCE[0]}" ]]; then
    WORKING_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
else
    WORKING_DIR="$(pwd -P)"
fi

PYTHON="$WORKING_DIR/.venv/bin/python"
RUN="$WORKING_DIR/run.py"

set -a
# Pipeline defaults (override via .env)
PG_TIMEOUT=180
COLLECTOR_TIMEOUT=120
MAX_DOCUMENT_LEN=6144

EMBEDDER_PATH=codefuse-ai/F2LLM-v2-80M
EMBEDDER_CONTEXT_LEN=8192
VECTOR_LEN=320

EXTRACTOR_PATH=knowledgator/modern-gliner-bi-base-v1.0
EXTRACTOR_CONTEXT_LEN=4096

CLUSTER_EPS=0.45

VLLM_ENABLE_V1_MULTIPROCESSING=0

DIGESTOR_PATH=vllm://nvidia/NVIDIA-Nemotron-3-Nano-4B-BF16
DIGESTOR_CONTEXT_LEN=8192
DIGESTOR_TEMPERATURE=0.3
DIGESTOR_TOP_P=0.95
DIGESTOR_REPETITION_PENALTY=1.25

CONSOLIDATOR_PATH=vllm://nvidia/NVIDIA-Nemotron-3-Nano-4B-BF16
CONSOLIDATOR_CONTEXT_LEN=16384
CONSOLIDATOR_TEMPERATURE=0.8
CONSOLIDATOR_TOP_P=0.95
CONSOLIDATOR_REPETITION_PENALTY=1.25
CONSOLIDATION_EPS=0.53
CONSOLIDATION_MAX_SIZE=48
# shellcheck disable=SC1091
[[ -f "$WORKING_DIR/.env" ]] && source "$WORKING_DIR/.env"
set +a

RUN_COLLECTOR=0
RUN_EMBEDDER=0
RUN_EXTRACTOR=0
RUN_CLUSTERING=0
RUN_DIGESTOR=0
RUN_CONSOLIDATOR=0
RUN_PORTER=0

COLLECTOR_BATCH_SIZE=128
EMBEDDER_BATCH_SIZE=128
EXTRACTOR_BATCH_SIZE=24
CLUSTERING_BATCH_SIZE=128
DIGESTOR_BATCH_SIZE=32
CONSOLIDATOR_BATCH_SIZE=32
PORTER_BATCH_SIZE=32

usage() {
    cat >&2 <<'EOF'
Usage: run_pipeline.sh [STAGE BATCH_SIZE]...

Each flag enables a stage and sets its batch size:
  --collector N
  --embedder N
  --extractor N
  --clustering N
  --digestor N
  --consolidator N
  --porter N
EOF
    exit 2
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --collector)
            RUN_COLLECTOR=1
            COLLECTOR_BATCH_SIZE="${2:?--collector requires batch_size}"
            shift 2
            ;;
        --embedder)
            RUN_EMBEDDER=1
            EMBEDDER_BATCH_SIZE="${2:?--embedder requires batch_size}"
            shift 2
            ;;
        --extractor)
            RUN_EXTRACTOR=1
            EXTRACTOR_BATCH_SIZE="${2:?--extractor requires batch_size}"
            shift 2
            ;;
        --clustering)
            RUN_CLUSTERING=1
            CLUSTERING_BATCH_SIZE="${2:?--clustering requires batch_size}"
            shift 2
            ;;
        --digestor)
            RUN_DIGESTOR=1
            DIGESTOR_BATCH_SIZE="${2:?--digestor requires batch_size}"
            shift 2
            ;;
        --consolidator)
            RUN_CONSOLIDATOR=1
            CONSOLIDATOR_BATCH_SIZE="${2:?--consolidator requires batch_size}"
            shift 2
            ;;
        --porter)
            RUN_PORTER=1
            PORTER_BATCH_SIZE="${2:?--porter requires batch_size}"
            shift 2
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo "Unknown argument: $1" >&2
            usage
            ;;
    esac
done

run_collector() {
    $PYTHON "$RUN" --mode COLLECTOR --batch_size "$COLLECTOR_BATCH_SIZE"
}

run_embedder() {
    $PYTHON "$RUN" --mode EMBEDDER --batch_size "$EMBEDDER_BATCH_SIZE"
}

run_extractor() {
    $PYTHON "$RUN" --mode EXTRACTOR --batch_size "$EXTRACTOR_BATCH_SIZE"
}

run_clustering() {
    local clscache_path="$WORKING_DIR/.cache/clscache"

    restore_clscache
    CLASSIFICATION_CACHE="$clscache_path" $PYTHON "$RUN" --mode CLUSTERING --batch_size "$CLUSTERING_BATCH_SIZE"
    backup_clscache
    rm -rf "$clscache_path"
}

run_digestor() {
    $PYTHON "$RUN" --mode DIGESTOR --batch_size "$DIGESTOR_BATCH_SIZE"
}

run_consolidator() {
    $PYTHON "$RUN" --mode CONSOLIDATOR --batch_size "$CONSOLIDATOR_BATCH_SIZE"
}

run_porter() {
    $PYTHON "$RUN" --mode PORTER --batch_size "$PORTER_BATCH_SIZE"
}

backup_clscache() {
    local clscache_root="$WORKING_DIR/.cache"
    local clscache_name="clscache"
    local aws_credentials_file="${AWS_CREDENTIALS_FILE:-$HOME/.aws/credentials}"
    local s3_endpoint="${S3_ENDPOINT:-https://t3.storage.dev}"
    local -a aws_s3_args=(--endpoint-url "$s3_endpoint")
    local backup_bucket="s3://cafecito-archives-new/processingcache"
    local dump_file="$clscache_root/clscache.tar.gz"
    local s3_key="clscache-${VECTOR_LEN}.tar.gz"

    echo "=== [STARTING] ZVEC Classification Cache Backup ==="
    tar -czf "$dump_file" -C "$clscache_root" "$clscache_name"
    AWS_SHARED_CREDENTIALS_FILE="$aws_credentials_file" AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-auto}" \
        aws s3 cp "$dump_file" "$backup_bucket/$s3_key" "${aws_s3_args[@]}"
    rm -f "$dump_file"
    echo "=== [FINISHED] ZVEC Classification Cache Backup ==="
}

restore_clscache() {
    local clscache_root="$WORKING_DIR/.cache"
    local aws_credentials_file="${AWS_CREDENTIALS_FILE:-$HOME/.aws/credentials}"
    local s3_endpoint="${S3_ENDPOINT:-https://t3.storage.dev}"
    local -a aws_s3_args=(--endpoint-url "$s3_endpoint")
    local backup_bucket="s3://cafecito-archives-new/processingcache"
    local dump_file="$clscache_root/clscache.tar.gz"
    local s3_key="clscache-${VECTOR_LEN}.tar.gz"

    echo "=== [STARTING] ZVEC Classification Cache Restore ==="
    mkdir -p "$clscache_root"
    AWS_SHARED_CREDENTIALS_FILE="$aws_credentials_file" AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-auto}" \
        aws s3 cp "$backup_bucket/$s3_key" "$dump_file" "${aws_s3_args[@]}"
    tar -xzf "$dump_file" -C "$clscache_root"
    rm -f "$dump_file"
    echo "=== [FINISHED] ZVEC Classification Cache Restore ==="
}

COLLECTOR_PID=""
PORTER_PID=""

echo "=== [STARTING PIPELINE] ==="

if [[ $RUN_COLLECTOR -eq 1 ]]; then
    run_collector &
    COLLECTOR_PID=$!
fi

if [[ $RUN_EMBEDDER -eq 1 ]]; then
    run_embedder
fi

CLUSTERING_PID=""

if [[ $RUN_CLUSTERING -eq 1 ]]; then
    run_clustering &
    CLUSTERING_PID=$!
fi

if [[ $RUN_EXTRACTOR -eq 1 ]]; then
    run_extractor
fi

if [[ $RUN_DIGESTOR -eq 1 ]]; then
    run_digestor
fi

if [[ -n "$CLUSTERING_PID" ]]; then
    wait "$CLUSTERING_PID"
fi

if [[ $RUN_CONSOLIDATOR -eq 1 ]]; then
    run_consolidator
fi

if [[ $RUN_PORTER -eq 1 ]]; then
    run_porter &
    PORTER_PID=$!
fi

if [[ -n "$COLLECTOR_PID" ]]; then
    wait "$COLLECTOR_PID"
fi
if [[ -n "$PORTER_PID" ]]; then
    wait "$PORTER_PID"
fi

echo "=== [FINISHED PIPELINE] ==="

: "${SHUTDOWN_URL:?SHUTDOWN_URL not set in .env}"
curl -fsS -X POST "$SHUTDOWN_URL"
