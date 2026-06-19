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
# shellcheck disable=SC1091
source "$WORKING_DIR/.env"
set +a

RUN_COLLECTOR=0
RUN_EMBEDDER=0
RUN_EXTRACTOR=0
RUN_CLASSIFIER=0
RUN_DIGESTOR=0
RUN_CONSOLIDATOR=0
RUN_PORTER=0

COLLECTOR_BATCH_SIZE=128
EMBEDDER_BATCH_SIZE=512
EXTRACTOR_BATCH_SIZE=24
CLASSIFIER_BATCH_SIZE=128
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
  --classifier N
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
        --classifier)
            RUN_CLASSIFIER=1
            CLASSIFIER_BATCH_SIZE="${2:?--classifier requires batch_size}"
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

run_classifier() {
    $PYTHON "$RUN" --mode CLASSIFIER --batch_size "$CLASSIFIER_BATCH_SIZE"
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
    local local_clscache="$WORKING_DIR/.cache/clscache"
    local aws_credentials_file="${AWS_CREDENTIALS_FILE:-$HOME/.aws/credentials}"
    local s3_endpoint="${S3_ENDPOINT:-https://t3.storage.dev}"
    local -a aws_s3_args=(--endpoint-url "$s3_endpoint")
    local backup_bucket="s3://cafecito-archives-new/processingcache"
    local dump_file="$WORKING_DIR/.cache/clscache.tar.gz"

    echo "=== [STARTING] ZVEC Classification Cache Backup ==="
    tar -czf "$dump_file" -C "$(dirname "$local_clscache")" "$(basename "$local_clscache")"
    AWS_SHARED_CREDENTIALS_FILE="$aws_credentials_file" AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-auto}" \
        aws s3 cp "$dump_file" "$backup_bucket/" "${aws_s3_args[@]}"
    rm -f "$dump_file"
    echo "=== [FINISHED] ZVEC Classification Cache Backup ==="
}

COLLECTOR_PID=""
BACKUP_PID=""
PORTER_PID=""

echo "=== [STARTING PIPELINE] ==="

if [[ $RUN_COLLECTOR -eq 1 ]]; then
    run_collector &
    COLLECTOR_PID=$!
fi

if [[ $RUN_EMBEDDER -eq 1 ]]; then
    run_embedder
fi

if [[ $RUN_EXTRACTOR -eq 1 || $RUN_CLASSIFIER -eq 1 ]]; then
    if [[ $RUN_EXTRACTOR -eq 1 ]]; then
        run_extractor &
    fi
    if [[ $RUN_CLASSIFIER -eq 1 ]]; then
        run_classifier &
    fi
    wait
fi

if [[ $RUN_CLASSIFIER -eq 1 ]]; then
    backup_clscache &
    BACKUP_PID=$!
fi

if [[ $RUN_DIGESTOR -eq 1 ]]; then
    run_digestor
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
if [[ -n "$BACKUP_PID" ]]; then
    wait "$BACKUP_PID"
fi
if [[ -n "$PORTER_PID" ]]; then
    wait "$PORTER_PID"
fi

echo "=== [FINISHED PIPELINE] ==="

: "${SHUTDOWN_URL:?SHUTDOWN_URL not set in .env}"
curl -fsS -X POST "$SHUTDOWN_URL"
