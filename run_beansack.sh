if [[ -n "${BASH_SOURCE[0]:-}" && -f "${BASH_SOURCE[0]}" ]]; then
    WORKING_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
else
    WORKING_DIR="$(pwd -P)"
fi

PYTHON="$WORKING_DIR/.venv/bin/python"
RUN="$WORKING_DIR/run.py"

LOCAL_CLSCACHE="$WORKING_DIR/.cache/clscache"
AWS_CREDENTIALS_FILE="${AWS_CREDENTIALS_FILE:-$HOME/.aws/credentials}"
S3_ENDPOINT="${S3_ENDPOINT:-https://t3.storage.dev}"
AWS_S3_ARGS=(--endpoint-url "$S3_ENDPOINT")
BACKUP_BUCKET="s3://cafecito-archives-new/processingcache"

EMBEDDER_BATCH_SIZE="${EMBEDDER_BATCH_SIZE:-512}"
EXTRACTOR_BATCH_SIZE="${EXTRACTOR_BATCH_SIZE:-24}"
CLASSIFIER_BATCH_SIZE="${CLASSIFIER_BATCH_SIZE:-128}"


while [[ $# -gt 0 ]]; do
    case "$1" in
        --embedder-batch-size|--embedder_batch_size)
            EMBEDDER_BATCH_SIZE="$2"
            shift 2
            ;;
        --extractor-batch-size|--extractor_batch_size)
            EXTRACTOR_BATCH_SIZE="$2"
            shift 2
            ;;
        --classifier-batch-size|--classifier_batch_size)
            CLASSIFIER_BATCH_SIZE="$2"
            shift 2
            ;;
        *)
            echo "Unknown argument: $1" >&2
            exit 2
            ;;
    esac
done

run_embedder() {
    $PYTHON $RUN --mode EMBEDDER --batch_size $EMBEDDER_BATCH_SIZE
}

run_extractor() {
    $PYTHON $RUN --mode EXTRACTOR --batch_size $EXTRACTOR_BATCH_SIZE
}

run_classifier() {
    $PYTHON $RUN --mode CLASSIFIER --batch_size $CLASSIFIER_BATCH_SIZE    
}

backup_clscache() {
    echo "=== [STARTING] ZVEC Classification Cache Backup ==="
    local dump_file="$WORKING_DIR/.cache/clscache.tar.gz"
    tar -czf "$dump_file" "$LOCAL_CLSCACHE"
    AWS_SHARED_CREDENTIALS_FILE="$AWS_CREDENTIALS_FILE" AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-auto}" \
        aws s3 cp "$dump_file" "$BACKUP_BUCKET/" "${AWS_S3_ARGS[@]}"
    rm -f "$dump_file"
    echo "=== [FINISHED] ZVEC Classification Cache Backup ==="
}

echo "=== [STARTING INDEXERS] ==="

run_embedder

run_extractor &
( run_classifier; backup_clscache ) &
wait
echo "=== [FINISHED INDEXERS] ==="

$PYTHON $WORKING_DIR/machine_ops.py --action stop