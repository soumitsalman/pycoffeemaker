if [[ -n "${BASH_SOURCE[0]:-}" && -f "${BASH_SOURCE[0]}" ]]; then
    WORKING_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
else
    WORKING_DIR="$(pwd -P)"
fi

PYTHON="$WORKING_DIR/.venv/bin/python"
RUN="$WORKING_DIR/run.py"

LOCAL_CLSCACHE="$WORKING_DIR/.cache/clscache"
S5CMD_BIN="$HOME/go/bin/s5cmd"
S5CMD_ARGS=(--credentials-file "$HOME/.aws/credentials" --endpoint-url https://t3.storage.dev)
BACKUP_BUCKET="s3://cafecito-archives-new/processingcache"

EMBEDDER_BATCH_SIZE="${EMBEDDER_BATCH_SIZE:-192}"
EXTRACTOR_BATCH_SIZE="${EXTRACTOR_BATCH_SIZE:-32}"
CLASSIFIER_BATCH_SIZE="${CLASSIFIER_BATCH_SIZE:-128}"
DIGESTOR_BATCH_SIZE="${DIGESTOR_BATCH_SIZE:-32}"
CONSOLIDATOR_BATCH_SIZE="${CONSOLIDATOR_BATCH_SIZE:-8}"

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
        --digestor-batch-size|--digestor_batch_size)
            DIGESTOR_BATCH_SIZE="$2"
            shift 2
            ;;
        --consolidator-batch-size|--consolidator_batch_size)
            CONSOLIDATOR_BATCH_SIZE="$2"
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

run_digestor() {
    $PYTHON $RUN --mode DIGESTOR --batch_size $DIGESTOR_BATCH_SIZE
}

run_consolidator() {
    $PYTHON $RUN --mode CONSOLIDATOR --batch_size $CONSOLIDATOR_BATCH_SIZE
}

run_classifier() {
    $PYTHON $RUN --mode CLASSIFIER --batch_size $CLASSIFIER_BATCH_SIZE    
}

backup_clscache() {
    echo "=== [STARTING] ZVEC Classification Cache Backup ==="
    local dump_file="$WORKING_DIR/.cache/clscache.tar.gz"
    tar -czf "$dump_file" "$LOCAL_CLSCACHE"
    "$S5CMD_BIN" "${S5CMD_ARGS[@]}" cp "$dump_file" "$BACKUP_BUCKET/"
    rm -f "$dump_file"
    echo "=== [FINISHED] ZVEC Classification Cache Backup ==="
}


# run_extractor_and_digestor() {
#     run_extractor
#     run_digestor
# }

# run_classifier_and_backup_clscache() {
#     run_classifier
#     backup_clscache
# }

# run sequence
# embedder runs first
# extractor and classifier starts parallelly after embedder finishes
# since the main work for classifier is finished early and most of the time it spends on optimization and backup

echo "=== [STARTING] ==="

run_embedder

run_extractor &
run_classifier &
wait
run_digestor &
backup_clscache &
wait
# run_consolidator
echo "=== [FINISHED] ==="

$PYTHON $WORKING_DIR/machine_ops.py --action stop