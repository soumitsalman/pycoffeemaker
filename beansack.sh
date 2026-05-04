if [[ -n "${BASH_SOURCE[0]:-}" && -f "${BASH_SOURCE[0]}" ]]; then
    WORKING_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
else
    WORKING_DIR="$(pwd -P)"
fi

PYTHON="$WORKING_DIR/.venv/bin/python"
RUN="$WORKING_DIR/run.py"

LOCAL_CLSCACHE="$WORKING_DIR/.cache/clsstore"
S5CMD_BIN="$HOME/go/bin/s5cmd"
S5CMD_ARGS=(--credentials-file "$HOME/.aws/credentials" --endpoint-url https://t3.storage.dev)
BACKUP_BUCKET="s3://cafecito-archives-new/processingcache"

EMBEDDER_BATCH_SIZE="${EMBEDDER_BATCH_SIZE:-192}"
EXTRACTOR_BATCH_SIZE="${EXTRACTOR_BATCH_SIZE:-32}"
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

backup_clscache() {
    echo "=== [STARTING] ZVEC Classification Cache Backup ==="
    local dump_file="$WORKING_DIR/.cache/clscache.tar.gz"
    tar -czf "$dump_file" "$LOCAL_CLSCACHE"
    "$S5CMD_BIN" "${S5CMD_ARGS[@]}" cp "$dump_file" "$BACKUP_BUCKET/"
    rm -f "$dump_file"
    echo "=== [FINISHED] ZVEC Classification Cache Backup ==="
}

run_classifier() {
    $PYTHON $RUN --mode CLASSIFIER --batch_size $CLASSIFIER_BATCH_SIZE
}

run_porter() {
    $PYTHON $RUN --mode PORTER 
}

# run sequence
# embedder runs first
# extractor and classifier starts parallelly after embedder finishes
# porter starts after extractor without waiting for classifier 
# since the main work for classifier is finished early and most of the time it spends on optimization and backup
for round in 1 2; do
    echo "=== [STARTING] Round $round/2 ==="

    run_embedder

    run_extractor &
    run_classifier &
    wait
    echo "=== [FINISHED] Round $round/2 ==="
done

run_porter &
backup_clscache &
wait

$PYTHON $WORKING_DIR/machine_ops.py --action stop --instance tensordock