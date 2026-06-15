if [[ -n "${BASH_SOURCE[0]:-}" && -f "${BASH_SOURCE[0]}" ]]; then
    WORKING_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
else
    WORKING_DIR="$(pwd -P)"
fi

PYTHON="$WORKING_DIR/.venv/bin/python"
RUN="$WORKING_DIR/run.py"

DIGESTOR_BATCH_SIZE="${DIGESTOR_BATCH_SIZE:-32}"
CONSOLIDATOR_BATCH_SIZE="${CONSOLIDATOR_BATCH_SIZE:-32}"

while [[ $# -gt 0 ]]; do
    case "$1" in
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

run_digestor() {
    $PYTHON $RUN --mode DIGESTOR --batch_size $DIGESTOR_BATCH_SIZE
}

run_consolidator() {
    $PYTHON $RUN --mode CONSOLIDATOR --batch_size $CONSOLIDATOR_BATCH_SIZE
}

echo "=== [STARTING CUPBOARD] ==="

run_consolidator
run_digestor


echo "=== [FINISHED CUPBOARD] ==="

$PYTHON $WORKING_DIR/machine_ops.py --action stop