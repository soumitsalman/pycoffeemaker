#!/bin/bash
# Install thundercompute tasks as an s6 oneshot in the user boot bundle.
set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
UP_SCRIPT="$ROOT/factory/thundercompute-s6-tasks.sh"
S6_ROOT="/etc/s6-overlay/s6-rc.d"
SERVICE="thundercompute-tasks"
ARGS_FILE="/etc/thundercompute/pipeline.args"

# Deploy-time defaults — edit here before reinstall/redeploy.
EMBEDDER_BATCH=128
CLUSTERING_BATCH=256
DIGESTOR_BATCH=128
CONSOLIDATOR_BATCH=128

usage() {
    cat >&2 <<EOF
Usage: $0 [MODE]
       $0 --STAGE BATCH_SIZE [--STAGE BATCH_SIZE ...]

Presets (default: all):
  embedder          --embedder $EMBEDDER_BATCH
  digestor          --clustering $CLUSTERING_BATCH --digestor $DIGESTOR_BATCH
  embedder_digestor --embedder $EMBEDDER_BATCH --clustering $CLUSTERING_BATCH --digestor $DIGESTOR_BATCH
  consolidator      --consolidator $CONSOLIDATOR_BATCH
  all               all analyzer stages

Custom flags replace preset mode entirely. Allowed stages:
  --collector N  --embedder N  --extractor N  --clustering N
  --digestor N   --consolidator N  --porter N
EOF
    exit 2
}

resolve_preset() {
    case "$1" in
        embedder)
            printf '%s\n' --embedder "$EMBEDDER_BATCH"
            ;;
        digestor)
            printf '%s\n' --clustering "$CLUSTERING_BATCH" --digestor "$DIGESTOR_BATCH"
            ;;
        embedder_digestor)
            printf '%s\n' \
                --embedder "$EMBEDDER_BATCH" \
                --clustering "$CLUSTERING_BATCH" \
                --digestor "$DIGESTOR_BATCH"
            ;;
        consolidator)
            printf '%s\n' --consolidator "$CONSOLIDATOR_BATCH"
            ;;
        all)
            printf '%s\n' \
                --embedder "$EMBEDDER_BATCH" \
                --clustering "$CLUSTERING_BATCH" \
                --digestor "$DIGESTOR_BATCH" \
                --consolidator "$CONSOLIDATOR_BATCH"
            ;;
        *)
            echo "unknown preset: $1" >&2
            usage
            ;;
    esac
}

validate_pipeline_args() {
    if [[ $# -eq 0 ]]; then
        echo "custom deploy requires at least one stage flag" >&2
        return 2
    fi

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --collector|--embedder|--extractor|--clustering|--digestor|--consolidator|--porter)
                if [[ $# -lt 2 ]]; then
                    echo "$1 requires batch_size" >&2
                    return 2
                fi
                if [[ ! "$2" =~ ^[1-9][0-9]*$ ]]; then
                    echo "$1 requires positive integer, got: $2" >&2
                    return 2
                fi
                shift 2
                ;;
            --classifier)
                echo "unknown flag: --classifier (use --clustering)" >&2
                return 2
                ;;
            -h|--help)
                usage
                ;;
            *)
                echo "unknown flag: $1" >&2
                return 2
                ;;
        esac
    done
}

if [[ $# -gt 0 && "$1" == --* ]]; then
    validate_pipeline_args "$@"
    DEPLOY_LABEL="custom"
    mapfile -t ARGS < <(printf '%s\n' "$@")
elif [[ $# -gt 0 ]]; then
    case "$1" in
        -h|--help) usage ;;
        embedder|digestor|embedder_digestor|consolidator|all)
            DEPLOY_LABEL="$1"
            mapfile -t ARGS < <(resolve_preset "$1")
            ;;
        *)
            echo "unknown preset: $1" >&2
            usage
            ;;
    esac
else
    DEPLOY_LABEL="all"
    mapfile -t ARGS < <(resolve_preset all)
fi

if [[ "$(id -u)" -ne 0 ]]; then
    echo "Run with sudo: sudo $0 [MODE|--STAGE BATCH_SIZE ...]" >&2
    exit 1
fi

chmod 755 "$UP_SCRIPT" "$ROOT/run_pipeline.sh"

install -d -m 755 /etc/thundercompute
printf '%s\n' "${ARGS[@]}" >"$ARGS_FILE"

install -d -m 755 "$S6_ROOT/$SERVICE/dependencies.d"
printf 'oneshot\n' >"$S6_ROOT/$SERVICE/type"
cat >"$S6_ROOT/$SERVICE/up" <<EOF
#!/bin/sh
exec $UP_SCRIPT
EOF
chmod 755 "$S6_ROOT/$SERVICE/up"
: >"$S6_ROOT/$SERVICE/dependencies.d/sshd"
: >"$S6_ROOT/user/contents.d/$SERVICE"

echo "Installed s6 oneshot '$SERVICE' (deploy=$DEPLOY_LABEL, args=${ARGS[*]}, runs at next boot)."
