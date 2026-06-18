#!/bin/bash
# Install thundercompute tasks as an s6 oneshot in the user boot bundle.
set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
UP_SCRIPT="$ROOT/factory/thundercompute-s6-tasks.sh"
S6_ROOT="/etc/s6-overlay/s6-rc.d"
SERVICE="thundercompute-tasks"

if [[ "$(id -u)" -ne 0 ]]; then
    echo "Run with sudo: sudo $0" >&2
    exit 1
fi

chmod 755 "$UP_SCRIPT" "$ROOT/run_pipeline.sh"

install -d -m 755 "$S6_ROOT/$SERVICE/dependencies.d"
printf 'oneshot\n' >"$S6_ROOT/$SERVICE/type"
cat >"$S6_ROOT/$SERVICE/up" <<EOF
#!/bin/sh
exec $UP_SCRIPT
EOF
chmod 755 "$S6_ROOT/$SERVICE/up"
: >"$S6_ROOT/$SERVICE/dependencies.d/sshd"
: >"$S6_ROOT/user/contents.d/$SERVICE"

echo "Installed s6 oneshot '$SERVICE' (depends on sshd, runs at next boot)."
