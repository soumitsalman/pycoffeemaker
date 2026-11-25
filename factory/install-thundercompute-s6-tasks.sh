#!/bin/bash
# Install thundercompute tasks to run at the next boot.
# Container images boot through s6-overlay (/init). Thunder VM-root boots
# through systemd and never starts s6, so both hooks are installed.
set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
UP_SCRIPT="$ROOT/factory/thundercompute-s6-tasks.sh"
S6_ROOT="/etc/s6-overlay/s6-rc.d"
SERVICE="thundercompute-tasks"

if [[ $# -ne 0 ]]; then
    echo "Usage: sudo $0" >&2
    exit 2
fi

if [[ "$(id -u)" -ne 0 ]]; then
    echo "Run with sudo: sudo $0" >&2
    exit 1
fi

chmod 755 "$UP_SCRIPT" "$ROOT/run_pipeline.sh"

install -d -m 755 "$S6_ROOT/$SERVICE/dependencies.d"
printf 'oneshot\n' >"$S6_ROOT/$SERVICE/type"

# Size run_pipeline.sh batches from this GPU and pin them for boot.
BATCH_ENV="$S6_ROOT/$SERVICE/batch.env"
batch_tmp="$(mktemp)"
trap 'rm -f "$batch_tmp"' EXIT
if ! "$UP_SCRIPT" --emit-batch-env >"$batch_tmp"; then
    echo "nvidia-smi did not report GPU VRAM; cannot size pipeline batches." >&2
    exit 1
fi
install -m 644 "$batch_tmp" "$BATCH_ENV"

# `up` is an execline command line, not a shell script. A shebang is
# discarded as a comment, so builtins such as `set` and `exec` are
# executed as programs and the oneshot fails at boot.
printf '%s\n' "$UP_SCRIPT --batch-env $BATCH_ENV" >"$S6_ROOT/$SERVICE/up"
chmod 755 "$S6_ROOT/$SERVICE/up"
: >"$S6_ROOT/$SERVICE/dependencies.d/sshd"
: >"$S6_ROOT/user/contents.d/$SERVICE"

echo "Installed s6 oneshot '$SERVICE' (runs at next boot when /init is PID 1)."

# VM-root never execs /init. A systemd oneshot is what actually runs there.
# RemainAfterExit keeps the cgroup alive: the tasks script backgrounds the
# pipeline and exits, and systemd would otherwise kill that child when the
# oneshot becomes inactive.
if [[ -d /run/systemd/system ]]; then
    unit="/etc/systemd/system/${SERVICE}.service"
    cat >"$unit" <<EOF
[Unit]
Description=Start the ThunderCompute pipeline at boot
After=network.target ssh.service thunder-vmroot-setup.service

[Service]
Type=oneshot
RemainAfterExit=yes
ExecStart=${UP_SCRIPT} --batch-env ${BATCH_ENV}

[Install]
WantedBy=multi-user.target
EOF
    chmod 644 "$unit"
    systemctl daemon-reload
    systemctl enable "$SERVICE.service"
    echo "Installed systemd oneshot '${SERVICE}.service' (runs at next VM-root boot)."
fi

echo "Pinned batches from $BATCH_ENV:"
cat "$BATCH_ENV"
