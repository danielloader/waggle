#!/bin/sh
# Reload systemd, enable the unit on first install, restart it after an
# upgrade (if it was running). Skip cleanly on systems without systemd
# (containers, chroots, Alpine without OpenRC bridging, etc.) — the unit
# is still on disk for any later boot under a systemd-aware host.
set -e

if [ ! -d /run/systemd/system ]; then
    exit 0
fi

systemctl daemon-reload >/dev/null 2>&1 || true

# Distinguish first-install from upgrade. nfpm doesn't pass a hint so we
# fall back to systemd's own view: if the unit is already enabled, this is
# an upgrade; otherwise enable it for the first time.
if systemctl is-enabled --quiet waggle.service 2>/dev/null; then
    # Upgrade: only kick the service if it was running. Don't start a
    # stopped service the admin had intentionally disabled.
    if systemctl is-active --quiet waggle.service; then
        systemctl restart waggle.service >/dev/null 2>&1 || true
    fi
else
    systemctl enable --now waggle.service >/dev/null 2>&1 || true
fi
