#!/bin/sh
# After files are removed, reload systemd so it forgets the unit. Leave
# /var/lib/waggle and the `waggle` user alone — the admin can `userdel
# waggle && rm -rf /var/lib/waggle` if they want a clean removal, and we
# avoid clobbering data on an accidental purge.
set -e

if [ ! -d /run/systemd/system ]; then
    exit 0
fi

systemctl daemon-reload >/dev/null 2>&1 || true
systemctl reset-failed waggle.service >/dev/null 2>&1 || true
