#!/bin/sh
# Stop and disable the service before files disappear under it. nfpm calls
# this for both "remove" and the pre-half of "upgrade"; in the upgrade case
# postinstall will restart the (now-new) binary.
set -e

if [ ! -d /run/systemd/system ]; then
    exit 0
fi

if systemctl is-active --quiet waggle.service 2>/dev/null; then
    systemctl stop waggle.service >/dev/null 2>&1 || true
fi
if systemctl is-enabled --quiet waggle.service 2>/dev/null; then
    systemctl disable waggle.service >/dev/null 2>&1 || true
fi
