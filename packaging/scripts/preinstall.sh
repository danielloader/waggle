#!/bin/sh
# Create the waggle system user/group used by the service unit. Idempotent
# (skip if it already exists); shells out to the tools each distro ships
# with so it works the same on deb, rpm, apk, and Arch.
set -e

if ! getent group waggle >/dev/null 2>&1; then
    if command -v groupadd >/dev/null 2>&1; then
        groupadd --system waggle
    elif command -v addgroup >/dev/null 2>&1; then
        addgroup -S waggle
    fi
fi

if ! getent passwd waggle >/dev/null 2>&1; then
    if command -v useradd >/dev/null 2>&1; then
        useradd --system --gid waggle --home-dir /var/lib/waggle \
            --shell /usr/sbin/nologin --comment "waggle service user" waggle
    elif command -v adduser >/dev/null 2>&1; then
        adduser -S -G waggle -H -h /var/lib/waggle -s /sbin/nologin waggle
    fi
fi

# StateDirectory= in the unit creates /var/lib/waggle at start, but if the
# admin runs the binary by hand before first start (or systemd is absent
# for some reason), having the directory pre-created with the right owner
# avoids a confusing permission failure.
install -d -m 0750 -o waggle -g waggle /var/lib/waggle
