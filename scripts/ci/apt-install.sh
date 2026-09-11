#!/usr/bin/env bash
# Install Debian packages behind a bounded `apt-get update`.
#
# GitHub runners intermittently lose egress to azure.archive.ubuntu.com. apt
# then walks /etc/apt/apt-mirrors.txt and can wedge fetching the package
# indices with no output and no timeout of its own, so the job sits dead until
# it burns the whole `timeout-minutes` budget. apt's own Acquire::*::Timeout
# does not bound this, so both phases are capped externally: short per-fetch
# timeouts make apt fall through to the next mirror quickly, each update
# attempt is killed after a fixed budget, and the install runs even when every
# update attempt failed, because a stale but present index set is not fatal.
#
# Usage: scripts/ci/apt-install.sh [apt-get install flags] <package>...
#
# Environment:
#   APT_UPDATE_TIMEOUT    Seconds allowed per update attempt (default: 120)
#   APT_UPDATE_ATTEMPTS   Update attempts before giving up (default: 3)
#   APT_INSTALL_TIMEOUT   Seconds allowed for the install (default: 600)
set -euo pipefail

if [[ $# -eq 0 ]]; then
  echo "usage: $0 [apt-get install flags] <package>..." >&2
  exit 1
fi

update_timeout="${APT_UPDATE_TIMEOUT:-120}"
attempts="${APT_UPDATE_ATTEMPTS:-3}"
install_timeout="${APT_INSTALL_TIMEOUT:-600}"

for attempt in $(seq 1 "${attempts}"); do
  if sudo timeout --kill-after=10 "${update_timeout}" apt-get \
    -o Acquire::Retries=2 \
    -o Acquire::http::Timeout=15 \
    -o Acquire::https::Timeout=15 \
    update; then
    break
  fi
  echo "::warning::apt-get update attempt ${attempt}/${attempts} timed out or failed"
  if [[ "${attempt}" -lt "${attempts}" ]]; then
    sleep 5
  fi
done

# Generous cap: a healthy install takes well under a minute, and killing dpkg
# mid-configure leaves a broken package database behind.
if ! sudo timeout --kill-after=10 "${install_timeout}" apt-get install -y \
  -o Acquire::Retries=2 \
  -o Acquire::http::Timeout=15 \
  -o Acquire::https::Timeout=15 \
  "$@"; then
  echo "::error::apt-get install exceeded ${install_timeout}s or failed"
  exit 1
fi
