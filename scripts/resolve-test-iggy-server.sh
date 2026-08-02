#!/usr/bin/env bash
set -euo pipefail

# Resolve the Iggy server binary the test harnesses run. An explicit
# LASER_TEST_IGGY_SERVER binary wins, otherwise the pinned release is
# downloaded once and cached. LASER_TEST_IGGY_VERSION overrides the pin and
# LASER_TEST_IGGY_SHA256, when set, pins the downloaded bytes.
readonly FORK_VERSION="${LASER_TEST_IGGY_VERSION:-0.8.102-ld}"
readonly RELEASES_URL="${LASER_TEST_IGGY_RELEASES_URL:-https://artifacts.laserdata.com}"

if [[ -n "${LASER_TEST_IGGY_SERVER:-}" ]]; then
  if [[ ! -x "$LASER_TEST_IGGY_SERVER" ]]; then
    echo "LASER_TEST_IGGY_SERVER is not executable: $LASER_TEST_IGGY_SERVER" >&2
    exit 1
  fi
  printf '%s\n' "$LASER_TEST_IGGY_SERVER"
  exit 0
fi

case "$(uname -m)" in
  x86_64)
    suffix="amd64-skylake"
    ;;
  *)
    echo "No $FORK_VERSION test server is published for $(uname -m)." >&2
    echo "Set LASER_TEST_IGGY_SERVER to a compatible local binary." >&2
    exit 1
    ;;
esac

cache_root="${XDG_CACHE_HOME:-${HOME}/.cache}/laser-sdk/iggy-server/$FORK_VERSION"
binary="$cache_root/iggy-server-linux-$suffix"
mkdir -p "$cache_root"

verify() {
  if [[ -n "${LASER_TEST_IGGY_SHA256:-}" ]]; then
    printf '%s  %s\n' "$LASER_TEST_IGGY_SHA256" "$1" | sha256sum --check --status
  fi
}

if [[ -x "$binary" ]] && verify "$binary"; then
  printf '%s\n' "$binary"
  exit 0
fi

temporary="$(mktemp "$cache_root/.download.XXXXXX")"
trap 'rm -f "$temporary"' EXIT
url="$RELEASES_URL/iggy-server/$FORK_VERSION/iggy-server-linux-$suffix"

echo "Downloading Iggy $FORK_VERSION from $url" >&2
curl -fsSL --retry 3 --connect-timeout 10 --max-time 300 "$url" -o "$temporary"
verify "$temporary"
chmod 755 "$temporary"
mv -f "$temporary" "$binary"
trap - EXIT

printf '%s\n' "$binary"
