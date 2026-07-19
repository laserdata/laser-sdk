#!/usr/bin/env bash
# Run the cross-SDK BDD conformance scenarios for one language runner against an
# Apache Iggy testcontainer.
#
# Usage: scripts/run-bdd-tests.sh [language]
#   language: rust   (default, more land as ports ship)
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
language="${1:-rust}"

case "$language" in
  rust)
    cd "$repo_root/bdd/rust"
    cargo test
    ;;
  *)
    echo "no runner for language: $language" >&2
    exit 2
    ;;
esac
