#!/usr/bin/env bash
# Run the cross-SDK BDD conformance scenarios for one language runner against an
# Apache Iggy testcontainer.
#
# Usage: scripts/run-bdd-tests.sh [language]
#   language: rust | python | typescript   (rust is the default)
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
language="${1:-rust}"

case "$language" in
  rust)
    cd "$repo_root/bdd/rust"
    cargo test
    ;;
  python|py)
    cd "$repo_root/foreign/python"
    uv run --extra testing pytest -q ../../bdd/python
    ;;
  typescript|ts)
    cd "$repo_root/foreign/typescript"
    npm run build
    cd "$repo_root/bdd/typescript"
    npm test
    ;;
  *)
    echo "no runner for language: $language" >&2
    exit 2
    ;;
esac
