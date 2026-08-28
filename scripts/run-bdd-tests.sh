#!/usr/bin/env bash
# Run the cross-SDK BDD conformance scenarios for one language runner against an
# Apache Iggy. Every runner starts the versioned native server unless
# LASER_TEST_IGGY_SERVER or an external LASER_BDD_ADDR is provided.
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
    uv sync --extra testing --locked --no-install-project
    uv run --no-sync maturin develop
    uv run --no-sync pytest -q ../../bdd/python
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
