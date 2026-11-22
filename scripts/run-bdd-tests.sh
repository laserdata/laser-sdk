#!/usr/bin/env bash
# Run the cross-SDK BDD conformance scenarios for one language runner against an
# Apache Iggy. Every runner starts the versioned native server unless
# LASER_TEST_IGGY_SERVER or an external LASER_BDD_ADDR is provided.
#
# Usage: scripts/run-bdd-tests.sh [language] [--recovering-catalog]
#   language: rust | python | typescript | go   (rust is the default)
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
language="${1:-rust}"

if [[ "${2:-}" == "--recovering-catalog" ]]; then
  exec python3 "$repo_root/scripts/run-with-recovering-catalog.py" "$repo_root/scripts/run-bdd-tests.sh" "$language"
fi

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
  go)
    # The Go runner skips rather than passing when it reaches no deployment, so
    # a run that found no server is visibly a skip and never read as evidence.
    cd "$repo_root/bdd/go"
    go test ./... -count=1 -v
    ;;
  *)
    echo "no runner for language: $language" >&2
    exit 2
    ;;
esac
