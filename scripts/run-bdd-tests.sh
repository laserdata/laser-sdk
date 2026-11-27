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
    # Through the same helper as the TypeScript runner, so this honours the
    # header's promise above: it starts the pinned native server unless
    # LASER_TEST_IGGY_SERVER or an external endpoint is already configured.
    # Without it the Go runner reaches no deployment and skips -- visibly, so
    # a run that found no server is never read as evidence, but it proves
    # nothing either.
    cd "$repo_root/bdd/go"
    node "$repo_root/scripts/run-with-test-iggy.mjs" go test ./... -count=1 -v
    ;;
  *)
    echo "no runner for language: $language" >&2
    exit 2
    ;;
esac
