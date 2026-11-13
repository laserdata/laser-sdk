#!/usr/bin/env bash
# Print the libtest filter arguments of one shard of the laser-sdk integration
# binary, one argument per line. Each named shard runs the tests of its modules.
# The rest shard skips every module the named shards run, so the shards always
# cover the whole suite and a new module lands in the rest shard.
#
# Usage: scripts/ci/integration-shard.sh <streaming|groups-and-agents|rest>
set -euo pipefail

streaming=(streaming)
groups_and_agents=(filters_group pause reliable workflow contract)

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <streaming|groups-and-agents|rest>" >&2
  exit 2
fi

case "$1" in
  streaming)
    printf '%s::\n' "${streaming[@]}"
    ;;
  groups-and-agents)
    printf '%s::\n' "${groups_and_agents[@]}"
    ;;
  rest)
    printf -- '--skip\n%s::\n' "${streaming[@]}" "${groups_and_agents[@]}"
    ;;
  *)
    echo "no shard named $1" >&2
    exit 2
    ;;
esac
