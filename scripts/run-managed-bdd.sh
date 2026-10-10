#!/usr/bin/env bash
# Run the cross-SDK BDD scenarios against a local managed stack: the Iggy fork
# with its AGDX layer forwarding to a plane over the plane socket, the same
# wiring the plane's own end-to-end tests use. The plane keeps its session
# store in the embedded engine, or in Postgres with --postgres.
#
# Usage: scripts/run-managed-bdd.sh [--postgres] [--release] <runner>...
#   runner: rust | python | typescript | all | stack
#   stack starts the stack, prints its address, and waits for Ctrl-C, so a
#   measurement or a single runner can be pointed at it by hand.
#
# Binaries: the fork server is built from LASER_IGGY_REPO (default ../iggy)
# and the plane from LASER_CLOUD_CORE_REPO (default ../cloud-core). Set
# LASER_MANAGED_IGGY_SERVER or LASER_MANAGED_PLANE to use a prebuilt binary.
# Any extra runner arguments go after `--`, for example
#   scripts/run-managed-bdd.sh rust -- --name "Managed sessions"
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
workspace="$(cd "$repo_root/.." && pwd)"
iggy_repo="${LASER_IGGY_REPO:-$workspace/iggy}"
cloud_core_repo="${LASER_CLOUD_CORE_REPO:-$workspace/cloud-core}"

profile=debug
postgres=false
runners=()
runner_args=()
while (($#)); do
  case "$1" in
    --postgres) postgres=true ;;
    --release) profile=release ;;
    --) shift; runner_args=("$@"); break ;;
    rust|python|py|typescript|ts|stack) runners+=("$1") ;;
    all) runners+=(rust python typescript) ;;
    -h|--help) sed -n '2,17p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
  shift
done
if ((${#runners[@]} == 0)); then
  echo "name at least one runner: rust, python, typescript, all, or stack" >&2
  exit 2
fi

cargo_flags=()
[[ "$profile" == release ]] && cargo_flags+=(--release)

resolve_iggy_server() {
  if [[ -n "${LASER_MANAGED_IGGY_SERVER:-}" ]]; then
    echo "$LASER_MANAGED_IGGY_SERVER"
    return
  fi
  (cd "$iggy_repo" && cargo build "${cargo_flags[@]}" -p server --bin iggy-server >&2)
  echo "$iggy_repo/target/$profile/iggy-server"
}

resolve_plane() {
  if [[ -n "${LASER_MANAGED_PLANE:-}" ]]; then
    echo "$LASER_MANAGED_PLANE"
    return
  fi
  (cd "$cloud_core_repo" && cargo build "${cargo_flags[@]}" -p plane --bin plane >&2)
  echo "$cloud_core_repo/target/$profile/plane"
}

free_port() {
  python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()'
}

wait_until() {
  local what="$1" seconds="$2"
  shift 2
  local deadline=$((SECONDS + seconds))
  until "$@" >/dev/null 2>&1; do
    if ((SECONDS >= deadline)); then
      echo "$what did not become ready within ${seconds}s" >&2
      return 1
    fi
    sleep 0.2
  done
}

tcp_open() {
  (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null
}

# The plane is ready once everything but the backend hello is: the fork sends
# that hello on the first client capability probe, so it cannot come first.
plane_ready() {
  local reply
  if ! kill -0 "$plane_pid" 2>/dev/null; then
    echo "the plane exited during boot, see $state/plane.log" >&2
    exit 1
  fi
  reply="$(curl -sS "http://127.0.0.1:$1/ready")" || return 1
  [[ "$reply" == *'"ready":true'* || "$reply" == *'"reason":"backend hello not established"'* ]]
}

iggy_server="$(resolve_iggy_server)"
plane_binary="$(resolve_plane)"
state="$(mktemp -d "${TMPDIR:-/tmp}/laser-managed-bdd.XXXXXX")"
iggy_pid=""
plane_pid=""
postgres_container=""

teardown() {
  local status=$?
  [[ -n "$plane_pid" ]] && kill "$plane_pid" 2>/dev/null && wait "$plane_pid" 2>/dev/null
  [[ -n "$iggy_pid" ]] && kill "$iggy_pid" 2>/dev/null && wait "$iggy_pid" 2>/dev/null
  [[ -n "$postgres_container" ]] && docker rm -f "$postgres_container" >/dev/null 2>&1
  if ((status == 0)) && [[ -z "${LASER_MANAGED_KEEP_STATE:-}" ]]; then
    rm -rf "$state"
  else
    echo "stack logs kept in $state" >&2
  fi
  return "$status"
}
trap teardown EXIT
trap 'exit 130' INT TERM

iggy_port="$(free_port)"
health_port="$(free_port)"
address="127.0.0.1:$iggy_port"
socket="$state/plane.sock"
mkdir -p "$state/iggy" "$state/plane/snapshots"

IGGY_ROOT_USERNAME=iggy \
IGGY_ROOT_PASSWORD=iggy \
IGGY_PATH="$state/iggy" \
IGGY_TCP_ENABLED=true \
IGGY_TCP_ADDRESS="$address" \
IGGY_HTTP_ENABLED=false \
IGGY_QUIC_ENABLED=false \
IGGY_WEBSOCKET_ENABLED=false \
IGGY_PLANE_ENABLED=true \
IGGY_PLANE_SOCKET_PATH="$socket" \
IGGY_PLANE_REQUEST_TIMEOUT=5s \
RUST_LOG="${LASER_MANAGED_IGGY_LOG:-warn}" \
  "$iggy_server" >"$state/iggy.log" 2>&1 &
iggy_pid=$!
wait_until "the Iggy fork on $address" 60 tcp_open "$iggy_port"

plane_env=()
if [[ "$postgres" == true ]]; then
  pg_port="$(free_port)"
  postgres_container="laser-managed-bdd-$$"
  docker run -d --rm --name "$postgres_container" -p "127.0.0.1:$pg_port:5432" \
    -e POSTGRES_DB=plane -e POSTGRES_USER=plane -e POSTGRES_PASSWORD=plane \
    pgvector/pgvector:pg16 >/dev/null
  # The image's first start runs a socket-only init server, then restarts.
  # Only the final server listens on TCP, so probe over TCP.
  wait_until "Postgres" 60 docker exec "$postgres_container" pg_isready -h 127.0.0.1 -U plane -d plane
  backends="[{\"id\":\"postgres\",\"kind\":\"postgres\",\"dsn\":\"postgres://plane:plane@127.0.0.1:$pg_port/plane\",\"ssl_mode\":\"disable\",\"serves\":\"kv,graph,sessions,folds,forks\"}]"
  plane_env+=("LD_PLANE_BACKENDS=$backends")
fi

env "${plane_env[@]}" \
  RUST_LOG="${LASER_MANAGED_PLANE_LOG:-plane=info,iggy=warn}" \
  LD_PLANE_IGGY_URL="tcp://$address" \
  LD_PLANE_IGGY_USERNAME=iggy \
  LD_PLANE_IGGY_PASSWORD=iggy \
  LD_PLANE_IGGY_TLS_DISABLED=true \
  LD_PLANE_IGGY_PAT_FILE=/nonexistent/plane.pat \
  LD_PLANE_INTERNAL_API_TOKEN=managed-bdd \
  LD_PLANE_DB_PATH="$state/plane/index.db" \
  LD_PLANE_SNAPSHOT_DIR="$state/plane/snapshots" \
  LD_PLANE_REGISTRY_PATH="$state/plane/projections.json" \
  LD_PLANE_QUERY_SOCKET="$socket" \
  LD_PLANE_HEALTH_ADDR="127.0.0.1:$health_port" \
  "$plane_binary" >"$state/plane.log" 2>&1 &
plane_pid=$!
wait_until "the plane" 120 plane_ready "$health_port"

echo "managed stack ready: LASER_BDD_ADDR=$address, plane health http://127.0.0.1:$health_port, logs in $state" >&2
export LASER_BDD_ADDR="$address"
export LASER_BDD_PLANE=1
unset LASER_BDD_URL LASER_CONNECTION_STRING

# The SDK builds mirror scripts/run-bdd-tests.sh. Set
# LASER_MANAGED_SKIP_SDK_BUILD=1 to run against the Python extension and the
# TypeScript package as they are already built.
run_runner() {
  local skip="${LASER_MANAGED_SKIP_SDK_BUILD:-}"
  case "$1" in
    rust)
      (cd "$repo_root/bdd/rust" && cargo test --test open -- "${runner_args[@]}")
      ;;
    python|py)
      (
        cd "$repo_root/foreign/python"
        if [[ -z "$skip" ]]; then
          uv sync --extra testing --locked --no-install-project
          uv run --no-sync maturin develop
        fi
        .venv/bin/python -m pytest -q ../../bdd/python "${runner_args[@]}"
      )
      ;;
    typescript|ts)
      (
        [[ -n "$skip" ]] || (cd "$repo_root/foreign/typescript" && npm run build)
        cd "$repo_root/bdd/typescript" && npm run build && npx cucumber-js --config cucumber.js "${runner_args[@]}"
      )
      ;;
    stack)
      echo "press Ctrl-C to stop the stack" >&2
      while kill -0 "$iggy_pid" 2>/dev/null && kill -0 "$plane_pid" 2>/dev/null; do sleep 1; done
      echo "a stack process exited, see $state" >&2
      return 1
      ;;
  esac
}

failed=0
for runner in "${runners[@]}"; do
  echo "== $runner against $address" >&2
  run_runner "$runner" || failed=1
done
exit "$failed"
