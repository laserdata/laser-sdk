#!/usr/bin/env bash
# Run the cross-SDK BDD scenarios against a local managed stack: the Laser
# Streams server forwarding managed operations to the managed backend over a
# local socket. The backend keeps its session store in the embedded engine,
# or in Postgres with --postgres.
#
# Usage: scripts/run-managed-bdd.sh [--postgres] [--release] <runner>...
#   runner: rust | python | typescript | all | stack
#   stack starts the stack, prints its address, and waits for Ctrl-C, so a
#   measurement or a single runner can be pointed at it by hand.
#
# Binaries: the streaming server is built from the checkout named by
# LASER_IGGY_REPO and the managed backend from the checkout named by
# LASER_MANAGED_BACKEND_REPO. Set LASER_MANAGED_IGGY_SERVER or
# LASER_MANAGED_BACKEND to use a prebuilt binary instead. Any extra runner
# arguments go after `--`, for example
#   scripts/run-managed-bdd.sh rust -- --name "Managed sessions"
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

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
  if [[ -z "${LASER_IGGY_REPO:-}" ]]; then
    echo "set LASER_MANAGED_IGGY_SERVER to a prebuilt streaming server or LASER_IGGY_REPO to its source checkout" >&2
    exit 2
  fi
  (cd "$LASER_IGGY_REPO" && cargo build "${cargo_flags[@]}" -p server --bin iggy-server >&2)
  echo "$LASER_IGGY_REPO/target/$profile/iggy-server"
}

resolve_backend() {
  if [[ -n "${LASER_MANAGED_BACKEND:-}" ]]; then
    echo "$LASER_MANAGED_BACKEND"
    return
  fi
  if [[ -z "${LASER_MANAGED_BACKEND_REPO:-}" ]]; then
    echo "set LASER_MANAGED_BACKEND to a prebuilt managed backend or LASER_MANAGED_BACKEND_REPO to its source checkout" >&2
    exit 2
  fi
  (cd "$LASER_MANAGED_BACKEND_REPO" && cargo build "${cargo_flags[@]}" -p plane --bin plane >&2)
  echo "$LASER_MANAGED_BACKEND_REPO/target/$profile/plane"
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

# The backend is ready once everything but the hello from the streaming server
# is: that hello rides the first client capability probe, so it cannot come first.
backend_ready() {
  local reply
  if ! kill -0 "$backend_pid" 2>/dev/null; then
    echo "the managed backend exited during boot, see $state/backend.log" >&2
    exit 1
  fi
  reply="$(curl -sS "http://127.0.0.1:$1/ready")" || return 1
  [[ "$reply" == *'"ready":true'* || "$reply" == *'"reason":"backend hello not established"'* ]]
}

iggy_server="$(resolve_iggy_server)"
backend_binary="$(resolve_backend)"
state="$(mktemp -d "${TMPDIR:-/tmp}/laser-managed-bdd.XXXXXX")"
iggy_pid=""
backend_pid=""
postgres_container=""

teardown() {
  local status=$?
  [[ -n "$backend_pid" ]] && kill "$backend_pid" 2>/dev/null && wait "$backend_pid" 2>/dev/null
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
socket="$state/backend.sock"
mkdir -p "$state/iggy" "$state/backend/snapshots"

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
wait_until "the streaming server on $address" 60 tcp_open "$iggy_port"

backend_env=()
if [[ "$postgres" == true ]]; then
  pg_port="$(free_port)"
  postgres_container="laser-managed-bdd-$$"
  docker run -d --rm --name "$postgres_container" -p "127.0.0.1:$pg_port:5432" \
    -e POSTGRES_DB=managed -e POSTGRES_USER=managed -e POSTGRES_PASSWORD=managed \
    pgvector/pgvector:pg16 >/dev/null
  # The image's first start runs a socket-only init server, then restarts.
  # Only the final server listens on TCP, so probe over TCP.
  wait_until "Postgres" 60 docker exec "$postgres_container" pg_isready -h 127.0.0.1 -U managed -d managed
  backends="[{\"id\":\"postgres\",\"kind\":\"postgres\",\"dsn\":\"postgres://managed:managed@127.0.0.1:$pg_port/managed\",\"ssl_mode\":\"disable\",\"serves\":\"kv,graph,sessions,folds,forks\"}]"
  backend_env+=("LD_PLANE_BACKENDS=$backends")
fi

# The backend process reads its configuration under these names.
env "${backend_env[@]}" \
  RUST_LOG="${LASER_MANAGED_BACKEND_LOG:-info,iggy=warn}" \
  LD_PLANE_IGGY_URL="tcp://$address" \
  LD_PLANE_IGGY_USERNAME=iggy \
  LD_PLANE_IGGY_PASSWORD=iggy \
  LD_PLANE_IGGY_TLS_DISABLED=true \
  LD_PLANE_IGGY_PAT_FILE=/nonexistent/backend.pat \
  LD_PLANE_INTERNAL_API_TOKEN=managed-bdd \
  LD_PLANE_CLUSTER_KIND=standalone \
  LD_PLANE_DB_PATH="$state/backend/index.db" \
  LD_PLANE_SNAPSHOT_DIR="$state/backend/snapshots" \
  LD_PLANE_REGISTRY_PATH="$state/backend/projections.json" \
  LD_PLANE_QUERY_SOCKET="$socket" \
  LD_PLANE_HEALTH_ADDR="127.0.0.1:$health_port" \
  "$backend_binary" >"$state/backend.log" 2>&1 &
backend_pid=$!
wait_until "the managed backend" 120 backend_ready "$health_port"

echo "managed stack ready: LASER_BDD_ADDR=$address, backend health http://127.0.0.1:$health_port, logs in $state" >&2
export LASER_BDD_ADDR="$address"
export LASER_BDD_MANAGED=1
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
      while kill -0 "$iggy_pid" 2>/dev/null && kill -0 "$backend_pid" 2>/dev/null; do sleep 1; done
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
