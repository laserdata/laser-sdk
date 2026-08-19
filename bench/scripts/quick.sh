#!/usr/bin/env bash
set -euo pipefail

if [[ $# -gt 3 ]]; then
  printf 'usage: just bench [seconds-per-arm] [repetitions] [parallelism]\n' >&2
  exit 2
fi

if ! command -v cargo >/dev/null 2>&1; then
  printf 'required command not found: cargo\n' >&2
  exit 127
fi

if [[ -f bench/.env ]]; then
  # shellcheck source=/dev/null
  set -a
  source bench/.env
  set +a
fi

readonly DEFAULT_STREAMING_BATCH_SIZE=100
readonly DEFAULT_DURATION_SECONDS=5
readonly DEFAULT_REPETITIONS=1

duration_override="${1:-${LASER_BENCH_DURATION_SECONDS:-$DEFAULT_DURATION_SECONDS}}"
repetitions_override="${2:-${LASER_BENCH_REPETITIONS:-$DEFAULT_REPETITIONS}}"
online_cpu_count="$(getconf _NPROCESSORS_ONLN 2>/dev/null || printf '1')"
default_parallelism=$((online_cpu_count / 4))
if [[ "$default_parallelism" -lt 1 ]]; then
  default_parallelism=1
fi
parallelism_override="${3:-${LASER_BENCH_PARALLELISM:-$default_parallelism}}"

if [[ ! "$duration_override" =~ ^[1-9][0-9]*$ ]]; then
  printf 'seconds per arm must be a positive integer: %s\n' "$duration_override" >&2
  exit 2
fi
if [[ ! "$repetitions_override" =~ ^[1-9][0-9]*$ ]]; then
  printf 'repetitions must be a positive integer: %s\n' "$repetitions_override" >&2
  exit 2
fi
if [[ ! "$parallelism_override" =~ ^[1-9][0-9]*$ ]]; then
  printf 'parallelism must be a positive integer: %s\n' "$parallelism_override" >&2
  exit 2
fi

readonly DEFAULT_IGGY_BENCH_VERSION="0.6.0-edge.3"
readonly DEFAULT_PLANE_VERSION="0.16.0"

server_version="${LASER_BENCH_IGGY_SERVER_VERSION:-$(./scripts/resolve-test-iggy-server.sh --version)}"
bench_version="${LASER_BENCH_IGGY_BENCH_VERSION:-$DEFAULT_IGGY_BENCH_VERSION}"
plane_version="${LASER_BENCH_PLANE_VERSION:-$DEFAULT_PLANE_VERSION}"
cpu_target="${LASER_BENCH_CPU_TARGET:-auto}"
iggy_server_binary="${LASER_BENCH_IGGY_SERVER_BINARY:-}"
iggy_bench_binary="${LASER_BENCH_IGGY_BENCH_BINARY:-}"
plane_binary="${LASER_BENCH_PLANE_BINARY:-}"

if [[ "$cpu_target" == "auto" ]]; then
  case "$(uname -m)" in
    x86_64)
      cpu_target="skylake"
      ;;
    aarch64 | arm64)
      cpu_target="arm64"
      ;;
    *)
      printf 'no published automatic CPU target for %s, set LASER_BENCH_CPU_TARGET\n' "$(uname -m)" >&2
      exit 2
      ;;
  esac
fi

case "$cpu_target" in
  skylake | icelake | sapphirerapids | znver3 | arm64) ;;
  *)
    printf 'unsupported CPU target: %s\n' "$cpu_target" >&2
    exit 2
    ;;
esac

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
output="${LASER_BENCH_OUTPUT:-target/laser-bench-results/$timestamp}"
manifest="$(mktemp "${TMPDIR:-/tmp}/laser-bench-quick.XXXXXX.toml")"

cleanup() {
  local status="$1"
  rm -f "$manifest"
  if [[ "$status" -ne 0 ]]; then
    printf '\nEvidence retained at %s\n' "$output" >&2
  fi
}

trap 'cleanup $?' EXIT

mode="default"
if [[ "${LASER_BENCH_SMOKE:-0}" == "1" ]]; then
  mode="smoke"
elif [[ "${LASER_BENCH_FULL:-0}" == "1" ]]; then
  mode="full"
fi

smoke=false
full=false
case "$mode" in
  smoke)
    smoke=true
    suite_name="native-artifact-smoke"
    environment_tier="developer_smoke"
    repetitions=2
    warmup_seconds=1
    duration_seconds=1
    printf 'Mode: smoke, 2 repetitions with 1 second per timed arm\n'
    ;;
  full)
    full=true
    suite_name="native-artifact-streaming-campaign"
    environment_tier="developer_campaign"
    repetitions=10
    warmup_seconds=5
    duration_seconds=30
    printf 'Mode: exhaustive local campaign, 10 repetitions with 30 seconds per timed arm\n'
    ;;
  *)
    suite_name="local-complete-campaign"
    environment_tier="developer_campaign"
    repetitions="$repetitions_override"
    warmup_seconds=1
    duration_seconds="$duration_override"
    printf 'Mode: complete local campaign, %s repetition(s) with %s seconds per timed arm\n' "$repetitions" "$duration_seconds"
    ;;
esac

parallelism="$parallelism_override"
if [[ "$smoke" == "true" ]]; then
  parallelism=1
fi

streaming_batch_size="$DEFAULT_STREAMING_BATCH_SIZE"
if [[ "$smoke" == "true" ]]; then
  streaming_batch_size=1
fi

managed=true
mcp_postgres=false
postgres_dsn_line=""
postgres_pid_line=""
if [[ -n "${BENCH_MCP_POSTGRES_DSN:-}" ]]; then
  mcp_postgres=true
  postgres_dsn_line='postgres_dsn_env = "BENCH_MCP_POSTGRES_DSN"'
fi
if [[ -n "${BENCH_MCP_POSTGRES_PID:-}" ]]; then
  postgres_pid_line='postgres_pid_env = "BENCH_MCP_POSTGRES_PID"'
fi
mcp_scenarios=2
if [[ "$mcp_postgres" == "true" ]]; then
  mcp_scenarios=8
fi

if [[ -n "$iggy_server_binary" && -z "$iggy_bench_binary" ]] || [[ -z "$iggy_server_binary" && -n "$iggy_bench_binary" ]]; then
  printf 'LASER_BENCH_IGGY_SERVER_BINARY and LASER_BENCH_IGGY_BENCH_BINARY must be set together\n' >&2
  exit 2
fi

if [[ -n "$iggy_server_binary" && -n "$iggy_bench_binary" ]]; then
  if [[ ! -x "$iggy_server_binary" || ! -x "$iggy_bench_binary" ]]; then
    printf 'caller-provided Iggy binaries must exist and be executable\n' >&2
    exit 2
  fi
  if [[ -z "$plane_binary" || ! -x "$plane_binary" ]]; then
    printf 'path mode requires an executable LASER_BENCH_PLANE_BINARY\n' >&2
    exit 2
  fi
  provisioning_mode="path"
  printf 'Stack: caller-provided native Iggy and plane binaries\n'
else
  if [[ -n "$plane_binary" ]]; then
    printf 'a local plane requires local Iggy binaries in path mode\n' >&2
    printf 'set LASER_BENCH_IGGY_SERVER_BINARY and LASER_BENCH_IGGY_BENCH_BINARY\n' >&2
    exit 2
  fi
  provisioning_mode="artifact"
  if ! command -v curl >/dev/null 2>&1; then
    printf 'required command not found: curl\n' >&2
    exit 127
  fi
  printf 'Stack: signed native Iggy and plane artifacts\n'
  printf 'Artifacts: Iggy %s, iggy-bench %s, plane %s\n' "$server_version" "$bench_version" "$plane_version"
fi
printf 'Surfaces: streaming, AGDX, managed data, and MCP interoperability\n'
printf 'Concurrency: %s logical CPUs online, %s producer and consumer lanes\n' "$online_cpu_count" "$parallelism"

if [[ "$smoke" != "true" ]]; then
  if [[ "$managed" == "true" && "$full" == "true" ]]; then
    printf 'Measures: 8 Iggy substrate, 4 streaming including raw-Iggy A/A calibration, 16 AGDX, 45 managed, 12 local memory, %s MCP scenarios, 1 Rust client startup, 4 recovery scenarios, and 1 direct-plane diagnostic scenario\n' "$mcp_scenarios"
    printf 'Expected runtime: at least 4 hours plus release builds, warmup, and setup\n'
  else
    printf 'Measures: 1 Iggy substrate, 3 streaming, 16 AGDX, 8 managed, 2 local memory, %s MCP scenarios, 1 Rust client startup, and 4 recovery scenarios\n' "$mcp_scenarios"
    printf 'Iggy profile: 10 GB total, 1 KiB payload, batch 100, pinned producer\n'
    printf 'Streaming profile: 1 KiB payload, batch %s, %s partitions, %s producers, %s consumers\n' "$streaming_batch_size" "$parallelism" "$parallelism" "$parallelism"
    printf 'Expected runtime: about 3 to 8 minutes plus initial release builds\n'
  fi
  if [[ "$mcp_postgres" != "true" ]]; then
    printf 'Optional: guarantee-matched MCP skipped because BENCH_MCP_POSTGRES_DSN is not set\n'
  fi
fi

cat >"$manifest" <<EOF
schema_version = 1
name = "$suite_name"
authoritative = false

[provisioning]
mode = "$provisioning_mode"
cpu_target = "$cpu_target"
EOF

if [[ "$provisioning_mode" == "path" ]]; then
  cat >>"$manifest" <<EOF
iggy_server = "$iggy_server_binary"
iggy_bench = "$iggy_bench_binary"
plane = "$plane_binary"
EOF
else
  cat >>"$manifest" <<EOF
iggy_server_version = "$server_version"
iggy_bench_version = "$bench_version"
plane_version = "$plane_version"
EOF
fi

cat >>"$manifest" <<EOF
[environment]
tier = "$environment_tier"
durability_profile = "release_default"
cache_state = "warm"
$postgres_dsn_line
$postgres_pid_line
EOF

append_iggy_scenario() {
  local name="$1"
  local driver="$2"
  local batches="$3"
  local partitions="$4"
  local producers="$5"
  local consumers="$6"
  cat >>"$manifest" <<EOF

[[scenarios]]
name = "$name"
layer = "L1"
arm = "raw_iggy"
driver = "$driver"
transport = "tcp_vsr"
repetitions = $repetitions
warmup_seconds = $warmup_seconds
duration_seconds = $duration_seconds
payload_bytes = 1024
batch_size = $streaming_batch_size
partitions = $partitions
producers = $producers
consumers = $consumers
operations = $batches
EOF
}

iggy_total_batches=100000
if [[ "$smoke" == "true" ]]; then
  iggy_total_batches=100
fi
iggy_batches=$((iggy_total_batches / parallelism))
if [[ "$iggy_batches" -lt 1 ]]; then
  iggy_batches=1
fi
append_iggy_scenario iggy_pinned_producer pinned_producer "$iggy_batches" 1 "$parallelism" "$parallelism"
if [[ "$full" == "true" ]]; then
  append_iggy_scenario iggy_pinned_consumer pinned_consumer "$iggy_batches" 1 "$parallelism" "$parallelism"
  append_iggy_scenario iggy_pinned_producer_and_consumer pinned_producer_and_consumer "$iggy_batches" 1 "$parallelism" "$parallelism"
  append_iggy_scenario iggy_balanced_producer balanced_producer "$iggy_batches" "$parallelism" "$parallelism" "$parallelism"
  append_iggy_scenario iggy_balanced_consumer_group balanced_consumer_group "$iggy_batches" "$parallelism" "$parallelism" "$parallelism"
  append_iggy_scenario iggy_balanced_producer_and_consumer_group balanced_producer_and_consumer_group "$iggy_batches" "$parallelism" "$parallelism" "$parallelism"
  append_iggy_scenario iggy_end_to_end_producing_consumer end_to_end_producing_consumer "$iggy_batches" "$parallelism" "$parallelism" "$parallelism"
  append_iggy_scenario iggy_end_to_end_producing_consumer_group end_to_end_producing_consumer_group "$iggy_batches" "$parallelism" "$parallelism" "$parallelism"
fi

cat >>"$manifest" <<EOF

[[scenarios]]
name = "stream_direct_publish_ack"
layer = "L2"
arm = "raw_iggy_vs_laser"
driver = "stream_direct"
transport = "tcp_vsr"
repetitions = $repetitions
warmup_seconds = $warmup_seconds
duration_seconds = $duration_seconds
payload_bytes = 1024
batch_size = $streaming_batch_size
partitions = $parallelism
producers = $parallelism
consumers = 1
operations = 1000000

[[scenarios]]
name = "stream_consumer_partitions"
layer = "L2"
arm = "raw_iggy_vs_laser"
driver = "stream_consumer_partition"
transport = "tcp_vsr"
repetitions = $repetitions
warmup_seconds = $warmup_seconds
duration_seconds = $duration_seconds
payload_bytes = 1024
batch_size = $streaming_batch_size
partitions = $parallelism
producers = 1
consumers = $parallelism
operations = 1000000

[[scenarios]]
name = "stream_consumer_group"
layer = "L2"
arm = "raw_iggy_vs_laser"
driver = "stream_consumer_group"
transport = "tcp_vsr"
repetitions = $repetitions
warmup_seconds = $warmup_seconds
duration_seconds = $duration_seconds
payload_bytes = 1024
batch_size = $streaming_batch_size
partitions = $parallelism
producers = 1
consumers = $parallelism
operations = 1000000
EOF

if [[ "$full" == "true" ]]; then
  cat >>"$manifest" <<EOF

[[scenarios]]
name = "stream_direct_aa_calibration"
layer = "L2"
arm = "raw_iggy_a_vs_raw_iggy_b"
driver = "stream_direct_aa"
transport = "tcp_vsr"
repetitions = $repetitions
warmup_seconds = $warmup_seconds
duration_seconds = $duration_seconds
payload_bytes = 1024
batch_size = $streaming_batch_size
partitions = $parallelism
producers = $parallelism
consumers = 1
operations = 1000000
EOF
fi

append_agdx_scenario() {
  local name="$1"
  local driver="$2"
  local arm="$3"
  local batch_size="$4"
  local consumers="$5"
  local history_messages="${6:-}"
  local context_limit="${7:-}"
  cat >>"$manifest" <<EOF

[[scenarios]]
name = "$name"
layer = "L3"
arm = "$arm"
driver = "$driver"
transport = "tcp_vsr"
repetitions = $repetitions
warmup_seconds = $warmup_seconds
duration_seconds = $duration_seconds
payload_bytes = 1024
batch_size = $batch_size
partitions = $parallelism
producers = $parallelism
consumers = $consumers
operations = 100000
timeout_millis = 5000
EOF
  if [[ -n "$history_messages" ]]; then
    printf 'history_messages = %s\n' "$history_messages" >>"$manifest"
  fi
  if [[ -n "$context_limit" ]]; then
    printf 'context_limit = %s\n' "$context_limit" >>"$manifest"
  fi
}

if [[ "$smoke" != "true" ]]; then
  append_agdx_scenario agdx_publish agdx_publish typed_command 1 1
  append_agdx_scenario agdx_request_reply request_reply deterministic_echo 1 "$parallelism"
  append_agdx_scenario agdx_chunk_stream agdx_stream eight_chunks 8 "$parallelism"
  append_agdx_scenario agdx_reliable_plain_group reliable_consume plain_group 1 "$parallelism"
  append_agdx_scenario agdx_reliable_commit_after_success reliable_consume commit_after_success 1 "$parallelism"
  append_agdx_scenario agdx_reliable_dedup_miss reliable_consume dedup_miss 1 "$parallelism"
  append_agdx_scenario agdx_reliable_dedup_hit reliable_consume dedup_hit 1 "$parallelism"
  append_agdx_scenario agdx_reliable_middleware reliable_consume middleware 1 "$parallelism"
  append_agdx_scenario agdx_reliable_retry_ready reliable_consume retry_ready 1 "$parallelism"
  append_agdx_scenario agdx_reliable_retry_once reliable_consume retry_once 1 "$parallelism"
  append_agdx_scenario agdx_reliable_dlq_terminal reliable_consume dlq_terminal 1 "$parallelism"
  append_agdx_scenario agdx_fan_out fan_out parallel_recipients 1 "$parallelism"
  append_agdx_scenario agdx_scatter scatter parallel_recipients 1 "$parallelism"
  append_agdx_scenario agdx_context_last_n context_fetch last_n 1 1 1000 32
  append_agdx_scenario agdx_context_role_filter context_fetch role_filter 1 1 1000 32
  append_agdx_scenario agdx_context_token_budget context_fetch token_budget 1 1 1000 32

  cat >>"$manifest" <<EOF

[[scenarios]]
name = "rust_client_startup"
layer = "L6"
arm = "cold_vs_warmed"
driver = "rust_startup"
transport = "tcp_vsr"
repetitions = $repetitions
warmup_seconds = 1
duration_seconds = 1
payload_bytes = 1024
batch_size = 1
partitions = 1
producers = 1
consumers = 1
operations = 2

[[scenarios]]
name = "consumer_restart_recovery"
layer = "L7"
arm = "committed_offset_resume"
driver = "consumer_restart"
transport = "tcp_vsr"
repetitions = $repetitions
warmup_seconds = 0
duration_seconds = $duration_seconds
payload_bytes = 1024
batch_size = 100
partitions = 1
producers = 1
consumers = 1
operations = 500

[[scenarios]]
name = "iggy_restart_recovery"
layer = "L7"
arm = "persisted_log_resume"
driver = "iggy_restart"
transport = "tcp_vsr"
repetitions = $repetitions
warmup_seconds = 0
duration_seconds = $duration_seconds
payload_bytes = 1024
batch_size = 100
partitions = 1
producers = 1
consumers = 1
operations = 100

[[scenarios]]
name = "plane_restart_memory_recovery"
layer = "L7"
arm = "fold_convergence"
driver = "plane_restart_memory"
transport = "tcp_vsr"
repetitions = $repetitions
warmup_seconds = 0
duration_seconds = 30
payload_bytes = 1024
batch_size = 100
partitions = 1
producers = 1
consumers = 1
operations = 100

[[scenarios]]
name = "plane_restart_projection_recovery"
layer = "L7"
arm = "projection_convergence"
driver = "plane_restart_projection"
transport = "tcp_vsr"
repetitions = $repetitions
warmup_seconds = 0
duration_seconds = 30
payload_bytes = 1024
batch_size = 100
partitions = 1
producers = 1
consumers = 1
operations = 100

[[scenarios]]
name = "mcp_bridge_overhead"
layer = "L5"
arm = "native_vs_streamable_http"
driver = "mcp_bridge"
transport = "tcp_vsr"
repetitions = $repetitions
warmup_seconds = $warmup_seconds
duration_seconds = $duration_seconds
payload_bytes = 1024
batch_size = 1
partitions = $parallelism
producers = $parallelism
consumers = $parallelism
operations = 100000

[[scenarios]]
name = "mcp_minimal_streamable_http"
layer = "L5"
arm = "minimal_mcp_control"
driver = "mcp_minimal"
transport = "tcp_vsr"
repetitions = $repetitions
warmup_seconds = $warmup_seconds
duration_seconds = $duration_seconds
payload_bytes = 1024
batch_size = 1
partitions = 1
producers = $parallelism
consumers = 1
operations = 100000
EOF

  if [[ "$mcp_postgres" == "true" ]]; then
    cat >>"$manifest" <<EOF

[[scenarios]]
name = "mcp_guarantee_matched"
layer = "L5"
arm = "postgres_durable_control"
driver = "mcp_guaranteed"
transport = "tcp_vsr"
repetitions = $repetitions
warmup_seconds = $warmup_seconds
duration_seconds = $duration_seconds
payload_bytes = 1024
batch_size = 1
partitions = 1
producers = $parallelism
consumers = 1
operations = 100000

[[scenarios]]
name = "mcp_guarantee_recovery"
layer = "L5"
arm = "committed_result_before_ack"
driver = "mcp_guaranteed_recovery"
transport = "tcp_vsr"
repetitions = $repetitions
warmup_seconds = 1
duration_seconds = 1
payload_bytes = 1024
batch_size = 1
partitions = 1
producers = 1
consumers = 1
operations = 1
EOF
    for recipients in 1 3 5 8; do
      cat >>"$manifest" <<EOF

[[scenarios]]
name = "mcp_triage_fanout_${recipients}"
layer = "L5"
arm = "agdx_vs_minimal_vs_guarantee_matched"
driver = "mcp_triage"
transport = "tcp_vsr"
repetitions = $repetitions
warmup_seconds = $warmup_seconds
duration_seconds = $duration_seconds
payload_bytes = 1024
batch_size = 1
partitions = $parallelism
producers = $parallelism
consumers = $recipients
operations = 100000
offered_rate = 100
EOF
    done
  fi
fi

append_managed_scenario() {
  local name="$1"
  local driver="$2"
  local arm="$3"
  local batch_size="$4"
  local producers="$5"
  local partitions="$6"
  local corpus_entries="${7:-}"
  local operations="${8:-100000}"
  local offered_rate="${9:-}"
  cat >>"$manifest" <<EOF

[[scenarios]]
name = "$name"
layer = "L4"
arm = "$arm"
driver = "$driver"
transport = "tcp_vsr"
repetitions = $repetitions
warmup_seconds = $warmup_seconds
duration_seconds = $duration_seconds
payload_bytes = 1024
batch_size = $batch_size
partitions = $partitions
producers = $producers
consumers = 1
operations = $operations
timeout_millis = 5000
EOF
  if [[ -n "$corpus_entries" ]]; then
    printf 'corpus_entries = %s\n' "$corpus_entries" >>"$manifest"
  fi
  if [[ -n "$offered_rate" ]]; then
    printf 'offered_rate = %s\n' "$offered_rate" >>"$manifest"
  fi
}

if [[ "$managed" == "true" && "$smoke" == "true" ]]; then
  append_managed_scenario managed_kv_get_miss kv get_miss 1 1 1
  append_managed_scenario managed_batch_batched batch batched 16 1 1
  append_managed_scenario managed_batch_individual batch individual 16 1 1
  append_managed_scenario managed_batch_partial_failure batch partial_failure 16 1 1
  append_managed_scenario managed_projection_query_visible_lag projection query_visible_lag 1 1 1
  append_managed_scenario managed_query_point_predicate query point_predicate 1 1 1 10000
  append_managed_scenario managed_memory_folded_recall memory folded_recall 10 1 1 1000
  append_managed_scenario managed_fork_create_severed fork create_severed 1 1 1
  append_managed_scenario managed_graph_neighbor_read graph neighbor_read 16 1 1
elif [[ "$managed" == "true" && "$full" != "true" ]]; then
  append_managed_scenario managed_kv_get_hit kv get_hit 1 "$parallelism" "$parallelism" '' 100
  append_managed_scenario managed_batch_batched batch batched 16 "$parallelism" "$parallelism" '' 100
  append_managed_scenario managed_projection_query_visible_lag projection query_visible_lag 1 1 "$parallelism" '' 100
  append_managed_scenario managed_query_point_predicate query point_predicate 1 "$parallelism" "$parallelism" 1000 100
  append_managed_scenario managed_memory_folded_recall memory folded_recall 100 "$parallelism" "$parallelism" 1000 100
  append_managed_scenario managed_fork_create_severed fork create_severed 1 "$parallelism" "$parallelism" '' 100
  append_managed_scenario managed_graph_neighbor_read graph neighbor_read 16 "$parallelism" "$parallelism" '' 100
  append_managed_scenario diagnostic_uds_kv_get_miss uds kv_get_miss 1 "$parallelism" 1 '' 100
elif [[ "$managed" == "true" ]]; then
  append_managed_scenario managed_kv_batch_get kv batch_get 16 "$parallelism" "$parallelism" '' 1000
  append_managed_scenario managed_kv_batch_individual_get kv batch_individual_get 16 "$parallelism" "$parallelism" '' 1000
  append_managed_scenario managed_kv_cas_hot_key kv cas_hot_key 1 "$parallelism" "$parallelism"
  append_managed_scenario managed_kv_cas_uncontended kv cas_uncontended 1 "$parallelism" "$parallelism" '' 10000
  append_managed_scenario managed_kv_get_hit kv get_hit 1 "$parallelism" "$parallelism" '' 10000
  append_managed_scenario managed_kv_get_miss kv get_miss 1 "$parallelism" "$parallelism"
  append_managed_scenario managed_kv_mixed_read_10 kv mixed_read_10 1 "$parallelism" "$parallelism" '' 10000
  append_managed_scenario managed_kv_mixed_read_50 kv mixed_read_50 1 "$parallelism" "$parallelism" '' 10000
  append_managed_scenario managed_kv_mixed_read_90 kv mixed_read_90 1 "$parallelism" "$parallelism" '' 10000
  append_managed_scenario managed_kv_scan_page kv scan_page 1000 "$parallelism" "$parallelism" 10000
  append_managed_scenario managed_kv_set_insert kv set_insert 1 "$parallelism" "$parallelism"
  append_managed_scenario managed_kv_set_overwrite kv set_overwrite 1 "$parallelism" "$parallelism" '' 10000

  append_managed_scenario managed_batch_batched batch batched 16 "$parallelism" "$parallelism"
  append_managed_scenario managed_batch_individual batch individual 16 "$parallelism" "$parallelism"
  append_managed_scenario managed_batch_partial_failure batch partial_failure 16 "$parallelism" "$parallelism"

  append_managed_scenario managed_projection_backlog_drain projection backlog_drain 100 1 "$parallelism"
  append_managed_scenario managed_projection_burst_ingest projection burst_ingest 100 "$parallelism" "$parallelism"
  append_managed_scenario managed_projection_change_record_lag projection change_record_lag 1 1 "$parallelism"
  append_managed_scenario managed_projection_query_visible_lag projection query_visible_lag 1 1 "$parallelism"
  append_managed_scenario managed_projection_sustained_ingest projection sustained_ingest 1 "$parallelism" "$parallelism"

  append_managed_scenario managed_query_point_predicate query point_predicate 1 "$parallelism" "$parallelism" 10000
  append_managed_scenario managed_query_selective_filter query selective_filter 100 "$parallelism" "$parallelism" 10000
  append_managed_scenario managed_query_page_scan_1 query page_scan 1 "$parallelism" "$parallelism" 10000
  append_managed_scenario managed_query_page_scan_100 query page_scan 100 "$parallelism" "$parallelism" 10000
  append_managed_scenario managed_query_page_scan_1000 query page_scan 1000 "$parallelism" "$parallelism" 10000
  append_managed_scenario managed_query_aggregate_group_by query aggregate_group_by 100 "$parallelism" "$parallelism" 10000
  append_managed_scenario managed_query_payload_off query payload_off 100 "$parallelism" "$parallelism" 10000
  append_managed_scenario managed_query_payload_on query payload_on 100 "$parallelism" "$parallelism" 10000

  append_managed_scenario managed_memory_backlog_drain memory backlog_drain 16 1 "$parallelism"
  append_managed_scenario managed_memory_fold_visibility memory fold_visibility 1 1 "$parallelism"
  append_managed_scenario managed_memory_folded_recall memory folded_recall 100 "$parallelism" "$parallelism" 10000
  append_managed_scenario managed_memory_remember_ack memory remember_ack 1 "$parallelism" "$parallelism"

  append_managed_scenario managed_fork_base_size fork base_size 1 1 "$parallelism" 10000
  append_managed_scenario managed_fork_create_continuous fork create_continuous 1 "$parallelism" "$parallelism"
  append_managed_scenario managed_fork_create_severed fork create_severed 1 "$parallelism" "$parallelism"
  append_managed_scenario managed_fork_delete fork delete 1 "$parallelism" "$parallelism" '' 1000 20
  append_managed_scenario managed_fork_overlay_put fork overlay_put 1 "$parallelism" "$parallelism" '' 1000 20
  append_managed_scenario managed_fork_overlay_query fork overlay_query 1 "$parallelism" "$parallelism" '' 1000 20
  append_managed_scenario managed_fork_promote fork promote 16 "$parallelism" "$parallelism" '' 1000 20
  append_managed_scenario managed_fork_squash fork squash 16 "$parallelism" "$parallelism" '' 1000 20

  append_managed_scenario managed_graph_edge_upsert graph edge_upsert 16 "$parallelism" "$parallelism" '' 2000
  append_managed_scenario managed_graph_neighbor_read graph neighbor_read 16 "$parallelism" "$parallelism"
  append_managed_scenario managed_graph_node_upsert graph node_upsert 16 "$parallelism" "$parallelism"
  append_managed_scenario managed_graph_traversal graph traversal 2 "$parallelism" "$parallelism"
  append_managed_scenario managed_graph_vector_start graph vector_start 100 "$parallelism" "$parallelism" 10000

  append_managed_scenario diagnostic_uds_kv_get_miss uds kv_get_miss 1 "$parallelism" 1
fi

append_local_memory_scenario() {
  local driver="${1:?append_local_memory_scenario requires driver}"
  local corpus_entries="${2:-1000}"
  local vector_dimensions="${3:-384}"
  cat >>"$manifest" <<EOF

[[scenarios]]
name = "local_${driver}_${corpus_entries}_${vector_dimensions}d"
layer = "L4"
arm = "in_process"
driver = "$driver"
transport = "tcp_vsr"
repetitions = $repetitions
warmup_seconds = $warmup_seconds
duration_seconds = $duration_seconds
payload_bytes = 1024
batch_size = 1
partitions = 1
producers = $parallelism
consumers = 1
operations = 100000
corpus_entries = $corpus_entries
vector_dimensions = $vector_dimensions
EOF
}

if [[ "$smoke" == "true" ]]; then
  append_local_memory_scenario vector_memory_recall 100 64
elif [[ "$full" == "true" ]]; then
  for corpus_entries in 1000 10000; do
    for vector_dimensions in 64 384 1536; do
      append_local_memory_scenario vector_memory_remember "$corpus_entries" "$vector_dimensions"
      append_local_memory_scenario vector_memory_recall "$corpus_entries" "$vector_dimensions"
    done
  done
else
  append_local_memory_scenario vector_memory_remember 1000 384
  append_local_memory_scenario vector_memory_recall 1000 384
fi

rust_cpu_target="$cpu_target"
if [[ "$cpu_target" == "arm64" ]]; then
  rust_cpu_target="native"
fi
CARGO_TARGET_DIR="bench/target/laser-bench-$cpu_target" \
RUSTFLAGS="-C target-cpu=$rust_cpu_target" \
  cargo run --manifest-path bench/Cargo.toml --locked --release -- suite "$manifest" --output "$output"
