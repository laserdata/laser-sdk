# Laser SDK Benchmarks

`bench/` measures Apache Iggy, Laser streaming, AGDX, managed data surfaces, recovery, and MCP interoperability with native release binaries. The harness is a detached Rust workspace, so its dependencies never enter the published SDK crates.

## Quick Start

Run the maintained local campaign from the repository root:

```bash
just bench
```

The default uses one repetition, five seconds per timed arm, and one producer and consumer lane per four online logical CPUs. A host with 32 logical CPUs therefore uses eight producer lanes, eight consumer lanes, and eight partitions for capacity scenarios. Override duration, repetitions, and parallelism directly:

```bash
just bench 15 3 8
```

`LASER_BENCH_PARALLELISM` provides the same parallelism override for scripts and CI. `just bench-smoke` always uses one lane because it validates mechanics rather than capacity.

Until plane `0.14.0` is available from the artifact service, local managed runs need three existing native release binaries. Put their paths in the ignored `bench/.env` file:

```bash
LASER_BENCH_IGGY_SERVER_BINARY=/absolute/path/to/iggy-server-ng
LASER_BENCH_IGGY_BENCH_BINARY=/absolute/path/to/iggy-bench
LASER_BENCH_PLANE_BINARY=/absolute/path/to/plane
```

All three overrides select local path mode. With no overrides, the harness downloads signed CPU-targeted artifacts. A partial set fails immediately. `just bench` compiles only `laser-bench` and runs the supplied service binaries without rebuilding them.

The maintained artifact versions are:

| Binary | Version |
| --- | --- |
| Iggy `server-ng` | `0.8.103-ld` |
| `iggy-bench` | `0.6.0-edge.1` |
| plane | `0.14.0` |

## Default Campaign

The default campaign contains 37 scenarios when the optional PostgreSQL MCP controls are disabled.

| Group | Coverage |
| --- | --- |
| Iggy | Upstream VSR pinned producers, 10 GB total, 1 KiB records, batch size 100 |
| Streaming | Matched raw Iggy and Laser publish acknowledgement, partition-consumer throughput, and consumer-group throughput |
| AGDX | Parallel publish, request and reply, chunk streams, reliable partition lanes, fan-out, scatter, and context policies |
| Managed | Parallel KV, batch operations, query, durable memory, forks, graph, plus projection visibility and a labeled direct-plane diagnostic |
| Local memory | Deterministic `VectorMemory` remember and recall |
| MCP | `McpBridge` and a minimal Streamable HTTP control |
| Rust client | Connect, capability negotiation, topology setup, first publish, and warmed publish |
| Recovery | SDK consumer resume, graceful Iggy restart, plane memory recovery, and plane projection recovery |

Set `BENCH_MCP_POSTGRES_DSN` to add the durable PostgreSQL MCP control, recovery case, and triage comparison. Set `BENCH_MCP_POSTGRES_PID` when process accounting must include PostgreSQL.

## Progress And Results

The terminal prints provisioning, scenario progress, service-log paths, per-arm throughput, latency, correctness outcomes, and final evidence paths. Terminal output pauses during timed regions.

Each run is stored under:

```text
target/laser-bench-results/<UTC timestamp>/
```

After a successful suite, open:

```text
analysis/report.html
```

The report is a standalone file with embedded charts, styles, and the LaserData logo. It follows the operating-system color preference and includes a persistent light and dark theme switch. It shows paired raw Iggy and Laser confidence intervals, offered-load latency and throughput curves, every measured arm, and percentile plots decoded directly from the digest-verified HDR sidecars. The same analysis is available as JSON and CSV. Compressed `.hdr` sidecars remain the lossless latency distributions.

Useful commands:

```bash
just bench-analyze target/laser-bench-results/<run>
just bench-histogram target/laser-bench-results/<run>/<scenario>/<repetition>/histograms/<file>.hdr
```

`bench-analyze` validates the evidence before rendering HTML, JSON, and CSV, including `latency-distributions.csv`. `bench-histogram` prints the sample count and percentiles for one HDR sidecar.

## Campaign Modes

| Command | Purpose | Typical duration |
| --- | --- | --- |
| `just bench` | Complete local development campaign | About 3 to 8 minutes plus setup |
| `just bench 15 3 8` | Longer local campaign with eight lanes | Depends on scenario count and services |
| `just bench-smoke` | One-second harness and provisioning check | Short |
| `just bench-full` | Exhaustive matrix with 10 repetitions and 30-second arms | Several hours |
| `just bench-suite <suite> <output>` | Caller-defined immutable campaign | Manifest-defined |

Smoke results validate mechanics only. Default local results support development and regression analysis. Publication claims require the authoritative controls described below.

## Measurement Rules

- Iggy uses `server-ng` with TCP VSR.
- Timed service paths use native release binaries.
- Raw Iggy and Laser streaming arms use identical payloads, batches, partitions, concurrency, routing, retries, server, and schedule.
- Maintained capacity cells pin producer lane `i` to partition `i`. Caller-defined suites with different producer and partition counts use balanced routing and record that choice.
- Consumer throughput cells preload identical records outside timing, then measure the aggregate drain rate across all consumer lanes. The terminal prints total records per second and the arithmetic average per lane, matching the distinction made by `iggy-bench`.
- Request and reliable-consumer handlers preserve serial order within each partition while processing independent partitions concurrently.
- Visibility, startup, and recovery cells remain serial when concurrent writers would make one completion boundary ambiguous. Their reports identify that topology instead of presenting it as a capacity result.
- Publish acknowledgement, partition consumption, and consumer-group consumption have separate matched raw Iggy and Laser measurements. Paired arms run as ten interleaved counterbalanced epochs. The three AGDX publish-decomposition arms run sequentially in a latin-square order that rotates across repetitions.
- Producer-to-consumer cells resolve the delivery clock the moment a record is received. The partition readers poll with the producer's batch length and do not commit offsets inside the measured path.
- Warmup is time-based (`warmup_seconds`) for streaming, AGDX, reliable-consumer, KV, query, batch, UDS, and local-memory cells. Fork, graph, memory, and projection cells run an exact warmup operation count derived from the same field because their prepared corpora are sized from it. Warmup records use a separate identifier range, so replay validation never confuses them with measured records.
- Missing records, duplicates, ordering violations, failed replies, timeouts, missed arrivals, and checksum failures invalidate a result. A record that lands after its operation already failed or timed out client-side is counted as an explained late arrival instead of an unexpected record. Explained retries remain subject to checksum validation, while their duplicate and sequence outcomes are tolerated because a timed-out retry may legally commit more than once and after later work.
- Latency histograms contain successful operations only. Failed and timed-out operations keep their counts in the outcome denominators and their service times in a separate `service-failed` histogram sidecar.
- Open-loop arrivals keep their intended schedule. Undispatched work is recorded as missed. A scenario may set `spin_dispatch = true` to arm a sleep-then-spin dispatcher that trades one busy client core for sub-millisecond dispatch precision, and the no-server scheduler calibration then enforces the tighter lateness bound through the same engine path.
- `timeout_millis` and `max_in_flight` are per-scenario manifest fields. The defaults are 30 seconds and the lane count.
- Every campaign runs allocation-budget and payload pointer-identity gates before provisioning.
- Every operation is classified as successful, failed, timed out, or missed.
- Direct, A/A, consumer, and producer-to-consumer arms use one TCP VSR connection per lane in both raw and Laser arms. End-to-end partition readers also own their connections. This matches the connection-per-actor topology of `iggy-bench`.
- Request-reply and MCP triage workers use connections separate from their requesters.
- Fluent, background, and SDK-batching arms intentionally use one connection because they measure single-client API shapes.
- Every arm records its connection count and topology.
- Output directories are immutable.
- One campaign may run from a checkout at a time.

The harness binary builds with `lto = true` and `codegen-units = 1`, matching the Iggy fork's release policy, and records that profile in the binary manifest. When a suite declares host CPU controls, the Tokio runtime is sized to the pinned client CPU set and the worker count is recorded in every report.

Reports include aggregate throughput, the terminal's per-lane average, record and byte rates where applicable, supported latency percentiles, correctness outcomes, one-second workload series, process metrics, cgroup metrics, Iggy statistics, and plane metrics. A total throughput value already includes every configured producer or consumer lane and must not be multiplied again. HDR histograms are stored separately and referenced by SHA-256.

## Provisioning

Artifact mode downloads binaries and adjacent Minisign signatures from `https://artifacts.laserdata.com`. Signature verification runs inside `laser-bench`.

```toml
[provisioning]
mode = "artifact"
cpu_target = "skylake"
iggy_server_version = "0.8.103-ld"
iggy_bench_version = "0.6.0-edge.1"
plane_version = "0.14.0"
```

Path mode runs caller-provided native binaries and records their digests:

```toml
[provisioning]
mode = "path"
cpu_target = "skylake"
iggy_server = "/absolute/path/to/iggy-server-ng"
iggy_bench = "/absolute/path/to/iggy-bench"
plane = "/absolute/path/to/plane"
```

Docker Compose is available for deployment checks. Compose results are labeled non-authoritative because container scheduling, networking, cgroups, and filesystems affect the measurement boundary.

## Authoritative Campaigns

An authoritative streaming campaign requires:

- signed native artifacts for every required service
- at least 10 paired repetitions and 120 seconds per arm
- raw-Iggy A/A host-stability calibration
- disjoint client, Iggy, and plane CPU sets
- declared NUMA node, clocksource, governor, SMT, turbo, filesystem, and disk
- accepted correctness, scheduler, telemetry, and statistical gates

The suite pins native processes to the declared CPUs and audits host state before and after execution. Host drift invalidates the campaign.

Publication uses `bundle` to create a sanitized immutable directory. Raw service logs and private paths stay outside that bundle. `verify-bundle` checks its signed manifest and every recorded file digest.

## Development Gates

Run from `bench/`:

```bash
cargo fmt --all --check
cargo sort --check
cargo machete
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked --all-targets
cargo test --locked --doc
cargo bench --locked --profile dev --bench micro --no-run
```

The compile-only benchmark gate uses the development profile. Measurement commands use the fully optimized release profile with LTO.

The root Rust CI runs these detached workspace gates explicitly.

Zed loads both the published workspace and the detached benchmark workspace through `.zed/settings.json`, so navigation, diagnostics, and references work inside `bench/` without adding benchmark dependencies to the published Cargo graph.
