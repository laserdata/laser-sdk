# LaserData - Laser SDK examples - Rust

The Rust examples share one connection helper. They run against Apache Iggy, Laser Stack, or LaserData Cloud. A managed phase runs only when the connected deployment advertises its capability.

Run the commands below from this directory (`examples/rust/`).

**Consumer filters: 98.5% less payload transfer in the CDC example.** The reader receives 4 of 240 records from the shared feed, with original bytes and offsets. The example also covers typed records, one-byte numeric headers, previews, and saved group policies. See the [Consumer Filters guide](https://docs.laserdata.cloud/laser-sdk/consumer-filters).

## Run locally

Start Apache Iggy, then run an example.

```sh
just up                              # start a server on 127.0.0.1:8090
cargo run --example event-analytics
just down                            # stop it
```

With no environment set, `laser()` uses `iggy:iggy@127.0.0.1:8090`.

All examples use Iggy's native VSR transport. Point the same connection setting at an Apache Iggy deployment.

```sh
LASER_CONNECTION_STRING='user:pwd@iggy-host:8090' \
  cargo run --example fleet-tape
```

For the complete managed surface, start Laser Stack with `./scripts/up` from its checkout and use the `LASER_CONNECTION_STRING` it prints.

## Run against LaserData Cloud

Pass a connection target through the environment. Two forms. The port defaults to 8090 when omitted.

```sh
# Form A: bare target with embedded credentials
LASER_CONNECTION_STRING='user:pwd@starter-123.us-west-1.aws.laserdata.cloud' \
  cargo run --example event-analytics

# a token works in place of user:pwd
LASER_CONNECTION_STRING='<token>@starter-123.us-west-1.aws.laserdata.cloud' \
  cargo run --example event-analytics

# Form B: host plus separate auth
LASER_SERVER='starter-123.us-west-1.aws.laserdata.cloud' \
LASER_TOKEN='<token>' \
  cargo run --example event-analytics
```

For a LaserData host (`*.laserdata.cloud` or `*.laserdata.com`) TLS and the SDK-embedded CA attach automatically, even when you pass only the connection string. `LASER_TLS_CERT=<path>` enables TLS with that CA for any host or overrides the embedded CA. A connection string that already sets `tls_ca_file=` remains authoritative, and `LASER_NO_TLS=1` disables automatic TLS setup.

### Environment variables

| variable | effect |
| --- | --- |
| `LASER_CONNECTION_STRING` | bare `user:pwd@host` or `token@host` target, transport and TLS resolved by the SDK |
| `LASER_SERVER` | bootstrap host, paired with the auth variables below |
| `LASER_TOKEN` | personal access token auth |
| `LASER_USERNAME`, `LASER_PASSWORD` | username and password auth |
| `LASER_TLS_CERT` | path to a CA cert, enables TLS for any host or overrides the embedded CA |
| `LASER_NO_TLS=1` | disable TLS |
| `LASER_NON_INTERACTIVE=1` | runs `orchestra` without waiting for Enter between phases |
| `LASER_STREAM` | overrides the data stream for every example (default: `laser-<example>-rust`, reset at the start of each run and kept afterwards). Set it to your provisioned stream on a managed deployment so the SDK uses it and does not auto-create one, at the cost of repeat runs sharing its state |

`event-analytics`, `fleet-tape`, and `incident-desk` also read two volume knobs, so the same binary runs a ten-record smoke test or a multi-million-record soak without a code edit. Each example picks its own default, and the variable wins when set. `fleet-tape` reads only `LASER_MESSAGES`.

| variable | meaning |
| --- | --- |
| `LASER_MESSAGES` | total records to publish |
| `LASER_BATCH` | records per send call |

The firehose reads its own `LASER_FIREHOSE_*` knobs, the incident desk reads `LASER_DESK_GRANT_TIMEOUT_SECS` (the grant-apply deadline in seconds), and the governance example reads `LASER_GOVERNANCE_USER_ID` (the user its roles are bound to, by default a `governance-demo` user it creates).

Each example uses `laser-<example>-rust` unless `LASER_STREAM` supplies a name, so different examples never share agent topics or offsets. Managed indexes carry a per-run token so a rerun counts only its own rows. A run deletes the previous run's stream first and keeps its own result on the server, so you can inspect it afterwards with the SDK, the Iggy CLI, or the LaserData Cloud Console. A stream supplied through `LASER_STREAM` is never deleted. Key-value entries, leases, forks, and memory views live under the stream's scoped names and belong to that stream, so a rerun of a managed example on a recreated stream starts from empty state.

## Primitives: start here

Nine focused examples cover the core data and agent primitives. Unsupported managed phases report the missing capability and exit cleanly.

| binary | primitive | shows | run | docs |
| --- | --- | --- | --- | --- |
| [`log`](src/log/README.md) | Log | write two messages, read them back through one typed handle | `just up && cargo run --example log` | [`/laser-sdk/log`](https://docs.laserdata.cloud/laser-sdk/log) |
| [`query`](src/query/README.md) | Views | declare a view over a topic, then query the materialized rows (managed) | `cargo run --example query` | [`/laser-sdk/views`](https://docs.laserdata.cloud/laser-sdk/views) |
| [`watch`](src/watch/README.md) | Change feed | react to an advancement record instead of re-querying blind (managed) | `cargo run --example watch` | [`/laser-sdk/change-feed`](https://docs.laserdata.cloud/laser-sdk/change-feed) |
| [`kv`](src/kv/README.md) | State | set/get keyed JSON with a TTL, change it under compare-and-swap, write under a revocable lease's fence behind a barriered read, write and promote a fork row (managed) | `cargo run --example kv` | [`/laser-sdk/state`](https://docs.laserdata.cloud/laser-sdk/state) |
| [`cdc`](src/cdc/README.md) | Consumer filters | read four safe-mode events out of a 240-record feed of typed serde records, sample-test and preview filters, route binary alerts on a header, then save filters and bind a consumer group (managed) | `cargo run --example cdc` | [`/laser-sdk/consumer-filters`](https://docs.laserdata.cloud/laser-sdk/consumer-filters) |
| [`graph`](src/graph/README.md) | Graph | relate entities, then traverse one relation out of a node (managed) | `cargo run --example graph` | [`/laser-sdk/graph`](https://docs.laserdata.cloud/laser-sdk/graph) |
| [`recall`](src/recall/README.md) | Memory | all four durable verbs: remember, recall recent, improve, forget | `just up && cargo run --example recall` | [`/laser-sdk/memory`](https://docs.laserdata.cloud/laser-sdk/memory) |
| [`context`](src/context/README.md) | Context | assemble one conversation under a last-N + token-budget policy | `just up && cargo run --example context` | [`/laser-sdk/context`](https://docs.laserdata.cloud/laser-sdk/context) |
| [`agent`](src/agent/README.md) | Fabric | spawn a handler, contract it a deadline-bounded task by capability | `just up && cargo run --example agent` | [`/laser-sdk/fabric`](https://docs.laserdata.cloud/laser-sdk/fabric) |

`recall` is the Memory primitive's example. The name `memory` already belongs to the deep-dive scenario below.

## Deep-dive scenarios

Nine deep-dive scenarios follow, for eighteen runnable programs with the primitives above. Each one runs green on an open server: a phase that needs a managed deployment prints how to point at one and skips. The workload examples scale with the volume knobs above. Every README follows the same shape: a tagline, What it does, Run it, Where to look (LaserData Cloud) where it produces managed artifacts, and Highlights. The incident-desk example is the full-AGDX showcase: it exercises every surface (streaming and the agent envelope, materialized views and query, key-value, and forks) in one story.

| binary | layer | shows |
| --- | --- | --- |
| [`native-streaming`](src/native-streaming/README.md) | generic | the focused Laser streaming path: configurable direct producer, exact-width headers, keyed and batch sends, live async consumer groups, automatic server-side offset commits, and explicit commit-after-success handling |
| [`event-analytics`](src/event-analytics/README.md) | generic | one clickstream, every read model: a live consumer-group ticker tails the raw log while the producer streams, LaserData Cloud materializes a queryable index (request mix, slowest routes, time windows), an independent reader resumes from a `Cursor` + `StateStore` checkpoint, and on a LaserData Cloud a registered JSON Schema guards the index against malformed events |
| [`fleet-tape`](src/fleet-tape/README.md) | generic | the latency-minded telemetry profile: a tuned Laser producer streams host CPU readings in paced bursts while a tight-poll consumer group folds a live fleet view (last CPU, sample-weighted mean CPU, samples), the same readings index to a queryable tape (integers end to end) audited back through a typed handle (`topic.json::<Reading>().records(..)`), and on a LaserData Cloud the readings replay as raw Avro datums decoded by a registered writer schema |
| [`firehose`](src/firehose/README.md) | generic | the load generator: millions of multi-KB messages across many org indexes (gigabytes of data) to drive LaserData Cloud's ingest and query path under real storage pressure, with env-configurable volume, payload size, and fan-out |
| [`incident-desk`](src/incident-desk/README.md) | agentic | an AI incident desk operating a live incident: ticket firehose into a queryable index, semantic memory recall, a four-agent desk (triage fans deadline-bounded specialist calls and synthesizes with the LLM, a KV-deduplicated resolver applies capacity grants effectively once behind a durable approval gate), a coordination demo (a quota-ledger compare-and-swap with a conflict-retry loop, a read-your-writes query, and the unified `ResultCode` classifying every outcome), speculative bulk-resolution in a fork promoted only when it clears the backlog, and the whole incident rebuilt from its conversation as the audit trail |
| [`memory`](src/memory/README.md) | agentic | in-process recall, durable memory, and graph traversal over one incident domain. Durable memory and graph operations run on Laser Stack or LaserData Cloud |
| [`interop`](src/interop/README.md) | agentic | edge interoperability over the log: one LLM-backed agent reached as an A2A agent (`SendMessage` -> `GetTask`), an MCP tool server (`tools/list` / `tools/call`), and an AG-UI event stream (`agui_events`), all bridged onto the Agent Data Exchange Protocol. It runs on the mock model or a real backend with the `llm-*` features |
| [`orchestra`](src/orchestra/README.md) | agentic | the orchestration showcase, matching the Python and TypeScript `orchestra`: an interactive, paced run (press Enter per phase, or set `LASER_NON_INTERACTIVE=1`) you watch live in the LaserData console's Orchestration view. Six long-running agents each on their own connection, then discovery, a directed contract, an all-capable fan-out (an unavailable agent routed around), a journalled triage/diagnose/remediate workflow with a budget and a verifier, operator quarantine and un-quarantine, and a deadline expiry that recovers on a healthy agent |
| [`governance`](src/governance/README.md) | agentic | capability RBAC and agent governance, matching the Python and TypeScript `governance`: define roles and bind them to a dedicated Iggy user when `authz` is served, then show deny-wins matching, on-behalf-of permission intersection, external-edge audience and step-up decisions, and budgeted session submission |

## Real LLM (optional)

The desk is LLM-agnostic and runs on a deterministic `MockLlm` by default. To use a real model, build with a feature and set the key.

```sh
ANTHROPIC_API_KEY=... cargo run --example incident-desk --features llm-anthropic
OPENAI_API_KEY=...    cargo run --example incident-desk --features llm-openai
```

## Managed query phases

Projection registration and queries require Laser Stack or LaserData Cloud. Without `laser-plane`, examples report the missing capability and skip those phases. Open streaming still runs.

## Forking the read model (agentic speculation)

On Laser Stack or LaserData Cloud, `laser.fork(id)` branches the materialized read model copy-on-write: write speculative rows, query the overlay with `laser.query(index).fork(id)`, then `promote()` (accept) or `squash()` (discard). The incident desk's bulk-resolve plan walks the whole loop and leaves the fork open by default so the LaserData Cloud UI can show it. `LASER_APPLY_PLAN=1` acts on the verdict instead (promote when the plan clears the backlog, squash when it does not).

```sh
LASER_CONNECTION_STRING=user:pwd@your-laserdata-cloud-host \
LASER_APPLY_PLAN=1 cargo run --example incident-desk
```

## Sessions

The sessions example records an incident root, two child tasks, and an independent maintenance root in one stream. It prints the recorded event and managed resource link counts for each session. Child results are collected explicitly, and child state stays separate.
