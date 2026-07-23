# Laser SDK examples - Rust

The Rust examples for the Laser SDK (the language-agnostic intro is one level up in [`../README.md`](../README.md)). One crate, one `[[example]]` binary per scenario under `src/<name>/`, each with its own `README.md`. Every example connects through the shared `laser_examples::laser()` helper, so the streaming phases run locally or against a LaserData Cloud deployment with no code change. Managed phases advertise their requirement and skip on raw Apache Iggy.

Run the commands below from this directory (`examples/rust/`).

## Run locally

Start a local server, then run an example.

```sh
just up                              # start a server on 127.0.0.1:8090
cargo run --example event-analytics
just down                            # stop it
```

With no environment set, `laser()` uses `iggy:iggy@127.0.0.1:8090`.

For a VSR cluster, use the same connection settings and enable the example crate's forwarding feature. The producer, consumer, and application code stays unchanged.

```sh
LASER_CONNECTION_STRING='user:pwd@vsr-host:3000' \
  cargo run --example order-book --features vsr
```

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

For a LaserData host (`*.laserdata.cloud` or `*.laserdata.com`) TLS and the SDK-embedded CA attach automatically, even when you pass only the connection string. Point `LASER_TLS_CERT=<path>` at any CA file to override, the same knob as the connection string's `tls_ca_file=`. A string that already sets `tls_ca_file=`, a non-LaserData host, or `LASER_NO_TLS=1` is left untouched.

### Environment variables

| variable | effect |
| --- | --- |
| `LASER_CONNECTION_STRING` | bare `user:pwd@host` or `token@host` target, transport and TLS resolved by the SDK |
| `LASER_SERVER` | bootstrap host, paired with the auth variables below |
| `LASER_TOKEN` | personal access token auth |
| `LASER_USERNAME`, `LASER_PASSWORD` | username and password auth |
| `LASER_TLS_CERT` | path to a CA cert, overrides auto-attach |
| `LASER_NO_TLS=1` | disable TLS |
| `LASER_STREAM` | overrides the data stream for every example (default: a per-example `laser-<example>` stream). Set it to your provisioned stream on a managed deployment so the SDK uses it and does not auto-create one |

Data-publishing examples also share four volume knobs, so the same binary runs a ten-record smoke test or a multi-million-record soak without a code edit. Each example picks its own default. The env var wins when set.

| variable | meaning |
| --- | --- |
| `LASER_MESSAGES` | total records to publish |
| `LASER_BATCH` | records per send call |
| `LASER_CONCURRENCY` | parallel publishers (where the example fans out) |
| `LASER_PAYLOAD_BYTES` | approximate body size (where the example pads bodies) |

Each example runs on its own data stream (`laser-<example>` by default, or `LASER_STREAM` for all). AGDX isolates workloads by stream, never by partition: two examples sharing one stream would also share the well-known agent topics (`agent.commands`, `agent.tool_calls`, ...), so each one's freshly joined consumer group would replay the other's traffic from offset 0 and dead-letter every message it cannot decode. Per-example streams let all examples run against one local server at once without colliding. The SDK creates a stream/topic only when it is missing (a local-dev convenience): point `LASER_STREAM` at your existing stream and nothing new is created. The `_agdx` ops stream belongs to LaserData Cloud (created by LaserData Cloud at boot), not the SDK.

## Examples

Nine examples, green on an open server (cloud-gated phases print how to point at a deployment and skip). The workload examples scale with the volume knobs above. Every README follows the same shape: a tagline, What it does, Run it, Where to look (LaserData Cloud) where it produces managed artifacts, and Highlights. The concierge example is the full-AGDX showcase: it exercises every surface (streaming and the agent envelope, materialized views and query, key-value, and forks) in one story.

| binary | layer | shows |
| --- | --- | --- |
| [`native-streaming`](src/native-streaming/README.md) | generic | the focused Laser streaming path: configurable direct producer, exact-width headers, keyed and batch sends, live async consumer groups, automatic server-side offset commits, and explicit commit-after-success handling |
| [`event-analytics`](src/event-analytics/README.md) | generic | one clickstream, every read model: a live consumer-group ticker tails the raw log while the producer streams, LaserData Cloud materializes a queryable index (funnel, slowest routes, time windows), an independent reader resumes from a `Cursor` + `StateStore` checkpoint, and on a LaserData Cloud a registered JSON Schema guards the index against malformed events |
| [`order-book`](src/order-book/README.md) | generic | the latency-minded market profile: a tuned Laser producer streams fills in paced bursts while a tight-poll consumer group folds a live book (last, VWAP, volume), the same fills index to a queryable tape (integer cents end to end) audited back through a typed handle (`topic.json::<Trade>().records(..)`), and on a LaserData Cloud the fills replay as raw Avro datums decoded by a registered writer schema |
| [`firehose`](src/firehose/README.md) | generic | the load generator: millions of multi-KB messages across many org indexes (gigabytes of data) to drive LaserData Cloud's ingest and query path under real storage pressure, with env-configurable volume, payload size, and fan-out |
| [`concierge`](src/concierge/README.md) | agentic | an AI support desk operating a live incident: ticket firehose into a queryable index, semantic memory recall, a four-agent desk (triage fans deadline-bounded specialist calls and synthesizes with the LLM, a KV-deduplicated resolver applies credits effectively once behind a durable approval gate), a coordination demo (a credit-ledger compare-and-swap with a conflict-retry loop, a read-your-writes query, and the unified `ResultCode` classifying every outcome), speculative bulk-resolution in a fork promoted only when it clears the backlog, and the whole incident rebuilt from its conversation as the audit trail |
| [`memory`](src/memory/README.md) | agentic | agentic memory over one incident domain, three facets. The four memory verbs as one loop in process over `VectorMemory` (remember, recall the semantically closest, improve the ranking from an operator upvote, forget a superseded fact, no server needed). Then durable memory, the single model, the same verbs over a memory topic configured with `memory_topic("incidents").partitions(..).ttl(..)`, persisted and browsable in the console's Memory view. Then the knowledge graph: upsert services and components as content-addressed nodes and typed edges, read a node's neighbors, and traverse from every `Service` to what it depends on. The durable and graph facets are LaserData Cloud surfaces and skip cleanly on raw Apache Iggy |
| [`interop`](src/interop/README.md) | agentic | edge interoperability over the log: one LLM-backed agent reached as an A2A agent (`message/send` -> `tasks/get`), an MCP tool server (`tools/list` / `tools/call`), and an AG-UI event stream (`agui_events`), all bridged onto the Agent Data Exchange Protocol. It runs on the mock model or a real backend with the `llm-*` features |
| [`orchestra`](src/orchestra/README.md) | agentic | the orchestration showcase, 1:1 with the Python `orchestra`: an interactive, paced run (press Enter per phase) you watch live in the LaserData console's Orchestration view. Six long-running agents each on their own connection, then discovery, a directed contract, an all-capable fan-out (an unavailable agent routed around), a journalled triage/diagnose/remediate workflow with a budget and a verifier, operator quarantine and un-quarantine, and a deadline expiry that recovers on a healthy agent |
| [`governance`](src/governance/README.md) | agentic | capability RBAC and agent governance, 1:1 with the Python `governance`: define roles and bind them to an Iggy user when `authz` is served, then show deny-wins matching, on-behalf-of permission intersection, external-edge audience and step-up decisions, and budgeted run submission when the run registry is served |

## Real LLM (optional)

The desk is LLM-agnostic and runs on a deterministic `MockLlm` by default. To use a real model, build with a feature and set the key.

```sh
ANTHROPIC_API_KEY=... cargo run --example concierge --features llm-anthropic
OPENAI_API_KEY=...    cargo run --example concierge --features llm-openai
```

## Managed query phases

Projection registration and query run on LaserData Cloud. On raw Apache Iggy, event analytics and order book still run their live streaming and replay phases, firehose still publishes its configured load, and each prints one pointer before skipping managed analytics. Concierge is a managed end-to-end scenario and exits green with the same pointer. Query index names use `_` not `.` (for example `clickstream`), because LaserData Cloud materializes each index under that exact name and accepts only `[A-Za-z0-9_]`.

## Forking the read model (agentic speculation)

On LaserData Cloud, `laser.fork(id)` branches the materialized read model copy-on-write: write speculative rows, query the overlay with `laser.query(index).fork(id)`, then `promote()` (accept) or `squash()` (discard). The concierge's bulk-resolve plan walks the whole loop and leaves the fork open by default so the LaserData Cloud can show it. `LASER_APPLY_PLAN=1` acts on the verdict instead (promote when the plan clears the backlog, squash when it does not).

```sh
LASER_CONNECTION_STRING=user:pwd@your-laserdata-cloud-host \
LASER_APPLY_PLAN=1 cargo run --example concierge
```
