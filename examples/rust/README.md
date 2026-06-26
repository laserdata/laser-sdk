# Laser SDK examples - Rust

The Rust examples for the Laser SDK (the language-agnostic intro is one level up in [`../README.md`](../README.md)). One crate, one `[[example]]` binary per scenario under `src/<name>/`, each with its own `README.md`. Every example connects through the shared `laser_examples::connect()` helper, so the same binary runs locally or against a LaserData Cloud deployment with no code change.

Run the commands below from this directory (`examples/rust/`).

## Run locally

Start a local server, then run an example.

```sh
just up                              # start a server on 127.0.0.1:8090
cargo run --example event-analytics
just down                            # stop it
```

With no environment set, `connect()` uses `iggy://iggy:iggy@127.0.0.1:8090`.

## Run against LaserData Cloud

Pass a connection target through the environment. Two forms. The port defaults to 8090 when omitted.

```sh
# Form A: full connection string with embedded credentials
LASER_CONNECTION_STRING='iggy+tcp://user:pwd@starter-123.us-west-1.aws.sandbox.laserdata.cloud' \
  cargo run --example event-analytics

# a token works in place of user:pwd
LASER_CONNECTION_STRING='iggy+tcp://<token>@starter-123.us-west-1.aws.laserdata.cloud' \
  cargo run --example event-analytics

# Form B: host plus separate auth
LASER_SERVER='starter-123.us-west-1.aws.sandbox.laserdata.cloud' \
LASER_TOKEN='<token>' \
  cargo run --example event-analytics
```

For a LaserData host (`*.laserdata.cloud`) TLS and the CA cert attach automatically, even when you pass only the connection string. A `.sandbox` or `.dev` host uses the dev CA, any other LaserData host the prod CA. Both CAs live in `../certs/` (examples-level, shared across language ports). A string that already sets `tls_ca_file=`, a non-LaserData host, or `LASER_NO_TLS=1` is left untouched.

### Environment variables

| variable | effect |
| --- | --- |
| `LASER_CONNECTION_STRING` | full iggy string with `user:pwd@` or `token@`, TLS auto-resolved |
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

Each example runs on its own data stream (`laser-<example>` by default, or `LASER_STREAM` for all). AGDX isolates workloads by stream, never by partition (the Iggy binding, spec B1.1): two examples sharing one stream would also share the well-known agent topics (`agent.commands`, `agent.tool_calls`, ...), so each one's freshly joined consumer group would replay the other's traffic from offset 0 and dead-letter every message it cannot decode. Per-example streams let all examples run against one local server at once without colliding. The SDK creates a stream/topic only when it is missing (a local-dev convenience): point `LASER_STREAM` at your existing stream and nothing new is created. The `_agdx` ops stream belongs to LaserData Cloud (created by LaserData Cloud at boot), not the SDK.

## Examples

Six examples, each a complete realistic system rather than a feature demo, all scaled by the volume knobs above and green on an open server (cloud-gated phases print how to point at a deployment and skip). Every README follows the same shape: a tagline, What it does, Run it, Where to look (LaserData Cloud) where it produces managed artifacts, and Highlights. The concierge example is the full-AGDX showcase: it exercises every surface (streaming and the agent envelope, materialized views and query, key-value, and forks) in one story.

| binary | layer | shows |
| --- | --- | --- |
| [`event-analytics`](src/event-analytics/README.md) | generic | one clickstream, every read model: a live consumer-group ticker tails the raw log while the producer streams, LaserData Cloud materializes a queryable index (funnel, slowest routes, time windows), an independent reader resumes from a `Cursor` + `StateStore` checkpoint, and on a LaserData Cloud a registered JSON Schema guards the index against malformed events |
| [`order-book`](src/order-book/README.md) | generic | the latency-minded market profile: a tuned raw producer streams fills in paced bursts while a tight-poll consumer-group folds a live book (last, VWAP, volume), the same fills index to a queryable tape (integer cents end to end), and on a LaserData Cloud the fills replay as raw Avro datums decoded by a registered writer schema |
| [`firehose`](src/firehose/README.md) | generic | the load generator: millions of multi-KB messages across many org indexes (gigabytes of data) to drive LaserData Cloud's ingest and query path under real storage pressure, with env-configurable volume, payload size, and fan-out |
| [`concierge`](src/concierge/README.md) | agentic | an AI support desk operating a live incident: ticket firehose into a queryable index, semantic memory recall, a four-agent desk (triage fans deadline-bounded specialist calls and synthesizes with the LLM, a KV-deduplicated resolver applies credits effectively once behind a durable approval gate), a coordination demo (a credit-ledger compare-and-swap with a conflict-retry loop, a read-your-writes query, and the unified `ResultCode` classifying every outcome), speculative bulk-resolution in a fork promoted only when it clears the backlog, and the whole incident rebuilt from its conversation as the audit trail |
| [`memory`](src/memory/README.md) | agentic | agentic memory, both halves over one incident domain. The four memory verbs as one loop in process over `VectorMemory` (remember, recall the semantically closest, improve the ranking from an operator upvote, forget a superseded fact, no server needed), then the durable knowledge graph: upsert services and components as content-addressed nodes and typed edges, read a node's neighbors, and traverse from every `Service` to what it depends on. The graph half is browsable in the console's graph explorer (a LaserData Cloud surface, AGDX A13) and skips cleanly on raw Apache Iggy |
| [`interop`](src/interop/README.md) | agentic | edge interoperability over the log: one LLM-backed agent reached as an A2A agent (`message/send` → `tasks/get`), an MCP tool server (`tools/list` / `tools/call`), and an AG-UI event stream (`agui_events`), all bridged onto the Agent Data Exchange Protocol. It runs on the mock model or a real backend with the `llm-*` features |

## Real LLM (optional)

The desk is LLM-agnostic and runs on a deterministic `MockLlm` by default. To use a real model, build with a feature and set the key.

```sh
ANTHROPIC_API_KEY=... cargo run --example concierge --features llm-anthropic
OPENAI_API_KEY=...    cargo run --example concierge --features llm-openai
```

## How the query examples adapt

The query examples call `start_projector(..)`, which picks the projector automatically:

- **local** (default server): spawns an in-process projector so the example runs offline.
- **remote** (`LASER_SERVER` / `LASER_CONNECTION_STRING` set): registers the projection plus binding on LaserData Cloud and lets the Cloud project.

Force either side with `LASER_LOCAL_WORKER=1` / `LASER_LOCAL_WORKER=0`. Query index names use `_` not `.` (e.g. `clickstream`), because LaserData Cloud materializes each index under that exact name and accepts only `[A-Za-z0-9_]`.

## Forking the read model (agentic speculation)

On LaserData Cloud, `laser.fork(id)` branches the materialized read model copy-on-write: write speculative rows, query the overlay with `laser.query(index).fork(id)`, then `promote()` (accept) or `squash()` (discard). The concierge's bulk-resolve plan walks the whole loop and leaves the fork open by default so the LaserData Cloud can show it. `LASER_APPLY_PLAN=1` acts on the verdict instead (promote when the plan clears the backlog, squash when it does not). Wire and lifecycle are in `the AGDX spec` §17.

```sh
LASER_CONNECTION_STRING=iggy://user:pwd@your-laserdata-cloud-host \
LASER_APPLY_PLAN=1 cargo run --example concierge
```
