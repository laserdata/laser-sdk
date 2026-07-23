# Laser SDK examples - TypeScript

The TypeScript examples mirror the nine non-benchmark Rust and Python scenarios. Each example uses the public `@laserdata/laser-sdk` package, the shared connection helper in `src/common.ts`, deterministic input, bounded waits, and the same managed capability gates as the other languages.

Run the commands below from `examples/typescript`.

## Setup

Install the example package and its local SDK dependency.

```sh
npm ci
```

The examples require Node 22.14 or later. SDK resources implement `AsyncDisposable`, so clients, producers, consumers, and agent handles use `await using` for deterministic cleanup.

## Run locally

Start Apache Iggy, then run any example.

```sh
docker run --rm -p 8090:8090 apache/iggy:latest
npm run example:native-streaming
```

With no environment set, the examples connect to `iggy:iggy@127.0.0.1:8090`. Each scenario gets its own `laser-<example>` stream so the agent topics and consumer offsets never collide.

## Run against LaserData Cloud

Pass a bare connection target through the environment. The SDK adds the transport scheme internally, defaults the port to 8090, and attaches TLS with the embedded LaserData CA for `*.laserdata.cloud` and `*.laserdata.com`.

```sh
# Form A: bare target with embedded credentials or a token
LASER_CONNECTION_STRING='user:pwd@starter-123.us-west-1.aws.laserdata.cloud' \
  npm run example:memory

# Form B: host plus separate auth
LASER_SERVER='starter-123.us-west-1.aws.laserdata.cloud' \
LASER_TOKEN='<token>' \
  npm run example:memory
```

Set `LASER_STREAM` to the stream provisioned for the deployment. The helper uses that stream as the default shortcut but the connection can still address every stream on the server.

## Environment

| Variable | Effect |
| --- | --- |
| `LASER_CONNECTION_STRING` | Bare `user:pwd@host` or `token@host` target, transport and TLS resolved by the SDK |
| `LASER_SERVER` | Host paired with `LASER_TOKEN` or username and password |
| `LASER_TOKEN` | Personal access token |
| `LASER_USERNAME`, `LASER_PASSWORD` | Username and password used with `LASER_SERVER` |
| `LASER_TLS_CERT` | CA file that overrides the embedded LaserData CA |
| `LASER_NO_TLS=1` | Disables automatic TLS |
| `LASER_STREAM` | Overrides the per-example `laser-<example>` stream |
| `LASER_MESSAGES` | Record count for examples that publish a configurable workload |
| `LASER_BATCH` | Records per batch |
| `LASER_CONCURRENCY` | Parallel publisher count where supported |
| `LASER_PAYLOAD_BYTES` | Approximate payload size where supported |
| `LASER_APPLY_PLAN=1` | Promotes the concierge fork instead of leaving it open for inspection |
| `LASER_NON_INTERACTIVE=1` | Runs orchestra without waiting for Enter between phases |
| `LASER_GOVERNANCE_USER_ID` | User whose role bindings the governance example manages |
| `ANTHROPIC_API_KEY`, `OPENAI_API_KEY` | Select a real LLM for concierge or interop instead of the deterministic mock |

The firehose also accepts `LASER_FIREHOSE_MESSAGES`, `LASER_FIREHOSE_ORGS`, `LASER_FIREHOSE_CONCURRENCY`, `LASER_FIREHOSE_PAYLOAD_BYTES`, `LASER_FIREHOSE_BATCH`, `LASER_FIREHOSE_PARTITIONS`, `LASER_FIREHOSE_REGISTER`, and `LASER_FIREHOSE_QUERY`.

## Examples

| Example | Layer | What it demonstrates |
| --- | --- | --- |
| [`native-streaming`](src/native-streaming/README.md) | Generic | Direct producer retries, exact typed headers, keyed routing, batch sends, live consumer groups, automatic commits, and explicit commit after successful handling |
| [`event-analytics`](src/event-analytics/README.md) | Generic | A deterministic clickstream, live tailing, checkpointed replay, inline materialized payloads, dashboard aggregates, windows, and registered-schema rejection |
| [`order-book`](src/order-book/README.md) | Generic | Separate hot feed and durable tape, exact live and managed VWAP, inline query payloads, typed replay audit, and schema-first Avro publishing |
| [`firehose`](src/firehose/README.md) | Generic | Bounded concurrent publishing across organization topics, configurable payload pressure, managed index registration, throughput reporting, and sample queries |
| [`concierge`](src/concierge/README.md) | Agentic | Ticket ingestion, semantic memory, a four-agent support desk, durable approval, KV-backed deduplication, speculative fork planning, and conversation replay |
| [`memory`](src/memory/README.md) | Agentic | Vector and durable memory, provenance records, incident blast radius, valid-time graph reads, and traced paths |
| [`interop`](src/interop/README.md) | Agentic | One agent reached through A2A, MCP, AG-UI, and human approval while correlation remains on the durable log |
| [`orchestra`](src/orchestra/README.md) | Agentic | Discovery, directed contracts, capability fan-out, journalled workflows, quarantine, recovery, and deadline rerouting |
| [`governance`](src/governance/README.md) | Agentic | Deny-wins grants, delegated permission intersection, edge step-up, managed RBAC, role bindings, and budgeted run submission |

Every example runs against stock Apache Iggy. Managed phases print one precise skip reason when the server does not advertise their capability. Point the same command at LaserData Cloud to run the full scenario without changing code.

## Verification

```sh
npm run style:check
npm run format:check
npm run typecheck
npm run build
node --test dist/test/common.test.js
```

The smoke suite additionally runs native streaming and interop against a live Apache Iggy instance.
