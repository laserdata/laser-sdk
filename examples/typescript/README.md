# LaserData -Laser SDK examples - TypeScript

The TypeScript examples cover the Rust and Python catalog: nine tiny primitive examples plus the nine non-benchmark deep-dive scenarios. The deep-dive scenarios are shorter than their Rust counterparts and exercise the same primitives. Each example uses the public `@laserdata/laser-sdk` package, the shared connection helper in `src/common.ts`, deterministic input, bounded waits, and the same managed capability gates as the other languages.

Run the commands below from `examples/typescript`.

**Consumer filters: 98.5% less payload transfer in the CDC example.** The reader receives 4 of 240 records from the shared feed, with original bytes and offsets. The example also covers typed records, one-byte numeric headers, previews, and saved group policies. See the [Consumer Filters guide](https://docs.laserdata.cloud/laser-sdk/consumer-filters).

## Setup

Install the example dependencies and build the local SDK package:

```sh
npm run setup
```

The examples require Node 22.14 or later. SDK resources implement `AsyncDisposable`, so clients, producers, consumers, and agent handles use `await using` for deterministic cleanup.

## Run locally

Start Apache Iggy, then run any example. The SDK uses Iggy's native VSR transport.

```sh
npm run example:native-streaming
```

With no environment set, the examples connect to `iggy:iggy@127.0.0.1:8090`. Each example uses its own `laser-<example>-typescript` stream so the agent topics, consumer offsets, and managed views of different examples never collide. A run deletes the previous run's stream first and keeps its own result on the server, so you can inspect it afterwards with the SDK, the Iggy CLI, or the LaserData Cloud Console. Managed index names carry a per-run token. A stream supplied through `LASER_STREAM` is never deleted.

For the complete managed surface, start Laser Stack with `./scripts/up` from its checkout and use the `LASER_CONNECTION_STRING` it prints.

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
| `LASER_TLS_CERT` | CA file that enables TLS for any host or overrides the embedded LaserData CA |
| `LASER_NO_TLS=1` | Disables automatic TLS |
| `LASER_STREAM` | Overrides the per-example `laser-<example>-typescript` stream |
| `LASER_MESSAGES` | Record count for examples that publish a configurable workload |
| `LASER_BATCH` | Records per batch |
| `LASER_APPLY_PLAN=1` | Promotes the incident-desk fork instead of leaving it open for inspection |
| `LASER_NON_INTERACTIVE=1` | Runs orchestra without waiting for Enter between phases |
| `LASER_GOVERNANCE_USER_ID` | User whose role bindings the governance example manages |
| `ANTHROPIC_API_KEY`, `OPENAI_API_KEY` | Select a real LLM for incident-desk or interop instead of the deterministic mock |

The firehose also accepts `LASER_FIREHOSE_MESSAGES`, `LASER_FIREHOSE_ORGS`, `LASER_FIREHOSE_CONCURRENCY`, `LASER_FIREHOSE_PAYLOAD_BYTES`, `LASER_FIREHOSE_BATCH`, `LASER_FIREHOSE_PARTITIONS`, `LASER_FIREHOSE_REGISTER`, and `LASER_FIREHOSE_QUERY`.

## Primitives - start here

One tiny, single-primitive example each, most under 100 lines including imports. `cdc` is longer because it walks every filter phase. Read one in a minute, then jump to the deep-dive scenario that uses the same primitive in anger.

| Example | Primitive | What it shows | Needs Cloud? | Docs |
| --- | --- | --- | --- | --- |
| [`log`](src/log/README.md) | Log | Ensure a topic, publish two JSON records, replay them back through one typed reader | no | [`/laser-sdk/log`](https://docs.laserdata.cloud/laser-sdk/log) |
| [`query`](src/query/README.md) | Views | Declare a view over a topic, publish host readings, query the maintained view | yes | [`/laser-sdk/views`](https://docs.laserdata.cloud/laser-sdk/views) |
| [`watch`](src/watch/README.md) | Change feed | React to an advancement record instead of re-querying blind | yes | [`/laser-sdk/change-feed`](https://docs.laserdata.cloud/laser-sdk/change-feed) |
| [`kv`](src/kv/README.md) | State | Set/get keyed JSON with a TTL, change it under compare-and-swap, write under a revocable lease's fence behind a barriered read, write and promote a fork row | yes | [`/laser-sdk/state`](https://docs.laserdata.cloud/laser-sdk/state) |
| [`cdc`](src/cdc/README.md) | Consumer filters | Read four safe-mode events out of a 240-record feed of typed records, sample-test and preview filters, route binary alerts on a header, then save filters and bind a consumer group (bindings need plane) | no | [`/laser-sdk/consumer-filters`](https://docs.laserdata.cloud/laser-sdk/consumer-filters) |
| [`graph`](src/graph/README.md) | Graph | Link entities and traverse one relation out of a node | yes | [`/laser-sdk/graph`](https://docs.laserdata.cloud/laser-sdk/graph) |
| [`recall`](src/recall/README.md) | Memory | All four durable verbs: remember, recall recent, improve, forget | no | [`/laser-sdk/memory`](https://docs.laserdata.cloud/laser-sdk/memory) |
| [`context`](src/context/README.md) | Context | Assemble one conversation under a `LastN` + `TokenBudget` policy chain | no | [`/laser-sdk/context`](https://docs.laserdata.cloud/laser-sdk/context) |
| [`agent`](src/agent/README.md) | Fabric | Spawn a handler agent and send it a deadline-bounded contract | no | [`/laser-sdk/fabric`](https://docs.laserdata.cloud/laser-sdk/fabric) |

## Deep-dive scenarios

| Example | Layer | What it demonstrates |
| --- | --- | --- |
| [`native-streaming`](src/native-streaming/README.md) | Generic | Direct producer retries, exact typed headers, keyed routing, batch sends, live consumer groups, automatic commits, and explicit commit after successful handling |
| [`event-analytics`](src/event-analytics/README.md) | Generic | A deterministic clickstream, live tailing, checkpointed replay, inline materialized payloads, dashboard aggregates, windows, and registered-schema rejection |
| [`fleet-tape`](src/fleet-tape/README.md) | Generic | Separate hot feed and durable tape, exact live and managed sample-weighted mean CPU, inline query payloads, typed replay audit, and schema-first Avro publishing |
| [`firehose`](src/firehose/README.md) | Generic | Bounded concurrent publishing across organization topics, configurable payload pressure, managed index registration, throughput reporting, and sample queries |
| [`incident-desk`](src/incident-desk/README.md) | Agentic | Ticket ingestion, semantic memory, a four-agent incident desk, durable approval, KV-backed deduplication, speculative fork planning, and conversation replay |
| [`memory`](src/memory/README.md) | Agentic | Vector and durable memory, provenance records, incident blast radius, valid-time graph reads, and traced paths |
| [`interop`](src/interop/README.md) | Agentic | One agent reached through A2A, MCP, AG-UI, and human approval while correlation remains on the durable log |
| [`orchestra`](src/orchestra/README.md) | Agentic | Discovery, directed contracts, capability fan-out, journalled workflows, quarantine, recovery, and deadline rerouting |
| [`governance`](src/governance/README.md) | Agentic | Deny-wins grants, delegated permission intersection, edge step-up, managed RBAC, role bindings, and budgeted run submission |

Every example runs its open phase against Apache Iggy. Managed phases print one precise skip reason when the server does not advertise their capability. Point the same command at Laser Stack or LaserData Cloud to run the full scenario without changing code.

## Verification

```sh
npm run style:check
npm run format:check
npm run typecheck
npm run build
node --test dist/test/common.test.js
```

The smoke suite additionally runs native streaming and interop against a live Apache Iggy instance.
