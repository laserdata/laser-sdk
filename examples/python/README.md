# LaserData - Laser SDK examples - Python

The Python examples share one connection helper. They run against Apache Iggy, Laser Stack, or LaserData Cloud. A managed phase runs only when the connected deployment advertises its capability.

Run the commands below from this directory (`examples/python/`).

## Setup

Choose either the published package or the local checkout.

Both paths use `uv` and do not require `pip` in `/usr/bin/python3`.

Published package:

```sh
uv venv
uv pip install laser-sdk
uv run python log.py
```

Local checkout:

```sh
uv sync --project ../../foreign/python --locked --extra testing
uv run --project ../../foreign/python python log.py
```

## Run locally

Start Apache Iggy, then run an example:

```sh
just up
python3 event_analytics.py
```

With no environment set, the examples connect to `iggy:iggy@127.0.0.1:8090`.

For projections, query, KV, forks, graph, and runs, start Laser Stack with `./scripts/up` from its checkout and use the `LASER_CONNECTION_STRING` it prints.

## Run against LaserData Cloud

Pass a connection target through the environment. The port defaults to 8090 when omitted. `Laser.connect` uses the Rust SDK connection path, so TLS and the embedded CA attach automatically for `*.laserdata.cloud` and `*.laserdata.com`. `LASER_TLS_CERT=<path>` enables TLS with that CA for any host or overrides the embedded CA. A connection string that already sets `tls_ca_file=` remains authoritative.

```sh
# Form A: bare target with embedded credentials or a token
LASER_CONNECTION_STRING='user:pwd@starter-123.us-west-1.aws.laserdata.cloud' \
  python3 event_analytics.py

# Form B: host plus separate auth
LASER_SERVER='starter-123.us-west-1.aws.laserdata.cloud' \
LASER_TOKEN='<token>' \
  python3 event_analytics.py
```

| variable | effect |
| --- | --- |
| `LASER_CONNECTION_STRING` | bare `user:pwd@host` or `token@host` target, transport and TLS resolved by the SDK |
| `LASER_SERVER` | bootstrap host, paired with the auth variables below |
| `LASER_TOKEN` | personal access token auth |
| `LASER_USERNAME`, `LASER_PASSWORD` | username and password auth |
| `LASER_NO_TLS=1` | disable the automatic TLS attach |
| `LASER_STREAM` | override the data stream for every example (default: a per-invocation `laser-<example>-<token>` stream, so repeat and concurrent runs never share state) |
| `LASER_MESSAGES`, `LASER_BATCH` | volume knobs for the publishing examples |
| `LASER_FIREHOSE_*` | the firehose's own knobs (`MESSAGES`, `ORGS`, `CONCURRENCY`, `PAYLOAD_BYTES`, `BATCH`, `PARTITIONS`, `REGISTER`, `QUERY`) |
| `LASER_APPLY_PLAN=1` | the concierge acts on the speculative fork's verdict (promote or squash) instead of leaving it open |
| `LASER_CONCIERGE_CREDIT_TIMEOUT_SECS` | the concierge's credit-apply deadline (default 180s), raise it for a heavily rate-limited deployment |
| `ANTHROPIC_API_KEY` | the concierge uses real Claude for its LLM seam instead of the deterministic mock (`ANTHROPIC_MODEL` optional) |

## Primitives - start here

Each focused script covers one primitive. Unsupported managed phases report the missing capability and exit cleanly.

| script | primitive | shows | needs plane? | docs |
| --- | --- | --- | --- | --- |
| [`log.py`](log.py) | Log | write two messages, read them back through one typed reader | no | [`/laser-sdk/log`](https://docs.laserdata.cloud/laser-sdk/log) |
| [`query.py`](query.py) | Views | declare a view over a topic, publish orders, query the maintained view | yes | [`/laser-sdk/views`](https://docs.laserdata.cloud/laser-sdk/views) |
| [`watch.py`](watch.py) | Change feed | react to an advancement record instead of re-querying blind | yes | [`/laser-sdk/change-feed`](https://docs.laserdata.cloud/laser-sdk/change-feed) |
| [`kv.py`](kv.py) | State | set/get keyed JSON with a TTL, upgrade it under compare-and-swap, write under a revocable lease's fence behind a barriered read, write and promote a fork row | yes | [`/laser-sdk/state`](https://docs.laserdata.cloud/laser-sdk/state) |
| [`graph.py`](graph.py) | Graph | link entities and traverse one relation out of a node | yes | [`/laser-sdk/graph`](https://docs.laserdata.cloud/laser-sdk/graph) |
| [`recall.py`](recall.py) | Memory | all four durable verbs: remember, recall recent, improve, forget | no | [`/laser-sdk/memory`](https://docs.laserdata.cloud/laser-sdk/memory) |
| [`context.py`](context.py) | Context | assemble one conversation under a last-N bound and a token budget | no | [`/laser-sdk/context`](https://docs.laserdata.cloud/laser-sdk/context) |
| [`agent.py`](agent.py) | Fabric | spawn a handler agent and send it a deadline-bounded contract | no | [`/laser-sdk/fabric`](https://docs.laserdata.cloud/laser-sdk/fabric) |

`recall.py` is the Memory primitive's example - it is not named `memory` because that name already belongs to the full scenario below (one of Memory's four verbs instead).

## Deep-dive scenarios

| script | layer | shows |
| --- | --- | --- |
| [`native_streaming.py`](native_streaming.py) | generic | Laser's direct VSR producer and live consumer-group path: tuned batching/linger/retries, exact-width typed headers, keyed routing, 1000 messages published and drained through interval-or-each auto commit, then again through explicit commit-after-success offsets. |
| [`event_analytics.py`](event_analytics.py) | generic | one clickstream, every read model: a cursor folds a live ops ticker while the producer streams, the managed plane materializes a queryable index for funnel / slowest-route / windowed analytics, a second cursor resumes from a checkpoint, and a registered JSON Schema guards the index against malformed events (the analytics, resume, and schema phases skip cleanly on Apache Iggy) |
| [`order_book.py`](order_book.py) | generic | a market-data tape with two readers on one connection: fills stream to a feed topic where a cursor folds a live book (last, VWAP, volume per symbol), and the same fills index to a queryable tape for VWAP / volume aggregates, then a typed handle (`laser.topic(name, cls=Fill)`) replays the tape as dataclass values to audit the totals (the tape analytics skip on Apache Iggy) |
| [`firehose.py`](firehose.py) | generic | a volume load generator: many concurrent producers publish big, richly indexed telemetry events across many org indexes, each materialized into its own queryable index, then a few sample analytics run. Scaled by the `LASER_FIREHOSE_*` knobs |
| [`concierge.py`](concierge.py) | agentic | the full-AGDX showcase, peer of the Rust `concierge`: a ticket firehose into a queryable index, semantic memory recall, a four-agent desk (triage queries the index and fans deadline-bounded specialist calls, the specialist answers from recalled memory plus the LLM, a key-value-deduplicated resolver applies credits effectively once behind a durable approval gate, the approver stands in for the human), a compare-and-swap credit-ledger retry loop with read-your-writes, speculative bulk-resolution in a copy-on-write fork, and the whole incident rebuilt from its conversation as the audit trail (semantic memory is in-process, the index, key-value, and fork phases skip cleanly on Apache Iggy) |
| [`memory.py`](memory.py) | agentic | agentic memory, three facets, peer of the Rust `memory`: the four memory verbs as one loop over a vector memory (remember, recall the semantically closest, improve from an operator upvote, forget a superseded fact), then the same verbs durable over a memory topic materialized into a versioned key-value read view, then the knowledge graph over the same ops domain (upsert services and components, read a node's neighbors, traverse from every `Service` to what it depends on). The durable-memory and graph facets skip cleanly on Apache Iggy, and the durable memories and named graph are browsable in the console's Memory and graph-explorer views |
| [`interop.py`](interop.py) | agentic | reach one agent four ways over the durable log: an A2A task source, an MCP tool server, an AG-UI event stream rendered from a typed AGDX chat stream, and a human-in-the-loop approval gate |
| [`orchestra.py`](orchestra.py) | agentic | the orchestration showcase, 1:1 with the Rust `orchestra`: an interactive, paced run (press Enter per phase) so you can watch it live in the LaserData console's Orchestration view. Six long-running agents connect on their own connections, then discovery, a directed contract, an all-capable fan-out (an unavailable agent routed around), a journalled triage/diagnose/remediate workflow with a budget and a verifier, operator quarantine and un-quarantine, and a deadline expiry that recovers on a healthy agent |
| [`governance.py`](governance.py) | agentic | capability RBAC and agent governance, 1:1 with the Rust `governance`: define roles and bind them to an Iggy user when `authz` is served, then show deny-wins matching, on-behalf-of permission intersection, external-edge audience and step-up decisions, and budgeted run submission when the run registry is served |

Every example runs green on a local Apache Iggy. The managed phases (query, key-value, graph, RBAC, and the run registry) print how to point at a deployment and skip when the connected server is Apache Iggy.
