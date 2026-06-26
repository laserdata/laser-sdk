# Laser SDK examples - Python

The Python examples for the Laser SDK (the language-agnostic intro is one level up in [`../README.md`](../README.md)). Each example is a single runnable script. They share `_common.py`, which resolves the connection the same way the Rust examples do, so the same environment points either language at a local server or a LaserData Cloud deployment with no code change.

Run the commands below from this directory (`examples/python/`).

## Setup

Install the SDK and point Python at it:

```sh
pip install laser-sdk
```

For a local-from-source build, run `maturin develop` in `../../foreign/python` inside a virtualenv, then run the examples with that interpreter.

## Run locally

Start a local Apache Iggy, then run an example:

```sh
docker run --rm -p 8090:8090 apache/iggy:latest   # or your own server on 127.0.0.1:8090
python event_analytics.py
```

With no environment set, the examples connect to `iggy://iggy:iggy@127.0.0.1:8090`.

## Run against LaserData Cloud

Pass a connection target through the environment. The port defaults to 8090 when omitted. TLS and the CA cert attach automatically for a `*.laserdata.cloud` host (a `.sandbox` or `.dev` host uses the dev CA, any other the prod CA, both vendored in `../certs/`).

```sh
# Form A: full connection string with embedded credentials or a token
LASER_CONNECTION_STRING='iggy+tcp://user:pwd@starter-123.us-west-1.aws.laserdata.cloud' \
  python event_analytics.py

# Form B: host plus separate auth
LASER_SERVER='starter-123.us-west-1.aws.laserdata.cloud' \
LASER_TOKEN='<token>' \
  python event_analytics.py
```

| variable | effect |
| --- | --- |
| `LASER_CONNECTION_STRING` | full iggy string with `user:pwd@` or `token@`, TLS auto-resolved |
| `LASER_SERVER` | bootstrap host, paired with the auth variables below |
| `LASER_TOKEN` | personal access token auth |
| `LASER_USERNAME`, `LASER_PASSWORD` | username and password auth |
| `LASER_NO_TLS=1` | disable the automatic TLS attach |
| `LASER_STREAM` | override the data stream for every example (default: a per-example `laser-<example>` stream) |
| `LASER_MESSAGES`, `LASER_BATCH` | volume knobs for the publishing examples |
| `LASER_FIREHOSE_*` | the firehose's own knobs (`MESSAGES`, `ORGS`, `CONCURRENCY`, `PAYLOAD_BYTES`, `BATCH`, `PARTITIONS`, `REGISTER`, `QUERY`) |
| `LASER_APPLY_PLAN=1` | the concierge acts on the speculative fork's verdict (promote or squash) instead of leaving it open |
| `ANTHROPIC_API_KEY` | the concierge uses real Claude for its LLM seam instead of the deterministic mock (`ANTHROPIC_MODEL` optional) |

## Examples

| script | layer | shows |
| --- | --- | --- |
| [`event_analytics.py`](event_analytics.py) | generic | one clickstream, every read model: a cursor folds a live ops ticker while the producer streams, the managed plane materializes a queryable index for funnel / slowest-route / windowed analytics, a second cursor resumes from a checkpoint, and a registered JSON Schema guards the index against malformed events (the analytics, resume, and schema phases skip cleanly on raw Apache Iggy) |
| [`order_book.py`](order_book.py) | generic | a market-data tape with two readers on one connection: fills stream to a feed topic where a cursor folds a live book (last, VWAP, volume per symbol), and the same fills index to a queryable tape for VWAP / volume aggregates (the tape analytics skip on raw Apache Iggy) |
| [`firehose.py`](firehose.py) | generic | a volume load generator: many concurrent producers publish big, richly indexed telemetry events across many org indexes, each materialized into its own queryable index, then a few sample analytics run. Scaled by the `LASER_FIREHOSE_*` knobs |
| [`concierge.py`](concierge.py) | agentic | the full-AGDX showcase, peer of the Rust `concierge`: a ticket firehose into a queryable index, semantic memory recall, a four-agent desk (triage queries the index and fans deadline-bounded specialist calls, the specialist answers from recalled memory plus the LLM, a key-value-deduplicated resolver applies credits effectively once behind a durable approval gate, the approver stands in for the human), a compare-and-swap credit-ledger retry loop with read-your-writes, speculative bulk-resolution in a copy-on-write fork, and the whole incident rebuilt from its conversation as the audit trail (the index, memory, key-value, and fork phases skip cleanly on raw Apache Iggy) |
| [`recall.py`](recall.py) | agentic | an agent that learns from feedback, peer of the Rust `recall`: the four agentic-memory verbs as one loop. Remember facts, recall the semantically closest for a question, improve the ranking from an operator upvote so the helpful fact rises next time, and forget a superseded fact. Runs the loop over an in-process vector memory |
| [`interop.py`](interop.py) | agentic | reach one agent four ways over the durable log: an A2A task source, an MCP tool server, an AG-UI event stream rendered from a typed AGDX chat stream, and a human-in-the-loop approval gate |

Every example runs green on a local Apache Iggy. The managed phases (query, key-value) print how to point at a deployment and skip when the connected server is raw Apache Iggy.
