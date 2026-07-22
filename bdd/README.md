# Cross-SDK conformance

Every LaserData SDK must behave identically on the wire and in its actions. This directory enforces that in Rust, Python, and TypeScript with no Cloud dependency. A conforming SDK passes both layers below.

## Layer 1: the wire (payload)

The golden fixture corpus in the wire crate (`wire/fixtures/`, the `.bin` and `.json` files) pins the exact canonical bytes of every envelope on every surface, including the managed query, key-value, fork, control, and hello request/reply frames. A conforming port encodes the same values to the same bytes and decodes those bytes back, byte-for-byte, no server involved. This is the contract for **spec, payload, and headers**, and it covers the managed surfaces even though their behavior is not exercised here. The Rust crate asserts it in `wire/tests/wire_fixtures.rs`, and other ports assert the same files.

## Layer 2: behavior (actions)

The Gherkin scenarios in `scenarios/` describe end-to-end behavior, run by every language's step definitions against a real Apache Iggy testcontainer:

```
bdd/
├── scenarios/
│   ├── agent.feature                  the AGDX agent envelope + conversation threading
│   ├── agent_must_understand.feature  the must-understand feature-bit rejection rule
│   ├── capabilities.feature           negotiation + the Unsupported boundary
│   ├── capabilities_injected.feature  the read-consistency pre-gate under injected caps
│   ├── governance.feature             action governance at the effect boundary
│   ├── graph.feature                  knowledge graph traversal semantics (reference engine)
│   ├── kv_cas.feature                 compare-and-swap semantics (reference engine)
│   ├── memory.feature                 agentic memory recall semantics (reference engine)
│   ├── provenance.feature             provenance + causality round-trip
│   ├── query.feature                  the query DSL semantics (reference engine)
│   ├── runs.feature                   the managed run registry + the Unsupported boundary
│   └── streaming.feature              typed publish on the log
├── rust/                              Rust reference runner (cucumber-rs)
├── python/                            Python runner (pytest-bdd), same scenarios
├── typescript/                        TypeScript runner (Cucumber), same scenarios
├── docker-compose.yml                 shared server + per-language runner services
└── README.md
```

Streaming, provenance, and agent scenarios run against open Apache Iggy. Managed query, key-value, graph, memory, and run semantics use deterministic transport-free reference engines. The production query client uses the managed `AGDX_QUERY` command and does not use a request topic.

## What is NOT here, and why

Key-value and forks run as managed operations sent as raw Iggy command codes over the connection (`send_raw_with_response`), which only the managed runtime dispatches. Vanilla Apache Iggy rejects them, so there is no honest way to run the live ops in this repo, and there is no managed deployment to point at. Their **byte** compatibility is fully covered by the fixture corpus (layer 1), and the compare-and-swap **race semantics** are pinned transport-free by the reference engine behind `kv_cas.feature` (`bdd/rust/src/kv_engine.rs`), the cross-SDK CAS contract. End-to-end **behavior** against a live backend is exercised in LaserData Cloud's own repository, which consumes this published SDK and drives it against its own binaries. We never run BDD against a managed deployment from here, and nothing in this repo pretends to.

So: a new SDK is conformant when it passes the fixture corpus (bytes, all surfaces) and every scenario here (behavior, against Apache Iggy).

## Running

```sh
just bdd                       # or: cd bdd/rust && cargo test
```

Needs Docker. The runner manages its own Apache Iggy testcontainer by default. Set `LASER_BDD_ADDR=host:3000` to run against an already-running server (the path other language runners share via docker-compose).

The Python runner executes the complete scenario inventory:

```sh
cd bdd/python && pytest -q     # needs the laser-sdk wheel installed and Docker
```

It runs the Iggy-backed scenarios against the shared server and uses the Python reference engines for transport-free managed semantics. Must-understand and injected-capability scenarios run in process in all three languages.

The TypeScript runner loads every canonical feature and resolves every step before execution:

```sh
scripts/run-bdd-tests.sh typescript
```

Set `LASER_BDD_URL` to a full Iggy connection string, or `LASER_BDD_ADDR` to
`host:port`. Without either it connects to `127.0.0.1:8090`. Query, KV, graph,
and memory semantics use the same transport-free reference split as the other
runners. Every Iggy-backed step uses the public package API.

## Adding a language

1. Create `bdd/<language>/` with that SDK's step-definition runner.
2. Load the **same** `scenarios/*.feature` files. Do not copy or fork them.
3. Implement the steps against your SDK, mapping each `Given`/`When`/`Then` to the same action the Rust reference runner performs.
4. Run the fixture-corpus assertions (layer 1) from your SDK's test suite too.
5. Add a runner service to `docker-compose.yml`.

The scenarios are the specification.
