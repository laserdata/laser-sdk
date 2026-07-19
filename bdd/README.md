# Cross-SDK conformance

Every LaserData SDK, in every language, must behave identically on the wire and in its actions. This directory enforces that with tests that **actually run** in this repo's CI, with no Cloud and no managed backend. Two layers, and a conforming SDK passes both.

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
├── docker-compose.yml                 shared server + per-language runner services
└── README.md
```

Everything here runs against open Apache Iggy. `query.feature` is served by the SDK's own in-process query worker (the offline path the example crate ships): the query surface rides a log topic, so a worker can serve it locally without a managed backend. The same query DSL hits LaserData Cloud unchanged in production.

## What is NOT here, and why

Key-value and forks run as managed operations sent as raw Iggy command codes over the connection (`send_raw_with_response`), which only the managed runtime dispatches. Vanilla Apache Iggy rejects them, so there is no honest way to run the live ops in this repo, and there is no managed deployment to point at. Their **byte** compatibility is fully covered by the fixture corpus (layer 1), and the compare-and-swap **race semantics** are pinned transport-free by the reference engine behind `kv_cas.feature` (`bdd/rust/src/kv_engine.rs`), the cross-SDK CAS contract. End-to-end **behavior** against a live backend is exercised in LaserData Cloud's own repository, which consumes this published SDK and drives it against its own binaries. We never run BDD against a managed deployment from here, and nothing in this repo pretends to.

So: a new SDK is conformant when it passes the fixture corpus (bytes, all surfaces) and every scenario here (behavior, against Apache Iggy).

## Running

```sh
just bdd                       # or: cd bdd/rust && cargo test
```

Needs Docker. The runner manages its own Apache Iggy testcontainer by default. Set `LASER_BDD_ADDR=host:3000` to run against an already-running server (the path other language runners share via docker-compose).

The Python runner covers most of the same scenarios:

```sh
cd bdd/python && pytest -q     # needs the laser-sdk wheel installed and Docker
```

It runs the Iggy-backed scenarios (streaming, capabilities, provenance, agent, governance, runs) against a testcontainer, and serves `query.feature`, `kv_cas.feature`, `graph.feature`, and `memory.feature` from a pure-Python port of the reference engines (`bdd/python/reference.py`), so both languages answer the semantics scenarios identically. `agent_must_understand.feature` and `capabilities_injected.feature` are in-process, no-transport wire checks and stay Rust-only for now.

## Adding a language

1. Create `bdd/<language>/` with that SDK's step-definition runner.
2. Load the **same** `scenarios/*.feature` files. Do not copy or fork them.
3. Implement the steps against your SDK, mapping each `Given`/`When`/`Then` to the same action the Rust reference runner performs.
4. Run the fixture-corpus assertions (layer 1) from your SDK's test suite too.
5. Add a runner service to `docker-compose.yml`.

The scenarios are the specification.
