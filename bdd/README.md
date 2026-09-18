# Cross-SDK conformance

The shared tests compare Rust, Python, and TypeScript data encoding and behavior. The default suite does not require LaserData Cloud. A conforming implementation passes both groups below.

## Layer 1: the wire (payload)

Reference files under `wire/fixtures/` define expected `.bin` and `.json` encodings. They cover envelopes, headers, and managed operations. Each client must encode matching values to the same bytes and decode those bytes correctly. Rust tests this through `wire/tests/wire_fixtures.rs`. Other clients use the same files without a server.

## Layer 2: behavior (actions)

The Gherkin scenarios in `scenarios/` describe end-to-end behavior. Rust, Python, and TypeScript run against the versioned Apache Iggy server:

```text
bdd/
├── scenarios/
│   ├── agent.feature                  the AGDX agent envelope + conversation threading
│   ├── agent_must_understand.feature  the must-understand feature-bit rejection rule
│   ├── capabilities.feature           negotiation + the Unsupported boundary
│   ├── capabilities_injected.feature  the read-consistency pre-gate under injected caps
│   ├── data_stack.feature              schemas, typed query, destinations, checkpoints, and Arrow
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

KV and forks use managed commands through `send_raw_with_response`. Apache Iggy without a managed backend rejects these calls. Reference files cover their bytes, and `kv_cas.feature` tests behavior through the reference engine in `bdd/rust/src/kv_engine.rs`. Full managed execution needs Laser Stack or LaserData Cloud. The default BDD suite uses Apache Iggy and local reference engines.

A new client must pass the shared reference files and applicable behavior scenarios.

## Running

```sh
just bdd                       # or: cd bdd/rust && cargo test
```

All three runners resolve the versioned Iggy binary by default. Set `LASER_TEST_IGGY_SERVER=/path/to/iggy-server` for a local build, or `LASER_BDD_ADDR=host:port` for an already-running Iggy server.

The Python runner executes the complete scenario inventory:

```sh
cd bdd/python && pytest -q     # needs the laser-sdk wheel installed
```

It runs Iggy-backed scenarios against the shared server and uses the Python reference engines for transport-free managed semantics. Must-understand and injected-capability scenarios run in process in all three languages.

The TypeScript runner loads every canonical feature and resolves every step before execution:

```sh
scripts/run-bdd-tests.sh typescript
```

Set `LASER_BDD_URL` to a connection string or `LASER_BDD_ADDR` to `host:port` for an existing server. The supplied test scripts otherwise start the versioned native server. Query, KV, graph, and memory scenarios use reference engines. Iggy-backed steps use the public SDK API.

## Adding a language

1. Create `bdd/<language>/` with that SDK's step-definition runner.
2. Load the same `scenarios/*.feature` files. Do not copy or fork them.
3. Implement the steps against your SDK, mapping each `Given`/`When`/`Then` to the same action the Rust reference runner performs.
4. Run the fixture-corpus assertions (layer 1) from your SDK's test suite too.
5. Add a runner service to `docker-compose.yml`.

The scenarios are the specification.
