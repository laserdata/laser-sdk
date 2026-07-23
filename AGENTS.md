# Laser SDK Agent Guidelines

This repo is one workspace holding two published crates. **laser-wire** is the LaserData wire contract (types, codes, envelopes, dictionaries, caps, the golden fixture corpus, and the Agent Data Exchange Protocol envelope), runtime-free and wasm-portable, consumed by LaserData Cloud, the Iggy server, and the SDK alike. **laser-sdk** is the open, customer-facing **data-platform SDK** over [Apache Iggy](https://iggy.apache.org) built on top of it: typed publish/consume, declared projections with a query DSL, a key-value store, and copy-on-write forks over one connection, plus an optional agent layer (provenance-tagged messages, a conversation/causality spine, agent topic routing, a reliable consumer, context assembly, a memory seam, and an `Agent` builder). It carries `gen_ai.*` provenance describing LLM calls but never makes them: it only moves and coordinates messages.

> Skills under `.claude/skills/` cover the SDK by area. Load [laser-sdk-overview](.claude/skills/laser-sdk-overview/SKILL.md) first, then the focused skill for the area you are touching.
>
> The [AGDX spec](docs/agdx.md) is the authoritative wire/convention reference (streams, topics, headers, envelopes, query DSL, caps), kept byte-identical to the LaserData Cloud's wire types. Update it whenever the wire contract changes.

## Contents

- [STOP and ask the user before](#stop-and-ask-the-user-before)
- [Verification order](#verification-order)
- [Structure](#structure)
- [Repo-wide principles](#repo-wide-principles)
- [Conventions](#conventions)
- [Testing](#testing)
- [What is shipped vs planned](#what-is-shipped-vs-planned)

## STOP and ask the user before

These change the on-the-wire or public contract and break data or downstreams:

- Editing ANYTHING in `wire/src/` that alters encoded payload: command codes, op versions, header keys (`wire/src/headers.rs`), topic names, envelope field names or serde attributes, the content-type / task-state / error-code u8 dictionaries, or the caps. Every message already on the log and every consumer (LaserData Cloud, the Iggy server) is pinned to those bytes. The golden corpus under `wire/fixtures/` will fail on drift, and an intentional change needs the fixtures regenerated and the op version bumped.
- Changing `ConversationId::derive` (the FNV-1a algorithm in `sdk/src/types/ids.rs`) without bumping `DERIVE_VERSION` - silently remaps every `SessionPolicy::PerUser` conversation. (`AgentId::wire_id` is no longer a hash: the wire agent id is the name string verbatim.)
- Renaming `AgentTopic` names (`sdk/src/provenance/topic.rs`) - repoints live topics.
- Changing `Provenance::partition_key` (currently `conversation_id`) - breaks the per-conversation ordering guarantee.
- Changing the public signatures of `Laser`, `AgentHandler`, `Agent`, `AgentConsumer::run`, or `AgentHandle` - these are the customer-facing API.
- Dropping an attribution field (`agent`, `root_conversation_id`, ...) when rebuilding `Provenance` in `spawn_subconversation` / `AgentCtx::reply_provenance` silently breaks causality and cost rollup across composed flows.
- Query is LaserData Cloud only: it rides the `AGDX_QUERY` managed command off the log (`send_raw_with_response`, no reply topic, no correlation poll), and against raw Apache Iggy returns `LaserError::Unsupported`. There is no topic request/reply query path. The connect-time `AGDX_HELLO` probe sets the connect-local `managed_host` **and** lights up `query.available` (with the other managed surfaces), so a plain `Laser::connect(..)` against LaserData Cloud queries with no manual caps.

## Verification order

Enforced by CI (`.github/workflows/ci-rust.yml`: jobs `lint`, `lint-detached`, `build`, `wire-feature-matrix`, `feature-matrix`, `wasm`, `deny`, `test`, `fuzz`, `bdd`). The `bdd/rust` and `fuzz` crates sit OUTSIDE the workspace, so `--workspace` does not reach them. `lint-detached` (and `just lint`) run fmt/sort/machete/clippy inside each. Run locally in this exact order, do not skip:

```bash
cargo fmt --all            # 1. formats, auto-applies
cargo sort --workspace     # 2. sorts Cargo.toml deps + feature arrays
cargo machete              # 3. no unused dependencies
cargo clippy --workspace --all-targets --all-features -- -D warnings  # 4.
cargo test --workspace --all-features                   # 5. unit + wire fixtures/robustness, compiles VSR
just test-it                                              # 6. classic Iggy integration suite (Docker)
cargo test --workspace --all-features --doc             # 7. doctests (Docker-free)
just wasm                  # 8. laser-wire on wasm32-unknown-unknown (needs the target)
just deny-wire             # 9. laser-wire dependency bans (needs cargo-deny)
just advisories            # 10. workspace vuln/unmaintained advisories (needs cargo-deny)
just fuzz                  # 11. bounded fuzzing of the wire decode surface (nightly + cargo-fuzz)
just bdd                   # 12. cross-SDK BDD conformance, Rust runner (needs Docker)
```

**Doctests are a required gate, not optional.** `clippy --all-targets` (step 4) does **not** compile doctests, and a bare `cargo test --workspace` runs them only for crates whose default features are on, so a doctest behind `kv` / `query` / any non-default feature is silently never built. Step 7 (`--all-features --doc`) is what actually compiles + runs every doc example. Skipping it means a broken `///` example ships green. It is Docker-free (doctests do not touch Apache Iggy), so there is no reason to skip it. The same `--all-features --doc` gate runs in CI.

**VSR and classic Iggy framing are compile-time alternatives.** Cargo's additive `--all-features` activates `vsr`, so step 5 compiles and tests every protocol-independent surface but excludes the classic single-node integration target. Step 6 runs that real-server suite without `vsr`. The isolated feature matrix proves `laser-sdk/vsr -> iggy/vsr -> iggy_common/vsr`. Run `native-streaming` with `--features vsr` against a VSR cluster for focused producer/consumer testing, and `order-book` for a sustained workload.

**The wasm and deny gates are what make laser-wire's portability guarantee real**: the wire crate must compile for `wasm32-unknown-unknown` with `cbor,codecs,fixtures,builders,http-client` (never `bson`, which is native-only by design), and its portable graph must never contain iggy, tokio, bytes, ulid, dashmap, tracing, or getrandom (`deny-wire.toml`). The `builders` (bon's `Query::builder`) and `http-client` (typed `/agdx/*` client + serde_urlencoded) features are wasm-facing and included in both gates. If either tool is missing locally, CI still enforces both.

`just lint`, `just test`, `just test-it`, and `just ci` (= all of the above) wrap these. Never run `cargo install` or mutate the toolchain. If a tool is missing, stop and ask.

## Structure

```
wire/                   the laser-wire crate: the wire CONTRACT, data + pure functions only
  src/
    codes.rs            managed command codes + per-surface op versions (incl. AGENT_OP_VERSION)
    headers.rs          agdx.* / gen_ai.* header dictionaries + header caps (incl. agdx.av)
    topics.rs           _agdx ops stream + topic names
    limits.rs           page / KV / frame / agent-envelope caps
    content.rs          ContentType + the agdx.ct u8 dictionary
    hello.rs            HelloReply / OpVersions (AGDX_HELLO probe body, additive `agent` field + `features` capability bitset: feature::{KV_CAS,READ_YOUR_WRITES,STRONG_CONSISTENCY,KV_CAS_FENCED,AGENT_WORKFLOW,KEYWORD_SEARCH,WATCH,AUTHZ}) + BackendAnnounce (backend->streaming-server capability announce, AGDX_BACKEND_HELLO_CODE)
    query.rs            query IR (incl. Consistency level) + QueryEnvelope/QueryReply/Row/QueryError (incl. Stale)
    result.rs           unified ResultCode space + HTTP status, From projections off every surface error
    browse.rs           registry browse requests + BrowseReply (incl. DecodeRecord)
    control.rs          Projection / ProjectionBinding / SchemaDef / ControlEnvelope + builders
    kv.rs               KV requests (incl. KvCas/CasExpect) + KvReply (incl. Committed) / KvError (incl. VersionConflict) + entry version
    fork.rs             fork requests + ForkReply/ForkError
    agent.rs            the Agent Data Exchange Protocol: AgentEnvelope, machine ids (16-byte u128 + Crockford), agent ids (bounded name strings)
                        base32), AgentKind, TaskState/AgentErrorCode/DeadLetterReason u8
                        dictionaries, TokenUsage, AgentDeadLetter, dormant Signature,
                        BodyRef (the agdx.ct=ref claim-check capsule), the pinned
                        operation vocabularies (task/card/progress, chunk-stream
                        chat/reasoning/tool_args, state_snapshot/state_delta) and
                        metadata keys (role, bridge_hops), validate() (the per-kind
                        validity matrix + caps)
    forward.rs          forwarded ForwardedQuery / ForwardedCommand frames
    commands.rs         Command trait pairing each managed command code with request/reply types
    http.rs             /agdx/* route constants + path builders + typed query-param structs + JSON views + the ErrorBody reply contract
    http_client.rs      (feature http-client) typed /agdx/* client over an injected Transport, the crate's one (runtime-agnostic) async surface
    framing.rs          (feature cbor) encode_named/decode_named + u32-LE frame codec, sans-io
    codecs.rs           (feature codecs, bson adds Bson) Codec/Decoder + Json/Msgpack/Cbor/Bson
    encoding.rs         the one shared bin-bytes serde helper module (internal)
    error.rs            DecodeError + InvalidError (the SDK maps them into LaserError)
    fixtures.rs         (feature fixtures) the golden corpus embedded via include_bytes!
  fixtures/             the golden corpus (CBOR .bin + HTTP .json), regen via
                        AGDX_WIRE_FIXTURES_REGEN=1 (just fixtures-regen)
  tests/
    wire_fixtures.rs    byte-identity suite over the corpus (incl. agent draft fixtures
                        and the negative validity-matrix cases)
    robustness.rs       deterministic decode-never-panics suite: random + byte-flipped
                        + truncated inputs through the framer and every envelope
    constants.rs        every code / key / topic / cap pinned as a LITERAL
fuzz/                   cargo-fuzz crate (nightly, outside the workspace): the
                        frame_decode and decode_envelope targets, run via just fuzz
sdk/src/
  lib.rs              module wiring + LaserError re-export + `pub use laser_wire as wire`
  error.rs            LaserError (the one crate error, maps wire DecodeError/InvalidError)
  prelude.rs          the single glob downstreams import
  laser.rs            Laser + LaserBuilder: connect/connect_env/connect_with_stream/local,
                      producer cache, send_raw_with_response (the raw managed-command
                      transport, Vec<u8> in/out, converts to/from iggy's own `Bytes` only at
                      the client call, never in the SDK's own signatures). `resolve_tls`
                      auto-attaches TLS + the bundled public CA (`certs/laserdata.crt`,
                      `include_bytes!`) for a `*.laserdata.cloud`/`*.laserdata.com` host with no
                      `tls_ca_file=` already set. `LASER_TLS_CERT` overrides the cert,
                      `LASER_NO_TLS` disables the whole check, any other host passes through
                      untouched
  types/              mod.rs re-exports the id types, ids.rs: ConversationId / AgentId /
                      MessageId (FromStr+Display) + the AGDX id bridge (MintUlid, ConversationId
                      <-> wire id, AgentId::wire_id = the name verbatim)
  provenance/         keys.rs re-exports the wire header dictionary, runtime.rs
                      (Provenance <-> headers) + topic.rs (AgentTopic) behind `provenance`
  poll.rs             shared partition drain helper (context + reply + cursor paths)
  stream.rs           the Log accessors: Laser::stream(name) -> Stream (real Iggy stream,
                      ensure/topic) and Laser::topic(name) -> Topic (default-stream shortcut,
                      publish()/publish_batch() fluent, send/batch raw (impl Into<Vec<u8>>,
                      matching PublishRequest/BatchPublishRequest), replay() -> Cursor,
                      ensure(partitions), producer/consumer/consumer_group, plus the raw
                      iggy_producer/iggy_consumer/iggy_consumer_group escape hatch)
                      `vsr` forwards to iggy/vsr and implies streaming, standard Iggy commands
                      switch protocols with no API change, managed custom codes stay unavailable
                      until upstream VSR admits them
  stream/             publish.rs (publish builders) and record.rs (Record lowering, Vec<u8>
                      payload, one-shot/typed publish, not the hot loop), transport.rs is the
                      Laser-native direct Producer plus live futures::Stream Consumer/
                      ConsumerMessage path - ProducerMessage/ConsumerMessage keep `bytes::Bytes`
                      (zero-copy clone) on this hot path specifically, including routing,
                      polling/replay, group lifecycle, retries, exact headers, automatic commits,
                      explicit commit-after-success, server offsets, and next_within(timeout) for
                      a bounded single-record wait
  batching.rs         Topic::batching() -> BatchingProducer: the governed size-and-time batcher
                      (max_records / max_bytes / linger) for typed agent paths (feature = "agent")
  blob.rs             BlobStore claim-check seam (feature = "agent"): check_in externalizes a
                      payload at or over a threshold to the BodyRef capsule, no default store ships
  context.rs          ContextAssembler + ContextPolicy (LastN, RoleFilter)
  context_scope.rs    Laser::context(conversation) -> ContextScope (append / fetch bounded /
                      fetch_with policy / block / state), the conversation-scoped accessor,
                      .memory(ns) -> ScopedMemory bakes the conversation into recall/remember/
                      block/consolidate (durable memory + graph stay cross-conversation)
  message.rs          Message: raw payload + MessageId + headers off the log, no Provenance
                      decoding (the agent layer reconstructs that on top from the same headers)
  cursor.rs           Cursor: resumable offset-addressable stream read (public face:
                      Topic::replay)
  typed.rs            typed topics on the streaming layer: Topic::json::<T>() / cbor::<T>() /
                      schema::<T>(id), TypedTopic publish (encode + validate + stamp) and
                      records(reader_name) -> TypedRecords (Cursor-backed, TypedDecodeError with
                      the record's log position)
  schema_codecs.rs    CompiledSchema: compile a registered writer schema client-side (Avro /
                      Protobuf / JSON Schema), encode and validate a body before it is published,
                      the same decode semantics the managed projector applies (feature =
                      "schema-codecs")
  runs.rs             Laser::runs() -> the managed run registry (submit / status / cancel /
                      fluent paged list), agent_workflow-gated (feature = "runs")
  memory.rs           Memory trait + MemoryHandle facade (Laser::memory, default Auto/Log:
                      publish to a memory topic -> materialized KV read view). One durable
                      backend LogMemory + in-process VectorMemory for similarity recall.
                      Handles built from Laser preserve content-addressed ids and run log
                      and vector writes through the enrolled governor over the item body
  govern.rs           ActionGovernor pre-effect policy hook (feature = "agent"): decide before
                      agent sends / AGDX verbs / typed publishes / memory writes,
                      allow|observe|block|step_up|
                      modify|defer under GovernorMode (observe/enforce), digest-chained
                      PolicyEvidence events on the audit topic, Laser::with_governor +
                      LaserBuilder::governor + Agent::builder().governor. QuorumGovernor runs
                      named voters under All/Any/AtLeast(n), SwappableGovernor hot-swaps the
                      active policy behind a lock
  intent.rs           Intent/Vote/Decision: SDK-level typed records for effects that need
                      asynchronous, replayable approval, not an AGDX wire extension (feature =
                      "agent")
  swarm.rs            SwarmActivity: a replay-safe supervisor read model over governance
                      evidence, deduplicated by decision id (feature = "agent")
  crash_context.rs    CrashContext::assemble: one-call bundle over an already-read journal tail,
                      optional dead-letter capsule, and latest policy evidence, control characters
                      escaped in every untrusted field (feature = "agent")
  state_store.rs      StateStore point-store seam: InMemoryStore + FileStore, managed Kv too
  snapshot.rs         SnapshotStore fold-checkpoint seam: KvSnapshotStore (managed, one key per
                      conversation) + TopicSnapshotStore (log-native), so ConversationState::load
                      resumes from the last checkpoint instead of a full replay
  testing.rs          handler unit-test seam (feature = "agent"): agent_message + agent_ctx
  capabilities.rs     Capabilities + Laser::capabilities(), re-exports wire OpVersions
  sign.rs             ed25519 envelope signing/verification (feature = "sign"): Agent pickup/
                      terminal signing, LaserBuilder::verifier, signed quarantine facts, detached-
                      JWS A2A card signing
  a2a.rs              A2A JSON-RPC <-> agent-topics bridge + task lifecycle (feature = "a2a-bridge")
  mcp.rs              McpBridge: MCP JSON-RPC over AGDX (initialize / tools / resources / prompts,
                      feature = "mcp-bridge", the axum router behind "mcp-http")
  agui.rs             AgUiEvent + state sync (feature = "agui"): publish_state_snapshot /
                      publish_state_delta / reconstruct_state, agui_events renders a conversation
                      into AG-UI events
  edge_auth.rs        EdgeClaims/EdgeDenial: the bearer-token claims an incoming edge request
                      asserts and why one is refused (wrong audience vs a scope step-up)
  managed.rs          Laser::execute_batch: up to MAX_BATCH_OPS independent managed commands in
                      one round trip over the AGDX_BATCH command, never a transaction (feature =
                      any of fork/graph/kv/projections/query/rbac/runs)
  query/              the managed materialized-view query surface: wire query/browse/control
                      type re-exports plus Laser::query and the bounded row walks
                      (feature = "query")
  projections.rs      Laser::projections()/bindings()/schemas(): projection, binding, and
                      writer-schema control plus the registry browse (feature = "projections")
  graph.rs            Laser::graph() -> GraphHandle: traversal, neighbors, upsert,
                      link/relink/unlink (feature = "graph")
  watch.rs            Laser::watch() -> Watch: the ChangeRecord feed a projection binding opts
                      into, await-then-query instead of poll-and-retry (feature = "watch")
  kv/                 mod.rs re-exports the wire KV surface, client.rs (Kv handle,
                      get/set/delete/scan builders) behind feature = "kv"
  fork/               mod.rs re-exports the wire fork surface, client.rs (ForkHandle,
                      create/promote/squash/put_row) behind feature = "fork"
  rbac/               mod.rs: Laser::whoami + list_roles/get_role/get_bindings/define_role/
                      delete_role/bind_roles/bind_roles_expect_revision/authz_history, wire authz
                      re-exports (feature = "rbac")
  agent/
    scope.rs          Laser::agent(id) -> AgentScope: identity-scoped send/ask/contract/
                      publish_card/advertise (the client face, the handler runtime stays
                      Agent::builder)
    agdx.rs            typed AGDX producer verbs: Laser::agdx -> Agdx (command/respond/emit/
                      status/fail/request_input), AgdxStream chunk writer, routing-header stamping
    assembler.rs      ChunkAssembler: pure per-channel reassembly state machine
    clock.rs          Clock seam: SystemClock (real time) + TestClock (deterministic, advance/set),
                      the SLA-timer and deadline-check seam a test drives without sleeping
    laser.rs          Laser facade: bootstrap, send_agent, request/reply, producer cache,
                      spawn_subconversation, capabilities
    consumer.rs       reliable consumer: Deduplicator seam, commit-after-handle, retry -> DLQ,
                      fence high-water gate, opt-in ack_on_pickup (Working status), AgentMessage::body(),
                      ConcurrencyPolicy (Serial | SerialPerPartition lanes), graceful drain,
                      AgentMiddleware + DeadLetterSink seams
    memory_handler.rs MemoryHandler<H>: wraps an AgentHandler so a successfully handled message is
                      remembered under its conversation, auto_remember selects the MemoryKind
    builder.rs        Agent::builder + AgentHandle (ready/shutdown=graceful drain/join/abort), capabilities +
                      ack_on_pickup + inbox_route + signing_key + retry/verifier/dedup_window/shutdown_grace/concurrency/
                      middleware/on_dead_letter, self-advertises card + presence on spawn
    ctx.rs            AgentCtx handed to a handler: respond/reply_on/send/request/respond_input,
                      fan_out (per-agent Gather under GatherPolicy), approval_gate, spawn_subconversation
    replies.rs        ReplyHub: one shared reply dispatcher per (stream, reply topic), correlation
                      -> waiter map, background task. Laser::request/fan_out ride it (agdx.corr)
    registry.rs       AgentRegistry: fused log card registry + live presence + quarantine fold,
                      resolve-by-capability (health-aware). Laser::publish_card / quarantine /
                      advertise_presence / client_metadata
    router.rs         Router (To / ToPrincipal / Broadcast / ToCapable / AllCapable),
                      principal-bound CapabilitySelector, RoutePolicy, InboxRoute
    contract.rs       Laser::contract -> Contract (Completed/Failed/NotConsumed/TimedOut), the
                      directed-task state machine. Laser::scatter + scatter_report (per-agent ScatterReport)
    workflow.rs       Laser::workflow -> the engine: topo-ordered steps, budgets, verifier panels,
                      saga compensation, journal/replay/resume, all_capable scatter, fenced steps
    session.rs        SessionPolicy (PerCall / PerUser)
    state.rs          ConversationState::load (fold the log)
sdk/tests/integration/  one shared Apache Iggy, one stream per test, BDD-named cases
  iggy_container.rs   TestIggy testcontainer harness (test-only, not shipped)
  query/              test-only query worker + backends (Memory, durable SQL)
foreign/python/         the Python SDK (outside the workspace): PyO3 bindings over the
                        laser-sdk crate (cdylib lib laser_sdk_py, import name laser_sdk),
                        src/ one module per area + bin/stub_gen.rs, tests/ pytest
                        (offline + a testcontainer), maturin packaging. sign.rs binds
                        SigningKey + KeyRegistry so both SDKs share signing,
                        verification, principal routing, and reply identity. transport.rs binds
                        the Laser Producer/Consumer/ConsumerMessage surface with direct batching,
                        partitioning, group polling, auto/manual commits, and offset control.
                        Its `vsr` feature forwards to both iggy/vsr and laser-sdk/vsr. See the
                        python-bindings skill.
foreign/typescript/     the native Node SDK: strict ESM, native wire codecs, Apache Iggy
                        transport, streaming, managed clients, agents, memory, governance,
                        signing, bridges, package exports, and release gates. See the
                        typescript-sdk skill.
bdd/                    cross-SDK conformance (outside the workspace): scenarios/
                        shared Gherkin (runs vs Apache Iggy, no Cloud), rust/ the cucumber-rs
                        runner (tests/) + src/query_engine.rs (the pure reference query
                        engine the query scenarios run against), python/ the pytest-bdd
                        runner over the Iggy-native scenarios, docker-compose for
                        the multi-language path
scripts/run-bdd-tests.sh  driver for the per-language BDD runners
examples/rust/          [[example]] bins under src/<scenario>/main.rs, LlmClient seam in lib.rs
examples/python/        one runnable script per scenario + a shared _common.py connect helper
examples/typescript/    nine non-benchmark mirrors, one entry point + README per scenario
docs/                   tutorial.md (progressive guide), building-agents.md (scenario
                        -> SDK recipe guide), agdx.md (the AGDX spec),
                        interop.md (A2A / MCP / AG-UI bridges)
```

## Repo-wide principles

- **The wire crate is the one source of truth.** Wire types, codes, header keys, topic names, and caps live in `wire/` and only there. The SDK re-exports them under its historical paths (`laser_sdk::query::Query` keeps working) and as `laser_sdk::wire`. Never re-declare a wire shape in the SDK, and never encode a band envelope with a serde codec crate directly: route every frame through `laser_wire::framing::encode_named`/`decode_named` (named-field CBOR), the one band-encoding entry point, so no consumer drifts to a different encoding.
- **laser-wire stays runtime-free.** No IO, no async, no clock, no randomness, no iggy/tokio/bytes/ulid dependency (CI-banned). Byte fields are `Vec<u8>` with bin-encoding serde, never `bytes::Bytes`. Id GENERATION (ULID entropy + clock) lives SDK-side (`MintUlid`).
- **Thin layer, never hide Iggy.** Reuse Iggy types directly (`IggyMessage`, `HeaderKey`/`HeaderValue`, `Partitioning`, `IggyProducer`, `IggyConsumer`, `Identifier`, `IggyTimestamp`). Do not reimplement the poll/commit loop. Wrap a handler and delegate to `IggyConsumerMessageExt::consume_messages`.
- **Delivery is at-least-once + idempotent, never exactly-once.** Agent records use per-conversation partitioning. Generic streaming is ordered within the caller-selected partition. There is no cross-partition order.
- **Fence the effect in the lease namespace.** `.exclusive()` uses the SDK coordination namespace for the consumer stale-holder gate. An external effect uses `.exclusive_in(namespace)` and the handler commits with `kv(namespace).cas_fenced(..)` against the run-id fence key. Pickup `Working` statuses use the agent signing key when one is configured.
- **Idiomatic Rust traits over free helpers.** Parsing = `FromStr`/`.parse()`, formatting = `Display`, conversion = `From`/`TryFrom`. `FromStr::Err` is a structured enum (see `IdError`, `ProvenanceError`), never `String`.
- **No em dashes** anywhere (code, comments, commits, docs). Use commas/colons.
- **Never hard-wrap prose.** Keep each Markdown paragraph on one physical line. Break only at real paragraph, list, table, quote, heading, or code boundaries.
- **TypeScript stays strict and semicolon-free.** No public `any`, default exports, or deep package exports. `src/iggy/apache-iggy.ts` is the only Apache Iggy and Node `Buffer` adaptation boundary. Reject generated-looking filler in code, comments, TSDoc, logs, errors, examples, tests, workflows, and release notes.
- **Docs are part of every change, never a follow-up.** When you touch code or the wire contract, update all affected docs in the same change: `README.md`, `sdk/README.md`, `wire/README.md`, `AGENTS.md`, `CLAUDE.md`, the relevant `.claude/skills/*`, the relevant `docs/*`, and `the AGDX spec`. Do not report a change "done" until a repo-wide grep for every renamed symbol / constant / string returns only the new form, across code **and** docs. Stale docs are a defect.

## Conventions

- **Terse code, minimal comments.** No module docs, no prose narration. Comments only for non-obvious decisions (e.g. why a lock is released before `.await`). One sorted import block per file.
- **Test names are BDD:** `given_<state>_when_<action>_then_should_<outcome>`. Use `.expect("a meaningful message")`, never bare `.unwrap()`.
- **Builders are `bon`** (`#[derive(bon::Builder)]`), matching `IggyMessage::builder()`.
- **No magic strings for enum-like values.** Protocol method names, states, kinds, header keys, etc. are enums (or named consts), not bare string literals. Give the enum `Display` + `FromStr` (use `strum` derives where they earn it) for the wire spelling, or serde `rename` for serialized forms, then dispatch on the enum. See `A2aMethod` in `a2a.rs` and `TaskState` in `wire/src/agent.rs`. Constant values (error codes, versions) get a named `const`. Vocabularies we own ride pinned u8 dictionaries with unknown-code passthrough (`TaskState`, `AgentErrorCode`, `DeadLetterReason`, the `agdx.ct` codes). Vocabularies owned by others stay strings (`finish_reason`).
- **Cite code by symbol, not line number** in reviews and docs (lines drift).

## Testing

- Unit tests live next to the code (`#[cfg(test)] mod tests`).
- The wire crate's `wire_fixtures` suite is the byte-identity gate: every frame re-encodes byte-for-byte against `wire/fixtures/`. Regenerate ONLY on an intentional wire change (`just fixtures-regen`), then propagate per the release process. `constants.rs` pins every code/key/topic/cap as a literal. The agent fixtures are draft-grade by design until a real multi-agent application has bent the envelope. The negative fixtures pin what every port's validator must reject.
- Integration tests (`sdk/tests/integration/`) run against a real classic-protocol Apache Iggy instance via `TestIggy` (testcontainers). One container is shared. Each test gets its own data stream **and** its own ops stream (`harness::laser()` sets both via `with_ops_stream`), so parallel query workers on the shared instance never serve each other's requests. The suite stays isolated and parallel. It is excluded under `vsr`, whose cluster server is not representable by this single-container harness.
- `streaming.rs` proves the ordinary Laser producer/consumer path without raw Apache Iggy calls: batching, linger, exact headers, key/partition routing, async `Stream` delivery, automatic and explicit server offset commits, offset resume after group rejoin, uncommitted redelivery after shutdown, and standalone replay.
- `harness::eventually` polls instead of fixed sleeps. Iggy visibility is eventual.
- Pin the Iggy image for reproducibility with `LASER_TEST_IGGY_TAG` (default `edge`).
- The managed surface (query, KV, forks) needs LaserData Cloud and is not run end-to-end here. Query is the off-log `AGDX_QUERY` managed command, KV and forks are raw managed commands, all returning `Unsupported` on raw Apache Iggy. There is no in-process query worker and no topic query path. The split: the client wire bytes are pinned by the fixture corpus, the full managed execution is verified in LaserData Cloud's own repository (it consumes this published SDK), and the **query DSL semantics** are covered here by a pure in-memory reference engine (`laser_bdd::query_engine` (in the `bdd/` harness), no Iggy, no transport) via its own unit tests and the `query.feature` BDD scenarios, so every SDK has a reference for what a query must return.
- TypeScript verification runs from `foreign/typescript`: `npm run verify`, then `npm run test:integration` against Apache Iggy. Run `scripts/run-bdd-tests.sh typescript` and the TypeScript example tests before release. The package gate installs the exact tarball into clean ESM and CommonJS-interoperating consumers and compiles its declarations. Node 22.14 and Node 24 are supported. Bun, Deno, and browsers are not supported.

## What is shipped vs planned

Audited against the current tree at `0.0.1-rc.18` (every symbol below grep-verified to exist with the described shape). This is the one canonical shipped/planned inventory for the workspace, and skill files point here rather than keeping their own copy. Do not document planned API as shipped.

These features exist to exercise the **seams** the paid tiers plug into: their premium forms are managed/durable backends (durable dedup, the knowledge graph, an A2A gateway) activated by capability negotiation, not code changes. Agentic memory has no managed surface of its own, it composes the query and graph surfaces.

**Core (Phase 1, vanilla Iggy):** provenance + causality, reliable consumer (graceful drain, `ConcurrencyPolicy::SerialPerPartition` lanes, `AgentMiddleware` + `DeadLetterSink` seams, `Agent::builder` retry/verifier/dedup_window, the `laser_sdk::testing` handler-test seam), context seam, memory seam, builder/router/session/state, the `respond_on` + `AgentCtx` handler seam. Isolation is an Iggy stream boundary (one connection drives any number of streams), not a per-message header. The open streaming layer gives ordinary services a Laser-native direct producer and live async partition/consumer-group reader with exact headers, routing, polling/replay, group lifecycle, retries, configurable automatic commits, explicit commit-after-success, and server offsets. The exact Apache Iggy builders remain an escape hatch. The `vsr` feature changes only the backing transport. Python exposes the same operational surface. Layered on top, early and behind feature flags: opt-in warm dedup on restart, a semantic `VectorMemory` (in-process, pluggable `Embedder`), the memory topic materialized into a versioned KV read view (`kv` feature). The `concierge` example runs on real Claude (`llm-anthropic`) or OpenAI (`llm-openai`).

**Action governance** (`sdk/src/govern.rs`, `agent` feature): the `ActionGovernor` pre-effect hook decides before an agent send, a typed or raw topic publish, an AGDX verb, or a memory write, returning a `Verdict` (`Allow`/`Observe`/`Block`/`StepUp`/`Modify`/`Defer`). Enrolled with `Laser::with_governor` under a `GovernorMode` (`Observe` shadow-runs and only warns on an evidence-write failure, `Enforce` applies the verdict and fails closed if the evidence cannot be recorded). Every decision is a digest-chained `PolicyEvidence` event on the audit topic (BLAKE3 receipt over the canonical encoding, `previous_digest` linking to the prior decision in the same conversation, so a dropped or reordered local record is detectable). `QuorumGovernor` runs named voters concurrently under `All`/`Any`/`AtLeast(n)`. Every `mandatory` voter must return an affirmative `Allow`/`Observe`/`Modify`. Its denial or error cannot be bypassed by another voter. Empty voter sets, zero or oversized thresholds, duplicate voter names, and conflicting `Modify` bodies block. A non-mandatory error abstains. Pure in-process composition, no durable log or protocol of its own. `SwappableGovernor` holds a hot-swappable active policy behind a lock: `swap` returns the replaced policy, `current` reads the active one, and `decide` uses whichever policy was active at that moment without reinterpreting older evidence.

**Durable intent** (`sdk/src/intent.rs`, `agent` feature): SDK-level typed records for effects that need asynchronous, replayable approval, not an AGDX wire extension. `Intent::builder().build() -> Result<Intent, IntentError>` validates a non-empty unique voter set, mandatory subset, nonzero reachable threshold, future deadline, and BLAKE3 body digest. `Vote::cast -> Result<Vote, IntentError>` accepts only eligible voters and binds the ballot to the exact intent id, digest, and policy version. `decide -> Result<Option<Decision>, IntentError>` validates deserialized intent again, ignores mismatched and out-of-window ballots, canonicalizes ballot order, requires every mandatory voter to allow, commits a reached quorum, and aborts an impossible quorum, deadline miss, or conflicting repeat. `Decision` carries the digest and policy version, and `Decision::authorizes` must verify that binding before an effect runs. Proposer and voter names remain claims unless the deployment uses the signed-principal or topology-isolated security profile. Callers publish and read these through ordinary typed topics. The module performs no I/O itself.

**Swarm activity** (`sdk/src/swarm.rs`, `agent` feature): a replay-safe supervisor read model over governance evidence. `SwarmActivity::observe` drops unattributed evidence and deduplicates by `decision_id`. `agent(name)` exposes counts plus the latest decision by `(at_micros, decision_id)`, and `agents()` lists agents busiest first. It reads no topic itself.

**Crash context** (`sdk/src/crash_context.rs`, `agent` feature): `CrashContext::assemble` combines an already-read journal, optional dead-letter capsule, and optional latest policy evidence. `.summarize()` renders a bounded fixed-order digest and escapes control characters in every untrusted text field, so a payload cannot forge diagnostic lines. Pure combination, no I/O or model call.

**Capability RBAC** (`rbac` feature, `sdk/src/rbac/`, gated on the `authz` capability): `Laser::whoami` + `list_roles`/`get_role`/`get_bindings`/`define_role`/`delete_role`/`bind_roles`/`bind_roles_expect_revision`/`authz_history`. Grants are `effect feature:action [on resource-pattern]` through roles bound to the server-stamped, unspoofable user (deny-wins, default-deny). Role names pass `validate_role_name` (`wire/src/authz.rs`, enforced on define and bind, never on journal replay). Orthogonal to Iggy's own `Permissions` (never touched), fork-native: the streaming server enforces feature+action+keyed-resource at the edge from a boot-built capability registry, only the query/graph DSL per-source depth is left to the plane.

**AGDX wire surface** (laser-wire): the envelope, ids, dictionaries, the validity matrix with its closed operation vocabularies, the pinned card body, the claim-check `BodyRef`, fixtures, the `agdx.av` version header, the additive `OpVersions.agent` advertisement, plus the SDK-side id bridge. SDK runtime on top: typed producer verbs (`Laser::agdx` -> `Agdx`: command/respond/emit/status/fail, the `AgdxStream` chunk writer), the human-in-the-loop interrupt/resume pair (`Agdx::request_input` + `AgentCtx::respond_input`, no new wire), and the pure `ChunkAssembler` reassembly state machine (duplicate/gap/late/double-terminal/abandonment rules).

**Dead letters and the envelope-aware read path:** the reliable consumer publishes an `AgentDeadLetter` capsule on every dead-letter path (decode failure, deadline, permanent rejection, retry exhaustion): typed `DeadLetterReason`, attempt count, the poison message's full `LogPosition`, the original payload verbatim (provenance headers kept, deadline cleared, `agdx.ct = cbor`). `Laser::redrive_dead_letter` republishes it for a fixed handler, re-keying the idempotency header by source position so a double redrive of one capsule stays idempotent. `AgentMessage.envelope`/`ContextMessage.envelope` carry the decoded `AgentEnvelope`, and routing/dedup/deadline synthesize from it for AGDX messages (dedup is envelope-keyed, not header-keyed, for these).

**Edge bridges**, all riding the AGDX verbs over the log, no SSE: the **A2A bridge** (`A2aBridge`: `SendMessage`/`SendStreamingMessage` -> typed `command` tunneling the params JSON, `GetTask`, `CancelTask` -> `Cancelled`, the v1.0 Agent Card at the well-known path with `supportedInterfaces`, optional detached-JWS card signing under `sign`), the **MCP bridge** (`McpBridge`: `initialize`/`tools/list`/`tools/call` -> AGDX `command`, `resources/*`/`prompts/*` served from config, 2025-11-25 schema), and `Laser::reassemble_channel` (log-native chunk-stream replay). **AG-UI** (`agui` feature): state sync (`publish_state_snapshot`/`publish_state_delta`/`reconstruct_state`, RFC 6902 replay) and event rendering (`agui_events`: chat -> `TEXT_MESSAGE_*`, reasoning -> `REASONING_MESSAGE_*`, `tool_args` -> `TOOL_CALL_*`, status -> `RUN_STARTED`/`RUN_FINISHED`, state -> `STATE_*`, error -> `RUN_ERROR`).

**Multi-agent orchestration**, all conventions over the log (client-side state machines over offsets/deadlines/leases/replies, no orchestration server):

- Discovery: a capability card (`Laser::publish_card`, auto-published by the builder), a live inbox (`AgentPresence` over `set_client_metadata`), fused by `AgentRegistry` into a resolve-by-capability view (health-aware, excludes quarantined agents). `Laser::quarantine`/`unquarantine` are reversible registry facts, and `quarantine_signed`/`unquarantine_signed` (`sign` feature) fold only with a verified signature. `Laser::agent_registry` caches the fold per data-stream and resumes instead of re-reading from offset 0.
- Routing: `Router::{To,Broadcast,ToCapable,AllCapable}` + `InboxRoute::{Advertised,Fixed}`, resolving to an advertised inbox, never a hard-coded shared topic.
- Coordination: `Laser::contract` -> `Contract::{Completed,Failed,NotConsumed,TimedOut}` (ack-on-pickup `Working`), `AgentCtx::fan_out` + `Laser::scatter` (gather under `GatherPolicy::{RequireAll,Quorum,BestEffort}`), `AgentCtx::approval_gate`. With a verifier enrolled, the contract reply path verifies a reply's signature before accepting a terminal.
- Workflow engine (`Laser::workflow`): topo-ordered steps, `Budget` (tokens/wall-clock/invocations), `verify_with` panels, saga `compensate_with`, crash-recovery journal/replay/resume, `all_capable` scatter steps, and a per-step `.exclusive()` claiming a fenced lease (`acquire_fence`, kv-gated, fails closed as `Unsupported` without the `KV_CAS_FENCED` capability). `StepHandle::on_timeout(OnTimeout::{Fail,Reassign})`: `Reassign` bumps the fence sequence to gate out the timed-out holder. The durable cross-holder at-most-once external effect is the handler's own fenced `Kv::cas_fenced` commit, and the engine only provides the token and the same-holder replay gate.
- Bound in Python (`Laser.contract`/`scatter`/`quarantine`, `spawn_agent` capabilities/ack_on_pickup/health, `AgentCtx.fan_out`/`approval_gate`, and the `agent_message`/`agent_ctx` handler-test seam), matched by the `orchestra` example (Rust + Python).

**Still planned, not present:**

- Durable infrastructure-side dedup, and a durable `VectorMemory` backed by an external relational store.
- A richer A2A surface beyond the above (streaming, further agent-card fields).
- The byte/latency benchmark suite (needs a real Iggy environment to measure).
- Niche AG-UI event types with no AGDX source (`MESSAGES_SNAPSHOT`, `ACTIVITY_*`, `RAW`/`CUSTOM`/`META`, `REASONING_ENCRYPTED_VALUE`).

See the AGDX spec for the wire contract.
