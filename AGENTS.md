# LaserData - Laser SDK Agent Guidelines

This workspace publishes `laser-wire` and `laser-sdk`. `laser-wire` defines shared data types, encoded messages, command codes, limits, and reference test data. It supports native targets and WebAssembly without an asynchronous runtime. `laser-sdk` provides streaming, queries, state, forks, and agent operations over [Apache Iggy](https://iggy.apache.org). It records `gen_ai.*` metadata but does not call language models.

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

These changes require explicit user authorization unless the current session already grants it:

- Changes to encoded data in `wire/src/` require coordinated updates. This includes codes, versions, header keys in `wire/src/headers.rs`, topics, fields, serde attributes, dictionaries, and limits. During the current pre-1.0 stage, authorized breaking changes do not require backward compatibility. Update `wire/fixtures/`, all clients, consumers, examples, and specifications together.
- Changing `ConversationId::derive` in `sdk/src/types/ids.rs` changes derived `SessionPolicy::PerUser` identities. Keep its version policy explicit through `DERIVE_VERSION`. `AgentId::wire_id` uses the agent name directly.
- Renaming `AgentTopic` names (`sdk/src/provenance/topic.rs`) - repoints live topics.
- Changing `Provenance::partition_key` (currently `conversation_id`) - breaks the per-conversation ordering guarantee.
- Changing the public signatures of `Laser`, `AgentHandler`, `Agent`, `AgentConsumer::run`, or `AgentHandle` - these are the customer-facing API.
- Preserve attribution fields such as `agent` and `root_conversation_id` when rebuilding `Provenance` through `spawn_subconversation` or `AgentCtx::reply_provenance`. Losing them breaks causal and cost attribution.
- Queries use `AGDX_QUERY` through `send_raw_with_response` on LaserData Cloud or Laser Stack. There is no request-topic query path. Original Apache Iggy without a managed backend returns `LaserError::Unsupported`. The connection-time `AGDX_HELLO` probe sets `managed_host` and available capabilities, including `query.available`.

## Verification order

`.github/workflows/ci-rust.yml` defines `lint`, `lint-detached`, `build`, `wire-feature-matrix`, `feature-matrix`, `wasm`, `deny`, `test`, `fuzz`, and `bdd`. Python publication also tests installed wheels and source packages through `ci-python.yml`. TypeScript publication tests its npm archive through `ci-typescript.yml`. Managed execution belongs to Laser Stack `scripts/smoke`, which accepts a local SDK archive.

`bdd/rust` and `fuzz` are outside the main workspace. `--workspace` does not include them. `lint-detached` and `just lint` run formatting, dependency, and clippy checks for those projects.

A `rust-v*` tag publishes both crates after the full gate. A `wire-v*` tag publishes only `laser-wire` after tests that need no server. This lets a changed contract publish before the matching server.

`[wire-break]` in a pull request or push description skips only tests that need the published server artifact. Server-free tests still run. Update the pin in `scripts/resolve-test-iggy-server.sh` afterward and run the full pipeline. Rust, Python, and TypeScript package release tags always run their full pipelines. The wire-only tag remains server-free.

Run locally in this exact order, do not skip:

GitHub Actions use readable upstream version tags. Never replace Action tags with commit hashes or add a workflow that enforces hashed Action references.

```bash
cargo fmt --all            # 1. formats, auto-applies
cargo sort --workspace     # 2. sorts Cargo.toml deps + feature arrays
cargo machete              # 3. no unused dependencies
cargo clippy --workspace --all-targets --all-features -- -D warnings  # 4.
cargo test --workspace --all-features                   # 5. unit + wire fixtures/robustness
just test-it                                              # 6. Iggy integration suite
cargo test --workspace --all-features --doc             # 7. doctests (Docker-free)
just wasm                  # 8. laser-wire on wasm32-unknown-unknown (needs the target)
just deny-wire             # 9. laser-wire dependency bans (needs cargo-deny)
just advisories            # 10. workspace vuln/unmaintained advisories (needs cargo-deny)
just fuzz                  # 11. bounded fuzzing of the wire decode surface (nightly + cargo-fuzz)
just bdd                   # 12. cross-SDK BDD conformance, Rust runner
```

Run `--all-features --doc` as a required gate. `clippy --all-targets` does not compile documentation examples. A default-feature `cargo test --workspace` can omit examples behind optional features such as `kv` and `query`. The doctest gate requires no Iggy server or Docker. CI runs it too.

VSR is native to Iggy. Laser SDK does not expose a transport feature or alternate framing option. Rust, Python, and TypeScript use the same Iggy transport. Integration and BDD suites run the versioned Iggy binary. Managed reads use the non-replicated extension path, and the three managed-authorization writes use dedicated replicated operations.

The wire crate must compile for `wasm32-unknown-unknown` with `cbor,codecs,fixtures,builders,http-client`. Exclude `bson` from this target. Its portable dependency graph must exclude iggy, tokio, bytes, ulid, dashmap, tracing, and getrandom, as specified in `deny-wire.toml`. Include `builders` and `http-client` in both portability gates. CI enforces these requirements even when local tools are unavailable.

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
    hello.rs            HelloReply / OpVersions (AGDX_HELLO probe body, additive `agent` field + `features` capability bitset: feature::{KV_CAS,READ_YOUR_WRITES,STRONG_CONSISTENCY,KV_CAS_FENCED,AGENT_WORKFLOW,KEYWORD_SEARCH,WATCH,AUTHZ,DESTINATIONS,KV_FENCED_LEASES}) + BackendAnnounce (backend->streaming-server capability announce, AGDX_BACKEND_HELLO_CODE)
    query.rs            query IR (incl. Consistency level) + QueryEnvelope/QueryReply/Row/QueryError (incl. Stale)
    result.rs           unified ResultCode space + HTTP status, From projections off every surface error
    browse.rs           registry browse requests + BrowseReply (incl. DecodeRecord)
    control.rs          Projection / ProjectionBinding / SchemaDef / ControlEnvelope + builders
    kv.rs               KV requests (incl. KvCas/CasExpect and the fenced-lease family KvLease/KvLeaseRenew/KvRelease/KvCasFenced at KV_LEASE_OP_VERSION 1: holder identity, delegated subject, coordination namespace, barriered KvGet.min_position) + KvReply (incl. Committed/Leased/Renewed) / KvError (incl. VersionConflict/LeaseLost/Stale) + entry version
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
                      `tls_ca_file=` query parameter already set. The bundled CA is cached in a
                      per-user, owner-only directory and reused only when its bytes still match
                      the bundled cert. `LASER_TLS_CERT` enables TLS with an explicit CA for any
                      host or overrides the bundled cert. `LASER_NO_TLS=1` disables automatic TLS
                      setup (read by value, so `0`/`false` do not). Any other host passes through
                      untouched when neither variable is set
  types/              mod.rs re-exports the id types, ids.rs: ConversationId / AgentId /
                      MessageId (FromStr+Display) + the AGDX id bridge (MintUlid, ConversationId
                      <-> wire id, AgentId::wire_id = the name verbatim)
  provenance/         keys.rs re-exports the wire header dictionary, runtime.rs
                      (Provenance <-> headers) + topic.rs (AgentTopic) behind `provenance`
  poll.rs             shared partition drain helper (context + reply + cursor paths)
  stream.rs           the Log accessors: Laser::stream(name) -> Stream (real Iggy stream,
                      ensure/topic) and Laser::topic(name) -> Topic (default-stream shortcut,
                      publish()/publish_batch() fluent, send/batch raw (impl Into<Vec<u8>>,
                      matching PublishRequest/BatchPublishRequest), all returning Iggy's
                      SendMessagesResponse commit positions, replay() -> Cursor,
                      ensure(partitions), producer/consumer/consumer_group, plus the raw
                      iggy_producer/iggy_consumer/iggy_consumer_group escape hatch)
                      Iggy provides the VSR transport. Standard Iggy commands and managed reads use the
                      non-replicated extension path, and authorization writes use dedicated
                      replicated operations
  stream/             publish.rs (publish builders) and record.rs (Record lowering, Vec<u8>
                      payload, one-shot/typed publish, not the hot loop), transport.rs is the
                      Laser-native direct Producer plus live futures::Stream Consumer/
                      ConsumerMessage path - ProducerMessage/ConsumerMessage keep `bytes::Bytes`
                      (zero-copy clone) on this hot path specifically, including routing,
                      polling/replay, group lifecycle, retries, exact headers, automatic commits,
                      explicit commit-after-success, producer commit confirmations, consumer
                      offsets, and next_within(timeout) for
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
                      get/set/delete/scan builders, holder-scoped lease/renew_lease/release,
                      barriered get_entry_at_least), coordination.rs (FencedLeaseClient: typed
                      fenced-lease client over an injectable ManagedKvTransport, PreparedMutation
                      minting one stable operation id per logical mutation and reusing the exact
                      envelope across ambiguous retries, DedicatedKvTransport actively closing a
                      retired connection before reconnect, DynManagedKvTransport/SharedKvTransport
                      = Arc<dyn ..> for a runtime-selected transport), behind feature = "kv"
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
    session.rs        SessionPolicy (PerCall / PerUser), Laser::sessions / sessions_with(SessionConfig)
                      -> Sessions (create / start / open) -> Session: typed turns (SessionTurnKind, one
                      conversation-level AgentTopic each), context, scoped memory, Checkpoint,
                      turns_at / turns_since, state_at / replay. A facade over ContextScope,
                      never a second store
    state.rs          ConversationState::load (fold the log)
sdk/tests/integration/  one shared Apache Iggy, one stream per test, BDD-named cases
  support/test_iggy.rs Native Iggy process harness (test-only, not shipped)
  query/              test-only query worker + backends (Memory, durable SQL)
foreign/python/         the Python SDK (outside the workspace): PyO3 bindings over the
                        laser-sdk crate (cdylib lib laser_sdk_py, import name laser_sdk),
                        src/ one module per area + bin/stub_gen.rs, tests/ pytest
                        (offline + native Iggy), maturin packaging. sign.rs binds
                        SigningKey + KeyRegistry so both SDKs share signing,
                        verification, principal routing, and reply identity. transport.rs binds
                        the Laser Producer/Consumer/ConsumerMessage surface with direct batching,
                        partitioning, group polling, auto/manual commits, and offset control.
                        Every build uses Iggy's native VSR transport. No transport feature is exposed. See the
                        python-bindings skill.
foreign/typescript/     the native Node SDK: strict ESM, native wire codecs, Apache Iggy
                        transport, streaming, managed clients, agents, memory, governance,
                        signing, bridges, package exports, and release gates. See the
                        typescript-sdk skill.
bdd/                    cross-SDK conformance (outside the workspace): scenarios/
                        shared Gherkin (runs vs Apache Iggy, no Cloud), rust/ the cucumber-rs
                        runner (tests/) + src/query_engine.rs (the pure reference query
                        engine the query scenarios run against), python/ the pytest-bdd
                        runner over Iggy-native scenarios, docker-compose for
                        the multi-language path
scripts/run-bdd-tests.sh  driver for the per-language BDD runners
examples/rust/          [[example]] bins under src/<scenario>/main.rs, LlmClient seam in lib.rs
examples/python/        one runnable script per scenario + a shared _common.py connect helper
examples/typescript/    nine non-benchmark mirrors, one entry point + README per scenario
                        all three also carry the eight per-primitive examples (log,
                        query, watch, kv, graph, recall, context, agent), held
                        step-for-step identical across the languages
docs/                   tutorial.md (progressive guide), building-agents.md (scenario
                        -> SDK recipe guide), agdx.md (the AGDX spec),
                        interop.md (A2A / MCP / AG-UI bridges)
```

## Repo-wide principles

- Define wire types, codes, headers, topics, and limits only in `wire/`. Re-export them through `laser_sdk::wire` and existing paths such as `laser_sdk::query::Query`. Use `laser_wire::framing::encode_named` and `decode_named` for AGDX envelopes. Do not define duplicate formats or bypass the shared encoder.
- Keep `laser-wire` independent of I/O, clocks, randomness, and asynchronous runtimes. Its optional HTTP client uses a caller-provided transport. Byte fields use `Vec<u8>` with shared encoding helpers, not `bytes::Bytes`. Generate IDs in SDK code through `MintUlid`.
- Reuse `IggyMessage`, `HeaderKey`, `HeaderValue`, `Partitioning`, `IggyProducer`, `IggyConsumer`, `Identifier`, and `IggyTimestamp`. Keep the standard producer and consumer loops in Iggy. Use `IggyConsumerMessageExt::consume_messages` for handler-based consumption.
- Delivery is at-least-once + idempotent, never exactly-once. Agent records use per-conversation partitioning. Generic streaming is ordered within the caller-selected partition. There is no cross-partition order.
- Fence the effect in the lease namespace. `.exclusive()` uses the SDK coordination namespace for the consumer stale-holder gate. An external effect uses `.exclusive_in(namespace)` and the handler commits with `kv(namespace).cas_fenced(..)` against the run-id fence key. Pickup `Working` statuses use the agent signing key when one is configured.
- Idiomatic Rust traits over free helpers. Parsing = `FromStr`/`.parse()`, formatting = `Display`, conversion = `From`/`TryFrom`. `FromStr::Err` is a structured enum (see `IdError`, `ProvenanceError`), never `String`.
- No em dashes anywhere (code, comments, commits, docs). Use commas/colons.
- Never hard-wrap prose. Keep each Markdown paragraph on one physical line. Break only at real paragraph, list, table, quote, heading, or code boundaries.
- TypeScript stays strict and semicolon-free. No public `any`, default exports, or deep package exports. `src/iggy/apache-iggy.ts` is the only Apache Iggy and Node `Buffer` adaptation boundary. Reject generated-looking filler in code, comments, TSDoc, logs, errors, examples, tests, workflows, and release notes.
- Docs are part of every change, never a follow-up. When you touch code or the wire contract, update all affected docs in the same change: `README.md`, `sdk/README.md`, `wire/README.md`, `AGENTS.md`, `CLAUDE.md`, the relevant `.claude/skills/*`, the relevant `docs/*`, and `the AGDX spec`. Do not report a change "done" until a repo-wide grep for every renamed symbol / constant / string returns only the new form, across code and docs. Stale docs are a defect.

## Conventions

- Terse code, minimal comments. No module docs, no prose narration. Comments only for non-obvious decisions (for example, why a lock is released before `.await`). One sorted import block per file.
- Test names are BDD: `given_<state>_when_<action>_then_should_<outcome>`. Use `.expect("a meaningful message")`, never bare `.unwrap()`.
- Builders are `bon` (`#[derive(bon::Builder)]`), matching `IggyMessage::builder()`.
- Use enums or named constants for owned protocol methods, states, kinds, headers, codes, and versions. Use `Display`, `FromStr`, `strum`, or serde names for their textual forms. See `A2aMethod` in `a2a.rs` and `TaskState` in `wire/src/agent.rs`. Keep owned dictionaries in fixed u8 codes with unknown-value preservation. This includes `TaskState`, `AgentErrorCode`, `DeadLetterReason`, and `agdx.ct`. External vocabularies such as `finish_reason` remain strings.
- Cite code by symbol, not line number in reviews and docs (lines drift).

## Testing

- Unit tests live next to the code (`#[cfg(test)] mod tests`).
- `wire_fixtures` compares encoded bytes with `wire/fixtures/`. Regenerate through `just fixtures-regen` only for an intentional wire change. Update every affected client and implementation together. `constants.rs` fixes codes, keys, topics, and limits. Negative cases define input that every client must reject.
- Integration tests (`sdk/tests/integration/`) run against Apache Iggy via a native `TestIggy` process. One process is shared. Each test gets its own data stream and ops stream, so the suite stays isolated and parallel. Set `LASER_TEST_IGGY_SERVER` to test a local Iggy binary instead of the R2 artifact.
- `streaming.rs` tests publication and consumption through Laser APIs. It covers batches, headers, routing, delivery, commits, group rejoin, uncommitted redelivery, and replay.
- `harness::eventually` polls instead of fixed sleeps. Iggy visibility is eventual.
- The test runner pins `FORK_VERSION`. `LASER_TEST_IGGY_SERVER` overrides the downloaded R2 binary for local validation.
- Full query, KV, and fork execution requires LaserData Cloud or Laser Stack. SDK tests use reference files for encoded data and managed deployments for execution. `laser_bdd::query_engine` tests query behavior locally through `query.feature`. Original Apache Iggy returns `Unsupported` for these managed commands. Do not add a query worker or request-topic fallback to the SDK.
- TypeScript verification runs from `foreign/typescript`: `npm run verify`, then `npm run test:integration` against Apache Iggy. Run `scripts/run-bdd-tests.sh typescript` and the TypeScript example tests before release. The package gate installs the exact tarball into clean ESM and CommonJS-interoperating consumers and compiles its declarations. Node 22.14 and Node 24 are supported. Bun, Deno, and browsers are not supported.

## What is shipped vs planned

This inventory describes the `0.4.0` source tree. Skills link here instead of duplicating the inventory. Do not describe planned APIs as implemented.

Capabilities identify managed support such as durable duplicate suppression, graphs, and an A2A gateway. Memory combines query and graph operations and has no separate managed command group.

The open SDK supports provenance, causality, context, memory, routing, sessions, and state. Reliable consumption supports graceful drain, `ConcurrencyPolicy::SerialPerPartition`, `AgentMiddleware`, `DeadLetterSink`, and `Agent::builder` retry, verifier, and duplicate-suppression controls. `laser_sdk::testing`, `respond_on`, and `AgentCtx` support handlers.

Streaming provides producers and continuous partition or consumer-group readers with exact headers, routing, retries, commits, and server offsets. Apache Iggy controls stream and topic access. Its builders remain available for detailed configuration. Python exposes the same underlying streaming implementation.

`sdk/src/govern.rs` defines `ActionGovernor` under `agent`. `Laser::with_governor` applies it before publication, AGDX operations, and memory writes. `Verdict` values are `Allow`, `Observe`, `Block`, `StepUp`, `Modify`, and `Defer`. `GovernorMode::Observe` records decisions without enforcing them and warns on evidence failure. `Enforce` applies the decision and rejects effects if required evidence cannot be stored.

`PolicyEvidence` uses a BLAKE3 digest over canonical data and links to the previous conversation decision through `previous_digest`. `QuorumGovernor` runs named voters under `All`, `Any`, or `AtLeast(n)`. Every `mandatory` voter must return `Allow`, `Observe`, or `Modify`. Mandatory denial or error blocks. Invalid voter sets, unreachable thresholds, duplicate names, and conflicting replacement bodies also block. Other voter errors abstain.

`SwappableGovernor` changes the active policy. `swap` returns the previous policy, `current` reads it, and `decide` uses the policy active for that call. Previous evidence remains unchanged.

`sdk/src/intent.rs` defines approval records under `agent`. `Intent::builder().build() -> Result<Intent, IntentError>` requires unique voters, a valid mandatory subset, a reachable threshold, future deadline, and BLAKE3 digest. `Vote::cast -> Result<Vote, IntentError>` binds an eligible voter to the intent ID, digest, and policy version.

`decide -> Result<Option<Decision>, IntentError>` rechecks decoded intents and ignores mismatched or out-of-window ballots. It orders ballots deterministically and requires all mandatory voters to allow. It commits an accepted quorum or aborts an impossible quorum, missed deadline, or conflicting repeat. `Decision::authorizes` checks the intent binding before an effect. Names remain claims unless signatures or topic isolation establish authorship. Applications publish these records through ordinary typed topics.

`sdk/src/swarm.rs` defines `SwarmActivity` under `agent`. `observe` ignores unattributed evidence and repeated `decision_id` values. `agent(name)` reports counts and the latest decision by `(at_micros, decision_id)`. `agents()` orders agents by activity. The caller supplies records.

`sdk/src/crash_context.rs` defines `CrashContext::assemble` under `agent`. It combines a supplied journal, optional dead letter, and optional policy evidence. `.summarize()` produces bounded output in fixed order and escapes untrusted control characters. It performs no I/O or model call.

The `rbac` API requires `authz` and lives in `sdk/src/rbac/`. It exposes `Laser::whoami`, `list_roles`, `get_role`, `get_bindings`, `define_role`, `delete_role`, `bind_roles`, `bind_roles_expect_revision`, and `authz_history`. Grants use `effect feature:action [on resource-pattern]` for the authenticated server identity. Deny takes precedence, and missing grants deny access.

`validate_role_name` in `wire/src/authz.rs` applies during definition and binding, not replay. Standard Iggy `Permissions` remain independent. The server checks feature, action, and resource before forwarding. The plane checks access to query and graph sources.

`laser-wire` defines the envelope, IDs, dictionaries, validity matrix, operation names, card body, `BodyRef`, reference data, `agdx.av`, and `OpVersions.agent`. SDK methods expose these through `Laser::agdx` and `Agdx`. `AgdxStream` writes chunks. `Agdx::request_input` and `AgentCtx::respond_input` use existing request and reply records. `ChunkAssembler` handles order, duplicates, gaps, late records, terminal records, and abandonment.

The reliable consumer publishes `AgentDeadLetter` for decode failures, expired deadlines, permanent rejection, and exhausted retries. It includes `DeadLetterReason`, attempts, the full `LogPosition`, and original payload. It retains provenance, clears the deadline, and sets `agdx.ct = cbor`.

`Laser::redrive_dead_letter` republishes with a source-position-based idempotency key. `AgentMessage.envelope` and `ContextMessage.envelope` expose the decoded `AgentEnvelope`. For these records, routing, duplicate suppression, and deadline handling use envelope fields.

`A2aBridge` maps `SendMessage` and `SendStreamingMessage` to AGDX commands. It also provides `GetTask`, `CancelTask`, the v1.0 card with `supportedInterfaces`, and optional JWS signing. `McpBridge` supports `initialize`, `tools/list`, `tools/call`, configured `resources/*`, and `prompts/*` under the 2025-11-25 schema. `Laser::reassemble_channel` reads chunk streams from the log.

AG-UI supports `publish_state_snapshot`, `publish_state_delta`, `reconstruct_state`, and `agui_events`. Event families include `TEXT_MESSAGE_*`, `REASONING_MESSAGE_*`, `TOOL_CALL_*`, `RUN_STARTED`, `RUN_FINISHED`, `STATE_*`, and `RUN_ERROR`.

Multi-agent orchestration, all conventions over the log (client-side state machines over offsets/deadlines/leases/replies, no orchestration server):

- `Laser::publish_card` records capabilities, and `AgentPresence` uses `set_client_metadata` for live discovery. `AgentRegistry` combines both and excludes unhealthy or quarantined agents. `Laser::quarantine` and `unquarantine` record reversible changes. Their signed variants require valid signatures. `Laser::agent_registry` caches state per stream and resumes from saved offsets.
- Routing: `Router::{To,Broadcast,ToCapable,AllCapable}` + `InboxRoute::{Advertised,Fixed}`, resolving to an advertised inbox, never a hard-coded shared topic.
- `Laser::contract` reports `Contract::{Completed,Failed,NotConsumed,TimedOut}` and optional pickup `Working` status. `AgentCtx::fan_out` and `Laser::scatter` use `GatherPolicy::{RequireAll,Quorum,BestEffort}`. `AgentCtx::approval_gate` waits for a decision. A configured verifier checks reply signatures before accepting completion.
- `Laser::workflow` runs dependency-ordered steps with `Budget`, `verify_with`, `compensate_with`, replay, and `all_capable` dispatch. `.exclusive()` uses `acquire_fence` and requires `KV_FENCED_LEASES`. Renewal stays bounded by lease expiry and workflow deadline. The lease covers verification and the completion journal write. `StepHandle::on_timeout(OnTimeout::Reassign)` can then reacquire under a new fence. The handler protects external state through its own `Kv::cas_fenced` commit.
- Bound in Python (`Laser.contract`/`scatter`/`quarantine`, `spawn_agent` capabilities/ack_on_pickup/health, `AgentCtx.fan_out`/`approval_gate`, and the `agent_message`/`agent_ctx` handler-test seam), matched by the `orchestra` example (Rust + Python).

Still planned, not present:

- Durable infrastructure-side dedup, and a durable `VectorMemory` backed by an external relational store.
- A richer A2A surface beyond the above (streaming, further agent-card fields).
- Published benchmark results still require measurements from a real Iggy environment. The maintained test runner is in `bench/`.
- Niche AG-UI event types with no AGDX source (`MESSAGES_SNAPSHOT`, `ACTIVITY_*`, `RAW`/`CUSTOM`/`META`, `REASONING_ENCRYPTED_VALUE`).

See the AGDX spec for the wire contract.

## Publish recovery

Rust, Python, and TypeScript publish attempts default to 60 seconds with three retries. Retry delays start at 250 ms, double after each failure, and stop increasing at 30 seconds. Explicit builder or connect configuration overrides `LASER_PUBLISH_TIMEOUT_MS`, `LASER_PUBLISH_MAX_RETRIES`, and `LASER_PUBLISH_RETRY_BACKOFF_MS`.

Preserve message identities and confirmed chunks across retries. Return permanent errors immediately. Return exhausted errors without panicking. Rust reconnects the shared client in place so consumers and reply readers stay attached. Recover only the connection that the attempt used. If another publish replaces that connection, skip recovery and use the replacement. See [publish recovery](docs/publish-recovery.md).
