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

- Editing ANYTHING in `wire/src/` that alters encoded bytes: command codes, op versions, header keys (`wire/src/headers.rs`), topic names, envelope field names or serde attributes, the content-type / task-state / error-code u8 dictionaries, or the caps. Every message already on the log and every consumer (LaserData Cloud, the Iggy server) is pinned to those bytes. The golden corpus under `wire/fixtures/` will fail on drift, and an intentional change needs the fixtures regenerated and the op version bumped.
- Changing `ConversationId::derive` (the FNV-1a algorithm in `sdk/src/types/ids.rs`) without bumping `DERIVE_VERSION` - silently remaps every `SessionPolicy::PerUser` conversation. (`AgentId::wire_id` is no longer a hash: the wire agent id is the name string verbatim.)
- Renaming `AgentTopic` names (`sdk/src/provenance/topic.rs`) - repoints live topics.
- Changing `Provenance::partition_key` (currently `conversation_id`) - breaks the per-conversation ordering guarantee.
- Changing the public signatures of `Laser`, `AgentHandler`, `Agent`, `AgentConsumer::run`, or `AgentHandle` - these are the customer-facing API.
- Dropping an attribution field (`agent`, `root_conversation_id`, ...) when rebuilding `Provenance` in `spawn_subconversation` / `AgentCtx::reply_provenance`
  - silently breaks causality and cost rollup across composed flows.
- Query is LaserData Cloud only: it rides the `AGDX_QUERY` managed command off the log (`send_raw_with_response`, no reply topic, no correlation poll), and against raw Apache Iggy returns `LaserError::Unsupported`. There is no topic request/reply query path. The connect-time `AGDX_HELLO` probe sets the connect-local `managed_host` **and** lights up `query.available` (with the other managed surfaces), so a plain `Laser::connect(..)` against LaserData Cloud queries with no manual caps.

## Verification order

Enforced by CI (`.github/workflows/ci-rust.yml`: jobs `lint`, `lint-detached`, `build`, `wire-feature-matrix`, `feature-matrix`, `wasm`, `deny`, `test`, `fuzz`, `bdd`). The `bdd/rust` and `fuzz` crates sit OUTSIDE the workspace, so `--workspace` does not reach them. `lint-detached` (and `just lint`) run fmt/sort/machete/clippy inside each. Run locally in this exact order, do not skip:

```bash
cargo fmt --all            # 1. formats, auto-applies
cargo sort --workspace     # 2. sorts Cargo.toml deps + feature arrays
cargo machete              # 3. no unused dependencies
cargo clippy --workspace --all-targets --all-features -- -D warnings  # 4.
cargo test --workspace --all-features                   # 5. all tests incl. wire fixtures/robustness + integration (Docker)
cargo test --workspace --all-features --doc             # 6. doctests (Docker-free)
just wasm                  # 7. laser-wire on wasm32-unknown-unknown (needs the target)
just deny-wire             # 8. laser-wire dependency bans (needs cargo-deny)
just advisories            # 9. workspace vuln/unmaintained advisories (needs cargo-deny)
just fuzz                  # 10. bounded fuzzing of the wire decode surface (nightly + cargo-fuzz)
just bdd                   # 11. cross-SDK BDD conformance, Rust runner (needs Docker)
```

**Doctests are a required gate, not optional.** `clippy --all-targets` (step 3) does **not** compile doctests, and a bare `cargo test --workspace` (step 4) runs them only for crates whose default features are on, so a doctest behind `kv` / `query` / any non-default feature is silently never built. Step 5 (`--all-features --doc`) is what actually compiles + runs every doc example. Skipping it means a broken `///` example ships green. It is Docker-free (doctests do not touch the Apache Iggy), so there is no reason to skip it. The same `--all-features --doc` gate runs in CI.

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
    hello.rs            HelloReply / OpVersions (AGDX_HELLO probe body, additive `agent` field + `features` capability bitset: feature::{KV_CAS,READ_YOUR_WRITES,STRONG_CONSISTENCY}) + BackendAnnounce (backend->streaming-server capability announce, AGDX_BACKEND_HELLO_CODE)
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
  types/ids.rs        ConversationId / AgentId / MessageId (FromStr+Display) + the AGDX id
                      bridge (MintUlid, ConversationId <-> wire id, AgentId::wire_id = the name verbatim)
  provenance/         keys.rs re-exports the wire header dictionary, runtime.rs
                      (Provenance <-> headers) + topic.rs (AgentTopic) behind `provenance`
  poll.rs             shared partition drain helper (context + reply + cursor paths)
  context.rs          ContextAssembler + ContextPolicy (LastN, RoleFilter)
  cursor.rs           Laser::reader -> Cursor: resumable offset-addressable stream read
  memory.rs           Memory trait + 4 backends: LogMemory, VectorMemory, QueryMemory, KvMemory
  state_store.rs      StateStore point-store seam: InMemoryStore + FileStore, managed Kv too
  capabilities.rs     Capabilities + Laser::capabilities(), re-exports wire OpVersions
  a2a.rs              A2A JSON-RPC <-> agent-topics bridge + task lifecycle (feature = "a2a-bridge")
  query/              mod.rs re-exports the wire query/browse/control surface, record.rs
                      (Record + agdx.idx.* lowering) and client.rs (Laser::publish/query and
                      the fluent builders) behind feature = "query"
  kv/                 mod.rs re-exports the wire KV surface, client.rs (Kv handle,
                      get/set/delete/scan builders) behind feature = "kv"
  fork/               mod.rs re-exports the wire fork surface, client.rs (ForkHandle,
                      create/promote/squash/put_row) behind feature = "query"
  agent/
    agdx.rs            typed AGDX producer verbs: Laser::agdx -> Agdx (command/respond/emit/
                      status/fail/request_input), AgdxStream chunk writer, routing-header stamping
    assembler.rs      ChunkAssembler: pure per-channel reassembly state machine
    laser.rs          Laser facade: bootstrap, send_agent, request/reply, producer cache,
                      spawn_subconversation, capabilities
    consumer.rs       reliable consumer: Deduplicator seam, commit-after-handle, retry -> DLQ
    builder.rs        Agent::builder + AgentHandle (ready/shutdown/join/abort)
    router.rs         Router (To / Broadcast)
    session.rs        SessionPolicy (PerCall / PerUser)
    state.rs          ConversationState::load (fold the log)
sdk/tests/integration/  one shared Apache Iggy, one stream per test, BDD-named cases
  iggy_container.rs   TestIggy testcontainer harness (test-only, not shipped)
  query/              test-only query worker + backends (Memory, durable SQL)
foreign/python/         the Python SDK (outside the workspace): PyO3 bindings over the
                        laser-sdk crate (cdylib lib laser_sdk_py, import name laser_sdk),
                        src/ one module per area + bin/stub_gen.rs, tests/ pytest
                        (offline + a testcontainer), maturin packaging. See the
                        python-bindings skill.
bdd/                    cross-SDK conformance (outside the workspace): scenarios/
                        shared Gherkin (runs vs Apache Iggy, no Cloud), rust/ the cucumber-rs
                        runner (tests/) + src/query_engine.rs (the pure reference query
                        engine the query scenarios run against), python/ the pytest-bdd
                        runner over the Iggy-native scenarios, docker-compose for
                        the multi-language path
scripts/run-bdd-tests.sh  driver for the per-language BDD runners
examples/rust/          [[example]] bins under src/<scenario>/main.rs, LlmClient seam in lib.rs
examples/python/        one runnable script per scenario + a shared _common.py connect helper
docs/                   tutorial.md (progressive guide), agdx.md (the AGDX spec),
                        interop.md (A2A / MCP / AG-UI bridges)
```

## Repo-wide principles

- **The wire crate is the one source of truth.** Wire types, codes, header keys, topic names, and caps live in `wire/` and only there. The SDK re-exports them under its historical paths (`laser_sdk::query::Query` keeps working) and as `laser_sdk::wire`. Never re-declare a wire shape in the SDK, and never encode a band envelope with a serde codec crate directly: route every frame through `laser_wire::framing::encode_named`/`decode_named` (named-field CBOR), the one band-encoding entry point, so no consumer drifts to a different encoding.
- **laser-wire stays runtime-free.** No IO, no async, no clock, no randomness, no iggy/tokio/bytes/ulid dependency (CI-banned). Byte fields are `Vec<u8>` with bin-encoding serde, never `bytes::Bytes`. Id GENERATION (ULID entropy + clock) lives SDK-side (`MintUlid`).
- **Thin layer, never hide Iggy.** Reuse Iggy types directly (`IggyMessage`, `HeaderKey`/`HeaderValue`, `Partitioning`, `IggyProducer`, `IggyConsumer`, `Identifier`, `IggyTimestamp`). Do not reimplement the poll/commit loop. Wrap a handler and delegate to `IggyConsumerMessageExt::consume_messages`.
- **Delivery is at-least-once + idempotent, never exactly-once.** Ordering is per-conversation (per-partition) only. There is no cross-conversation order.
- **Idiomatic Rust traits over free helpers.** Parsing = `FromStr`/`.parse()`, formatting = `Display`, conversion = `From`/`TryFrom`. `FromStr::Err` is a structured enum (see `IdError`, `ProvenanceError`), never `String`.
- **No em dashes** anywhere (code, comments, commits, docs). Use commas/colons.
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
- Integration tests (`sdk/tests/integration/`) run against a real Apache Iggy instance via `TestIggy` (testcontainers). One container is shared. Each test gets its own data stream **and** its own ops stream (`harness::laser()` sets both via `with_ops_stream`), so parallel query workers on the shared instance never serve each other's requests. The suite stays isolated and parallel.
- `harness::eventually` polls instead of fixed sleeps. Iggy visibility is eventual.
- Pin the Iggy image for reproducibility with `LASER_TEST_IGGY_TAG` (default `edge`).
- The managed surface (query, KV, forks) needs LaserData Cloud and is not run end-to-end here. Query is the off-log `AGDX_QUERY` managed command, KV and forks are raw managed commands, all returning `Unsupported` on raw Apache Iggy. There is no in-process query worker and no topic query path. The split: the client wire bytes are pinned by the fixture corpus, the full managed execution is verified in LaserData Cloud's own repository (it consumes this published SDK), and the **query DSL semantics** are covered here by a pure in-memory reference engine (`laser_bdd::query_engine` (in the `bdd/` harness), no Iggy, no transport) via its own unit tests and the `query.feature` BDD scenarios, so every SDK has a reference for what a query must return.

## What is shipped vs planned

This crate is **not "all Phase 1."** The Phase 1 core (vanilla Iggy) is shipped: provenance + causality, reliable consumer, context seam, memory seam, `Agent` builder / router / session / state, plus the `respond_on` + `AgentCtx` handler seam. Isolation is an Iggy stream boundary (one connection drives any number of streams), not a per-message header - see `examples/rust/src/multi-stream/main.rs`. On top of it we landed, early and behind feature flags, several features the spec files as later / Tier B: opt-in warm dedup on restart, a semantic `VectorMemory` (in-memory, pluggable `Embedder`), a durable `KvMemory` (`Memory` over the managed KV store, feature `kv`), and the `a2a-bridge` feature (A2A JSON-RPC over agent topics). The `distributed-agents` example runs on real Claude (`llm-anthropic`) or OpenAI (`llm-openai`).

The **Agent Data Exchange Protocol (AGDX)** wire surface is shipped in laser-wire (the envelope, ids, dictionaries, the validity matrix with its closed operation vocabularies, the pinned card body, the claim-check `BodyRef`, draft fixtures embedded in the corpus, the complete AGDX record fixture, the `agdx.av` version header, the additive `OpVersions.agent` advertisement) together with the SDK-side id bridge. Shipped SDK runtime pieces on top: the typed producer verbs (`Laser::agdx` -> `Agdx`: command/respond/emit/status/fail plus the `AgdxStream` chunk writer, validated envelopes, typed routing headers, conversation partition key), the human-in-the-loop interrupt/resume pair (`Agdx::request_input` + `AgentCtx::respond_input`, composed from command/response, no new wire), and the pure `ChunkAssembler` reassembly state machine (duplicate/gap/late/double-terminal/abandonment rules), plus the A2A and MCP mapping functions with byte-identical tunneled payload tests. The reliable consumer publishes the `AgentDeadLetter` capsule on every dead-letter path (decode failure, deadline, permanent rejection, retry exhaustion): the typed `DeadLetterReason`, the attempt count, the poison message's full `LogPosition`, and the original payload verbatim. The capsule message keeps the original provenance headers (deadline cleared) and is marked `agdx.ct = cbor`, and `Laser::redrive_dead_letter` reads the original record at `source` and republishes it verbatim for a fixed handler to reprocess. The provenance decoder matches keys first and fails-not-skips (a known key with a non-string value is a decode error, and only foreign keys are ignored), so AGDX typed headers and the string provenance dictionary coexist on one record because the AGDX keys are foreign to provenance, not because non-string values are dropped. The **reliable consumer and the read path are envelope-aware**: `AgentMessage.envelope`/`ContextMessage.envelope` carry the decoded `AgentEnvelope`, and routing/dedup/deadline are synthesized from it for AGDX messages. On that foundation the bridges ride the AGDX verbs: the **A2A bridge** (`A2aBridge`: `message/send` and `message/stream` -> typed `command` tunneling the params JSON, `tasks/get` mapping the answering envelope, `tasks/cancel` -> `Cancelled` terminal, the Agent Card at the well-known path), the **MCP bridge** (`McpBridge`: `initialize`/`tools/list`/`tools/call` -> AGDX `command` awaiting the correlated reply, plus `resources/list`/`resources/read`/`prompts/list`/`prompts/get` served from config), and `Laser::reassemble_channel` (log-native chunk-stream replay over Iggy, no SSE). Dedup is envelope-keyed for AGDX messages (the consumer synthesizes the idempotency key from the envelope). **AG-UI** (`agui` feature) ships state sync (`publish_state_snapshot`/`publish_state_delta`/`reconstruct_state`, RFC 6902 replay) and event rendering (`agui_events` turns a conversation into `AgUiEvent`s: chat chunk streams -> `TEXT_MESSAGE_*`, reasoning streams -> `REASONING_MESSAGE_*`, `tool_args` streams -> `TOOL_CALL_START`/`ARGS`/`END`, a tool `response`/`error` -> `TOOL_CALL_RESULT`, `status` task updates -> `RUN_STARTED`/`RUN_FINISHED`, state events -> `STATE_*`, an error terminal -> `RUN_ERROR`, and the per-channel stream kind is threaded since the purpose rides only the opening chunk), all over the log. The edge schemas match the current external specs: the A2A Agent Card follows the v0.3.0 `AgentCard` shape, and the MCP `Tool`/`Resource`/`Prompt`, `tools/call` result, and `initialize` result follow the 2025-11-25 schema. Still genuinely outstanding: the byte/latency **benchmark suite** (needs a real Iggy environment to measure), and the niche AG-UI event types with no AGDX source (`MESSAGES_SNAPSHOT`, `ACTIVITY_*`, `RAW`/`CUSTOM`/`META`, `REASONING_ENCRYPTED_VALUE`).

These early features exist to exercise the **seams** the paid tiers plug into. Their premium forms are managed/durable backends (durable dedup, the knowledge graph, an A2A gateway) activated by capability negotiation, not code changes. Agentic memory has no managed surface of its own: it composes the query and graph surfaces. Their tiers run from raw Apache Iggy up to the managed LaserData Cloud runtime.

Planned and intentionally not here yet: durable infrastructure-side dedup, a backend-backed durable `VectorMemory`, and a richer A2A surface (streaming, agent card). See the AGDX spec for the wire contract.
