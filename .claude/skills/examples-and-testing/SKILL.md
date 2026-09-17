---
name: examples-and-testing
description: The example crate and the test harness - `examples/rust/` and `sdk/tests/integration/` (including the native `TestIggy` harness). Use when adding/changing an example, the `LlmClient` seam, the server harness, or an integration test.
---

# Examples and testing

TypeScript peers live under `examples/typescript`, `bdd/typescript`, and `foreign/typescript/test`. Release verification includes its packed package, real-Iggy integration, shared BDD, and example tests.

Examples demonstrate SDK behavior, and integration tests make assertions about it. Both use Apache Iggy. Load [laser-sdk-overview](../laser-sdk-overview/SKILL.md) first and follow [AGENTS.md](../../../AGENTS.md).

## STOP and ask the user before

- Making the SDK call an LLM. The SDK is LLM-agnostic transport. Model calls live in example/app code behind the `LlmClient` seam only.
- Wiring examples as CI tests, or changing the shared-process / one-stream-per-test isolation in `harness.rs` (it keeps the suite parallel and isolated).

## Examples (`examples/rust/`)

- One crate, `[[example]]` bins under `src/<scenario>/main.rs`, run with `cargo run --example <name>` against `just up`.
- Use `laser_examples::laser(&stream_for("<example>"), caps)` from `src/lib.rs`. It resolves credentials and creates `Laser::builder().connection_string(...)`. `stream_for(example)` uses `LASER_STREAM` or creates `laser-<example>-<token>` from `run_token()`. `index_for(base)` applies the same run suffix to indexes. Keep unrelated examples in separate streams.

The default server is `iggy:iggy@127.0.0.1:8090`. Accept `LASER_CONNECTION_STRING`, or `LASER_SERVER` with `LASER_TOKEN` or `LASER_USERNAME` and `LASER_PASSWORD`. Credentials can use `user:pwd@host` or `<token>@host`.

The SDK selects TLS and uses `sdk/certs/laserdata.crt` for LaserData hosts. `LASER_TLS_CERT` selects an explicit CA, and `LASER_NO_TLS=1` disables automatic TLS. Do not add certificates, TLS selection, or fixed endpoints to examples.
- Use `laser_examples::start_projector(laser, topic, content_type, fields)` or `laser_examples::ensure_view(laser, topic, index, content_type, fields)`. A separate view name lets `orders` feed `orders_v1`. `laser_examples::wait_for_rows(laser, index, expected)` waits for rows and uses the change feed when available. Python uses `wait_for_projection`, and TypeScript uses `waitForProjection` and `ensureView`.

The helpers call `laser.projections().register(..)` and `laser.bindings().apply(..)`, then let `laser-plane` process records. The returned `Projector.shutdown()` performs no work. Original Apache Iggy returns `LaserError::Unsupported` for managed registration and queries. Use names such as `api_calls` that match `[A-Za-z0-9_]` for managed indexes.
- `src/lib.rs` defines `LlmClient` with asynchronous `complete`. `MockLlm` is deterministic and requires no key. `AnthropicLlm` uses `llm-anthropic`, `ANTHROPIC_API_KEY`, and `ANTHROPIC_MODEL`. `OpenAiLlm` uses `llm-openai`, `OPENAI_API_KEY`, and `OPENAI_MODEL`. `default_llm()` prefers a configured Anthropic client, then OpenAI, then the mock. Keep model calls in application and example code.
- Keep examples deterministic. Test provenance, causality, delivery, dead letters, duplicate suppression, context, fan-out, handoff, sessions, memory, and deadlines.
- Keep the focused `log`, `query`, `watch`, `kv`, `graph`, `recall`, `context`, and `agent` examples aligned across languages. Match their steps, output, and capability requirements. When behavior changes, update all three clients, the related guide, and landing examples together. `recall` is the focused memory example, while `memory` covers the larger scenario.
- Maintain the following nine larger examples:
  - `native-streaming` sends a keyed record and 1000 records in batches through Laser APIs. Separate groups demonstrate automatic and explicit commits. Python uses `examples/python/native_streaming.py`.
  - `event-analytics` combines a live `consumer_group`, `CommitPolicy::Polling`, queries, and a `Cursor` with `StateStore`. Queries cover `count`, `group_by`, `order_desc`, `message_type`, `ts`, and `time_range`. `start_projector` supplies managed projection.
  - `order-book` sends fills through a Laser `Producer` and live consumer group. It calculates a book, queries the tape, and demonstrates schema-based Avro publication. Managed steps use `query`, `schema-codecs`, and `add_avro`.
  - `firehose` generates records across organizations under `LASER_FIREHOSE_MESSAGES`, `LASER_FIREHOSE_ORGS`, `LASER_FIREHOSE_CONCURRENCY`, and `LASER_FIREHOSE_PAYLOAD_BYTES`. Bound these controls before running load tests.
  - `concierge` combines tickets, `VectorMemory`, an `Embedder`, agents, credits, approval, and a proposed change in a fork. Triage uses `ctx.request`, and the resolver demonstrates a KV-backed `Deduplicator`. `LASER_APPLY_PLAN=1` enables the fork decision. `ConversationState::load` reconstructs the recorded incident. Managed phases require the relevant capabilities.
  - `interop` exposes the same worker through `A2aBridge`, `McpBridge`, and AG-UI. It also uses `Agdx::request_input` and `AgentCtx::respond_input`. Rust integration peers are `a2a.rs`, `mcp.rs`, `agui.rs`, and `human_input.rs`.
  - `memory` demonstrates local vector recall, durable memory, context scoping, and graph relationships. It skips unavailable managed phases. Python uses `examples/python/memory.py`.
  - `orchestra` demonstrates discovery, contracts, fan-out, workflows, quarantine, reinstatement, and timeout recovery. `InboxRoute::Fixed` supports the open server. Python uses `examples/python/orchestra.py` with the same phases.
  - `governance` defines `support-reader`, `projection-operator`, `agent-runner`, and `safety-deny` roles for `LASER_GOVERNANCE_USER_ID` when `authz` is available. It demonstrates deny precedence, delegated permission intersection, edge authorization, and run budgets. Python uses `examples/python/governance.py`.
- Begin examples with `Capabilities::OPEN` and inspect the negotiated feature before managed work. Use `cloud_feature_ready` with the relevant query, graph, KV, or authorization capability. If unavailable, report the required deployment and skip that phase. Keep the feature declarations in the example crate aligned with the operations it uses.

## Integration tests (`sdk/tests/integration/`)

- `main.rs` registers each scenario as a module. `harness.rs` owns `TestIggy` (shared `OnceCell` process) and `laser()` (a fresh bootstrapped `Laser` on a unique stream per test). `eventually(...)` polls instead of fixed sleeps.
- `tests/support/test_iggy.rs` is the native Iggy process wrapper. It resolves the versioned artifact or `LASER_TEST_IGGY_SERVER`, and is test-only.
- `decomposition.rs` tests a contract as one targeted AGDX command and scatter as one command per selected agent. Correlations remain distinct per branch. `context(..).append` preserves the body and conversation metadata in an ordinary record.
- `runtime.rs` tests graceful drain, dead-letter callbacks, middleware, and `ConcurrencyPolicy::SerialPerPartition`. It requires a real server. Unit tests cover the error classifier and local testing helpers.
- `streaming.rs` tests the Laser producer and consumer APIs against Iggy. It covers batches, routing, exact headers, delivery, commits, group rejoin, uncommitted replay, and standalone reads.
- For a handler unit test without a server, `laser_sdk::testing` (`agent` feature) ships `agent_message` + `agent_ctx` so `handle(&message, &ctx)` runs directly. That is the offline path, and `runtime.rs` here is the live-server path.
- Gated by the zero-dep `integration` feature so `cargo test --workspace` stays unit-only. Run it with `just test-it`. The suite uses the released Iggy binary.
- Managed query, KV, and fork execution belongs to Laser Stack or LaserData Cloud. Original Apache Iggy returns `Unsupported`. Reference files cover wire bytes, and `laser_bdd::query_engine` covers query behavior through `bdd/scenarios/query.feature`. Do not add a query worker or request-topic fallback to these tests.

## Cross-SDK BDD conformance (`bdd/`)

- Use the shared `bdd/scenarios/*.feature` files for Rust, Python, and TypeScript. Streaming and agent cases use Apache Iggy. Managed query, KV, graph, and memory cases use local reference engines. Run clients through `scripts/run-bdd-tests.sh`. `LASER_BDD_ADDR` selects an existing server address.
- Keep `data_stack.feature` implemented in all three clients. It covers logical schemas, typed queries, explicit targets, paging, execution control, destinations, checkpoints, and Arrow metadata.
- KV and fork _behavior_ is not exercised by the default Apache Iggy BDD gate: they are Iggy command codes (`send_raw_with_response`) only the managed backend dispatches. The default BDD covers the unsupported boundary and unified result-code classification. Their byte compatibility is covered by the fixture corpus, and managed behavior is tested in `laser-plane` and can be exercised through Laser Stack with the managed BDD option.
- `bdd/rust` is a crate OUTSIDE the workspace (its own `[workspace]`), so `cargo test --workspace` does not touch it and it has its own fmt/sort/machete/clippy gate (the `lint-detached` CI job, `just lint`). Same for the `fuzz/` crate.
- Reference files define encoded data, and BDD scenarios define shared behavior. A client must pass both. Use its public API in step definitions and keep scenario files shared. Read `bdd/README.md` for details.

## Rules specific to this area

- Test names are BDD (`given_..._when_..._then_should_...`), assertions use `.expect("message")`, never bare `.unwrap()`.
- Add an example for a new scenario and a regression test for behavior that it must preserve. Include relevant timeout, dead-letter, shutdown, or isolation failures.
- To exercise a raw/edge wire case (for example, a header-less message), publish via Iggy producer directly (`iggy` is an sdk dev-dependency) rather than through `send_agent`, which always stamps provenance.
- `native-streaming`, `order-book`, and `event-analytics` use the Laser `Topic::producer` and `Topic::consumer_group` surface with direct batching, keyed routing, live async delivery, consumer groups, and commit policies. Rust, Python, and TypeScript use Iggy's native VSR transport.

## Review smells

- An LLM call leaking into `sdk/` (must stay behind `LlmClient` in examples).
- A new fix with only a happy-path test and no edge case.
- Fixed `sleep`s in integration tests instead of `eventually`.
- A test that shares a stream with another test (breaks isolation).

## Publish recovery

Publish attempts default to 60 seconds with three retries. Retry delays start at 250 ms, double after each failure, and stop increasing at 30 seconds.

Rust builder methods are `publish_timeout`, `publish_max_retries`, and `publish_retry_backoff`. Python `Laser.connect` keywords are `publish_timeout_ms`, `publish_max_retries`, and `publish_retry_backoff_ms`. TypeScript builder methods are `publishTimeout`, `publishMaxRetries`, and `publishRetryBackoff`. Explicit configuration overrides `LASER_PUBLISH_TIMEOUT_MS`, `LASER_PUBLISH_MAX_RETRIES`, and `LASER_PUBLISH_RETRY_BACKOFF_MS`.

Exhausted retries return an error for the application to handle. They do not exit the process. Preserve message identity and confirmed chunks across retries. See [publish recovery](../../../docs/publish-recovery.md).
