---
name: python-bindings
description: The Python SDK - foreign/python/, PyO3 bindings over the Rust laser-sdk crate. Use when changing the Python surface (PyLaser, the publish/query/kv/fork builders, the agent runtime and the async-callback handler consumer, errors, stubs), the maturin packaging, the .pyi stubs, the pytest suite, or the Python BDD runner under bdd/python/.
---

# Python bindings (foreign/python)

Python uses PyO3 bindings over the Rust `laser-sdk` crate. Keep data encoding and runtime behavior in Rust. The native TypeScript implementation is maintained separately under `foreign/typescript/`.

## Layout

- `foreign/python/Cargo.toml` - the binding crate (`laser_sdk_py` lib, `cdylib`), depends on `laser-sdk` with the managed and agent surfaces, and uses Iggy's native VSR transport. The crate is excluded from the root workspace and has its own lock.
- `pyproject.toml` - maturin build, PyPI name `laser-sdk`, import name `laser_sdk` (set via `tool.maturin.module-name`, since the cdylib lib name is `laser_sdk_py` to avoid clashing with the Rust crate's `laser_sdk` lib).
- `src/client.rs` and `src/stream.rs` bind `Laser`, capabilities, the stream/topic accessor grammar, typed topics, publish/batch/replay/ensure, and `producer`/`consumer`/`consumer_group`.
- `src/transport.rs` binds `Producer`, `Consumer`, and `ConsumerMessage`. Preserve routing, batching, retry, polling, group, header, and commit behavior. It also provides offset inspection and continuous asynchronous reads.
- Direct and fluent sends return immutable `SendMessagesResponse` and `SendMessagesConfirmation` classes. Keep their stream, topic, partition, and base-offset fields in parity with Rust, regenerate the stub after changes, and never turn an empty confirmation list into a client error.
- `src/publish.rs`, `reader.rs`, `typed.rs`, `schema.rs`, `query.rs`, `watch.rs`, `kv.rs`, `fork.rs`, and `graph.rs` bind the data-platform write and read surfaces. Complex managed shapes cross through serde instead of hand-maintained mirror classes.
- `src/agent.rs`, `agdx.rs`, `agent_runtime.rs`, `workflow.rs`, `runs.rs`, `rbac.rs`, `context.rs`, `memory.rs`, `state_store.rs`, and `sign.rs` expose agent behavior. Keep these bindings consistent with Rust. `laser.agdx(..., signing_key=)` signs producer envelopes, and `spawn_agent(signing_key=)` signs replies through `PyAgentCtx`. `verifier=` selects receive-side keys. The `agent_ctx` test helper also accepts `signing_key=`.

`laser.sessions(*, stream=None, topics=None, memory_namespace=None, context_turns=None, context_tokens=None)` returns `Sessions`. Its `create(id)`, `start()`, and `open(conversation_id)` methods return a `Session`. `append(kind, data)` records a turn. `context(*, last_n, token_budget)` returns `list[SessionTurn]`, with `kind`, `payload`, `text()`, and `message` fields. The message includes its source `topic`.

`memory(memory=None)` returns `ScopedMemory`, which provides `search(query)`. `checkpoint()` returns a `Checkpoint` with `to_json` and `from_json` methods. `turns_at` and `turns_since` read around its saved offsets. `state_at(checkpoint, initial, fold)` and `replay(checkpoint, initial, fold)` rebuild state. `topics` maps each turn kind to a distinct topic name. Two kinds that share a topic raise `InvalidError`.

`laser.topic(name).replay()` returns `Cursor`. `Laser.assemble_context` reads a conversation with topic, role, last-count, and token-budget controls. `laser.context(conv).fetch` and `block` map `last_n` and `token_budget` to Rust `Chain(LastN, TokenBudget)`. RBAC includes `bind_roles(..., expect_revision=...)` and `authz_history_*`.

`Laser.a2a_bridge` provides submit, task, cancel, and card operations. `Laser.mcp_bridge` provides initialization, tools, resources, and prompts through configured `McpPrompt` values. AG-UI methods include `publish_state_snapshot`, `publish_state_delta`, `reconstruct_state`, and `agui_events`. `AgentCtx.respond_input` answers an interrupt. Python applications host HTTP endpoints through their own framework rather than the Rust axum `router()`.

`Laser.memory`, `Laser.memory_on_topic(name)`, and `Laser.memory_topic(name, partitions=, ttl_secs=)` provide log-based memory. Topic expiry defaults to 30 days. `Memory.vector(embedder)` is standalone, while `Laser.vector_memory(embedder)` inherits the Laser governor. Embedders can return vectors directly or return awaitables.

Default `recall` reads the managed KV view. `recall(folded=True)` and `fetch_folded` read the log locally. `MemoryItem.source` identifies the original record. The durable handle also provides `set`, `fetch`, `update`, and `remove` by name. Vector memory returns `UnsupportedError` for those named-state operations. `InMemoryStore` and `FileStore` implement `StateStore`.

`Laser.graph(name)` provides `upsert`, `neighbors`, and `query`. `node_id`, `edge_id`, `graph_node`, and `graph_edge` construct content IDs. `register_graph` defines a graph projection, as demonstrated in `examples/python/memory.py`.

Graph reads accept `conversation=`, while query and KV use `.conversation(id)`. These restrict result scope. Build borrowed Rust handles for each Python call and retain owned state between calls. The local vector backend shares its items through `Arc`.

- `src/bin/stub_gen.rs` - generates `laser_sdk.pyi` and appends the exception hierarchy (which `create_exception!` does not expose to the stub gatherer).
- `tests/` - pytest: `test_smoke.py` (offline) and `test_integration.py` (native Iggy).
- `bdd/python/` - the Python BDD runner over the shared Gherkin in `bdd/scenarios/`. Streaming, capabilities, provenance, and agent run against Iggy. Query and key-value CAS use the transport-free reference engine.

## Data stack parity

`src/query.rs` binds Query with explicit operational and lakehouse targets, snapshot selection, typed raw SQL parameters, cursor paging, execution status and cancellation, and rich result context. Query results expose `fields` and positional tagged `Row.values`. Never restore `row.headers`. Use `QueryResult.value` and `value_text` for field-name access.

`src/destinations.rs` binds revision-guarded destination mutations, bounded checkpoint reads, and explicit query routes. `client.rs` exposes `Laser.destinations()` and `Laser.query_lakehouse()`.

`src/publish.rs` exposes `arrow_ipc` and `add_arrow_ipc`. Metadata crosses through serde and exact payload length is checked before I/O. Keep the self-contained Arrow stream policy in stubs and README examples.

After a data-contract change, regenerate `laser_sdk.pyi` and update stub tests, Python tests, shared `@data_stack` scenarios, and README examples. Use serde-derived dictionaries for large managed types rather than separate Python data definitions.

## How the binding works

- Every async Rust method becomes a Python awaitable via `pyo3_async_runtimes::tokio::future_into_py`. `Laser` is `Clone` (Arc inside), so each method clones it into the async block.
- Lifetime-bound builders are not exposed directly. The Rust builders borrow `&Laser`, so the Python builder classes hold owned, accumulated state and reconstruct + run the Rust builder inside the async block at the terminal (`send`/`fetch`). Fluent setters take `PyRefMut<'_, Self>` and mutate in place.
- Complex managed inputs/outputs ride serde. A Python dict deserializes straight into a wire type (`projection`, `binding`, `schema source`, dead-letter capsule) via `pythonize::depythonize`, and structured replies serialize back to dicts. This binds the whole registry/control surface without a class per type.
- `spawn_agent` calls Python `async def handle(ctx, message)` through the Rust consumer. Capture the event-loop locals and run within `scope(locals, ...)`. `into_future` then schedules the coroutine on the correct loop.
- Map `LaserError` to the hierarchy in `errors.rs` and retain classification attributes such as `code`, `retryable`, and `unsupported`. `TimeoutError` also inherits `builtins.TimeoutError`. `CancelledError` also inherits `asyncio.CancelledError`. Cache these generated exception types in `OnceLock` so both SDK and standard exception handlers recognize them.
- `Cursor`, `WatchReader`, and `TypedRecords` provide `__aiter__` and `__anext__`. They yield buffered records and raise `StopAsyncIteration` when caught up. A later iteration resumes from saved offsets. Keep `poll()` and typed `next()` available.

`Consumer` instead waits for new records until `shutdown()`. It supports automatic and explicit commits. `header_kinds` reports exact Iggy types. Use `(kind, value)`, such as `("uint16", 7)`, for an explicit numeric width.
- The live consumer avoids an intermediate payload copy. `PyConsumerMessage` retains Apache Iggy's reference-counted `Bytes` until its `payload` getter creates the one Python-owned `bytes` object required by the language boundary.
- `Laser` provides `__aenter__` and `__aexit__` for `async with`. The connection closes when the last shared handle is dropped. `with_stream`, `with_ops_stream`, `with_control_topic`, `with_dlq_topic`, and `with_changes_topic` share that connection. `Laser.capabilities()` and `refresh_capabilities()` use Rust discovery. Preserve unavailable-backend handling and explicit handle overrides.
- `spawn_agent` exposes `max_partitions`, `shutdown_grace_ms`, `retry_max_attempts`, `retry_base_delay_ms`, `dead_letter`, and `middleware`. The dead-letter callback receives message, reason, attempts, and publication result through `PyDeadLetterSink`. Middleware can implement asynchronous `before_handle` and `after_handle`. `Laser.scatter_report(...)` returns branch results. `Provenance` and `AgentMessage` retain `correlation_id` separately from `idempotency_key`.
- `AgentCtx.fan_out(skill, payload, *, policy, quorum, deadline_ms, fixed_inbox, principal)` returns successful branches and failures. `approval_gate` waits for a decision through a temporary `laser_sdk::testing::agent_ctx`. The same approach supports the owned Python wrapper over borrowed Rust context. Module functions `agent_message` and `agent_ctx` let tests call handlers without a live consumer.
- Governance delegates to Rust. Preserve mandatory-voter rules and rejection of invalid configurations. `SwappableGovernor.swap` returns the previous policy, and `current` returns the active one. `Intent`, `Vote`, and `Decision` use typed topic encoding. Invalid construction, voting, or folding raises `InvalidError`.

Call `Decision.authorizes(intent)` before the effect. Names remain claims unless signatures or topic permissions establish authorship. `SwarmActivity` preserves unknown verdict names and ignores repeated evidence. `CrashContext` exposes `journal`, `dead_letter`, and `last_decision` and escapes control characters in summaries. Keep parity tests and stubs current.
- `Laser.execute_batch` accepts Rust `BatchItem` dictionaries and returns exact reply bytes. `Agdx.status`, `Agdx.fail`, and `AgdxStream.fail` retain their typed data. Convert floating-point durations through `Duration::try_from_secs_f64`. Negative, non-finite, and out-of-range values must raise `InvalidError` rather than panic.
- `Kv.lease` and `Kv.renew_lease` return `Lease` with token, granted lifetime, and position. Python inherits the dedicated Rust coordination connection, validation, and timeout reset. An uncertain acquisition waits through its requested lifetime before raising.

Pass `MutationPosition { topic_generation, partition, offset }` to `Kv.get_entry_at_least` for takeover reads. An unmet position returns stale rather than absent data. `LaserError.ambiguous_mutation` requires operation-specific recovery and must not trigger generic handler retries.

## Versioning and naming

- The Python package is `laser-sdk` on PyPI, imported as `laser_sdk`. The internal Rust crate is `laser-sdk-python` (`publish = false`) with cdylib lib `laser_sdk_py`, named to avoid clashing with the `laser_sdk` dependency crate. Maturin renames the built module to `laser_sdk` via `module-name`.
- Python follows the shared workspace version, currently `0.4.0`. Its dependency must select the matching Rust `laser-sdk` crate.

## Working on it

- Build into a venv: `maturin develop` (the venv lives at `foreign/python/.venv` in local dev).
- Regenerate stubs after any surface change: `cargo run --bin stub_gen`, then check `laser_sdk.pyi` is current.
- `cargo check` for fast type-checking. The crate is outside the workspace, so the workspace clippy/test gates do not cover it: run them here too.
- Lint and format with ruff (configuration in the repo-root `ruff.toml`): `ruff check` and `ruff format --check` over `foreign/python`, `bdd/python`, and `examples/python`. The generated `.pyi` is excluded.
- Tests: `pytest -q` against the versioned Iggy server, plus the BDD suite in `bdd/python`. `LASER_TEST_IGGY_SERVER` selects a local Iggy binary for development.
- After a Rust API or wire change, update Python bindings and regenerate stubs. Update the corresponding tests and documentation in the same authorized change.

## Publish recovery

Publish attempts default to 60 seconds with three retries. Retry delays start at 250 ms, double after each failure, and stop increasing at 30 seconds.

Rust builder methods are `publish_timeout`, `publish_max_retries`, and `publish_retry_backoff`. Python `Laser.connect` keywords are `publish_timeout_ms`, `publish_max_retries`, and `publish_retry_backoff_ms`. TypeScript builder methods are `publishTimeout`, `publishMaxRetries`, and `publishRetryBackoff`. Explicit configuration overrides `LASER_PUBLISH_TIMEOUT_MS`, `LASER_PUBLISH_MAX_RETRIES`, and `LASER_PUBLISH_RETRY_BACKOFF_MS`.

Exhausted retries return an error for the application to handle. They do not exit the process. Preserve message identity and confirmed chunks across retries. See [publish recovery](../../../docs/publish-recovery.md).
