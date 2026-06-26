---
name: python-bindings
description: The Python SDK - foreign/python/, PyO3 bindings over the Rust laser-sdk crate. Use when changing the Python surface (PyLaser, the publish/query/kv/fork builders, the agent runtime and the async-callback handler consumer, errors, stubs), the maturin packaging, the .pyi stubs, the pytest suite, or the Python BDD runner under bdd/python/.
---

# Python bindings (foreign/python)

The Python SDK is a PyO3 binding crate that wraps the Rust `laser-sdk` crate. It does not reimplement anything: the wire contract, codecs, and runtime are the Rust ones. Other languages will live beside it under `foreign/` (the same layout Apache Iggy uses).

## Layout

- `foreign/python/Cargo.toml` - the binding crate (`laser_sdk_py` lib, `cdylib`), depends on `laser-sdk` with the bridge, kv, provenance, query, and schema-codecs features. Excluded from the root workspace (it has its own lock). pyo3 0.28 with `abi3-py310` and `multiple-pymethods`, plus `pyo3-async-runtimes` (tokio), `pyo3-stub-gen`, and `pythonize`.
- `pyproject.toml` - maturin build, PyPI name `laser-sdk`, import name `laser_sdk` (set via `tool.maturin.module-name`, since the cdylib lib name is `laser_sdk_py` to avoid clashing with the Rust crate's `laser_sdk` lib).
- `src/` - one module per area: `client` (PyLaser + connect + Capabilities), `publish` (the single and batch builders, incl. `raw_bytes`/`avro` and batch `add_raw_bytes`/`add_avro` for already-encoded and schema-first bodies), `query` (+ projection/schema registry), `schema` (`CompiledSchema`: `compile` a registered schema source, then `encode_avro` / `validate` / `validate_value` / `decode` client-side), `kv`, `fork`, `agent` (Provenance, ids, `new_conversation_id` / `new_correlation_id`, the `Topics` constants, AgentMessage, send_agent/request/bootstrap), `agdx` (the typed AGDX producer `Laser.agdx` -> command/respond/emit, the `AgdxStream` chunk writer, and `request_input` for the human-in-the-loop interrupt/resume), `agent_runtime` (the handler consumer, AgentCtx incl. `respond_input`, a Python-callback `Deduplicator`, AgentHandle with `async with`, `assemble_context` conversation replay), `reader` (Cursor + Message), `memory` (the `Memory` backends + a Python-callback `Embedder` + MemoryItem), `graph` (the managed `Graph` handle `Laser.graph(name)` -> `upsert`/`neighbors`/`query`, the `node_id`/`edge_id`/`graph_node`/`graph_edge` module functions, and `Laser.register_graph`/`drop_graph`, with nodes and edges crossing the boundary as plain string-id dicts converted by hand since the wire ids serialize as bytes), `state_store` (`InMemoryStore` + `FileStore`), `interop` (the A2A and MCP bridge adapters + the AG-UI methods), `errors`, `convert`.

Read-side coverage is the resumable `Cursor` (`Laser.reader`) and `Laser.assemble_context` (conversation replay with a last-N or role filter). Edge interop is the bridge **adapters**: `Laser.a2a_bridge` (submit/task/cancel/card), `Laser.mcp_bridge` (initialize/list_tools/list_resources/read_resource/call_tool, built with tools/resources/timeout), and the AG-UI methods (`publish_state_snapshot`/`publish_state_delta`/`reconstruct_state`/`agui_events`). A Python agent answers a bridge request with `AgentCtx.respond_input`. The bridges' axum HTTP `router()` is not served from Python (host the endpoint with a Python web framework over the adapter methods). Agent memory and durable state are bound too: `Laser.memory` (log-backed, works on raw Apache Iggy), `Laser.vector_memory(embedder)` (in-process semantic, embedder is an `async def embed(text) -> list[float]`), `Laser.query_memory` and `Laser.kv_memory` (managed), all sharing the `Memory.remember` / `recall` / `forget` surface, plus the `InMemoryStore` and `FileStore` `StateStore` backends (`get` / `set` / `delete`). The durable relationship layer is the managed `Laser.graph(name)` handle (`upsert` / `neighbors` / `query`), with `node_id`/`edge_id`/`graph_node`/`graph_edge` building the content-addressed string-id dicts and `register_graph` registering a graph projection (exercised by `examples/python/memory.py`, the peer of the Rust `memory` example, whose second half builds and traverses a graph). The borrowing backends are rebuilt per call from the owned `Laser`; the in-process vector backend keeps its items in an `Arc`. MCP tools, resources, and prompts are all supported (the prompt dict deserializes into the SDK's `McpPrompt`, which derives `Deserialize` for this).
- `src/bin/stub_gen.rs` - generates `laser_sdk.pyi` and appends the exception hierarchy (which `create_exception!` does not expose to the stub gatherer).
- `tests/` - pytest: `test_smoke.py` (offline) and `test_integration.py` (a real Apache Iggy testcontainer).
- `bdd/python/` - the Python BDD runner over the shared Gherkin in `bdd/scenarios/`, covering every feature the Rust runner does. Streaming, capabilities, provenance, and agent run the SDK against a testcontainer. Query and key-value-CAS run against `bdd/python/reference.py` (a pure-Python port of the Rust reference engines, no Iggy and no client) - the cross-SDK semantics contract that pins all languages to the same answers. Three scenarios skip with a documented reason (capability injection, and the must-understand validity matrix which the wire fixture corpus pins).

## How the binding works

- **Every async Rust method becomes a Python awaitable** via `pyo3_async_runtimes::tokio::future_into_py`. `Laser` is `Clone` (Arc inside), so each method clones it into the async block.
- **Lifetime-bound builders are not exposed directly.** The Rust builders borrow `&Laser`, so the Python builder classes hold owned, accumulated state and reconstruct + run the Rust builder inside the async block at the terminal (`send`/`fetch`). Fluent setters take `PyRefMut<'_, Self>` and mutate in place.
- **Complex managed inputs/outputs ride serde.** A Python dict deserializes straight into a wire type (`projection`, `binding`, `schema source`, dead-letter capsule) via `pythonize::depythonize`, and structured replies serialize back to dicts. This binds the whole registry/control surface without a class per type.
- **The agent handler** drives a Python `async def handle(ctx, message)` from the Rust reliable consumer: `spawn_agent` captures the running event loop's task locals and runs the consumer inside `scope(locals, ...)`, so `into_future` schedules the coroutine on the caller's loop.
- **Errors** map `LaserError` onto a typed Python exception hierarchy (`errors.rs`), attaching `code` / `retryable` / `unsupported` / ... as instance attributes.

## Versioning and naming

- The Python package is `laser-sdk` on PyPI, imported as `laser_sdk`. The internal Rust crate is `laser-sdk-python` (`publish = false`) with cdylib lib `laser_sdk_py`, named to avoid clashing with the `laser_sdk` dependency crate. Maturin renames the built module to `laser_sdk` via `module-name`.
- The Python package tracks its own version, independent of the Rust workspace, so each language SDK releases on its own cadence. It currently sits at `0.0.1-rc.3` while binding the Rust `laser-sdk` at `=0.0.1-rc.4` (the dependency pin).

## Working on it

- Build into a venv: `maturin develop` (the venv lives at `foreign/python/.venv` in local dev).
- Regenerate stubs after any surface change: `cargo run --bin stub_gen`, then check `laser_sdk.pyi` is current.
- `cargo check` for fast type-checking. The crate is outside the workspace, so the workspace clippy/test gates do not cover it: run them here too.
- Lint and format with ruff (config in the repo-root `ruff.toml`): `ruff check` and `ruff format --check` over `foreign/python`, `bdd/python`, and `examples/python`. The generated `.pyi` is excluded.
- Tests: `pytest -q` (needs Docker for the integration suite, and skips cleanly without it), plus the BDD suite in `bdd/python`.
- Keep the surface in step with the Rust SDK: when a Rust public method or wire type changes, mirror it here and regenerate stubs, the same docs-currency rule the rest of the repo follows.
