---
name: a2a-bridge
description: The A2A JSON-RPC bridge - `sdk/src/a2a.rs`. The adapter (submit / task / cancel / Agent Card, riding the typed AGDX verbs) is behind the `a2a-bridge` feature (serde only, no HTTP). The ready-made axum `router()` is behind the additive `a2a-http` feature. Use when changing the A2A task lifecycle (TaskState), the JSON-RPC envelope, the adapter, the router, or the mapping from A2A methods onto agent topics. Sibling: the MCP bridge `McpBridge` in `sdk/src/mcp.rs` (`mcp-bridge` adapter, `mcp-http` router).
---

# A2A bridge

The TypeScript A2A, MCP, AG-UI, and hop-guard peers live under `foreign/typescript/src/bridges` and share the cross-language bridge scenarios.

`a2a.rs` maps A2A requests to durable AGDX records. `a2a-bridge` enables the transport-independent `submit`, `task`, `cancel`, and `card` adapter. `a2a-http` adds the axum `router()`, `A2aMethod`, and JSON-RPC handlers. Neither feature is enabled by default. Load [laser-sdk-overview](../laser-sdk-overview/SKILL.md) first and follow [AGENTS.md](../../../AGENTS.md).

## STOP and ask the user before

- Changing the task-state dictionary or its kebab-case names: `TaskState` is the laser-wire agent dictionary (`wire/src/agent.rs`, pinned u8 codes), only re-exported here, and external A2A clients match the names. The JSON boundary (`task_state_json` in `a2a.rs`) maps codes to names via the dictionary's `Display`/`FromStr`, and an unknown inbound name is a protocol error.
- Changing the topic mapping (`SendMessage` -> request topic keyed by a fresh task conversation. `GetTask` -> replay of the reply topic).

## Key symbols

- `TaskState` (re-exported from `laser_wire::agent`, 9 states: submitted, working, input-required, completed, canceled, failed, rejected, auth-required, unknown, plus unknown-code passthrough), `Task`, `TaskStatus`, `Artifact`.
- `A2aMethod` (`SendMessage`, `SendStreamingMessage`, `GetTask`, `CancelTask` - the v1.0 PascalCase spellings. V1.0's `ListTasks` is not served, the bridge is stateless over the log and an unknown method answers the standard method-not-found) - the served methods as an enum with `Display`/`FromStr` (strum). Dispatch parses `request.method` into it, never match on bare method-name string literals. `SendStreamingMessage` maps to the same publish as `SendMessage` (streaming is consumed log-natively via `Laser::reassemble_channel`, not re-emitted as SSE).
- `JsonRpcRequest` / `JsonRpcResponse` / `JsonRpcError` - the 2.0 envelope. `JSONRPC_VERSION` and `APP_ERROR_CODE` are named consts, not literals.
- `AgentCard` / `AgentCardCapabilities` - the bridge's discovery doc (name = `source`, version, methods, `streaming`).
- `A2aBridge::new(laser, source, request_topic, reply_topic)` - rides the typed AGDX verbs (`Laser::agdx`), not raw `send_agent`:
  - `submit(params_json) -> Task` (`SendMessage`): publish a typed AGDX `command` tunneling the whole params JSON byte-identical (`agdx.ct = json`) on a fresh task conversation. The task identity rides `correlation`, derived from the conversation via `correlation_of` so lookup stays stateless. Returns `Submitted`.
  - `task(id) -> Task` (`tasks/get`): read the reply topic (envelope-aware `ContextAssembler`), map the answering `response`/`error` envelope with the matching `correlation` via `task_from_envelope`, else `Working`.
  - `cancel(id) -> Task` (`CancelTask`): publish an AGDX `error` terminal (`Cancelled`, `task_state = Canceled`), returns `Canceled`.
  - `card() -> AgentCard`: served at `GET /.well-known/agent-card.json`.
  - `router() -> axum::Router` (requires `a2a-http`): the JSON-RPC endpoint at `POST /` plus the card route. The adapter above is usable without it (serve it over any transport, or call `submit` / `task` / `cancel` directly).
  - A worker behind the bridge reads `message.envelope` (the decoded command) and answers via `ctx.laser().agdx(reply_topic, source, conversation).respond(correlation, body)`.

## Rules specific to this area

- The bridge owns no state: truth is the log. `submit`/`task` are pure functions over `Laser`, so the HTTP layer (`router`) is a thin shell and is testable by calling `submit`/`task` directly against Apache Iggy.
- A bridge uses the stream selected by its `Laser` handle. Use `laser.with_stream(name)` to share one connection across stream-scoped views. Use separate connections for separate credentials. Reply reads use `AgentReplyReader`, `Laser::await_agdx_reply`, or `find_agdx_reply`. Avoid repeated full scans for an active reply wait.
- `authorize_edge` in `sdk/src/edge_auth.rs` checks token audience and required scopes. `EdgeDenial::StepUp` reports additional scope through `WWW-Authenticate`. Do not forward external tokens to the log. `sign::verify_delegation` binds `on_behalf_of` to the signed envelope. `laser_wire::authz::delegated_allow` intersects agent and invoking-user grants.
- Keep model calls and business logic out of the bridge. It only translates the protocol to topic sends and log replays.

## Testing

- Pure types (the `TaskStatus` JSON boundary, JSON-RPC parsing) are unit-tested in `a2a.rs`, and the dictionary itself is tested and fixtured in laser-wire.
- The Iggy-backed flow lives in `tests/integration/a2a.rs` (gated on the feature). Run with `cargo test -p laser-sdk --features "integration a2a-bridge query"`.

## Review smells

- Business logic or model calls leaking into the bridge handlers.
- A `TaskState` or `A2aMethod` wire name drifting from the A2A spelling.
- A bare method-name string literal in the dispatch instead of `A2aMethod`.
- The router holding task state instead of replaying the log.
