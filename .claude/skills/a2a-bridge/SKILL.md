---
name: a2a-bridge
description: The A2A JSON-RPC bridge - `sdk/src/a2a.rs`. The adapter (submit / task / cancel / Agent Card, riding the typed AGDX verbs) is behind the `a2a-bridge` feature (serde only, no HTTP). The ready-made axum `router()` is behind the additive `a2a-http` feature. Use when changing the A2A task lifecycle (TaskState), the JSON-RPC envelope, the adapter, the router, or the mapping from A2A methods onto agent topics. Sibling: the MCP bridge `McpBridge` in `sdk/src/mcp.rs` (`mcp-bridge` adapter, `mcp-http` router).
---

# A2A bridge

`a2a.rs` exposes internal agents to A2A-speaking clients over JSON-RPC, mapping the synchronous edge onto durable agent topics so tasks survive a bridge restart and stay replayable. The transport-agnostic adapter (`submit` / `task` / `cancel` / `card`) is behind `a2a-bridge` (serde only). The axum `router()`, the `A2aMethod` dispatch enum, and the JSON-RPC handlers are behind the additive `a2a-http` feature, so a caller can drive the bridge over any transport (or from Python) without compiling axum. Neither is in the default build. Load [laser-sdk-overview](../laser-sdk-overview/SKILL.md) first. Repo rules in [AGENTS.md](../../../AGENTS.md).

## STOP and ask the user before

- Changing the task-state dictionary or its kebab-case names: `TaskState` is the laser-wire agent dictionary (`wire/src/agent.rs`, pinned u8 codes), only re-exported here, and external A2A clients match the names. The JSON boundary (`task_state_json` in `a2a.rs`) maps codes to names via the dictionary's `Display`/`FromStr`, and an unknown inbound name is a protocol error.
- Changing the topic mapping (`message/send` -> request topic keyed by a fresh task conversation. `tasks/get` -> replay of the reply topic).

## Key symbols

- `TaskState` (re-exported from `laser_wire::agent`, 9 states: submitted, working, input-required, completed, canceled, failed, rejected, auth-required, unknown, plus unknown-code passthrough), `Task`, `TaskStatus`, `Artifact`.
- `A2aMethod` (`message/send`, `message/stream`, `tasks/get`, `tasks/cancel`) - the served methods as an enum with `Display`/`FromStr` (strum). Dispatch parses `request.method` into it, never match on bare method-name string literals. `message/stream` maps to the same publish as `message/send` (streaming is consumed log-natively via `Laser::reassemble_channel`, not re-emitted as SSE).
- `JsonRpcRequest` / `JsonRpcResponse` / `JsonRpcError` - the 2.0 envelope. `JSONRPC_VERSION` and `APP_ERROR_CODE` are named consts, not literals.
- `AgentCard` / `AgentCardCapabilities` - the bridge's discovery doc (name = `source`, version, methods, `streaming`).
- `A2aBridge::new(laser, source, request_topic, reply_topic)` - rides the typed AGDX verbs (`Laser::agdx`), not raw `send_agent`:
  - `submit(params_json) -> Task` (`message/send`): publish a typed AGDX `command` tunneling the whole params JSON byte-identical (`agdx.ct = json`) on a fresh task conversation. The task identity rides `correlation`, derived from the conversation via `correlation_of` so lookup stays stateless. Returns `Submitted`.
  - `task(id) -> Task` (`tasks/get`): read the reply topic (envelope-aware `ContextAssembler`), map the answering `response`/`error` envelope with the matching `correlation` via `task_from_envelope`, else `Working`.
  - `cancel(id) -> Task` (`tasks/cancel`): publish an AGDX `error` terminal (`Cancelled`, `task_state = Canceled`), returns `Canceled`.
  - `card() -> AgentCard`: served at `GET /.well-known/agent-card.json`.
  - `router() -> axum::Router` (requires `a2a-http`): the JSON-RPC endpoint at `POST /` plus the card route. The adapter above is usable without it (serve it over any transport, or call `submit` / `task` / `cancel` directly).
  - A worker behind the bridge reads `message.envelope` (the decoded command) and answers via `ctx.laser().agdx(reply_topic, source, conversation).respond(correlation, body)`.

## Rules specific to this area

- The bridge owns no state: truth is the log. `submit`/`task` are pure functions over `Laser`, so the HTTP layer (`router`) is a thin shell and is testable by calling `submit`/`task` directly against Apache Iggy.
- **Stream is not pinned.** A bridge (and every agent) runs on the stream of the `Laser` handed to it: pass `laser.with_stream(name)` (a cheap view sharing the one connection) to run on a non-default stream. Multi-stream topologies and per-stream Iggy RBAC are a `with_stream` (single credential) or separate-`connect` (per-credential) question, never an SDK limit. The reply wait uses the forward-advancing `AgentReplyReader` (`Laser::await_agdx_reply` for MCP's synchronous loop, `find_agdx_reply` for A2A's stateless `tasks/get`) - never a full re-scan from offset 0.
- Keep model calls and business logic out of the bridge. It only translates the protocol to topic sends and log replays.

## Testing

- Pure types (the `TaskStatus` JSON boundary, JSON-RPC parsing) are unit-tested in `a2a.rs`, and the dictionary itself is tested and fixtured in laser-wire.
- The Iggy-backed flow lives in `tests/integration/a2a.rs` (gated on the feature). Run with `cargo test -p laser-sdk --features "integration a2a-bridge query"`.

## Review smells

- Business logic or model calls leaking into the bridge handlers.
- A `TaskState` or `A2aMethod` wire name drifting from the A2A spelling.
- A bare method-name string literal in the dispatch instead of `A2aMethod`.
- The router holding task state instead of replaying the log.
