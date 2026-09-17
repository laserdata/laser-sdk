# Edge interoperability: A2A, MCP, AG-UI

AGDX is the SDK message format for agents on the log. A2A, MCP, and AG-UI adapters expose external interfaces over these records. Fields shared with AGDX map to envelope fields. Other data stays in the body with `agdx.ct = json` and returns unchanged.

Select the optional `a2a-bridge`, `mcp-bridge`, and `agui` features as needed. They use log records through the Iggy transport. This guide describes their use. The [AGDX data exchange model](agdx.md) defines the underlying contract.

## Streams, topics, and RBAC

A bridge or agent uses the stream selected by its `Laser` handle. `laser.with_default_stream(name)` selects another default while sharing the connection. Each stream can contain several topics:

```rust
let orders = laser.with_default_stream("orders-agents");
let billing = laser.with_default_stream("billing-agents");

// An A2A gateway on the orders stream.
A2aBridge::new(
    orders.clone(),
    "orders-gateway".parse()?,
    AgentTopic::Commands,
    AgentTopic::Responses,
);

// An agent on the billing stream, sharing the one connection.
Agent::builder()
    .id("collector".parse()?)
    .listen_on(AgentTopic::Commands)
    .handler(handler)
    .build()
    .spawn(billing.clone());
```

`AgentTopic` variants name the standard `agent.*` topics. `AgentTopic::Custom(&id)` accepts another Iggy topic name. Deployments can use their own topic layout.

Credentials and the hosted HTTP endpoint control access:

- A `with_default_stream` view can access only streams permitted to its connection credentials.
- Use separate `Laser::connect` calls for separate principals. Apache Iggy enforces stream and topic permissions. Protect a bridge JSON-RPC `router` with the hosting application authentication middleware.

Clients read streamed records by offset. After a disconnect, they can resume from a saved position and reconstruct the retained transcript. The [AGDX spec](agdx.md) defines the mapping. The `interop` example demonstrates the complete flow.

## A2A (`a2a-bridge`)

`A2aBridge` serves A2A JSON-RPC and the v1.0 card at `/.well-known/agent-card.json`. The card uses `supportedInterfaces` for endpoint and protocol information instead of top-level `protocolVersion` and `url`. With `sign`, `A2aBridge::signed_card` adds a detached JWS over the canonical card. This uses RFC 8785, RFC 7515, and Ed25519. Use `sign::verify_card` to check it.

A2A v1.0 uses PascalCase operation names. The stateless bridge does not serve `ListTasks`. Unknown methods return the standard JSON-RPC error.

| A2A method | Mapping |
| --- | --- |
| `SendMessage` | Publish a typed AGDX `command` on a fresh task conversation, tunneling the whole params JSON in the body. The task id is the conversation. The A2A task identity rides `correlation` (derived from the conversation, so lookup stays stateless). Returns `Submitted`. |
| `SendStreamingMessage` | Same publish as `SendMessage`. The stream is consumed log-natively (`Laser::reassemble_channel`), not re-emitted as SSE. |
| `GetTask` | Read the reply topic, map the answering `response`/`error` envelope (matched by `correlation`) to the A2A task. `Working` until one lands. |
| `CancelTask` | Publish an AGDX `error` terminal (`Cancelled`, `task_state = Canceled`). Returns `Canceled`. |

```rust
use laser_sdk::prelude::*;
use std::sync::Arc;

let bridge = Arc::new(A2aBridge::new(
    laser.clone(),
    "a2a-gateway".parse()?,        // the bridge's agent id
    AgentTopic::Commands,           // request topic
    AgentTopic::Responses,          // reply topic
));
// Mount the JSON-RPC endpoint + the Agent Card route on your HTTP server:
let app = bridge.router();
```

A worker behind the bridge consumes the decoded command envelope (`message.envelope`) and answers with an AGDX `response` echoing the correlation.

## MCP (`mcp-bridge`)

`McpBridge` is an MCP JSON-RPC server (2025-11-25 schema) mapping tool calls onto AGDX commands and awaiting the correlated reply over the log.

| MCP method | Mapping |
| --- | --- |
| `initialize` | Echo the client's protocol version, and advertise only the capabilities served. |
| `tools/list` | The tools configured via `with_tool` (`name`, optional `title`/`description`, `inputSchema`). |
| `tools/call` | Publish an AGDX `command` (tool name in `tool`, params tunneled in the body), await the correlated `response`/`error` within a timeout, render the `tools/call` result (`content` + `isError`). |
| `resources/list` / `resources/read` | Resources configured via `with_resource`, served from configuration. |
| `prompts/list` / `prompts/get` | Prompts configured via `with_prompt`, rendered into MCP prompt messages. |

```rust
use std::sync::Arc;

let mcp = Arc::new(
    McpBridge::new(
        laser.clone(),
        "mcp-gateway".parse()?,
        AgentTopic::ToolCalls,
        AgentTopic::ToolResults,
        "my-server",
    )
    .with_tool(
        "ask",
        Some("ask the assistant".into()),
        serde_json::json!({ "type": "object" }),
    ),
);
let app = mcp.router();
```

## AG-UI (`agui`)

AG-UI provides interfaces for frontends. The SDK supports state synchronization and event rendering through the log:

- Use `publish_state_snapshot` and `publish_state_delta` for full state and RFC 6902 patches. They produce `state_snapshot` and `state_delta` events. `reconstruct_state` applies a snapshot and subsequent deltas through the selected offset.
- Use `agui_events` to convert chat, reasoning, tool, task, state, and error records into AG-UI events. The mappings include `TEXT_MESSAGE_*`, `REASONING_MESSAGE_*`, `TOOL_CALL_START`, `ARGS`, `END`, `TOOL_CALL_RESULT`, `RUN_STARTED`, `RUN_FINISHED`, `STATE_*`, and `RUN_ERROR`.

```rust
laser
    .publish_state_snapshot(
        AgentTopic::Audit,
        "ui".parse()?,
        conversation,
        &serde_json::json!({ "count": 0 }),
    )
    .await?;

let events = laser.agui_events(conversation, AgentTopic::LlmIo).await?;
```

The niche AG-UI events with no AGDX source (`MESSAGES_SNAPSHOT`, `ACTIVITY_*`, `RAW`/`CUSTOM`/`META`) are not rendered: they are application extensions, not substrate primitives.

## Other edge protocols (ATP, LangChain agent streaming)

### ATP

The `draft-li-atp` Agent Transfer Protocol describes federation between agent servers. It uses `local-part@domain` identities, DNS-based authentication, and store-and-forward delivery.

ATP is a candidate external bridge. AGDX already uses the log for storage, ordering, and replay. An ATP adapter can map external delivery to those operations without changing the substrate.

A proposed ATP mapping uses these fields:

- `from` / `to` onto `source` / `target`
- the nonce onto `idempotency_key`
- `in_reply_to` onto `correlation` / `cause`
- Map signing through an explicitly defined ATP-to-AGDX signing policy. AGDX already provides `Signature` and SDK key registries.

Two ATP ideas are already in AGDX: the claim-check `BodyRef` (reference, size, digest) and the `bridge_hops` loop guard. AGDX's `AgentId` accepts the email-like `local@domain` form, so federated identity round-trips without a lossy hash.

ATP's trust-score-in-the-envelope admission model is deliberately _not_ adopted: every agent-written field stays a claim, and enforcement lives at the capability owner.

### LangChain agent streaming protocol

The `agent-protocol` streaming proposal describes agent-to-client events through SSE and WebSocket. It defines types in CDDL with TypeScript and Python bindings. AGDX treats it as a candidate external adapter.

Its channels include `messages`, `tools`, `lifecycle`, `values`, `updates`, `checkpoints`, `input`, and `custom:*`. Its reconnect model uses `seq`, `since`, and `lastEventId` with a server buffer. An AGDX adapter can map replay to retained log offsets.

The mapping onto AGDX:

- its channels onto AGDX operations
- `seq` / `since` / `lastEventId` onto log offsets
- its thread onto the conversation
- its checkpoint-fork onto the AGDX fork
- its lifecycle `cause` (`toolCall` / `send` / `edge`) onto `cause` / `causal_parent`

`Agdx::request_input(reply_topic, prompt, timeout)` publishes a prompt with a new correlation ID and waits for a response. `AgentCtx::respond_input(reply_topic, decision)` answers it. An AGDX `error` becomes `LaserError::Rejected`. These calls use existing command and response records. The `interop` example demonstrates them on `AgentTopic::HumanInput`.

```rust
// Pause for a human decision, resume with their answer.
let decision = laser
    .agdx(AgentTopic::HumanInput, "orchestrator".parse()?, conversation.into())
    .request_input(AgentTopic::Responses, b"approve a $500 credit?".to_vec(), Duration::from_secs(300))
    .await?;

// The approver agent's handler resolves the interrupt it is handling:
ctx.respond_input(AgentTopic::Responses, b"approved".to_vec()).await?;
```

Two proposals remain unimplemented. One adds typed `text`, `reasoning`, `data`, and `tool_call` blocks with start, delta, and finish events. The other adds a reply position that identifies where a command took effect.

## Real models

The worker behind a bridge selects the model. The `interop` example uses a deterministic `LlmClient` mock by default. `--features llm-anthropic` and `--features llm-openai` select external model clients. The bridge behavior remains the same.

## Claim-check bodies (any bridge, any topic)

A [`BlobStore`] can store a body outside the log. The published `BodyRef` records its location, size, and digest with content-type `ref`. Readers fetch the body and compare its digest before using it. A mismatch returns an integrity error. Supply your own storage implementation, such as object storage, KV, or a development filesystem.

```rust
# use laser_sdk::prelude::full::*;
# async fn run(laser: &Laser, store: &dyn BlobStore, big: Vec<u8>) -> Result<(), LaserError> {
// Publish: at or over the threshold the body is stored and the capsule rides the log.
laser.stream("interop").topic("reports").publish()
    .payload(big)
    .claim_check(store, 256 * 1024)
    .send().await?;

// Consume: resolve through the same store, digest-verified.
# let message: AgentMessage = todo!();
let body = message.resolve_body(store).await?;
# Ok(()) }
```

[`BlobStore`]: https://docs.rs/laser-sdk
