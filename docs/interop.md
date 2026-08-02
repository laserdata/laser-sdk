# Edge interoperability: A2A, MCP, AG-UI

The Agent Data Exchange Protocol (AGDX) is the SDK's internal, on-log agent wire format. The edge standards - A2A (agent-to-agent), MCP (agent-to-tool), AG-UI (agent-to-frontend) - bridge _into_ AGDX, so an internal agent only ever speaks to the log while external clients keep their own public contracts. The bridge rule is always the same: **map the core, tunnel the remainder.** The fields AGDX shares with a standard map structurally onto envelope fields. Everything else rides byte-identical in the body (`agdx.ct = json`) and round-trips untouched.

All three are optional, behind feature flags (`a2a-bridge`, `mcp-bridge`, `agui`), and ride the durable log over Iggy's own transports - never SSE. This doc is the bridge usage guide. For what AGDX itself is and why agent messaging on a durable log beats the edge transports these standards ship on, see the [AGDX data exchange model](agdx.md).

## Streams, topics, and RBAC

Nothing in the agent layer is pinned to one stream. A bridge or agent runs on the stream of the `Laser` you hand it, and `laser.with_default_stream(name)` is a cheap view that re-scopes to another stream while sharing the one connection. So a single cluster scales to many streams, each with many topics, by handing each agent or bridge a stream-scoped `Laser`:

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

Topics are equally free: the well-known `AgentTopic` variants name `agent.*` topics, and `AgentTopic::Custom(&id)` takes any Iggy topic name, so a deployment can carry its own stream/topic convention for thousands of agents.

Two authorization layers, both Iggy's, neither in the SDK:

- **Within one credential** (one `Laser` connection), `with_default_stream` views address every stream that credential may touch.
- **Across credentials**, open a separate `Laser::connect` per principal. Iggy RBAC enforces which streams and topics each credential may read or write, so per-stream / per-topic permission isolation is a topology and credential question, decided below the protocol. The HTTP edge of a bridge (the JSON-RPC `router`) is unauthenticated by design - wrap it in the embedder's auth middleware (A2A and MCP each define their own edge auth schemes).

Streaming is consumed log-natively (offset replay), which is what lets a token stream resume after a disconnect and reassemble later as an auditable transcript. The wire-level mapping is normative in the [AGDX spec](agdx.md). This doc is the usage guide. A runnable end-to-end demo is the `interop` example.

## A2A (`a2a-bridge`)

`A2aBridge` exposes an internal agent to A2A JSON-RPC clients and serves the v1.0 Agent Card at `/.well-known/agent-card.json`: the endpoint and protocol version ride `supportedInterfaces` (v1.0 dropped the top-level `protocolVersion`/`url`), and with the `sign` feature `A2aBridge::signed_card` attaches a detached JWS over the JCS-canonicalized card (RFC 8785 + RFC 7515, EdDSA with the same enrolled Ed25519 key the envelope scheme uses. Verify with `sign::verify_card`). v1.0 renamed the JSON-RPC operations to PascalCase. `ListTasks` is not served (the bridge is stateless over the log): an unrecognized method gets the same JSON-RPC error every other unknown method gets.

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
| `resources/list` / `resources/read` | Resources configured via `with_resource`, served from config. |
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

AG-UI is frontend-facing. Two pieces ship today, both over the log:

- **State sync.** `publish_state_snapshot` / `publish_state_delta` emit the shared state and RFC 6902 patches as `state_snapshot` / `state_delta` events. `reconstruct_state` replays a snapshot plus its later deltas into the current state at any historical offset.
- **Event rendering.** `agui_events` turns a conversation into AG-UI events: chat chunk streams -> `TEXT_MESSAGE_*`, reasoning streams -> `REASONING_MESSAGE_*`, `tool_args` streams -> `TOOL_CALL_START`/`ARGS`/`END`, a tool result -> `TOOL_CALL_RESULT`, `status` task updates -> `RUN_STARTED`/`RUN_FINISHED`, state events -> `STATE_*`, an error terminal -> `RUN_ERROR`.

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

The Agent Transfer Protocol (the IETF `draft-li-atp` line) is an internet-scale _federation_ layer: email-like agent identity (`local-part@domain`), DKIM/SPF-style auth published over DNS, and server-mediated store-and-forward delivery.

It sits at the same layer as A2A and MCP, not at AGDX's substrate layer. The durable log already _is_ the transport, store, ordering, retry (deadline plus retention), dedup, and dead-letter that ATP builds out of relays. So ATP is a **candidate edge bridge**, like A2A and MCP, not a substrate change.

An ATP message maps onto the envelope cleanly:

- `from` / `to` onto `source` / `target`
- the nonce onto `idempotency_key`
- `in_reply_to` onto `correlation` / `cause`
- a DKIM-style signature onto the dormant AGDX `Signature`, once the key registry lands

Two ATP ideas are already in AGDX: the claim-check `BodyRef` (reference, size, digest) and the `bridge_hops` loop guard. AGDX's `AgentId` accepts the email-like `local@domain` form, so federated identity round-trips without a lossy hash.

ATP's trust-score-in-the-envelope admission model is deliberately _not_ adopted: every agent-written field stays a claim, and enforcement lives at the capability owner.

### LangChain agent streaming protocol

LangChain's agent streaming protocol (the `agent-protocol` streaming line, defined in CDDL with generated TypeScript and Python bindings) is a thread-centric agent-to-client streaming wire format over Server-Sent Events and WebSocket. It is an edge protocol at the **same layer as AG-UI**, not a substrate.

It carries a common event envelope across channels (`messages`, `tools`, `lifecycle`, `values` / `updates` / `checkpoints`, `input`, `custom:*`) and reconstructs reconnection with a server-side ring buffer plus per-event sequence numbers (`seq`, `since`, `lastEventId`). The durable log provides that replay natively and without a bounded buffer, which is why it is a **candidate edge bridge**, like AG-UI, not a substrate change.

The mapping onto AGDX:

- its channels onto AGDX operations
- `seq` / `since` / `lastEventId` onto log offsets
- its thread onto the conversation
- its checkpoint-fork onto the AGDX fork
- its lifecycle `cause` (`toolCall` / `send` / `edge`) onto `cause` / `causal_parent`

One of its ideas shipped here as a result of this comparison: the **human-in-the-loop interrupt/resume** verb. `Agdx::request_input(reply_topic, prompt, timeout)` pauses on a human (it publishes a prompt `command` under a fresh interrupt correlation, then awaits the human's correlated `response` and returns its body), and a responder resolves it with `AgentCtx::respond_input(reply_topic, decision)` or rejects with an AGDX `error` (which surfaces as `LaserError::Rejected`). It composes the existing `command` / `response` verbs, so it adds nothing to the wire. The interop demo shows it on `AgentTopic::HumanInput`.

```rust
// Pause for a human decision, resume with their answer.
let decision = laser
    .agdx(AgentTopic::HumanInput, "orchestrator".parse()?, conversation.into())
    .request_input(AgentTopic::Responses, b"approve a $500 credit?".to_vec(), Duration::from_secs(300))
    .await?;

// The approver agent's handler resolves the interrupt it is handling:
ctx.respond_input(AgentTopic::Responses, b"approved".to_vec()).await?;
```

Two ideas remain on the AGDX **roadmap** (planned, not shipped today): a finer-grained content-block lifecycle inside a single message (typed `text` / `reasoning` / `data` / `tool_call` blocks, each with explicit start / delta / finish), and an applied-through-offset acknowledgement carried on a reply so a client knows the exact log position a command took effect at.

## Real models

The bridges are model-agnostic - a worker behind them calls whatever LLM it wants. The `interop` example wires the bridges to a worker that uses the examples' `LlmClient` seam: a deterministic mock by default, a real backend with `--features llm-anthropic` / `--features llm-openai`. Nothing in the bridges changes between mock and real.

## Claim-check bodies (any bridge, any topic)

A body too large to ride the log inline externalizes to a [`BlobStore`] at publish and travels as the `BodyRef` capsule (content-type `ref`). The reader resolves and digest-verifies before it ever sees the bytes, so a store that returns the wrong content is a typed integrity error, never a silent wrong body. No default store ships: bring an S3-compatible bucket, the kv surface, or a filesystem in dev.

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
