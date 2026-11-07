# Edge interoperability: A2A, MCP, AG-UI

AGDX is the SDK message format for agents on the log. A2A, MCP, and AG-UI adapters expose external interfaces over these records. Fields shared with AGDX map to envelope fields. Other data stays in the body with `agdx.ct = json` and returns unchanged.

Select the optional `a2a-bridge`, `mcp-bridge`, and `agui` features as needed. Add `a2a-http` or `mcp-http` for the ready-made axum `router()`. They use log records through the Iggy transport. This guide describes their use with Rust. Python and TypeScript expose the bridge adapters. The [parity matrix](parity.md) lists their spellings and the Rust-only pieces, such as the axum `router()`. The [AGDX data exchange model](agdx.md) defines the underlying contract.

## Streams, topics, and RBAC

A bridge or agent uses the stream selected by its `Laser` handle. `laser.with_default_stream(name)` selects another default while sharing the connection. Each stream can contain several topics:

```rust
let ops = laser.with_default_stream("ops-agents");
let metrics = laser.with_default_stream("metrics-agents");

// An A2A gateway on the ops stream.
let gateway = A2aBridge::new(
    ops.clone(),
    "ops-gateway".parse::<AgentId>()?,
    AgentTopic::Sessions,
    AgentTopic::Sessions,
);

// An agent on the metrics stream, sharing the one connection.
// Keep the handle: dropping it signals a graceful shutdown.
let collector = Agent::builder()
    .id("collector".parse()?)
    .listen_on(AgentTopic::Sessions)
    .handler(handler)
    .build()
    .spawn(metrics.clone());
```

`AgentTopic` variants name the standard `agent.*` topics. Requests and replies share `agent.sessions` by default, and every record there carries its addressee in `agdx.to`. `AgentTopic::Custom(&id)` accepts another Iggy topic name. Deployments can use their own topic layout.

Credentials and the hosted HTTP endpoint control access:

- A `with_default_stream` view can access only streams permitted to its connection credentials.
- Use separate `Laser::connect` calls for separate principals. Apache Iggy enforces stream and topic permissions. Protect a bridge JSON-RPC `router` with the hosting application authentication middleware. `laser_sdk::edge_auth::authorize_edge` checks the audience and scope of a decoded bearer token.

Clients read streamed records by offset. After a disconnect, they can resume from a saved position and reconstruct the retained transcript. The [AGDX spec](agdx.md) defines the mapping. The `interop` example demonstrates the complete flow.

## A2A (`a2a-bridge`)

`A2aBridge` serves A2A v1.0 JSON-RPC and the v1.0 card at `/.well-known/agent-card.json`. The card uses `supportedInterfaces` for endpoint and protocol information instead of top-level `protocolVersion` and `url`. Its interface URL is the absolute URL clients reach the endpoint at, set with `with_public_url(url)`, and `card()` refuses with `HandlerConfig` until it is set. `with_description` and `with_version` fill the card's description and agent version. The card does not advertise streaming: chunk channels ride the log, not SSE. With `sign`, `A2aBridge::signed_card` adds a detached JWS over the canonical card. This uses RFC 8785, RFC 7515, and Ed25519. The payload is the RFC 8785 form of the card without `signatures`: members sorted by the UTF-16 code units of their names, no whitespace, and numbers limited to integers up to 2^53 - 1. The protected header is `{"alg":"EdDSA","kid":"<key id>"}`, where the key id is the lowercase hex of the first 8 SHA-256 bytes of the public key. Use `sign::verify_card` to check it. It refuses a header that names another algorithm, carries `crit`, or names a `kid` other than the verifying key's. Rust, Python, and TypeScript produce identical bytes for the shared vector in `sdk/tests/fixtures/card_signature.json`. The `router()` serves the signed card when the bridge has a signing key, and answers 404 on the card path until the public URL is set.

Tasks follow the v1.0 shapes. A task carries `id`, `contextId` and `status.state` with the `TASK_STATE_*` names, and an answer with a body adds one artifact `{ artifactId, parts: [{ text }] }` named by the answer's record id. `contextId` is the client's `message.contextId`, or the task's own conversation when the client sent none. The bridge echoes it and does not group tasks by it.

| A2A method | Mapping |
| --- | --- |
| `SendMessage` | Without `message.taskId`, publish a typed AGDX `command` on a fresh task conversation, tunneling the whole params JSON in the body. The task id is the conversation. The A2A task identity rides `correlation` (derived from the conversation, so lookup stays stateless). Answers `{ "task": ... }` in `TASK_STATE_SUBMITTED`. With `message.taskId`, publish the message into that task's conversation with the same correlation, addressed to the agent the task went to, and answer it in `TASK_STATE_WORKING`. A task that already ended answers `-32004`. |
| `SendStreamingMessage` | Not served, as the card does not advertise streaming. Answers `-32004`. The stream is consumed log-natively (`Laser::reassemble_channel`). |
| `GetTask` | Read the topics forward from the time the task id was minted, find the bridge's own command and the answering `response`/`error` envelopes (matched by `correlation`). The first answer with a terminal state settles the task, else the latest one in progress, so an `input-required` answer is superseded by the next. `TASK_STATE_WORKING` until an answer lands, and `TASK_STATE_UNSPECIFIED` when the bounded scan could not reach the end of the topic, so the caller should look again. An id the bridge published no task for answers `-32001`. |
| `CancelTask` | Publish an AGDX `error` terminal (`Cancelled`, `task_state = Canceled`) for a task still in progress. Answers `TASK_STATE_CANCELED`. A task that already ended answers `-32002`, an unknown one `-32001`. |

Refusals use the JSON-RPC and A2A error codes: `-32600` invalid request, `-32601` method not found (including `ListTasks`, which the stateless bridge does not serve), `-32602` invalid params, `-32603` internal error, `-32001` task not found, `-32002` task not cancelable, `-32004` unsupported operation. The programmatic `task` and `cancel` refuse an unknown task with `HandlerConfig` and a task that already ended with `Rejected`.

The lookup starts at the time in the task id less five minutes, so a server clock up to five minutes behind the bridge still finds the command, and it reads at most 32 passes of 10,000 records per partition.

```rust
use laser_sdk::prelude::full::*;
use std::sync::Arc;

let bridge = Arc::new(
    A2aBridge::new(
        laser.clone(),
        "a2a-gateway".parse::<AgentId>()?, // the bridge's agent id
        AgentTopic::Sessions,              // request topic
        AgentTopic::Sessions,              // reply topic
    )
    .with_public_url("https://agents.example.com/a2a")?
    .with_target("assistant".parse::<AgentId>()?),
);
// Mount the JSON-RPC endpoint + the Agent Card route on your HTTP server:
let app = bridge.router();
```

A worker behind the bridge consumes the decoded command envelope (`message.envelope`) and answers with an AGDX `response` echoing the correlation. The reply must be addressed to the bridge's agent id, which `ctx.respond` does.

`A2aBridge::submit_in(parent, root, params)` sends a task as a child session of `parent` in the tree rooted at `root`. It writes the child's submitted start on `agent.sessions` with that ancestry before the command, and the handling agent ends the child. Python takes `submit_in(parent, params, root=)`, and TypeScript names it `submitIn`.

A task the bridge submits is addressed to the agent set with `with_target(agent)`, the agent the bridge fronts, else to every agent (`agdx.to = *`), so on a shared `agent.sessions` topic every listening agent receives it. To send one task to another agent, use `A2aBridge::submit_to(target, params)` or `submit_in_to(target, parent, root, params)`. The command then carries `agdx.to = <target>`, and only that agent handles it. Python takes `submit(params, target=)`, `submit_in(parent, params, root=, target=)` and the constructor's `target=`. TypeScript takes `submit(params, { target })`, `submitIn(parent, root, params, { target })` and `withTarget(agent)`. The JSON-RPC `router()` and `handle_rpc` paths address the `with_target` agent, so set it, or run one worker per request topic behind them.

### Signing and verified answers

A consumer with a verifier dead-letters every unsigned record before its handler runs. `with_signing_key(key)` signs every record the bridge publishes, the task command and the cancel, so an agent that verifies its input accepts the bridge's tasks. Without it such an agent dead-letters every task this bridge submits. Python passes `signing_key=` to the constructor, TypeScript calls `withSigningKey(key)`.

When the bridge's `Laser` has a verifier, `GetTask` honors only answers that verify. An answer to a task addressed to an agent must be signed by that agent, so another enrolled key cannot settle it, and the bridge's own signed cancel is honored too. An unaddressed task is a broadcast: every listening agent may take it, so an answer from any enrolled signer is a valid answer by definition. Address the task, or set `with_target`, when the answer must come from one agent. With a signing key, `GetTask` also takes the task's command only when this bridge's key signed it.

### Binding tasks to callers

By default anyone who knows a task id can read or cancel the task. `A2aBridge::with_task_binding(key, ttl)` ties each task id to the caller that submitted it. The id then has three dot-separated parts: the conversation, an expiry in Unix microseconds, and a keyed BLAKE3 digest. The digest covers the caller, the stream creation time, the conversation and the expiry, under the 32-byte `key`. So an id works only for its own caller, only on the stream it was made on, and only until it expires. A recreated stream with the same name does not accept old ids.

Once binding is on, every task operation needs a caller. Use `bridge.caller(principal)` and call `submit`, `submit_to`, `submit_with_hops`, `submit_in`, `submit_in_to`, `task`, `cancel` or `handle_rpc` on it. The bridge methods without a caller refuse with `HandlerConfig` before they publish anything. Another caller's id, an expired id and a changed id are refused the same way as a malformed id, and over JSON-RPC they answer `-32001` task not found. So a caller cannot tell whether a task exists. The `router()` reads the caller from an `A2aCallerPrincipal` request extension. Your authentication layer must insert it, and a request without it is refused. Keep the key secret and the same on every bridge replica that serves the same stream. Rotating the key ends every open task id. A bound id proves the bridge made it, so `GetTask` answers it even after its command has left retention.

Python passes `task_binding_key=` (32 bytes) and `task_binding_ttl_ms=` to the `A2aBridge` constructor, and `caller=` to `submit`, `submit_in`, `task`, `cancel` and `handle_rpc`. TypeScript uses `withTaskBinding(key, ttlMs)` and `caller(principal)`, which returns an `A2aCaller` with the same methods. All three SDKs make the same digest, so a task id made by one works in the others.

```rust
let bridge = Arc::new(
    A2aBridge::new(laser.clone(), "a2a-gateway".parse::<AgentId>()?, AgentTopic::Sessions, AgentTopic::Sessions)
        .with_task_binding(task_key, std::time::Duration::from_secs(3600)),
);
let task = bridge.caller("user-42").submit(params).await?;
let current = bridge.caller("user-42").task(&task.id).await?;
```

## MCP (`mcp-bridge`)

`McpBridge` is an MCP JSON-RPC server (2025-11-25 schema) mapping tool calls onto AGDX commands and awaiting the correlated reply over the log.

| MCP method | Mapping |
| --- | --- |
| `initialize` | Answer the client's protocol version when the bridge serves it (`2025-11-25`, `2025-06-18`, `2025-03-26`), else the latest it serves, and advertise only the capabilities served. |
| `ping` | An empty result. |
| `notifications/*` | Never answered. Any message without an `id` is a notification: `handle_rpc` returns no response, and the `router()` answers `202 Accepted` with no body. |
| `tools/list` | The tools configured via `with_tool` and `with_memory_tools` (`name`, optional `title`/`description`, `inputSchema`). |
| `tools/call` | For an advertised tool, publish an AGDX `command` (tool name in `tool`, params tunneled in the body), await the correlated `response`/`error` within a timeout (30 seconds by default, `with_timeout` sets another), render the `tools/call` result (`content` + `isError`). A tool the bridge does not advertise answers `-32602` without publishing anything. |
| `resources/list` / `resources/read` | Resources configured via `with_resource`, served from configuration. An unknown URI answers `-32002`. |
| `prompts/list` / `prompts/get` | Prompts configured via `with_prompt`, rendered into MCP prompt messages. A prompt without a description renders no `description` field. An unknown prompt answers `-32602`. |

Refusals use the JSON-RPC codes: `-32600` invalid request, `-32601` method not found, `-32602` invalid params, `-32603` internal error, and `-32002` resource not found.

The `router()` follows the Streamable HTTP rules. It applies the [browser origin guard](#browser-origins) the A2A router shares, the DNS rebinding guard the transport requires. A request whose `MCP-Protocol-Version` header names a version the bridge does not serve is refused with `400`. Bind a local server to `127.0.0.1` and put your authentication in front of it.

```rust
use laser_sdk::prelude::full::*;
use std::sync::Arc;

let mcp = Arc::new(
    McpBridge::new(
        laser.clone(),
        "mcp-gateway".parse::<AgentId>()?,
        AgentTopic::Sessions,
        AgentTopic::Sessions,
        "my-server",
    )
    .with_tool(
        "ask",
        Some("ask the assistant".into()),
        serde_json::json!({ "type": "object" }),
    )?
    .with_target("assistant".parse::<AgentId>()?)
    .with_allowed_origins(vec!["https://app.example.com".to_owned()]),
);
let app = mcp.router();
```

`McpBridge::call_tool_in(parent, root, name, params)` runs one tool call as a child session of `parent`. It writes the child's submitted start before the command and ends the child by the result: completed on a tool result and failed on a tool error. Python takes `call_tool_in(parent, name, params, root=)`, and TypeScript names it `callToolIn`.

A tool call is addressed to the agent set with `with_target(agent)`, else to every agent, unless you name one. `McpBridge::call_tool_to(target, name, params)` and `call_tool_in_to(target, parent, root, name, params)` send it to that agent only. Python takes `call_tool(name, params, target=)`, `call_tool_in(parent, name, params, root=, target=)` and the constructor's `target=`. TypeScript takes `callTool(name, params, { target })`, `callToolIn(parent, root, name, params, { target })` and `withTarget(agent)`. The JSON-RPC paths address the `with_target` agent.

`with_signing_key(key)` signs every tool call, so an agent that verifies its input accepts it. When the bridge's `Laser` has a verifier, the reply to an addressed call must be signed by its target, so another enrolled key cannot answer for it. An unaddressed call is a broadcast that any listening agent may answer, so a reply from any enrolled signer is valid by definition. Python passes `signing_key=` to the constructor, TypeScript calls `withSigningKey(key)`.

### Browser origins

Both Rust routers guard their JSON-RPC endpoint the same way. A request without an `Origin` header comes from a non-browser client and is accepted. A browser request is accepted when its `Origin` is in `with_allowed_origins(origins)` (exact values such as `https://app.example.com`), or when it is same-origin with a `Host` that is an IP literal or `localhost`, such as a page the bridge serves itself on `http://127.0.0.1:8080`. Every other browser request is refused with `403`. A same-origin page on a named host needs the allowlist: in a DNS rebinding attack the browser sends the attacker's name as both `Origin` and `Host`, so a same-origin rule on names would let the rebound page through. Development servers such as Vite (`server.allowedHosts`) and webpack-dev-server (`allowedHosts`) draw the same line. The A2A card path stays open to any origin, as public discovery. TypeScript and Python ship no HTTP router, so the guard is Rust-only there. An application that serves `handle_rpc` from its own HTTP framework applies its own origin policy.

### The hop path across bridges

Each bridge stamps its own id at the end of `bridge_hops` on every record it publishes, and `with_bridge_hops(previous)` continues an existing path for the whole bridge. A single call can carry the path of the work it came from. Over JSON-RPC the request names it under `laserdata.com/bridge_hops` in `params._meta` (MCP) or `params.metadata` (A2A), so a bridge that forwards work to another bridge's endpoint passes the path on. Programmatically, `A2aBridge::submit_with_hops(previous, target, params)` and `McpBridge::call_tool_with_hops(previous, target, name, params)` take it. Python takes `previous_hops=` on `submit` and `call_tool`, TypeScript `submitWithHops(previousHops, params)` and `callToolWithHops(previousHops, name, params)`. A path that already holds the bridge is refused as a loop before anything is published: `InvalidError` programmatically, `-32602` over JSON-RPC. An agent handling bridged work passes the `bridge_hops` of the record it handles, so a cycle such as A2A to MCP to A2A is refused at its second A2A entry.

## AG-UI (`agui`)

AG-UI provides interfaces for frontends. The SDK supports state synchronization and event rendering through the log:

- Use `publish_state_snapshot` and `publish_state_delta` for full state and RFC 6902 patches. They write the session state document on the session lane, the same revision-guarded `state_delta` and `state_snapshot` records `session.state()` writes, so every reader folds one state model. `reconstruct_state` returns the session state document. It folds the retained lane first and uses the managed state view only when the lane no longer holds the document's baseline and the view is not behind the lane.
- Use `agui_events` to convert chat, reasoning, tool, task, state, and error records into AG-UI events. The mappings include `TEXT_MESSAGE_*`, `REASONING_MESSAGE_*`, `TOOL_CALL_START`, `ARGS`, `END`, `TOOL_CALL_RESULT`, `RUN_STARTED`, `RUN_FINISHED`, `STATE_*`, and `RUN_ERROR`. It reads every retained record of the conversation on the topic, never a recent window, so a long reply keeps its opening events. `TOOL_CALL_RESULT` carries the answering record's id as `messageId`.
- The rendered events form runs that an AG-UI client verifier accepts. A submitted task status opens a run (`threadId` is the conversation, `runId` the correlation). An event outside any run opens one with the record's correlation as `runId`, or its record id when it has none. A message or tool call whose opening chunk is no longer retained opens before its first content, and a run closes its open messages and tool calls before `RUN_FINISHED`. The rendering ends with every run finished, so a conversation still streaming renders as a finished run with the messages read so far.

```rust
laser
    .publish_state_snapshot(
        "ui".parse::<AgentId>()?,
        conversation,
        &serde_json::json!({ "count": 0 }),
    )
    .await?;

let events = laser.agui_events(conversation, AgentTopic::Sessions).await?;
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

`Agdx::request_input(reply_topic, prompt, timeout)` publishes an `input-required` task status and the prompt as an `input_request` command with a new correlation ID, then waits for a response. Only an agent that lists `input_request` among its operations takes the prompt as work. `AgentCtx::respond_input(reply_topic, response)` answers it. An AGDX `error` becomes `LaserError::Rejected`. The `interop` example demonstrates them on `AgentTopic::Sessions`.

The prompt is addressed to every agent, so on a shared session topic any approver that opted in can answer it. `Agdx::request_input_from(target, reply_topic, prompt, timeout)` addresses it to one agent and accepts only that agent's reply, signed by it when a verifier is configured. Python takes `request_input(reply_topic, prompt, timeout_ms=, target=)`, and TypeScript takes `requestInput(replyTopic, prompt, timeoutMs, { target })`.

```rust
// Pause for a human decision, resume with their answer.
let decision = laser
    .agdx(AgentTopic::Sessions, "orchestrator".parse::<AgentId>()?, conversation.into())
    .request_input_from("approver".parse::<AgentId>()?, AgentTopic::Sessions, b"approve draining node-7?".to_vec(), Duration::from_secs(300))
    .await?;

// The approver agent, spawned with
// `.operations(vec![OPERATION_INPUT_REQUEST.to_owned()])`, resolves the
// interrupt it is handling:
ctx.respond_input(AgentTopic::Sessions, b"approved".to_vec()).await?;
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

`A2aBridge::handle_rpc` and `McpBridge::handle_rpc` accept a JSON value and return a JSON-RPC response without requiring HTTP. `McpBridge::handle_rpc` returns no response for a notification. Python uses `await bridge.handle_rpc(request)`, and TypeScript uses `await bridge.handleRpc(input)`. Malformed requests return the error codes listed above with fixed public messages. Each bridge stamps its own ID in `bridge_hops`, and [the hop path across bridges](#the-hop-path-across-bridges) describes how a call continues it.
