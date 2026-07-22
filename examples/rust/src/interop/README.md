# interop - one agent reached as A2A, MCP, and AG-UI

Edge interoperability over the log. Layer: agentic. AGDX surfaces: the agent envelope (commands, responses, chunk streams) plus the edge bridges over it. The same LLM-backed agent is reachable three ways, each edge standard bridged onto AGDX on one Iggy connection, so an internal agent only ever speaks to the log while external clients keep their own contracts. No SSE: streams are consumed log-natively.

## What it does

- **A2A.** `A2aBridge` exposes the worker to A2A JSON-RPC clients. `message/send` publishes a typed AGDX command on a fresh task conversation, `tasks/get` reads back the worker's answer mapped onto the A2A task.
- **MCP.** `McpBridge` is an MCP tool server. `tools/list` advertises the configured tools, and `tools/call` reaches the same worker as an AGDX command and renders the correlated reply as a tool result.
- **AG-UI.** A chat answer is streamed onto the log as an AGDX chunk stream, then `agui_events` renders that conversation as AG-UI `TEXT_MESSAGE_*` events.
- **Human-in-the-loop.** An orchestrator pauses on a human with `Agdx::request_input`, and an approver agent resolves the interrupt with `AgentCtx::respond_input`, both riding AGDX command and response over the log (no new wire).

A single worker sits behind the bridges. It reads the decoded AGDX command, asks the model, and answers with an AGDX response echoing the correlation. The bridges produce AGDX. The worker only ever speaks AGDX. The model is the shared `LlmClient` seam: a deterministic mock by default, a real backend with `--features llm-anthropic` (`ANTHROPIC_API_KEY`) or `--features llm-openai` (`OPENAI_API_KEY`). Nothing in the bridges changes between mock and real.

## Run it

```sh
just up                                                # start a local server
cargo run --example interop                            # deterministic mock model
cargo run --example interop --features llm-anthropic   # real Claude
cargo run --example interop --features llm-openai      # real OpenAI
```

It runs in order: an A2A task answered by the worker through the bridge, an MCP tool call answered by the same worker, a chat answer streamed onto the log and rendered as AG-UI events, then a human-in-the-loop pause resolved by the approver agent.

## Highlights

- `A2aBridge` / `McpBridge`: the edge JSON-RPC surfaces, each a thin shell that publishes an AGDX command and replays the correlated reply off the log.
- `agui_events`: a conversation rendered as AG-UI events, with offset replay instead of SSE, so a dropped stream resumes.
- `Agdx::request_input` / `AgentCtx::respond_input`: human-in-the-loop pause and resume composed from the existing command and response verbs, adding nothing to the wire.
- Map the core, tunnel the remainder: the edge protocols' shared fields map onto envelope fields, everything else rides byte-identical in the body. The mapping is documented in `docs/interop.md` and pinned normatively in the AGDX spec.
