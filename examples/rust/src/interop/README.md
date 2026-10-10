# interop: one agent through A2A, MCP, AG-UI, and human input

This example reaches an agent through A2A, MCP, and AG-UI adapters and pauses on a human decision. The agent only reads AGDX commands from the log and answers with correlated AGDX responses.

## What it does

- A2A. `A2aBridge::submit_to` publishes an A2A message as an AGDX command. The `assistant` worker answers, and `A2aBridge::task` reads the answer back as a completed task.
- MCP. `McpBridge` lists its `ask` tool. `call_tool_to` sends the MCP `tools/call` params unchanged as an AGDX command, and the `tool-runner` worker's reply comes back as a tool result.
- AG-UI. A chat answer is streamed onto the log as AGDX chunks, then `agui_events` renders the conversation as AG-UI `TEXT_MESSAGE_*` events.
- Human input. An orchestrator pauses with `Agdx::request_input_from`, and an approver agent resolves it with `AgentCtx::respond_input`.

The assistant, the tool runner, and the approver all read `agent.sessions` for the whole run. Each bridge call and the input request name the agent they are for, so only that agent takes them as work. The untargeted `submit`, `call_tool`, and `request_input` address every agent.

`LlmClient` uses a deterministic mock by default. `--features llm-anthropic` uses `ANTHROPIC_API_KEY`, and `--features llm-openai` uses `OPENAI_API_KEY`. The bridges work the same with either model.

## Run it

Run from `examples/rust`:

```sh
just up
cargo run --example interop                            # deterministic mock model
cargo run --example interop --features llm-anthropic   # Anthropic
cargo run --example interop --features llm-openai      # OpenAI
```

## Highlights

- `A2aBridge` and `McpBridge` publish an AGDX command and read the correlated reply back from the log.
- `agui_events` renders a conversation as AG-UI events by replaying offsets instead of holding an SSE connection.
- `request_input_from` and `respond_input` build a human pause from the existing command and response records.
- The edge protocols' shared fields map onto envelope fields, and everything else rides unchanged in the body. The AGDX specification defines the mapping.

The Python and TypeScript `interop` examples send the same inputs and print the same lines.
