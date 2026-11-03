# interop: one agent through A2A, MCP, AG-UI, and human input

This example reaches an agent through A2A, MCP, and AG-UI adapters and pauses on a human decision. The agent only reads AGDX commands from the log and answers with correlated AGDX responses.

## What it does

1. A2A. `A2aBridge.submit(..)` publishes an A2A message as an AGDX command. The `assistant` worker answers, and `A2aBridge.task(..)` reads the answer back as a completed task.
2. MCP. `McpBridge` lists its `ask` tool. `callTool(..)` sends the MCP `tools/call` params unchanged as an AGDX command, and the `tool-runner` worker's reply comes back as a tool result.
3. AG-UI. A chat answer is streamed onto the log as AGDX chunks, then `laser.aguiEvents(..)` renders the conversation as AG-UI events.
4. Human input. An orchestrator pauses with `requestInput(..)`, and an approver agent resolves it with `context.respondInput(..)`.

The assistant, the tool runner, and the approver all read `agent.sessions` for the whole run. Each call passes `{ target }` with the agent it is for, so only that agent takes it as work. Without a target, a bridge call or input request addresses every agent.

## Run it

Run `npm run setup` once, then run from `examples/typescript`:

```sh
npm run example:interop
```

The deterministic model is the default. Set one provider key to use a real model behind the same `LlmClient` interface:

```sh
ANTHROPIC_API_KEY=... npm run example:interop
OPENAI_API_KEY=... npm run example:interop
```

It runs on Apache Iggy without LaserData Cloud, and against LaserData Cloud with a bare target:

```sh
LASER_CONNECTION_STRING=user:pwd@your-laserdata-cloud-host \
  npm run example:interop
```

## Where to look (LaserData Cloud)

- Sessions: the A2A task, the MCP call, the chat stream, and the input request.
- Messages: commands and responses with one correlation across each protocol boundary.

## Highlights

- A2A task state comes from the durable reply log, not from bridge memory.
- MCP tool calls use the same request and response path as other agents.
- AG-UI events come from replayable AGDX chunks rather than a one-shot SSE connection.
- Human input uses the existing command and response records.
- One `AsyncResourceGroup` owns the three agents and stops them in reverse order when the run ends.
