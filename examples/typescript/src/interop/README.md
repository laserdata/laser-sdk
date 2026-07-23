# interop - one agent through A2A, MCP, AG-UI, and human input

> Edge protocols remain thin adapters while workers communicate only through correlated AGDX records on Apache Iggy.

## What it does

1. Starts an assistant, a tool runner, and an approver as long-lived agents.
2. Exposes the assistant through `A2aBridge`, submits an A2A message, polls the task, and prints the completed artifact.
3. Builds an `McpBridge` with a tool, resource, and prompt, then calls the tool through a correlated agent request.
4. Writes a two-chunk AGDX chat stream and reconstructs its conversation as AG-UI events.
5. Publishes a human input request and waits for the approver agent to send the correlated response.

The bridge classes translate protocol-shaped input at the edge. The workers only decode AGDX commands and publish AGDX responses, so protocol adapters never leak into agent business logic.

## Run it

```sh
npm run example:interop
```

The deterministic model is the default. Set one provider key to use native `fetch` behind the same example-owned `LlmClient` interface.

```sh
ANTHROPIC_API_KEY=... npm run example:interop
OPENAI_API_KEY=... npm run example:interop
```

No managed service is required. The same example also runs against LaserData Cloud.

```sh
LASER_CONNECTION_STRING=user:pwd@your-laserdata-cloud-host \
  npm run example:interop
```

## Where to look (LaserData Cloud)

- **Conversations**: the A2A task, MCP call, chunked chat, and approval request.
- **Agent registry**: assistant, tool runner, and approver presence while the example runs.
- **Messages**: commands and responses with stable correlations across each protocol boundary.

## Highlights

- A2A task state is reconstructed from the durable reply log instead of held in bridge memory.
- MCP tool execution reuses the same request and response path as other agents.
- AG-UI events come from replayable AGDX chunks rather than a one-shot SSE connection.
- Human input uses the existing command and response vocabulary with no TypeScript-only wire shape.
- All agent handles implement `AsyncDisposable` and shut down in reverse startup order.
