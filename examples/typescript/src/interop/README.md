# interop - one agent, three protocol views

One connection carries A2A task lifecycle, MCP catalog and tool calls, AG-UI
chat events, and a human approval interrupt over AGDX. Workers only consume and
produce AGDX records. The bridge classes translate protocol-shaped input at the
edge and preserve correlation on the durable log.

## Run it

```sh
npm run build
node dist/src/interop/main.js
```

The deterministic mock model is the default. The example runs on Apache Iggy
without managed services.

## Highlights

- A2A submit and task polling reach a correlated agent response.
- MCP exposes a tool, resource, and prompt before executing a tool call.
- AG-UI events are reconstructed from a chunked chat stream.
- The human input path pauses for a correlated approval response.
- Bridge hop metadata rejects a bridge that appears twice in one route.
