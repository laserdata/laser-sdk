# TypeScript examples

Nine runnable examples mirror the non-benchmark Rust and Python scenarios. They
use the public package API and the same environment variables and deterministic
data conventions.

| Example | Layer | Focus |
| --- | --- | --- |
| `native-streaming` | generic | direct producer, batch, routing, commits |
| `event-analytics` | generic | typed clickstream, checkpoints, projection, schema |
| `order-book` | generic | ordered binary market tape and managed views |
| `firehose` | generic | sustained chunked publishing and bounded draining |
| `concierge` | agentic | routing, memory, approvals, managed coordination |
| `memory` | agentic | recall strategies, feedback, consolidation |
| `interop` | agentic | A2A, MCP, AG-UI, and hop guards |
| `orchestra` | agentic | contracts, scatter, workflow resume and compensation |
| `governance` | agentic | policy evidence, quorum, signing, RBAC gate |

## Run

Start Apache Iggy, then:

```sh
cd examples/typescript
npm ci
npm run build
node dist/src/native-streaming/main.js
```

Set `LASER_CONNECTION_STRING` and `LASER_STREAM` to select another deployment.
`LASER_MESSAGES` and `LASER_BATCH` control volume where a scenario supports
them. Managed phases run only when the connected server advertises the needed
capability. On stock Apache Iggy they print one skip reason and keep the open
path runnable.

Each scenario directory contains its exact behavior, run commands, managed
boundary, and expected output in a local README.
