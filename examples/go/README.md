# LaserData - Go examples

From the repository root, start a local Apache Iggy server with `just up`. Set `LASER_CONNECTION_STRING` to that server or your development deployment, then run an example from this directory. The examples module uses the Go client in the same checkout.

```sh
cd examples/go
export LASER_CONNECTION_STRING='iggy:iggy@127.0.0.1:8090'
go run ./log
go run ./native-streaming
go run ./workflow
```

Each example defaults to its own stream, `laser-<example>-go`, emptied at the start of a run and kept afterwards. Set `LASER_STREAM` to use a provisioned stream, which is never deleted. The memory correction works in a scratch stream it deletes, and the workflow runs on `LASER_STREAM` or a stream named `pipeline`. The managed examples check the capabilities of the connected deployment and say which one is missing.

## Primitives

| Example | What to look for |
| --- | --- |
| [Log](log/main.go) | Publish two JSON readings, then replay them as typed records from offset 0 with caller-owned offsets. |
| [Context](context/main.go) | Append two turns to a conversation and assemble them under a turn cap and a token budget. |
| [KV](kv/main.go) | Set and get keyed state, compare-and-swap on a version, take a lease and write through its fence, then promote a fork. Requires managed KV. Each later phase checks its own capability. |
| [Query](query/main.go) | Declare a view over a topic, publish three readings, wait for the projector, then query the degraded hosts. Requires managed queries. |
| [Watch](watch/main.go) | Declare a view, open its change feed at the end, publish a reading and print the change that advanced the view. Requires managed queries and the change feed. |
| [CDC](cdc/main.go) | Give a consumer group its own filter and receive 4 of 240 change records. Page the matches with the group reader, test and preview the filter, route alerts on a header, select inside CBOR, Avro and Protobuf payloads, and pause, release and delete policies. Requires the managed filter catalog. |
| [Graph](graph/main.go) | Link a host to two services, then read its outgoing neighbors from a locally rebuilt node id. Requires managed graph. |
| [Recall](recall/main.go) | Remember a fact in a conversation, recall the newest facts, reinforce the fact and forget it. |
| [Agent](agent/main.go) | Spawn a handler that advertises a capability, send it a deadline-bounded contract routed by that capability, and print the reply. |
| [Sessions](sessions/main.go) | Run a root incident with two child task sessions whose results the parent collects, record a model call over the assembled context, and link KV, graph and memory writes to the sessions. The session index and link counts need a managed deployment. |
| [Workflow](workflow/main.go) | Run three plain functions as journalled steps, one sleeping for a second, and print the outcome as JSON. Runs when copied alone. |

## Worked examples

| Example | What it runs |
| --- | --- |
| [Native streaming](native-streaming/main.go) | Send a keyed message with a uint16 header and batches through a direct producer, then read every record through one group with automatic commits and one that commits after handling. `LASER_MESSAGES` and `LASER_BATCH` set the volume. |
| [Event analytics](event-analytics/main.go) | Fold a clickstream live while it is published, resume an export from a saved checkpoint, then on a managed deployment run counts, top-N, time-range, per-minute windows and averages, and guard a second topic with a JSON Schema. |
| [Fleet tape](fleet-tape/main.go) | Build a live per-host view from a telemetry feed, publish the readings to a durable tape, audit the tape by typed replay, and on a managed deployment query per-host totals and publish an Avro tape under a registered writer schema. |
| [Firehose](firehose/main.go) | Publish batches to one topic per organization with bounded concurrency, and on a managed deployment index each topic and sample one. Configure with `LASER_FIREHOSE_MESSAGES`, `LASER_FIREHOSE_ORGS`, `LASER_FIREHOSE_CONCURRENCY`, `LASER_FIREHOSE_PAYLOAD_BYTES`, `LASER_FIREHOSE_BATCH`, `LASER_FIREHOSE_PARTITIONS`, `LASER_FIREHOSE_REGISTER` and `LASER_FIREHOSE_QUERY`. |
| [Governance](governance/main.go) | Define four roles and bind them to a dedicated demo user, then read the caller's grants, the roles and the binding. Check deny-wins delegation and the edge step-up challenge, and submit a budgeted session. The role calls need server-side authorization. Set `LASER_GOVERNANCE_USER_ID` to bind another user. |
| [Incident desk](incident-desk/main.go) | Materialize a seeded ticket feed, seed vector memory, and run triage, specialist, approver and resolver agents. Triage asks the specialist in subconversations, capacity grants sent twice produce exact totals, a plan is staged in a fork, and the incident is rebuilt from the log. Requires managed queries, KV compare-and-swap and forks. Set `LASER_MESSAGES`, `LASER_BATCH`, and `LASER_APPLY_PLAN=1` to promote the fork. |
| [Memory](memory/main.go) | Remember, recall, improve and forget on an in-process vector memory, which needs no server. With a connection, store durable facts in a memory topic and read them through a context scope. On a managed deployment, build a knowledge graph and traverse it by neighbors, predicate, time and path. |
| [Memory correction](memory-correction/main.go) | Store three facts for one user across two conversations, supersede the city, and recall the current facts as JSON. Works in a scratch stream it deletes. Runs when copied alone. |
| [Orchestra](orchestra/main.go) | Coordinate six long-running capability agents: a contract, a fan-out that skips an unavailable agent, a journalled workflow, quarantine and reinstatement, and recovery from a missed deadline. It waits for Enter at each phase. Set `LASER_NON_INTERACTIVE=1` to run straight through. |
| [Interop](interop/main.go) | Reach the same agents through an A2A task, an MCP tool call, an AG-UI event stream and a human input request. Set `ANTHROPIC_API_KEY` or `OPENAI_API_KEY` to answer with a real model. |
