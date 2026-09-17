# LaserData - Laser SDK

This package provides the Python Laser SDK for Apache Iggy. [LaserData, Inc.](https://laserdata.com) maintains it. PyO3 exposes the Rust SDK to Python, so both clients use the same data contract, codecs, and runtime.

Rust and Python share the data contract and Rust implementation. The bindings expose Python forms of the SDK operations, configuration, and errors. Shared examples and behavior scenarios cover language-neutral behavior.

> The current release is `0.4.0`. The wire contract and public API use semantic versioning. Before `1.0.0`, minor releases can contain breaking changes.

`spawn_agent(agent_id, ..., consumer_group=None)` separates agent identity from its consumer group. The default group uses the agent ID spelling. Set `consumer_group` when the deployment needs a different group.

One Apache Iggy connection supports streaming and managed operations for queries, key-value state, graphs, and forks. The optional AGDX agent runtime sends messages and runs asynchronous handlers. It supports at-least-once delivery, per-conversation ordering, duplicate suppression, retries, and dead letters.

Apache Iggy is the underlying streaming core. Projections, the query layer, the key-value store, the knowledge graph, and forks are served by Laser Stack or LaserData Cloud over that same connection. Against Apache Iggy without a managed backend those calls raise `UnsupportedError`.

## Install

```bash
pip install laser-sdk
```

Wheels ship for Linux (x86_64, aarch64) and macOS (Intel, Apple Silicon), Python 3.10 through 3.13.

Every wheel and source build uses Apache Iggy's native VSR transport. Standard streaming commands, LaserData's non-replicated managed command band, and dedicated replicated authorization operations use the same connection.

## Connect

```python
import asyncio
from laser_sdk import Laser


async def main():
    laser = await Laser.connect("iggy:iggy@127.0.0.1:8090")
    orders = laser.stream("commerce").topic("orders")
    await orders.ensure(partitions=4)
    caps = await laser.capabilities()
    print(caps)


asyncio.run(main())
```

Use a `user:password@host:port` connection string. The SDK supplies the Apache Iggy TCP scheme. Select a stream with `laser.stream(name)` and a topic with `.topic(name)`. The optional `stream=` selects a default for the shorter `laser.topic(name)` form. It does not restrict access to other streams. Accessors select objects, and operations such as `publish`, `replay`, and `ensure` perform I/O.

Python uses the Rust client reconnect policy. TCP connections retry initial connections and reconnect dropped sockets. The default is unlimited retries at one-second intervals. Set `reconnection_retries=<count|unlimited>` and `reconnection_interval=<duration>` in the connection string. After reconnecting, the client reapplies those credentials.

`Laser.connect` calls Rust `Laser::connect`. Hosts under `*.laserdata.cloud` and `*.laserdata.com` use TLS with the bundled LaserData root CA. `LASER_TLS_CERT=<path>` selects an explicit certificate. `LASER_NO_TLS=1` disables automatic TLS. Other hosts retain their connection-string configuration.

LaserData Cloud and Laser Stack enable managed surfaces only when their backend announcement reports ready. `await laser.refresh_capabilities()` re-probes a long-lived connection after startup or a backend restart. The returned `Capabilities` includes `versions: OpVersions | None` and advertised backends. Apache Iggy keeps every managed surface off and reports no operation versions.

## Publish and consume

```python
orders = laser.stream("commerce").topic("orders")
await orders.ensure(partitions=4)

committed = await (
    orders.publish()
    .index("customer_id", "alice")
    .index("total", "129")
    .inline_payload()
    .json({"id": "o-1", "customer": "alice", "amount": 129})
    .send()
)
print(committed.confirmations)
```

Producers and publish builders return `SendMessagesResponse`. Each `SendMessagesConfirmation` identifies a committed batch by stream, topic, partition, and first offset. The list can be empty when the server does not report offsets. Completion follows the topic durability policy.

## Batch and any payload

`publish_batch` groups records for sending. A `topic(..).replay()` cursor reads retained records and saves the next offset for each partition. Each poll reads at most 10,000 messages per partition. Later polls resume from the saved offsets. Failed or canceled polls leave those offsets unchanged.

The payload contains bytes in the application-selected format. `add_json`, `add_msgpack`, and `extend_json` provide encoding helpers. `add_payload` sends raw `bytes` without inspecting their format. Compressed data and application-defined formats use the same path. The following sections cover Avro and Protobuf.

```python
batch = orders.publish_batch().inline_payload()
batch.extend_json([{"id": "o-1", "amount": 129}, {"id": "o-2", "amount": 80}])
batch.add_payload(b"\x00any-bytes-any-format")  # raw bytes, untouched by the SDK
committed = await batch.send()  # the whole batch, one round-trip
```

## Live producer and consumer

Use `Topic.producer`, `Topic.consumer`, and `Topic.consumer_group` for continuous streaming through Apache Iggy. Producers support batches, delays, retries, resource creation, expiry, size, replication factor, and routing. Consumers support first, last, next, offset, and timestamp reads. They also support groups, retries, automatic commits, explicit offset storage, and offset inspection.

```python
topic = laser.stream("commerce").topic("events")
producer = topic.producer(
    batch_length=1000,
    linger_ms=5,
    retries=3,
    partitions=4,
)
await producer.init()
committed = await producer.send(b"one", headers={"type": ("uint16", 7)}, key=b"account-42")
batch_committed = await producer.send_batch(
    [(b"two", {"type": 8}), b"three"],
    key=b"account-42",
)

consumer = topic.consumer_group(
    "workers",
    batch_length=1000,
    poll_interval_ms=5,
    auto_commit="disabled",
)
await consumer.init()
try:
    message = await consumer.next()
    if message is not None:
        await handle(message.payload, message.headers)
        await consumer.commit(message)
finally:
    await consumer.shutdown()
```

Header values accept ordinary Python scalar values. For an exact Apache Iggy numeric type, pass `(kind, value)`. `ConsumerMessage.header_kinds` reports the received types. Use `auto_commit="each"` with `commit_interval_ms=1000` for interval-or-each storage. Other modes are `"polling"`, `"all"`, `"every"` with `commit_every=`, `"interval"`, and `"disabled"`.

With automatic commits disabled, call `commit(message)` after successful handling. `shutdown()` does not advance the offset past that commit. `Consumer` waits for new records. A `replay()` cursor reads retained records in bounded polls and stops its iterator when caught up.

## Typed topics

Pass `cls=` to bind a topic to a dataclass or pydantic model. `publish(order)` encodes an instance as JSON. `records(reader_name)` reads typed records with the same client-owned offsets as `replay()`. `next()` returns a decoded record or `None` when caught up. If decoding fails, it raises `TypedDecodeError` with the log position. The next read continues past that record.

```python
from dataclasses import dataclass


@dataclass
class Order:
    customer: str
    amount: int


orders = laser.stream("commerce").topic("orders", cls=Order)
await orders.publish(Order(customer="alice", amount=129)).send()

records = orders.records("billing")
while (record := await records.next()) is not None:
    order: Order = record.value  # an Order instance, record.position names the log slot
```

## Schema-first bodies (Avro / Protobuf)

Compile the registered writer schema before publishing records that use it. The client encodes each value and rejects values that do not match. The managed plane resolves the schema ID to extract indexed columns.

```python
from laser_sdk import CompiledSchema

source = {"kind": "avro", "schema": fill_avro_schema}
schema_id = await laser.register_schema(source, name="fill")
compiled = CompiledSchema.compile(source, id=schema_id)

batch = laser.stream("markets").topic("trades_avro").publish_batch().inline_payload()
for fill in fills:
    batch = batch.add_avro(compiled, schema_id, fill)
await batch.send()
```

`CompiledSchema` provides `validate`, `validate_value`, and `decode`. The publish builder provides `.avro(compiled, schema_id, value)`. For an encoded Protobuf body, use `.raw_bytes(bytes, "protobuf")` or batch `.add_raw_bytes(..)`. Schema registration requires the `laser-plane` registry in Laser Stack or LaserData Cloud.

## Query (managed)

```python
result = await (
    laser.query("orders")
    .where_eq("customer_id", "alice")
    .filter_gte("total", 100)
    .order_desc("total")
    .limit(10)
    .fetch()
)
for row in result.rows:
    print(result.value_text(row, "customer_id"), result.value(row, "total"))
```

`result.fields` defines the ordered result schema. Each row contains tagged values in that order. Use `value()` to retain the value type or `value_text()` for stable display text. Values preserve integer widths, decimal precision, timestamps, UUIDs, bytes, structs, lists, maps, and nullability.

Filters and parameters accept `bool`, `int`, `float`, `str`, `bytes`, `uuid.UUID`, `decimal.Decimal`, date and time types, `None`, and lists. `datetime.date` and `datetime.time` retain their types. `datetime.datetime` with a timezone becomes a UTC instant, and a naive value retains no timezone. Non-finite numbers, out-of-range integers, and a `time` with `tzinfo` raise `InvalidError`.

`fetch()` returns one bounded page. `has_more` is true exactly when `next_cursor` is present. `fetch_all()` follows the server-provided cursor. Request an exact match count only when needed. It requires a separate count over the full filter:

```python
result = await laser.query("orders").where_eq("customer_id", "alice").with_total().fetch()
print(result.total, result.has_more)
```

Each query keeps one execution identity and an absolute deadline. Use the same builder to inspect or cancel a running query:

```python
request = laser.query("orders").filter_gte("total", 100).deadline_micros(deadline_micros)
result = await request.fetch()
status = await request.status()
if status["state"] == "running":
    await request.cancel()
```

Lakehouse queries always name one destination generation and can select a retained snapshot explicitly:

```python
result = await (
    laser.query_lakehouse(destination_id, destination_generation)
    .at_snapshot(snapshot_id)
    .filter_eq("customer_id", "alice")
    .limit(100)
    .fetch()
)
print(result.context)
```

The returned context proves the resolved engine and target. Lakehouse pages also carry destination, table, snapshot, schema, partition-spec, materialization-boundary, checkpoint, and global-state evidence.

## Destinations and Arrow IPC

Destination reads and query routes are available from one managed accessor. Reads choose potentially stale or linearizable checkpoint state explicitly:

```python
destinations = laser.destinations()
page = await destinations.list(limit=50, consistency="linearizable")
routes = await destinations.query_routes(
    consistency="potentially_stale", name_contains="orders", limit=50
)
current = await destinations.get(destination_id, consistency="linearizable")
```

Registration and desired-state changes are revision-guarded. `register()` takes one complete destination declaration and `set_desired_state()` compares both the global state revision and destination definition revision before applying a change.

For analytical batches, publish one complete self-contained Arrow IPC stream per message. The SDK validates the contract version, schema fingerprint, counts, bounds, and exact byte length before transport I/O:

```python
metadata = {
    "contract_version": 1,
    "schema_fingerprint": schema_fingerprint,
    "encoded_bytes": len(arrow_stream),
    "field_count": 8,
    "record_batch_count": 1,
    "row_count": 10_000,
    "dictionary_count": 0,
}
await orders.publish().arrow_ipc(arrow_stream, metadata).send()
```

Arrow input must use stream format, be self-contained, use microsecond timestamps, avoid dictionary replacement and deltas, keep decimals within 128 bits, and contain no unions or extension types.

Query, the key-value store, the knowledge graph, and forks are managed features served by LaserData Cloud or Laser Stack. Against Apache Iggy without a managed backend they raise `UnsupportedError`.

## Key-value

```python
kv = laser.kv("sessions")
await kv.set("user:42").json({"state": "online"}).ttl(300).send()
state = await kv.get_typed("user:42")
entry = await kv.get_entry("user:42")
print(entry.source)  # origin stream/topic ids, partition, and offset when stamped
values = await kv.get_many(["user:42", "user:43"])  # one round trip (the mixed-operation batch)
await kv.copy_to("user:42", "user:42:2026", to_namespace="archive")  # one backend transaction
await kv.move_to("plan:draft", "plan:current")  # copy plus source delete
lease = await kv.lease("source-owner", "worker-1", 30)
state = await kv.get_entry_at_least("source-state", lease.position)
lease = await kv.renew_lease("source-owner", "worker-1", lease.token, 30)
await kv.release("source-owner", "worker-1", lease.token)
await kv.delete("user:42")
```

`Lease` exposes `token`, `granted_ttl_secs`, and `MutationPosition`. After takeover, pass that position to `get_entry_at_least` to exclude state older than the grant. Request a lifetime from 1 second to 5 minutes. The store can grant less time, but never more. Values outside the range fail before sending.

Renew a lease before its granted lifetime expires. Reacquisition does not extend a live lease. Acquisition uses a dedicated coordination connection. If the outcome is unknown, the SDK retires the connection and waits through the requested lifetime. It then raises an ambiguous-mutation error that requires operation-specific recovery.

## Knowledge graph

```python
from laser_sdk import graph_edge, graph_node

checkout = graph_node("Service", "checkout")
pool = graph_node("Component", "db-pool")

graph = laser.graph("ops")
await graph.upsert([checkout, pool], [graph_edge(checkout, "depends_on", pool)])
await graph.link("service:checkout", "mitigated_by", "component:read-replica")

around = await graph.neighbors(checkout["id"], direction="out", depth=2)
deps = await graph.query(match_label="Service", hops=[("depends_on", "out")])
print(around["nodes"], deps["nodes"])
```

`graph_node` derives an ID from label and value. Repeating the same entity preserves its ID. `link(from, relation, to)` connects two `kind:value` entities. `relink` closes active edges of the same single-valued relation before recording the new value. `unlink` closes an edge valid-time window and retains its nodes.

`query` starts from `start_ids` or vector `nearest` results. `returns` selects `"nodes"`, `"edges"`, `"triplets"`, or `"paths"`. `as_of` uses epoch microseconds to select edges valid at that instant.

## Agents

```python
from laser_sdk import Laser


async def handle(ctx, message):
    text = message.payload.decode()
    await ctx.respond(f"echo: {text}".encode())


laser = await Laser.connect("iggy:iggy@127.0.0.1:8090", stream="agents")
await laser.bootstrap(partitions=4)

handle_agent = laser.spawn_agent("echo", "agent.commands", handle, respond_on="agent.responses")
await handle_agent.ready()

from laser_sdk import Provenance

reply = await laser.request(
    "agent.commands",
    "agent.responses",
    b"hello",
    Provenance(agent="caller"),
    timeout_secs=10,
)
print(reply.payload.decode())

await handle_agent.shutdown()
```

### Signed, principal-bound contracts

Rust and Python use the same Ed25519 verifier and routing rules. Enroll trusted keys before connecting. Give each signing agent its key. For sensitive routes, require an authenticated principal. One connection can advertise one agent. A second identity raises a conflict without replacing the first.

```python
from laser_sdk import KeyRegistry, Laser, SigningKey

risk_key = SigningKey(bytes(range(32)))
keys = KeyRegistry()
keys.enroll("42", risk_key.verifying_key)

laser = await Laser.connect(connection, stream="agents", verifier=keys)
risk = laser.spawn_agent(
    "risk",
    "risk.commands",
    handle,
    capabilities=["screen-order"],
    signing_key=risk_key,
    verifier=keys,
)
await risk.ready()

result = await laser.contract_report(
    "screen-order",
    b'{"order":"o-1"}',
    source="orders",
    principal=42,
)
assert result["state"] == "completed"
assert result["verified_principal"] == "42"
```

`contract` and `scatter` remain body-only conveniences. Use `contract_report` or `scatter_report` when policy or UI code must inspect `verified_principal`. With a verifier configured, unsigned, invalid, and wrong-principal replies are ignored rather than returned with an empty identity.

For a human-in-the-loop pause, the typed AGDX producer's `request_input` publishes a prompt and blocks on the human's correlated reply, which a handler resolves with `AgentCtx.respond_input`:

```python
decision = await laser.agdx("agent.human_input", "orchestrator", conversation_id).request_input(
    "agent.responses", b"approve a $500 refund?", timeout_secs=15
)
```

The same identity rules as contracts apply. A caller connected with `verifier=` accepts only signed responses, so an approver spawned with `signing_key=` resumes it and nothing else can. A producer signs its own sends the same way: `laser.agdx(..., signing_key=key)` signs every envelope it publishes (`command`/`respond`/`emit`/`status`/`fail`), the Python spelling of the per-send `.signed_by` builder in Rust and TypeScript.

### Fan-out and human approval from a handler

`AgentCtx` (the `ctx` a handler receives) carries two more coordination verbs beyond `respond`/`send`/`request`, mirroring the Rust `AgentCtx`:

```python
async def orchestrate(ctx, message):
    # Fan a task out to every agent advertising "diagnose", gathering
    # replies on this handler's own respond_on topic.
    gather = await ctx.fan_out("diagnose", b"scan", deadline_ms=10_000)
    for entry in gather["ok"]:
        print(entry["agent"], entry["body"])
    for entry in gather["failures"]:
        print(entry["agent"], entry["error"])


async def gatekeeper(ctx, message):
    # Pause on a human decision before continuing, the ctx-scoped sibling
    # of the top-level request_input above.
    decision = await ctx.approval_gate(
        "agent.responses", b"approve a $500 refund?", timeout_secs=15
    )
    await ctx.respond(decision)
```

`fan_out`'s `policy` is `"require_all"` (default, wait for every branch), `"quorum"` (pass `quorum=<n>` to stop once that many succeed), or `"best_effort"` (take whatever landed by `deadline_ms`). Unavailable and quarantined agents are excluded, and a target that resolves no inbox is a `failures` entry, never silently rerouted. `fixed_inbox` routes every branch to a fixed topic instead of each agent's advertised inbox, the same knob `contract`/`scatter` take. Presence is connection-scoped (one connection may advertise one agent), so capability-advertising workers under test each need their own connection.

### Unit-testing a handler

`agent_message` and `agent_ctx` build a message and a ctx directly, with no live consumer or server involved, so a handler function is testable like any other callable:

```python
from laser_sdk import agent_ctx, agent_message, Provenance

message = agent_message(b"hello", Provenance(agent="tester"))
ctx = agent_ctx(laser, message, agent="tester", respond_on="agent.responses")
await handle(ctx, message)  # call your handler function directly
```

`laser` only needs to be live for whatever ctx helpers the handler actually calls (`respond`/`fan_out`/...). A handler that only reads its message needs no server at all.

A policy decides before an SDK effect. It can allow, observe, block, require approval, modify, or defer the action. Enforce mode applies the decision, while observe mode records it. Decisions that are not allow produce linked evidence on the audit topic. Typed refusals include `PolicyBlockedError`, `StepUpRequiredError`, and `PolicyDeferredError`:

```python
from laser_sdk import ActionDecision, PolicyBlockedError


class NoWires:
    async def decide(self, action):
        if action.payload.startswith(b"wire-funds"):
            return ActionDecision.block("wires need approval").with_policy(
                "finance", "3", ["no-wires"]
            )
        return ActionDecision.allow()


governed = laser.with_governor(NoWires(), mode="enforce")
try:
    await governed.send_agent("agent.commands", b"wire-funds", provenance)
except PolicyBlockedError as refused:
    print(refused)  # policy blocked: no wire transfers

# Per-agent: everything the handler publishes is governed too.
handle_agent = laser.spawn_agent("clerk", "agent.commands", handle, governor=NoWires())
```

`QuorumGovernor` combines named voters through `all`, `any`, or `at_least(n)`. A voter can implement deterministic rules or call a model. Every `mandatory` voter must return `allow`, `observe`, or `modify`. A mandatory denial or error blocks the operation under every policy:

```python
from laser_sdk import QuorumGovernor, QuorumPolicy

quorum = QuorumGovernor(QuorumPolicy.at_least(2))
quorum.voter("safety", NoWires(), mandatory=True)
quorum.voter("llm_reviewer", llm_voter, mandatory=False)

governed = laser.with_governor(quorum, mode="enforce")
```

`SwappableGovernor` replaces the active policy without reconnecting or dropping existing handles. An operator, configuration reload, or recorded update can trigger the replacement. It affects the next decision and leaves recorded decisions unchanged:

```python
from laser_sdk import SwappableGovernor

swappable = SwappableGovernor(NoWires())
governed = laser.with_governor(swappable, mode="enforce")
...
previous = swappable.swap(a_stricter_policy)  # returns the replaced policy
```

Durable approvals are native typed records. They publish and replay directly, while the SDK keeps log ownership explicit:

```python
import time
from laser_sdk import Decision, Intent, IntentPolicy, Vote, decide

intent = Intent(
    conversation=conversation_id,
    proposer="planner",
    body=b"reserve inventory",
    eligible_voters=["safety"],
    policy=IntentPolicy.all(),
    policy_version=7,
    deadline_micros=time.time_ns() // 1_000 + 30_000_000,
)
await laser.stream("agents").topic("intents", cls=Intent).publish(intent).send()
vote = Vote.cast(intent, "safety", "allow")
decision = decide(intent, [vote], time.time_ns() // 1_000)
if decision and decision.authorizes(intent):
    await laser.stream("agents").topic("decisions", cls=Decision).publish(decision).send()
```

Construction, casting, and folding fail with `InvalidError` on malformed state. Mandatory voters must affirm, and ballots outside the intent's time window never count. A voter name remains a record claim unless signing or topic ACLs bind it to an authenticated principal.

`SwarmActivity` builds a read model from governance evidence. Read `PolicyEvidence` records from the audit topic, then apply them to inspect each agent activity:

```python
from laser_sdk import PolicyEvidence, SwarmActivity, Topics

swarm = SwarmActivity()
for message in await laser.assemble_context(conversation_id, topics=[Topics.AUDIT]):
    envelope = message.envelope
    if envelope and envelope.get("operation") == "policy_decision":
        swarm.observe(PolicyEvidence.decode(bytes(message.agdx_body)))

activity = swarm.agent("planner")
if activity:
    print(activity.decisions, activity.count("block"))
```

`CrashContext` combines a journal tail, an optional dead-letter record, and the latest available decision for the conversation. Its summary is deterministic. It does not call a model:

```python
from laser_sdk import CrashContext

journal = await laser.assemble_context(conversation_id, topics=[Topics.COMMANDS])
context = CrashContext(journal=journal, dead_letter=None, last_decision=activity.last_decision)
print(context.summarize())
```

## Runs (managed)

The managed run registry answers "what happened to that task" without folding topics yourself. Gated on the `agent_workflow` capability, `UnsupportedError` elsewhere.

```python
runs = laser.runs()
await runs.register_source("agents", "agent.status")
run = await runs.submit("diagnoser", b'{"incident": "INC-7"}')
info = await runs.status(run.run_id)
page = await runs.list(state="running", limit=25)
await runs.cancel(run.run_id)  # records the intent, the engine observes it
await runs.remove_source("agents", "agent.status")

wf = laser.workflow("incident-response")
wf.registered()  # the run's lifecycle lands in the registry

# A fenced external effect must use the same namespace in the workflow lease
# and in the handler's kv("payments").cas_fenced(...) commit.
wf.step(
    "charge",
    to="charger",
    build=lambda outputs: b'{"order":"o-1"}',
    fence_namespace="payments",
    on_timeout="reassign",
)
```

## Change feed (managed)

Await a view's advance instead of polling it blind. A projection binding built with notify makes the plane publish one change record per committed batch, and `laser.watch()` reads that feed. Gated on the `watch` capability, `UnsupportedError` elsewhere.

```python
feed = laser.watch(index="orders_v1")
for change in await feed.poll():
    print(change.index, change.from_offset, change.to_offset, change.rows)
    rows = await laser.query(
        "orders_v1"
    ).fetch_all()  # the record is a wakeup, the rows come from query
saved = feed.offsets  # persist to resume after a restart
```

## Consume and replay

```python
# A resumable reader over a topic. Each poll drains what is new. Persist the
# offsets to resume after a restart.
cursor = laser.stream("commerce").topic("orders").replay()
for message in await cursor.poll():
    print(message.json())
saved = cursor.offsets

# Replay a conversation's history off the log (agent runtime). `token_budget`
# trims the selection to an estimated token count, applied after `last_n`.
history = await laser.assemble_context(conversation_id, last_n=50, token_budget=4_000)

# An agent session: typed turns over the conversation-level topics, a
# model-ready context, scoped memory, and checkpointed replay.
session = laser.sessions().create("agent-42")
await session.append("instruction", b"summarize the ticket")
await session.append("model.response", b"it is a login bug")
for turn in await session.context():
    print(turn.kind, turn.text())
checkpoint = await session.checkpoint()
saved = checkpoint.to_json()
later = await session.turns_since(ls.Checkpoint.from_json(saved))
```

`laser.sessions(stream=..., topics={"instruction": "support.turns"}, memory_namespace=..., context_turns=..., context_tokens=...)` configures the stream, topics, memory namespace, and context limits for sessions. Every turn kind needs a distinct topic.

## Memory and state

Agent memory provides `remember`, `recall`, and `forget` over a log-based backend or a local vector backend. The log-based handle also supports named state through `set(key, value)`, `fetch(key)`, `update(key, patch)`, and `remove(key)`. These named operations return `UnsupportedError` on the vector backend.

Log-based writes publish to the memory topic and work on Apache Iggy. Default `recall` and `fetch` reads use a managed key-value view. Use `recall(folded=True)` or `fetch_folded` to build the view locally from the topic. The local vector backend ranks records by similarity. Its embedder can return `list[float]` or an awaitable for an external model call.

```python
async def embed(text: str) -> list[float]: ...  # your model, or a deterministic stand-in


memory = laser.vector_memory(embed)
await memory.remember("checkout latency traces to the database pool", conversation=cid)
hits = await memory.recall(conversation=cid, semantic="why is checkout slow", limit=3)
print([item.text for item in hits])

# A vector memory created from a governed Laser applies the same pre-write policy.

# A durable key/value seam for agent state, the same vocabulary as the managed store.
from laser_sdk import InMemoryStore  # or FileStore("/var/lib/agent")

store = InMemoryStore()
await store.set("cursor", saved_bytes)
value = await store.get("cursor")
```

`vector_memory` inherits the governor enrolled on the `Laser` that creates it. A blocked write never mutates the local index, and a modified decision replaces the proposed memory body before embedding. Rust and Python therefore apply the same effect-boundary policy to local semantic memory.

## Edge interop (A2A / MCP / AG-UI)

Reach an agent as an A2A task source or an MCP tool server, and render a conversation as AG-UI events, all over the durable log:

```python
# A2A: submit a task, poll for the result.
a2a = laser.a2a_bridge("a2a-gateway", "agent.commands", "agent.responses")
task = await a2a.submit({"message": {"role": "user", "parts": [{"kind": "text", "text": "hi"}]}})
status = await a2a.task(task["id"])

# MCP: advertise tools, route tools/call to the agent.
mcp = laser.mcp_bridge(
    "mcp-gateway",
    "agent.tool_calls",
    "agent.tool_results",
    "laser-mcp",
    tools=[{"name": "ask", "input_schema": {"type": "object"}}],
)
tools = mcp.list_tools()
result = await mcp.call_tool("ask", {"q": "what is AGDX?"})


# An agent answers a bridge request from its handler:
async def handle(ctx, message):
    await ctx.respond_input("agent.responses", b"the answer")


# AG-UI: snapshot + deltas reconstruct shared state off the log.
await laser.publish_state_snapshot("agent.llm_io", "ui", conversation_id, {"count": 1})
state = await laser.reconstruct_state(conversation_id, "agent.llm_io")
events = await laser.agui_events(conversation_id, "agent.llm_io")
```

Host the actual HTTP endpoint with your Python web framework over these adapter methods.

## Errors

Every failure raises a subclass of `LaserError`: `QueryError`, `KvError`, `ForkError`, `GraphError`, `AuthzError`, `SignatureError`, `UnsupportedError`, `InvalidError`, `CodecError`, `TypedDecodeError`, `ProtocolError`, `TimeoutError`, `ConfigError`, `TransportError`, `BudgetExceededError`, `PolicyBlockedError`, `StepUpRequiredError`, `PolicyDeferredError`, `CancelledError`. Each instance carries `code`, `retryable`, `unsupported`, `not_found`, `version_skew`, `version_conflict`, `stale`, `permission_denied`, `stream_or_topic_not_found`, `no_capable_agent`, `lease_lost`, `fence_violation`, `budget_exceeded`, `quarantined`, and `not_leader` attributes so you can branch without matching on the type. `TimeoutError` also subclasses the builtin `TimeoutError` and `CancelledError` also subclasses `asyncio.CancelledError`, so stdlib-style `except TimeoutError` / `except asyncio.CancelledError` catch them too.

## Reading

Use `async for message in laser.stream("commerce").topic("events").replay()` to read raw records. `WatchReader` and `topic.records(reader_name)` also support asynchronous iteration. They stop when caught up. A later iteration resumes from the same offsets. Use `poll()` for a batch of records.

## Lifecycle

Use `async with await Laser.connect(conn) as laser:` to manage a connection. The shared connection closes when its last handle is dropped. `with_stream` and `with_ops_stream` return handles that share that connection.

## License

Apache-2.0. Copyright LaserData, Inc.

Apache and Apache Iggy are trademarks of the Apache Software Foundation. Use of these marks does not imply endorsement by the Apache Software Foundation.

## Publish recovery

Publish attempts default to 60 seconds with three retries. Retry delays start at 250 milliseconds, double after each failure, and stop increasing at 30 seconds. Configure these values through the client builder or connect arguments. The corresponding environment variables are `LASER_PUBLISH_TIMEOUT_MS`, `LASER_PUBLISH_MAX_RETRIES`, and `LASER_PUBLISH_RETRY_BACKOFF_MS`. Explicit configuration overrides these variables. Exhausted retries return an error for the application to handle.

See [publish recovery and outage handling](../../docs/publish-recovery.md).
