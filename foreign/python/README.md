# laser-sdk (Python)

The LaserData SDK for Python: an open data-platform SDK over Apache Iggy. Native bindings to the Rust SDK via PyO3, so the wire contract, codecs, and runtime are the same ones the Rust client uses.

Rust and Python are one v1 contract. Every public primitive, builder option, validation rule, error classification, capability, and transport limitation ships in both SDKs with matched examples and shared BDD coverage where the behavior is language-neutral.

> **Prerelease (`0.0.1-rc.18`).** The wire contract and the public API may still change between release candidates, so pin an exact version.

`spawn_agent(agent_id, ..., consumer_group=None)` keeps logical identity separate from Iggy replica topology. The group defaults to the agent id spelling, set it explicitly when deployment grouping differs.

One Apache Iggy connection gives you typed streaming, declared projections and a query DSL, a key-value store, a knowledge graph, copy-on-write forks of the read model, and an optional agent runtime with the Agent Data Exchange Protocol (AGDX): publish, request/reply, and a consumer that drives your `async def` handler with at-least-once delivery, per-conversation (per-partition) ordering, dedup, retry, and dead-lettering.

Apache Iggy is the underlying streaming core. Projections, the query layer, the key-value store, the knowledge graph, and forks are served by LaserData Cloud over that same connection. Against raw Apache Iggy those calls raise `UnsupportedError`.

## Install

```
pip install laser-sdk
```

Wheels ship for Linux (x86_64, aarch64) and macOS (Intel, Apple Silicon), Python 3.10 through 3.13.

Apache Iggy's VSR cluster protocol is a compile-time switch while it remains an upstream feature. Build the extension from this repository with `maturin develop --features vsr` to forward the switch through both the Python binding and `laser-sdk`. Standard wheels remain on Iggy's classic protocol until VSR becomes its default. VSR supports the standard streaming commands used below. LaserData's custom managed command band remains unavailable until upstream's VSR encoder admits those codes.

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

Use the bare `user:password@host:port` connection string. The SDK supplies the Apache Iggy TCP scheme internally. One connection addresses every stream on the server. Select a stream with `laser.stream(name)`, then a topic with `.topic(name)`. Passing `stream=` only pins a default stream so `laser.topic(name)` can be used as a shortcut. It does not limit the connection to that stream. The accessors are free and synchronous. IO happens at the verbs (`publish`, `replay`, `ensure`), mirroring the Rust grammar one-to-one.

Python uses the Rust client's Apache Iggy reconnect policy. TCP connections retry the initial handshake and reconnect after a dropped socket, with unlimited retries at one-second intervals by default. Add `reconnection_retries=<count|unlimited>` and `reconnection_interval=<duration>` to the connection string to tune it. Reconnection reuses the connection-string credentials, so a restarted server is authenticated again before traffic resumes.

`Laser.connect` goes through the same Rust `Laser::connect` as the Rust SDK, so a `*.laserdata.cloud`/`*.laserdata.com` host gets the same auto-attached TLS and bundled public CA with no extra Python-side setup. `LASER_TLS_CERT=<path>` overrides the cert, `LASER_NO_TLS=1` disables the check, and every other host is left untouched.

## Publish and consume

```python
orders = laser.stream("commerce").topic("orders")
await orders.ensure(partitions=4)

await (
    orders.publish()
    .index("customer_id", "alice")
    .index("total", "129")
    .inline_payload()
    .json({"id": "o-1", "customer": "alice", "amount": 129})
    .send()
)
```

## Batch and any payload

A single publish is the simplest call, not the common one. `publish_batch` accumulates records and sends them in one network round-trip, the largest throughput lever the SDK offers, and reads mirror it: a `topic(..).replay()` cursor drains every record that arrived since the last poll in one call. Batching on both sides is what makes the path efficient.

The payload is yours, in any format. `add_json` / `add_msgpack` (and `extend_json` for a whole list) are conveniences over `add_payload`, which takes raw `bytes` the SDK never inspects, so a compressed blob or your own framing rides unchanged. Schema-first Avro and Protobuf bodies are below.

```python
batch = orders.publish_batch().inline_payload()
batch.extend_json([{"id": "o-1", "amount": 129}, {"id": "o-2", "amount": 80}])
batch.add_payload(b"\x00any-bytes-any-format")  # raw bytes, untouched by the SDK
await batch.send()                              # the whole batch, one round-trip
```

## Live producer and consumer

For a regular streaming service, `Topic.producer`, `Topic.consumer`, and `Topic.consumer_group` are the Laser live-streaming surface, backed directly by Apache Iggy rather than approximated through replay. The producer exposes batching, linger, retries, stream/topic creation, expiry/size, replication factor, and balanced/key/partition routing. Consumers expose first/last/next/offset/timestamp polling, batch and poll intervals, group create/join, init and reconnect retries, replay, every iterator-safe auto-commit mode, explicit offset storage/deletion, and local consumed/stored offset inspection.

```python
topic = laser.stream("commerce").topic("events")
producer = topic.producer(
    batch_length=1000,
    linger_ms=5,
    retries=3,
    partitions=4,
)
await producer.init()
await producer.send(b"one", headers={"type": ("uint16", 7)}, key=b"account-42")
await producer.send_batch(
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

Header values accept ordinary Python scalars. When a Rust consumer expects an exact Apache Iggy numeric kind, pass `(kind, value)` as above. `ConsumerMessage.header_kinds` reports the exact kinds received. Use `auto_commit="each"` with `commit_interval_ms=1000` for interval-or-each storage. Use `"polling"`, `"all"`, `"every"` plus `commit_every=`, `"interval"`, or `"disabled"` for the other iterator-safe modes. With automatic commits disabled, `commit(message)` stores the offset only after successful handling, and `shutdown()` does not advance past that explicit commit. `Consumer` is a live async iterator that waits for new records, while `replay()` remains the bounded cursor that drains what exists and stops when caught up.

## Typed topics

One handle binds a topic to a class: pass `cls=` (a dataclass or pydantic model) and the topic encodes on the way in and decodes with the log position attached on the way out. `publish(order)` encodes the instance as JSON in one call, `records(reader_name)` is the typed reader over the same caller-owned offsets as `replay()`: `next()` yields the next record decoded into the class (`None` when caught up), and a record that does not decode raises `TypedDecodeError` naming its exact log position, then the reader moves past it.

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
    order: Order = record.value            # an Order instance, record.position names the log slot
```

## Schema-first bodies (Avro / Protobuf)

Compile a registered writer schema once, then publish raw datums under it. The body is encoded client-side, so a value that stops matching the schema fails before publishing rather than as a managed-side warning you cannot see. The managed plane resolves the schema by id and extracts indexed columns from the binary body.

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

`CompiledSchema` also offers `validate` / `validate_value` / `decode`, and the single-record builder has `.avro(compiled, schema_id, value)`. For Protobuf or your own framing, encode the body yourself and ship it with `.raw_bytes(bytes, "protobuf")` (or batch `.add_raw_bytes(..)`). Writer schemas live on LaserData Cloud, so registration is a managed feature.

## Query (managed)

```python
rows = await (
    laser.query("orders")
    .where_eq("customer_id", "alice")
    .filter_gte("total", 100)
    .order_desc("total")
    .limit(10)
    .with_payload()
    .fetch_all()
)
for row in rows:
    print(row.headers, row.json())
```

`fetch()` returns the page metadata. `page.has_more` is always exact. An exact match count is opt-in because it runs a separate count over the full filter:

```python
result = await laser.query("orders").where_eq("customer_id", "alice").with_total().fetch()
print(result.total, result.has_more)
```

Query, the key-value store, the knowledge graph, and forks are managed features served by LaserData Cloud. Against raw Apache Iggy they raise `UnsupportedError`.

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
await kv.delete("user:42")
```

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

`graph_node` content-addresses the id from its label and value, so an entity named by many writers stays one node and re-upserting it is a no-op. `link(from, relation, to)` relates two `kind:value` entities in one call, `relink` asserts the latest value of a single-valued relationship by closing every live same-relation edge first, and `unlink` closes an edge bitemporally while the nodes stay. `query` also starts from explicit `start_ids` or from the `nearest` nodes to an embedding, `returns` picks `"nodes"`, `"edges"`, `"triplets"`, or `"paths"`, and `as_of` (epoch micros) follows only the edges valid at that instant.

## Agents

```python
from laser_sdk import Laser

async def handle(ctx, message):
    text = message.payload.decode()
    await ctx.respond(f"echo: {text}".encode())

laser = await Laser.connect("iggy:iggy@127.0.0.1:8090", stream="agents")
await laser.bootstrap(partitions=4)

handle_agent = laser.spawn_agent(
    "echo", "agent.commands", handle, respond_on="agent.responses"
)
await handle_agent.ready()

from laser_sdk import Provenance
reply = await laser.request(
    "agent.commands", "agent.responses", b"hello",
    Provenance(agent="caller"), timeout_secs=10,
)
print(reply.payload.decode())

await handle_agent.shutdown()
```

### Signed, principal-bound contracts

Rust and Python use the same Ed25519 verifier and routing rules. Enroll keys before connecting, give an agent its signing key, and constrain sensitive capability routes to the server-authenticated principal. One connection may advertise one agent. Attempting to advertise another raises a typed conflict instead of replacing the first presence.

```python
from laser_sdk import KeyRegistry, Laser, SigningKey

risk_key = SigningKey(bytes(range(32)))
keys = KeyRegistry()
keys.enroll("42", risk_key)

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

Govern what an agent does before the effect runs: a policy object decides per action (allow, observe, block, step_up, modify, defer), enforce or shadow mode, and every non-allow decision lands as a digest-chained evidence event on the audit topic. `PolicyBlockedError` / `StepUpRequiredError` / `PolicyDeferredError` are the typed refusals:

```python
from laser_sdk import ActionDecision, PolicyBlockedError

class NoWires:
    async def decide(self, action):
        if action.payload.startswith(b"wire-funds"):
            return ActionDecision.block("wires need approval").with_policy("finance", "3", ["no-wires"])
        return ActionDecision.allow()

governed = laser.with_governor(NoWires(), mode="enforce")
try:
    await governed.send_agent("agent.commands", b"wire-funds", provenance)
except PolicyBlockedError as refused:
    print(refused)  # policy blocked: no wire transfers

# Per-agent: everything the handler publishes is governed too.
handle_agent = laser.spawn_agent("clerk", "agent.commands", handle, governor=NoWires())
```

`QuorumGovernor` composes several named voters under a policy (`all`, `any`, or `at_least(n)`) into one governor, so a deterministic safety voter and an LLM voter combine into a single decision instead of picking one. Every `mandatory` voter must return `allow`, `observe`, or `modify`. A denial or error cannot be bypassed by a permissive `any` policy:

```python
from laser_sdk import QuorumGovernor, QuorumPolicy

quorum = QuorumGovernor(QuorumPolicy.at_least(2))
quorum.voter("safety", NoWires(), mandatory=True)
quorum.voter("llm_reviewer", llm_voter, mandatory=False)

governed = laser.with_governor(quorum, mode="enforce")
```

`SwappableGovernor` hot-swaps the active policy at runtime, driven by anything (an operator call, a config reload, a folded policy-update topic), without dropping enrolled clones or reconnecting. A swap only changes the _next_ decision, never one already recorded:

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

`SwarmActivity` is a supervisor's read model over governance evidence: fold `PolicyEvidence` records already read off the audit topic and ask "what has this agent been doing" without hand-rolled bookkeeping:

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

`CrashContext` is a recovery tool's one-call bundle: combine an already-read journal tail, the crashed message's dead-letter capsule (if any), and the conversation's most recent decision (if any) into one deterministic digest, never invoking a model itself:

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
    rows = await laser.query("orders_v1").fetch_all()  # the record is a wakeup, the rows come from query
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

# Replay a conversation's history off the log (agent runtime).
history = await laser.assemble_context(conversation_id, last_n=50)
```

## Memory and state

Agent memory shares one `remember` / `recall` / `forget` surface over two backends: the log-backed default and the in-process vector backend. The log-backed handle adds the named-item altitude: `set(key, value)` / `fetch(key)` / `update(key, patch)` / `remove(key)` for working notes addressed by name (`UnsupportedError` on the vector backend). Its writes (`remember`, `set`, `forget`) always publish to the memory topic and work on raw Apache Iggy. Its default reads (`recall`, `fetch`) serve the deployment's materialized key-value view, a managed feature. Pass `recall(folded=True)` or call `fetch_folded` to fold the topic in process instead, which works on raw Apache Iggy too. The in-process vector backend ranks recall by semantic similarity. An embedder can return `list[float]` directly for local work or return an awaitable when it calls a model service.

```python
async def embed(text: str) -> list[float]:
    ...  # your model, or a deterministic stand-in

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
    "mcp-gateway", "agent.tool_calls", "agent.tool_results", "laser-mcp",
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

The readers are async-iterable: `async for message in laser.stream("commerce").topic("events").replay()`, `async for record in reader` on a `WatchReader`, and `async for record in topic.records(reader_name)` on a typed reader all drain what is currently appended and stop when caught up. A fresh `async for` later resumes from the same offsets. `poll()` is still there for one batch at a time.

## Lifecycle

`Laser` supports `async with`: `async with await Laser.connect(conn) as laser:`. The connection is reference-counted and closes when the last handle drops, and `with_stream` / `with_ops_stream` return aliasing clones that share it.

## License

Apache-2.0. Copyright LaserData, Inc.

Apache and Apache Iggy are trademarks of the Apache Software Foundation. Use of these marks does not imply endorsement by the Apache Software Foundation.
