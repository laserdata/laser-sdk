# laser-sdk (Python)

The LaserData SDK for Python: an open data-platform SDK over Apache Iggy. Native bindings to the Rust SDK via PyO3, so the wire contract, codecs, and runtime are the same ones the Rust client uses.

One Apache Iggy connection gives you typed streaming, declared projections and a query DSL, a key-value store, copy-on-write forks of the read model, and an optional agent runtime with the Agent Data Exchange Protocol (AGDX): publish, request/reply, and a consumer that drives your `async def` handler with at-least-once delivery, dedup, retry, and a dead-letter queue.

Apache Iggy is the underlying streaming core. Projections, the query layer, the key-value store, and forks are served by LaserData Cloud over that same connection. Against raw Apache Iggy those calls raise `UnsupportedError`.

## Install

```
pip install laser-sdk
```

Wheels ship for Linux (x86_64, aarch64) and macOS (Intel, Apple Silicon), Python 3.10 through 3.13.

## Connect

```python
import asyncio
from laser_sdk import Laser

async def main():
    laser = await Laser.connect("iggy://iggy:iggy@127.0.0.1:8090", stream="agents")
    caps = await laser.capabilities()
    print(caps)

asyncio.run(main())
```

The connection string scheme is optional (`iggy://` is assumed). Pin a default `stream=` so the convenience methods take just a topic, or name the stream per operation with the `stream=` keyword on each call.

## Publish and consume

```python
await laser.ensure_topic("orders", partitions=4)

await (
    laser.publish("orders")
    .index("customer_id", "alice")
    .index("total", "129")
    .inline_payload()
    .json({"id": "o-1", "customer": "alice", "amount": 129})
    .send()
)
```

## Batch and any payload

A single publish is the simplest call, not the common one. `publish_batch` accumulates records and sends them in one network round-trip, the largest throughput lever the SDK offers, and reads mirror it: a `reader` cursor drains every record that arrived since the last poll in one call. Batching on both sides is what makes the path efficient.

The payload is yours, in any format. `add_json` / `add_msgpack` (and `extend_json` for a whole list) are conveniences over `add_payload`, which takes raw `bytes` the SDK never inspects, so a compressed blob or your own framing rides unchanged. Schema-first Avro and Protobuf bodies are below.

```python
batch = laser.publish_batch("orders").inline_payload()
batch.extend_json([{"id": "o-1", "amount": 129}, {"id": "o-2", "amount": 80}])
batch.add_payload(b"\x00any-bytes-any-format")  # raw bytes, untouched by the SDK
await batch.send()                              # the whole batch, one round-trip
```

## Schema-first bodies (Avro / Protobuf)

Compile a registered writer schema once, then publish raw datums under it. The
body is encoded client-side, so a value that stops matching the schema fails
before publishing rather than as a managed-side warning you cannot see. The
managed plane resolves the schema by id and extracts indexed columns from the
binary body.

```python
from laser_sdk import CompiledSchema

source = {"kind": "avro", "schema": fill_avro_schema}
schema_id = await laser.register_schema(source, name="fill")
compiled = CompiledSchema.compile(source, id=schema_id)

batch = laser.publish_batch("trades_avro").inline_payload()
for fill in fills:
    batch = batch.add_avro(compiled, schema_id, fill)
await batch.send()
```

`CompiledSchema` also offers `validate` / `validate_value` / `decode`, and the
single-record builder has `.avro(compiled, schema_id, value)`. For Protobuf or
your own framing, encode the body yourself and ship it with
`.raw_bytes(bytes, "protobuf")` (or batch `.add_raw_bytes(..)`). Writer schemas
live on LaserData Cloud, so registration is a managed feature.

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

Query, the key-value store, and forks are managed features served by LaserData Cloud. Against raw Apache Iggy they raise `UnsupportedError`.

## Key-value

```python
kv = laser.kv("sessions")
await kv.set("user:42").json({"state": "online"}).ttl(300).send()
state = await kv.get_typed("user:42")
await kv.delete("user:42")
```

## Agents

```python
from laser_sdk import Laser

async def handle(ctx, message):
    text = message.payload.decode()
    await ctx.respond(f"echo: {text}".encode())

laser = await Laser.connect("iggy://iggy:iggy@127.0.0.1:8090", stream="agents")
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

For a human-in-the-loop pause, the typed AGDX producer's `request_input` publishes
a prompt and blocks on the human's correlated reply, which a handler resolves with
`AgentCtx.respond_input`:

```python
decision = await laser.agdx("agent.human_input", "orchestrator", conversation_id).request_input(
    "agent.responses", b"approve a $500 refund?", timeout_secs=15
)
```

## Consume and replay

```python
# A resumable reader over a topic. Each poll drains what is new. Persist the
# offsets to resume after a restart.
cursor = laser.reader("orders")
for message in await cursor.poll():
    print(message.json())
saved = cursor.offsets

# Replay a conversation's history off the log (agent runtime).
history = await laser.assemble_context(conversation_id, last_n=50)
```

## Memory and state

Agent memory shares one `remember` / `recall` / `forget` surface over four backends. The log-backed default works on raw Apache Iggy. The in-process vector backend ranks recall by semantic similarity, embedding through your own `async def embed(text) -> list[float]`. The query and key-value backends are managed.

```python
async def embed(text: str) -> list[float]:
    ...  # your model, or a deterministic stand-in

memory = laser.vector_memory(embed)
await memory.remember("checkout latency traces to the database pool", conversation=cid)
hits = await memory.recall(conversation=cid, semantic="why is checkout slow", limit=3)
print([item.text for item in hits])

# A durable key/value seam for agent state, the same vocabulary as the managed store.
store = ls.InMemoryStore()        # or ls.FileStore("/var/lib/agent")
await store.set("cursor", saved_bytes)
value = await store.get("cursor")
```

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

Every failure raises a subclass of `LaserError`: `QueryError`, `KvError`, `ForkError`, `UnsupportedError`, `InvalidError`, `CodecError`, `ProtocolError`, `TimeoutError`, `ConfigError`, `TransportError`. Each instance carries `code`, `retryable`, `unsupported`, `not_found`, `version_skew`, `version_conflict`, and `stale` attributes so you can branch without matching on the type.

## License

Apache-2.0.
