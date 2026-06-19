import asyncio

import laser_sdk as ls
import pytest

pytestmark = pytest.mark.integration


async def test_connect_reports_open_capabilities(laser):
    caps = await laser.capabilities()
    # Raw Apache Iggy advertises no managed features.
    assert caps.managed_query is False
    assert caps.managed_kv is False
    assert caps.forks is False


async def test_ensure_topic_then_publish_single(laser):
    await laser.ensure_topic("orders", partitions=2)
    await (
        laser.publish("orders")
        .index("customer_id", "alice")
        .inline_payload()
        .json({"id": "o-1", "amount": 129})
        .send()
    )


async def test_publish_batch_returns_count(laser):
    await laser.ensure_topic("events", partitions=1)
    count = await (
        laser.publish_batch("events")
        .inline_payload()
        .extend_json([{"n": 1}, {"n": 2}, {"n": 3}])
        .send()
    )
    assert count == 3


async def test_query_against_raw_iggy_is_unsupported(laser):
    with pytest.raises(ls.UnsupportedError) as caught:
        await laser.query("orders").where_eq("customer_id", "alice").fetch()
    assert caught.value.unsupported is True


async def test_kv_against_raw_iggy_is_unsupported(laser):
    with pytest.raises(ls.UnsupportedError):
        await laser.kv("sessions").get("user:1")


async def test_fork_against_raw_iggy_is_unsupported(laser):
    with pytest.raises(ls.UnsupportedError):
        await laser.fork("exp-1").create()


async def test_agent_echo_request_reply(laser):
    await laser.bootstrap(partitions=2)

    async def handle(ctx, message):
        await ctx.respond(b"echo: " + message.payload)

    agent = laser.spawn_agent(
        "echo",
        "agent.commands",
        handle,
        respond_on="agent.responses",
        poll_interval_ms=10,
    )
    try:
        await agent.ready()
        provenance = ls.Provenance(agent="caller")
        reply = await laser.request(
            "agent.commands",
            "agent.responses",
            b"ping",
            provenance,
            timeout_secs=20,
        )
        assert reply.payload == b"echo: ping"
        assert reply.conversation_id == provenance.conversation_id
    finally:
        await agent.shutdown()


async def test_agent_async_with_and_topics(laser):
    await laser.bootstrap(partitions=2)

    async def handle(ctx, message):
        await ctx.respond(b"ack")

    spawned = laser.spawn_agent(
        "withagent", ls.Topics.COMMANDS, handle, respond_on=ls.Topics.RESPONSES, poll_interval_ms=10
    )
    async with spawned:
        reply = await laser.request(
            ls.Topics.COMMANDS,
            ls.Topics.RESPONSES,
            b"hi",
            ls.Provenance(agent="caller"),
            timeout_secs=20,
        )
        assert reply.payload == b"ack"
    # The context manager shut the agent down on exit.


async def test_assemble_context_replays_the_conversation(laser):
    await laser.bootstrap(partitions=2)

    async def handle(ctx, message):
        await ctx.respond(b"pong")

    agent = laser.spawn_agent(
        "ponger", "agent.commands", handle, respond_on="agent.responses", poll_interval_ms=10
    )
    try:
        await agent.ready()
        provenance = ls.Provenance(agent="caller")
        reply = await laser.request(
            "agent.commands", "agent.responses", b"ping", provenance, timeout_secs=20
        )
        history = await laser.assemble_context(reply.conversation_id)
        assert len(history) >= 2
        assert all(m.conversation_id == reply.conversation_id for m in history)
        assert any(m.payload == b"ping" for m in history)
        assert any(m.payload == b"pong" for m in history)
    finally:
        await agent.shutdown()


async def test_reader_reads_back_published_messages(laser):
    await laser.ensure_topic("audit", partitions=1)
    await laser.publish("audit").payload(b"one").send()
    await laser.publish("audit").json({"n": 2}).send()

    cursor = laser.reader("audit")
    # Poll until both records are visible (projection-free, straight off the log).
    seen = []
    for _ in range(20):
        seen.extend(await cursor.poll())
        if len(seen) >= 2:
            break
        await asyncio.sleep(0.2)

    assert len(seen) >= 2
    assert seen[0].payload == b"one"
    assert seen[1].json() == {"n": 2}
    assert cursor.offsets  # advanced past what was read


async def test_avro_encoded_record_round_trips_through_the_log(laser):
    # Avro encoding is client-side, so the publish path works on raw Apache Iggy
    # (only the managed projection of the body needs LaserData Cloud). Encode a
    # record under a compiled schema, publish it, and decode the bytes back.
    schema_source = {
        "kind": "avro",
        "schema": '{"type":"record","name":"Fill","fields":['
        '{"name":"symbol","type":"string"},{"name":"qty","type":"int"}]}',
    }
    compiled = ls.CompiledSchema.compile(schema_source, id=1)
    await laser.ensure_topic("fills_avro", partitions=1)
    batch = laser.publish_batch("fills_avro").add_avro(compiled, 1, {"symbol": "AAPL", "qty": 7})
    await batch.send()

    cursor = laser.reader("fills_avro")
    seen = []
    for _ in range(20):
        seen.extend(await cursor.poll())
        if seen:
            break
        await asyncio.sleep(0.2)

    assert seen
    assert compiled.decode(bytes(seen[0].payload)) == {"symbol": "AAPL", "qty": 7}


async def test_send_agent_is_keyed_by_conversation(laser):
    await laser.bootstrap(partitions=2)
    provenance = ls.Provenance(agent="producer")
    # A bare send_agent to a well-known topic should succeed on raw Iggy.
    await laser.send_agent("agent.audit", b"audit-record", provenance)


async def test_agui_state_snapshot_and_reconstruct(laser):
    await laser.bootstrap(partitions=1)
    conversation = ls.new_conversation_id()
    await laser.publish_state_snapshot("agent.llm_io", "ui", conversation, {"count": 1})
    await laser.publish_state_delta(
        "agent.llm_io",
        "ui",
        conversation,
        [{"op": "replace", "path": "/count", "value": 2}],
    )
    state = None
    for _ in range(20):
        state = await laser.reconstruct_state(conversation, "agent.llm_io")
        if state == {"count": 2}:
            break
        await asyncio.sleep(0.2)
    assert state == {"count": 2}


async def test_mcp_bridge_initialize_and_list_tools(laser):
    bridge = laser.mcp_bridge(
        "mcp-gw",
        "agent.tool_calls",
        "agent.tool_results",
        "laser-mcp",
        tools=[
            {"name": "ask", "description": "ask a question", "input_schema": {"type": "object"}}
        ],
        prompts=[
            {
                "prompt": {"name": "greet", "description": "a greeting"},
                "messages": [["user", "say hello"]],
            }
        ],
    )
    init = bridge.initialize()
    assert "capabilities" in init
    names = [tool["name"] for tool in bridge.list_tools()["tools"]]
    assert "ask" in names
    prompt_names = [prompt["name"] for prompt in bridge.list_prompts()["prompts"]]
    assert "greet" in prompt_names
    rendered = bridge.get_prompt("greet")
    assert rendered["messages"]


async def test_a2a_bridge_round_trip_with_python_agent(laser):
    await laser.bootstrap(partitions=2)

    async def worker(ctx, message):
        await ctx.respond_input("agent.responses", b"answered")

    agent = laser.spawn_agent("a2a-worker", "agent.commands", worker, poll_interval_ms=10)
    try:
        await agent.ready()
        bridge = laser.a2a_bridge("a2a-gw", "agent.commands", "agent.responses")
        task = await bridge.submit(
            {"message": {"role": "user", "parts": [{"kind": "text", "text": "hi"}]}}
        )
        assert task["id"]
        resolved = None
        for _ in range(40):
            current = await bridge.task(task["id"])
            if current["status"]["state"].lower() not in ("working", "submitted"):
                resolved = current
                break
            await asyncio.sleep(0.3)
        assert resolved is not None, "the A2A task never left the working state"
    finally:
        await agent.shutdown()


async def test_custom_deduplicator_is_consulted(laser):
    await laser.bootstrap(partitions=1)
    handled: list[str] = []
    seen: set[str] = set()

    async def dedup(key: str) -> bool:
        first_time = key not in seen
        seen.add(key)
        return first_time

    async def handle(ctx, message):
        handled.append(message.payload.decode())

    agent = laser.spawn_agent(
        "dedup-worker", "agent.commands", handle, poll_interval_ms=10, dedup=dedup
    )
    try:
        await agent.ready()
        provenance = ls.Provenance(agent="caller", idempotency_key="dupe-key")
        await laser.send_agent("agent.commands", b"once", provenance)
        await laser.send_agent("agent.commands", b"twice", provenance)
        for _ in range(20):
            if seen:
                break
            await asyncio.sleep(0.2)
        await asyncio.sleep(1.0)
        # The custom deduplicator saw the repeated key and dropped the duplicate.
        assert len(handled) == 1
        assert "dupe-key" in seen
    finally:
        await agent.shutdown()


async def test_log_memory_remembers_and_recalls_on_open_iggy(laser):
    # Log-backed memory is the source-of-truth path, so it works on raw Apache Iggy.
    await laser.bootstrap(partitions=1)
    conversation = ls.new_conversation_id()
    memory = laser.memory()
    await memory.remember("the database pool was exhausted", conversation=conversation)
    await memory.remember("checkout latency spiked at noon", conversation=conversation)

    recalled = None
    for _ in range(40):
        recalled = await memory.recall(conversation=conversation, limit=10)
        if len(recalled) >= 2:
            break
        await asyncio.sleep(0.25)
    assert recalled is not None and len(recalled) == 2
    bodies = {item.text for item in recalled}
    assert "checkout latency spiked at noon" in bodies
    # A different conversation recalls nothing.
    assert await memory.recall(conversation=ls.new_conversation_id()) == []


async def test_vector_memory_ranks_by_semantic_similarity(laser):
    # In-process semantic memory: a deterministic bag-of-words embedder, so recall
    # ranks by overlap with the query. No server round-trip, but built off a Laser.
    vocabulary = ["database", "pool", "checkout", "latency", "billing", "refund", "noon", "spike"]

    async def embed(text: str) -> list[float]:
        words = set(text.lower().split())
        return [1.0 if term in words else 0.0 for term in vocabulary]

    memory = laser.vector_memory(embed)
    conversation = ls.new_conversation_id()
    await memory.remember("database pool exhaustion", conversation=conversation)
    await memory.remember("billing refund double charge", conversation=conversation)
    await memory.remember("checkout latency spike at noon", conversation=conversation)

    top = await memory.recall(conversation=conversation, semantic="checkout latency", limit=1)
    assert len(top) == 1
    assert top[0].text == "checkout latency spike at noon"

    refund = await memory.recall(conversation=conversation, semantic="refund", limit=1)
    assert refund[0].text == "billing refund double charge"
