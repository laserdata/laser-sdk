import asyncio
import time

import laser_sdk as ls
import pytest

pytestmark = pytest.mark.integration


def test_signing_keys_enroll_for_agent_and_operator_verification():
    key = ls.SigningKey(bytes(range(32)))
    registry = ls.KeyRegistry()
    registry.enroll("agent-7", key)
    registry.enroll_operator("operator-9", ls.SigningKey(bytes(reversed(range(32)))))

    assert len(key.key_id) == 8


def test_signing_key_rejects_a_seed_with_the_wrong_length():
    with pytest.raises(ValueError, match="exactly 32"):
        ls.SigningKey(b"short")


async def test_connect_reports_open_capabilities(laser):
    caps = await laser.capabilities()
    # Raw Apache Iggy advertises no managed features.
    assert caps.query is False
    assert caps.kv is False
    assert caps.forks is False


async def test_topic_ensure_then_publish_single(laser):
    await laser.topic("orders").ensure(partitions=2)
    await (
        laser.topic("orders")
        .publish()
        .index("customer_id", "alice")
        .inline_payload()
        .json({"id": "o-1", "amount": 129})
        .send()
    )


async def test_publish_batch_returns_count(laser):
    await laser.topic("events").ensure(partitions=1)
    count = await (
        laser.topic("events")
        .publish_batch()
        .inline_payload()
        .extend_json([{"n": 1}, {"n": 2}, {"n": 3}])
        .send()
    )
    assert count == 3


async def test_given_laser_streaming_when_consumed_then_should_preserve_delivery_and_offsets(
    laser,
):
    topic = laser.topic("live-streaming")
    producer = topic.producer(
        batch_length=128,
        linger_ms=1,
        retries=3,
        retry_interval_ms=10,
        partition=0,
        partitions=1,
    )
    await producer.init()
    await producer.send(
        b"one",
        headers={"kind": ("uint16", 7), "source": "python"},
        key=b"account-42",
    )
    count = await producer.send_batch(
        [(b"two", {"kind": 8}), b"three"],
        partition=0,
    )
    assert count == 2

    uncommitted = topic.consumer_group(
        "uncommitted-workers",
        poll_interval_ms=1,
        polling="next",
        auto_commit="disabled",
    )
    try:
        first = await asyncio.wait_for(uncommitted.next(), timeout=10)
    finally:
        await uncommitted.shutdown()
    uncommitted = topic.consumer_group(
        "uncommitted-workers",
        poll_interval_ms=1,
        polling="next",
        auto_commit="disabled",
    )
    try:
        replayed = await asyncio.wait_for(uncommitted.next(), timeout=10)
        assert (replayed.partition_id, replayed.offset) == (
            first.partition_id,
            first.offset,
        )
        await uncommitted.commit(replayed)
    finally:
        await uncommitted.shutdown()

    consumer = topic.consumer_group(
        "manual-workers",
        batch_length=32,
        poll_interval_ms=1,
        polling="first",
        auto_commit="disabled",
        allow_replay=True,
    )
    try:
        await consumer.init()
        received = [await asyncio.wait_for(consumer.next(), timeout=10) for _ in range(3)]
        assert [message.payload for message in received] == [b"one", b"two", b"three"]
        assert received[0].headers == {"kind": 7, "source": "python"}
        assert received[0].header_kinds == {"kind": "uint16", "source": "string"}
        assert received[0].partition_id == 0
        assert received[0].offset == 0

        last = received[-1]
        await consumer.commit(last)
        assert await consumer.last_consumed_offset(0) == last.offset
        assert await consumer.last_stored_offset(0) == last.offset
    finally:
        await consumer.shutdown()

    await producer.send(b"manual-resumed")
    resumed = topic.consumer_group(
        "manual-workers",
        poll_interval_ms=1,
        polling="next",
        auto_commit="disabled",
    )
    try:
        message = await asyncio.wait_for(resumed.next(), timeout=10)
        assert message.payload == b"manual-resumed"
    finally:
        await resumed.shutdown()

    auto_consumer = topic.consumer_group(
        "auto-workers",
        batch_length=32,
        poll_interval_ms=1,
        polling="first",
        auto_commit="each",
        commit_interval_ms=10,
        allow_replay=True,
    )
    try:
        await auto_consumer.init()
        received = [await asyncio.wait_for(auto_consumer.next(), timeout=10) for _ in range(4)]
        last = received[-1]
        async with asyncio.timeout(10):
            while await auto_consumer.last_stored_offset(0) != last.offset:
                await asyncio.sleep(0.01)
    finally:
        await auto_consumer.shutdown()

    await producer.send(b"auto-resumed")
    resumed = topic.consumer_group(
        "auto-workers",
        poll_interval_ms=1,
        polling="next",
        auto_commit="disabled",
    )
    try:
        message = await asyncio.wait_for(resumed.next(), timeout=10)
        assert message.payload == b"auto-resumed"
    finally:
        await resumed.shutdown()


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


async def test_runs_against_raw_iggy_are_unsupported(laser):
    runs = laser.runs()
    with pytest.raises(ls.UnsupportedError):
        await runs.submit("planner", b"task")
    with pytest.raises(ls.UnsupportedError):
        await runs.cancel("run-1")
    with pytest.raises(ls.UnsupportedError):
        await runs.status("run-1")
    with pytest.raises(ls.UnsupportedError):
        await runs.list()


async def test_runs_list_rejects_an_unknown_state_word(laser):
    with pytest.raises(ValueError):
        await laser.runs().list(state="paused")


async def test_workflow_builder_error_fails_before_dispatch(laser):
    await laser.bootstrap(partitions=1)

    def broken_builder(_outputs):
        raise RuntimeError("cannot build task")

    workflow = laser.workflow("broken-workflow", fixed_inbox="agent.commands")
    workflow.step("broken", to="worker", build=broken_builder)

    with pytest.raises(ls.ConfigError, match="workflow step builder failed"):
        await workflow.run()


async def test_graph_against_raw_iggy_is_unsupported(laser):
    alice = ls.graph_node("Person", "Alice")
    acme = ls.graph_node("Company", "Acme")
    edge = ls.graph_edge(alice, "works_at", acme)
    with pytest.raises(ls.UnsupportedError):
        await laser.graph("knowledge").upsert([alice, acme], [edge])


def test_graph_ids_are_content_addressed_and_match_the_cross_sdk_golden():
    # The same entity yields the same id, a different label or value a different
    # one, pinned to the cross-SDK golden vector the wire crate fixes, so a graph
    # shared across languages converges on one node.
    assert ls.node_id("Person", "Alice") == ls.node_id("Person", "Alice")
    assert ls.node_id("Person", "Alice") != ls.node_id("Company", "Alice")
    assert ls.node_id("Person", "Alice") == "13NCEPHNVFHHGNK9GD3MT0W1AB"
    alice = ls.graph_node("Person", "Alice")
    assert alice["id"] == "13NCEPHNVFHHGNK9GD3MT0W1AB"
    assert alice["labels"] == ["Person"]
    acme = ls.graph_node("Company", "Acme")
    edge = ls.graph_edge(alice, "works_at", acme)
    assert edge["from"] == alice["id"]
    assert edge["to"] == acme["id"]
    assert edge["edge_type"] == "works_at"
    assert edge["id"] == ls.edge_id(alice["id"], "works_at", acme["id"])


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
    await laser.topic("audit").ensure(partitions=1)
    await laser.topic("audit").publish().payload(b"one").send()
    await laser.topic("audit").publish().json({"n": 2}).send()

    cursor = laser.topic("audit").replay()
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


async def test_governor_blocks_a_business_publish(laser):
    await laser.topic("business.audit").ensure(partitions=1)

    class BlockBusinessWires:
        async def decide(self, action):
            if action.kind == "publish" and bytes(action.payload).startswith(b"wire-funds"):
                return ls.ActionDecision.block("no wire transfers")
            return ls.ActionDecision.allow()

    governed = laser.with_governor(BlockBusinessWires(), mode="enforce")
    provenance = ls.Provenance(agent="publisher")
    with pytest.raises(ls.PolicyBlockedError):
        await (
            governed.topic("business.audit")
            .publish()
            .provenance(provenance)
            .payload(b"wire-funds to acct 7")
            .send()
        )


async def test_quorum_governor_mandatory_voter_blocks_regardless_of_policy(laser):
    await laser.topic("business.audit").ensure(partitions=1)

    class AlwaysAllow:
        async def decide(self, action):
            return ls.ActionDecision.allow()

    class BlockBusinessWires:
        async def decide(self, action):
            if action.kind == "publish" and bytes(action.payload).startswith(b"wire-funds"):
                return ls.ActionDecision.block("no wire transfers")
            return ls.ActionDecision.allow()

    quorum = ls.QuorumGovernor(ls.QuorumPolicy.any())
    quorum.voter("safety", BlockBusinessWires(), mandatory=True)
    quorum.voter("llm", AlwaysAllow(), mandatory=False)

    governed = laser.with_governor(quorum, mode="enforce")
    provenance = ls.Provenance(agent="publisher")
    with pytest.raises(ls.PolicyBlockedError):
        await (
            governed.topic("business.audit")
            .publish()
            .provenance(provenance)
            .payload(b"wire-funds to acct 7")
            .send()
        )


async def test_quorum_governor_at_least_policy_commits_when_met(laser):
    await laser.topic("business.audit").ensure(partitions=1)

    class AlwaysAllow:
        async def decide(self, action):
            return ls.ActionDecision.allow()

    class AlwaysObserve:
        async def decide(self, action):
            return ls.ActionDecision.observe()

    quorum = ls.QuorumGovernor(ls.QuorumPolicy.at_least(2))
    quorum.voter("a", AlwaysAllow(), mandatory=False)
    quorum.voter("b", AlwaysObserve(), mandatory=False)

    governed = laser.with_governor(quorum, mode="enforce")
    provenance = ls.Provenance(agent="publisher")
    await (
        governed.topic("business.audit")
        .publish()
        .provenance(provenance)
        .payload(b"ordinary payload")
        .send()
    )


async def test_native_durable_intent_records_round_trip_through_the_log(laser):
    conversation = ls.new_conversation_id()
    intent = ls.Intent(
        conversation=conversation,
        proposer="planner",
        body=b"reserve inventory",
        eligible_voters=["safety"],
        policy=ls.IntentPolicy.all(),
        policy_version=7,
        deadline_micros=time.time_ns() // 1_000 + 10_000_000,
    )
    vote = ls.Vote.cast(intent, "safety", "allow")
    decision = ls.decide(intent, [vote], time.time_ns() // 1_000)
    assert decision is not None

    records = [
        ("native-intents", ls.Intent, intent),
        ("native-votes", ls.Vote, vote),
        ("native-decisions", ls.Decision, decision),
    ]
    for topic_name, cls, value in records:
        topic = laser.topic(topic_name, cls=cls)
        await topic.ensure(partitions=1)
        await topic.publish(value).send()
        reader = topic.records(f"{topic_name}-reader")
        record = await reader.next()
        assert record is not None
        assert record.value.intent_id == intent.intent_id

    decoded_intent = (
        await laser.topic("native-intents", cls=ls.Intent)
        .records("native-intents-second-reader")
        .next()
    ).value
    assert bytes(decoded_intent.body) == b"reserve inventory"
    assert decoded_intent.digest == intent.digest

    decoded_decision = (
        await laser.topic("native-decisions", cls=ls.Decision)
        .records("native-decisions-second-reader")
        .next()
    ).value
    assert decoded_decision.intent_digest == intent.digest
    assert decoded_decision.policy_version == 7
    assert decoded_decision.outcome == "committed"


async def test_swappable_governor_swap_changes_the_next_decision(laser):
    await laser.topic("business.audit").ensure(partitions=1)

    class AlwaysAllow:
        async def decide(self, action):
            return ls.ActionDecision.allow()

    class AlwaysBlock:
        async def decide(self, action):
            return ls.ActionDecision.block("policy hot-swapped to deny-all")

    swappable = ls.SwappableGovernor(AlwaysAllow())
    governed = laser.with_governor(swappable, mode="enforce")
    provenance = ls.Provenance(agent="publisher")

    # Decides under the initial policy.
    await (
        governed.topic("business.audit")
        .publish()
        .provenance(provenance)
        .payload(b"first payload")
        .send()
    )

    # A swap changes what the very next decision runs under, with no
    # reconnect and no new governor enrollment.
    initial = swappable.current()
    assert swappable.swap(AlwaysBlock()) is initial
    with pytest.raises(ls.PolicyBlockedError):
        await (
            governed.topic("business.audit")
            .publish()
            .provenance(provenance)
            .payload(b"second payload")
            .send()
        )


async def test_swarm_activity_folds_policy_evidence_by_agent(laser):
    await laser.topic("business.audit").ensure(partitions=1)

    class BlockWires:
        async def decide(self, action):
            if action.kind == "publish" and bytes(action.payload).startswith(b"wire-funds"):
                return ls.ActionDecision.block("no wire transfers")
            return ls.ActionDecision.allow()

    governed = laser.with_governor(BlockWires(), mode="enforce")
    provenance = ls.Provenance(agent="publisher")
    with pytest.raises(ls.PolicyBlockedError):
        await (
            governed.topic("business.audit")
            .publish()
            .provenance(provenance)
            .payload(b"wire-funds to acct 9")
            .send()
        )

    # Evidence lands asynchronously with the send, so poll briefly.
    swarm = ls.SwarmActivity()
    deadline = time.monotonic() + 10
    while not swarm.agent("publisher"):
        messages = await laser.assemble_context(
            provenance.conversation_id, topics=[ls.Topics.AUDIT]
        )
        for message in messages:
            envelope = message.envelope
            if not envelope or envelope.get("operation") != "policy_decision":
                continue
            swarm.observe(ls.PolicyEvidence.decode(bytes(message.agdx_body)))
        if time.monotonic() > deadline:
            pytest.fail("no policy decision landed on the audit topic")
        await asyncio.sleep(0.2)

    activity = swarm.agent("publisher")
    assert activity.count("block") >= 1
    assert activity.last_decision is not None
    assert activity.last_decision.source == "publisher"

    agents = swarm.agents()
    assert agents[0][0] == "publisher"


async def test_crash_context_assembles_journal_and_last_decision(laser):
    class BlockWires:
        async def decide(self, action):
            if action.kind == "send" and bytes(action.payload).startswith(b"wire-funds"):
                return ls.ActionDecision.block("no wire transfers")
            return ls.ActionDecision.allow()

    governed = laser.with_governor(BlockWires(), mode="enforce")
    provenance = ls.Provenance(agent="publisher")

    # A normal send lands in the journal.
    await governed.send_agent(ls.Topics.COMMANDS, b"do the thing", provenance)

    # A blocked send is recorded as a decision on the audit topic.
    with pytest.raises(ls.PolicyBlockedError):
        await governed.send_agent(ls.Topics.COMMANDS, b"wire-funds to acct 9", provenance)

    journal = await laser.assemble_context(provenance.conversation_id, topics=[ls.Topics.COMMANDS])
    assert journal

    last_decision = None
    deadline = time.monotonic() + 10
    while last_decision is None:
        audit = await laser.assemble_context(provenance.conversation_id, topics=[ls.Topics.AUDIT])
        for message in audit:
            envelope = message.envelope
            if envelope and envelope.get("operation") == "policy_decision":
                last_decision = ls.PolicyEvidence.decode(bytes(message.agdx_body))
        if time.monotonic() > deadline:
            pytest.fail("no policy decision landed on the audit topic")
        await asyncio.sleep(0.2)

    context = ls.CrashContext(journal=journal, dead_letter=None, last_decision=last_decision)
    summary = context.summarize()
    assert "do the thing" in summary
    assert "last decision: block (blocked)" in summary
    assert "dead letter: none" in summary


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
    await laser.topic("fills_avro").ensure(partitions=1)
    batch = (
        laser.topic("fills_avro")
        .publish_batch()
        .add_avro(compiled, 1, {"symbol": "AAPL", "qty": 7})
    )
    await batch.send()

    cursor = laser.topic("fills_avro").replay()
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
        assert any("dupe-key" in key for key in seen)
    finally:
        await agent.shutdown()


async def test_log_memory_remembers_and_recalls_on_open_iggy(laser):
    # Log-backed memory is the source-of-truth path, so it works on raw Apache Iggy.
    await laser.bootstrap(partitions=1)
    conversation = ls.new_conversation_id()
    memory = laser.memory("notes")
    await memory.remember("the database pool was exhausted", conversation=conversation)
    await memory.remember("checkout latency spiked at noon", conversation=conversation)

    # Recall folds the topic in process (the opt-in path), since raw Apache Iggy
    # serves no key-value read view for the default recall to read.
    recalled = None
    for _ in range(40):
        recalled = await memory.recall(conversation=conversation, limit=10, folded=True)
        if len(recalled) >= 2:
            break
        await asyncio.sleep(0.25)
    assert recalled is not None and len(recalled) == 2
    bodies = {item.text for item in recalled}
    assert "checkout latency spiked at noon" in bodies
    # A different conversation recalls nothing.
    assert await memory.recall(conversation=ls.new_conversation_id(), folded=True) == []


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


async def test_governor_blocks_a_vector_memory_write(laser):
    await laser.bootstrap(partitions=1)

    async def embed(text: str) -> list[float]:
        return [float(len(text))]

    class BlockFabricatedMemory:
        async def decide(self, action):
            if action.kind == "memory_write" and b"[skew:fabricate_memory]" in bytes(
                action.payload
            ):
                return ls.ActionDecision.block("fabricated memory marker")
            return ls.ActionDecision.allow()

    governed = laser.with_governor(BlockFabricatedMemory(), mode="enforce")
    memory = governed.vector_memory(embed)
    conversation = ls.new_conversation_id()
    with pytest.raises(ls.PolicyBlockedError):
        await memory.remember(
            "customer prefers blue [skew:fabricate_memory]",
            conversation=conversation,
        )
    assert await memory.recall(conversation=conversation) == []


async def test_vector_memory_improve_promotes_a_recalled_item(laser):
    # Feedback re-ranks recall: a promoted item floats to the front on the next
    # recall, mirroring the Rust feedback contract.
    async def embed(text: str) -> list[float]:
        return [1.0 if term in set(text.lower().split()) else 0.0 for term in ("cat", "dog")]

    memory = laser.vector_memory(embed)
    conversation = ls.new_conversation_id()
    await memory.remember("the cat sat", conversation=conversation)
    dog = await memory.remember("the dog ran", conversation=conversation)

    before = await memory.recall(conversation=conversation, limit=2)
    assert before[0].text == "the cat sat"

    await memory.improve(dog, 5.0, conversation=conversation)
    after = await memory.recall(conversation=conversation, limit=2)
    assert after[0].text == "the dog ran"
    assert after[0].score == 5.0


async def test_recall_with_unknown_strategy_raises(laser):
    memory = laser.memory("notes")
    with pytest.raises(ls.CodecError):
        await memory.recall(conversation=ls.new_conversation_id(), strategy="nonsense")


async def test_agent_message_and_agent_ctx_build_without_a_live_consumer(laser):
    # The handler unit-test seam: build a message and a ctx directly, then call
    # the handler function like a plain callable, no spawn_agent/consumer group
    # needed at all.
    provenance = ls.Provenance(agent="tester")
    message = ls.agent_message(b"hello", provenance)
    assert message.payload == b"hello"
    assert message.conversation_id == provenance.conversation_id

    ctx = ls.agent_ctx(laser, message, agent="tester")
    assert ctx.message.payload == b"hello"

    handled = []

    async def handle(ctx, message):
        handled.append(message.payload)

    await handle(ctx, message)
    assert handled == [b"hello"]


async def test_fan_out_gathers_every_capable_agents_reply(laser, iggy_endpoint):
    await laser.bootstrap(partitions=2)

    def make_worker(name):
        async def handle(ctx, message):
            await ctx.respond(f"{name}:{message.payload.decode()}".encode())

        return handle

    # Presence is connection-scoped (one connection may advertise one agent),
    # so each capability-advertising worker needs its own connection, mirroring
    # the Rust integration test's `harness::reconnect` per worker.
    connections = []
    workers = []
    for name in ("worker-a", "worker-b"):
        connection = await ls.Laser.connect(iggy_endpoint, stream=laser.default_stream)
        connections.append(connection)
        agent = connection.spawn_agent(
            name,
            "agent.commands",
            make_worker(name),
            respond_on="agent.responses",
            capabilities=["diagnose"],
            poll_interval_ms=10,
        )
        await agent.ready()
        workers.append(agent)

    gathered = {}

    async def orchestrate(ctx, message):
        gathered["result"] = await ctx.fan_out(
            "diagnose",
            b"scan",
            deadline_ms=10_000,
            fixed_inbox="agent.commands",
        )

    orchestrator = laser.spawn_agent(
        "orchestrator",
        "agent.tool_calls",
        orchestrate,
        respond_on="agent.responses",
        poll_interval_ms=10,
    )
    try:
        await orchestrator.ready()
        await laser.send_agent("agent.tool_calls", b"go", ls.Provenance(agent="trigger"))

        for _ in range(50):
            if "result" in gathered:
                break
            await asyncio.sleep(0.2)

        result = gathered["result"]
        assert len(result["failures"]) == 0
        assert {entry["agent"] for entry in result["ok"]} == {"worker-a", "worker-b"}
        assert {entry["body"] for entry in result["ok"]} == {b"worker-a:scan", b"worker-b:scan"}
    finally:
        for worker in workers:
            await worker.shutdown()
        await orchestrator.shutdown()


async def test_approval_gate_resumes_a_handler_with_the_human_decision(laser):
    await laser.bootstrap(partitions=2)

    async def approve(ctx, message):
        await ctx.respond_input("agent.responses", b"approved")

    approver = laser.spawn_agent("approver", "agent.human_input", approve, poll_interval_ms=10)

    async def gatekeeper_handle(ctx, message):
        decision = await ctx.approval_gate(
            "agent.responses", b"approve a $500 credit?", timeout_secs=10
        )
        await ctx.reply_on("agent.audit", decision)

    gatekeeper = laser.spawn_agent(
        "gatekeeper",
        "agent.tool_calls",
        gatekeeper_handle,
        respond_on="agent.responses",
        poll_interval_ms=10,
    )
    try:
        await approver.ready()
        await gatekeeper.ready()
        provenance = ls.Provenance(agent="trigger")
        await laser.send_agent("agent.tool_calls", b"go", provenance)

        decision = None
        for _ in range(50):
            audit = await laser.assemble_context(provenance.conversation_id, topics=["agent.audit"])
            match = next((m for m in audit if m.payload == b"approved"), None)
            if match is not None:
                decision = match.payload
                break
            await asyncio.sleep(0.2)

        assert decision == b"approved"
    finally:
        await gatekeeper.shutdown()
        await approver.shutdown()
