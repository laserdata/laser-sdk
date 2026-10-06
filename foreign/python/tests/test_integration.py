import asyncio
import datetime
import decimal
import time
import uuid

import laser_sdk as ls
import pytest

pytestmark = pytest.mark.integration


def test_signing_keys_enroll_for_agent_and_operator_verification():
    key = ls.SigningKey(bytes(range(32)))
    operator_key = ls.SigningKey(bytes(reversed(range(32))))
    registry = ls.KeyRegistry()
    registry.enroll("agent-7", key.verifying_key)
    registry.enroll_operator("operator-9", operator_key.verifying_key)
    record = ls.KeyRecord("agent-7", key.verifying_key)

    assert len(key.key_id) == 8
    assert len(key.verifying_key) == 32
    assert record.verifying_key == key.verifying_key


def test_signing_key_rejects_a_seed_with_the_wrong_length():
    with pytest.raises(ValueError, match="exactly 32"):
        ls.SigningKey(b"short")

    with pytest.raises(ValueError, match="exactly 32"):
        ls.KeyRecord("agent-7", b"short")


@pytest.mark.parametrize("stop", ["shutdown", "context"])
async def test_given_periodic_consolidation_when_the_agent_stops_then_should_stop_background_passes(
    laser, stop
):
    await laser.bootstrap(1)
    passes = []
    called = asyncio.Event()

    class Consolidator:
        async def consolidate(self, scope):
            passes.append(scope)
            called.set()

    async def handle(context, message):
        pass

    agent = laser.spawn_agent(
        "periodic-memory",
        "agent.commands",
        handle,
        consolidate_every_ms=10,
        consolidator=Consolidator(),
    )
    try:
        if stop == "context":
            async with agent:
                await asyncio.wait_for(called.wait(), 2)
        else:
            await agent.ready()
            await asyncio.wait_for(called.wait(), 2)
            await agent.shutdown()
        count = len(passes)
    finally:
        await agent.shutdown()
    await asyncio.sleep(0.04)
    assert len(passes) == count
    assert passes[0] == {}


async def test_given_an_active_consolidation_pass_when_shutdown_then_should_cancel_the_callback(
    laser,
):
    await laser.bootstrap(1)
    active = asyncio.Event()
    cancelled = asyncio.Event()

    class Consolidator:
        async def consolidate(self, scope):
            active.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

    async def handle(context, message):
        pass

    agent = laser.spawn_agent(
        "cancel-memory",
        "agent.commands",
        handle,
        consolidate_every_ms=10,
        consolidator=Consolidator(),
    )
    try:
        await agent.ready()
        await asyncio.wait_for(active.wait(), 2)
    finally:
        await agent.shutdown()
    await asyncio.wait_for(cancelled.wait(), 2)


async def test_given_join_waiting_when_the_agent_is_running_then_should_keep_consolidation_active(
    laser,
):
    await laser.bootstrap(1)
    passes = []

    class Consolidator:
        async def consolidate(self, scope):
            passes.append(scope)

    async def handle(context, message):
        pass

    agent = laser.spawn_agent(
        "join-memory",
        "agent.commands",
        handle,
        consolidate_every_ms=10,
        consolidator=Consolidator(),
    )
    await agent.ready()
    waiting = asyncio.ensure_future(agent.join())
    try:
        first = len(passes)
        deadline = asyncio.get_running_loop().time() + 2
        while len(passes) <= first:
            assert asyncio.get_running_loop().time() < deadline
            await asyncio.sleep(0.01)
        assert not waiting.done()
    finally:
        await agent.shutdown()
        await asyncio.wait_for(waiting, 2)


@pytest.mark.parametrize("verb", ["command", "respond", "emit", "status", "fail"])
async def test_given_a_causal_position_when_an_agdx_verb_sends_then_should_retain_every_coordinate(
    laser, verb
):
    conversation = ls.new_conversation_id()
    correlation = ls.new_correlation_id()
    cause = ls.new_correlation_id()
    producer = laser.agdx("agent.audit", "worker", conversation)
    options = {"cause": cause, "cause_at": (10, 20, 3, 42), "metadata": {"host": "node-7"}}
    if verb == "emit":
        await producer.emit(b"event", **options)
    elif verb == "status":
        await producer.status("task", correlation=correlation, task_state="working", **options)
    elif verb == "fail":
        await producer.fail(
            correlation, {"code": 6, "message": "refused", "retryable": False}, **options
        )
    else:
        await getattr(producer, verb)(correlation, b"body", **options)
    records = await laser.assemble_context(conversation, topics=["agent.audit"])
    assert len(records) == 1
    envelope = records[0].envelope
    assert envelope["cause"] == cause
    assert envelope["cause_at"] == b"".join(
        value.to_bytes(width, "big") for value, width in [(10, 4), (20, 4), (3, 4), (42, 8)]
    )


@pytest.mark.parametrize("verb", ["command", "respond", "emit", "status", "fail"])
async def test_given_cause_at_without_cause_when_sending_then_should_refuse_before_publish(
    laser, verb
):
    producer = laser.agdx("agent.audit", "worker", ls.new_conversation_id())
    options = {"cause_at": (10, 20, 3, 42)}
    with pytest.raises(ls.InvalidError, match="cause_at requires cause"):
        if verb == "emit":
            producer.emit(b"event", **options)
        elif verb == "status":
            producer.status("card", **options)
        elif verb == "fail":
            producer.fail(ls.new_correlation_id(), {"code": 6}, **options)
        else:
            getattr(producer, verb)(ls.new_correlation_id(), b"body", **options)


@pytest.mark.parametrize(
    "failure",
    [
        "invalid",
        "config",
        "blocked",
        "permission",
        "ambiguous",
        "transient",
        "memory_invalid",
        "memory_ambiguous",
    ],
)
async def test_given_a_python_handler_failure_when_consuming_then_should_preserve_its_retry_class(
    laser, failure
):
    await laser.bootstrap(1)
    calls = []
    dead_lettered = asyncio.Event()
    attempts = []

    class MemoryHooks:
        async def remember(self, scope, payload):
            if failure == "memory_invalid":
                raise ls.InvalidError("memory write refused")
            error = ls.LaserError("memory write outcome unknown")
            error.ambiguous_mutation = True
            error.retryable = False
            raise error

        async def recall(self, scope, query):
            return []

        async def improve(self, scope, feedback):
            return ls.new_conversation_id()

        async def forget(self, scope, target):
            pass

    memory = laser.memory_custom(MemoryHooks())

    async def handle(context, message):
        calls.append(message.payload)
        if failure.startswith("memory_"):
            await memory.remember(b"effect")
            return
        if failure == "ambiguous":
            error = ls.LaserError("mutation result unknown")
            error.ambiguous_mutation = True
            error.retryable = False
            raise error
        errors = {
            "invalid": ls.InvalidError,
            "config": ls.ConfigError,
            "blocked": ls.PolicyBlockedError,
            "permission": PermissionError,
            "transient": RuntimeError,
        }
        raise errors[failure]("handler failed")

    async def dead_letter(message, capsule, publish_error):
        assert publish_error is None
        attempts.append(capsule["attempts"])
        dead_lettered.set()

    agent = laser.spawn_agent(
        "retry-class",
        "agent.commands",
        handle,
        dead_letter=dead_letter,
        retry_max_attempts=3,
        retry_base_delay_ms=1,
    )
    try:
        await agent.ready()
        await laser.send_agent("agent.commands", b"effect", ls.Provenance())
        await asyncio.wait_for(dead_lettered.wait(), 5)
        assert len(calls) == (3 if failure == "transient" else 1)
        assert attempts == [len(calls)]
    finally:
        await agent.shutdown()


async def test_connect_reports_open_capabilities(open_laser):
    laser = open_laser
    caps = await laser.capabilities()
    # Without a managed plane nothing managed is advertised.
    assert caps.query is False
    assert caps.kv is False
    assert caps.forks is False

    refreshed = await laser.refresh_capabilities()
    assert refreshed.managed is False
    assert refreshed.query is False
    assert refreshed.kv is False
    assert refreshed.forks is False
    assert refreshed.backends == []
    # The server answers the probe for its own consumer filters.
    assert refreshed.filters is True
    assert refreshed.filters_catalog is False


async def test_capability_override_survives_refresh(laser):
    original = await laser.capabilities()
    scoped = await laser.with_capabilities(query=True, query_consistency="read_your_writes")

    refreshed = await scoped.refresh_capabilities()

    assert refreshed.managed == original.managed
    assert refreshed.query is True
    assert refreshed.query_consistency == "read_your_writes"


async def test_topic_ensure_then_publish_single(laser):
    await laser.topic("readings").ensure(partitions=2)
    await (
        laser.topic("readings")
        .publish()
        .index("host_id", "node-7")
        .inline_payload()
        .json({"host": "node-7", "cpu": 82})
        .send()
    )


async def test_publish_batch_returns_commit_confirmation(laser):
    await laser.topic("events").ensure(partitions=1)
    committed = await (
        laser.topic("events")
        .publish_batch()
        .inline_payload()
        .extend_json([{"n": 1}, {"n": 2}, {"n": 3}])
        .send()
    )
    assert len(committed.confirmations) == 1
    assert committed.confirmations[0].partition_id == 0


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
    first_send = await producer.send(
        b"one",
        headers={"kind": ("uint16", 7), "source": "python"},
        key=b"account-42",
    )
    batch_send = await producer.send_batch(
        [(b"two", {"kind": 8}), b"three"],
        partition=0,
    )
    assert len(first_send.confirmations) == 1
    assert first_send.confirmations[0].partition_id == 0
    assert len(batch_send.confirmations) == 1
    assert batch_send.confirmations[0].partition_id == 0
    assert batch_send.confirmations[0].base_offset > first_send.confirmations[0].base_offset

    uncommitted = topic.consumer_group("uncommitted-workers").consumer(
        poll_interval_ms=1,
        polling="next",
        auto_commit="disabled",
    )
    try:
        first = await asyncio.wait_for(uncommitted.next(), timeout=10)
    finally:
        await uncommitted.shutdown()
    uncommitted = topic.consumer_group("uncommitted-workers").consumer(
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

    consumer = topic.consumer_group("manual-workers").consumer(
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
    resumed = topic.consumer_group("manual-workers").consumer(
        poll_interval_ms=1,
        polling="next",
        auto_commit="disabled",
    )
    try:
        message = await asyncio.wait_for(resumed.next(), timeout=10)
        assert message.payload == b"manual-resumed"
    finally:
        await resumed.shutdown()

    auto_consumer = topic.consumer_group("auto-workers").consumer(
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
        # The next read completes the preceding delivery before it polls again.
        waiting = asyncio.ensure_future(auto_consumer.next())
        try:
            async with asyncio.timeout(10):
                while (
                    await asyncio.wait_for(auto_consumer.last_stored_offset(0), timeout=1)
                    != last.offset
                ):
                    await asyncio.sleep(0.01)
        finally:
            waiting.cancel()
            await asyncio.gather(waiting, return_exceptions=True)
    finally:
        await auto_consumer.shutdown()

    await producer.send(b"auto-resumed")
    resumed = topic.consumer_group("auto-workers").consumer(
        poll_interval_ms=1,
        polling="next",
        auto_commit="disabled",
    )
    try:
        message = await asyncio.wait_for(resumed.next(), timeout=10)
        assert message.payload == b"auto-resumed"
    finally:
        await resumed.shutdown()


async def test_query_against_raw_iggy_is_unsupported(open_laser):
    laser = open_laser
    with pytest.raises(ls.UnsupportedError) as caught:
        await laser.query("readings").where_eq("host_id", "node-7").fetch()
    assert caught.value.unsupported is True


async def test_query_values_accept_the_full_typed_input_surface(open_laser):
    laser = open_laser
    request = (
        laser.query("readings")
        .where_eq("flag", True)
        .where_eq("count", 7)
        .where_eq("ratio", 1.5)
        .where_eq("name", "alice")
        .where_eq("payload", b"\x00\xff")
        .where_eq("reading_id", uuid.UUID("00112233-4455-6677-8899-aabbccddeeff"))
        .where_eq("load", decimal.Decimal("123.45"))
        .where_eq("day", datetime.date(2026, 8, 13))
        .where_eq("at", datetime.time(23, 59, 59, 999999))
        .where_eq("created", datetime.datetime(2026, 8, 13, 12, 0, 0))
        .where_eq(
            "created_utc",
            datetime.datetime(2026, 8, 13, 12, 0, 0, tzinfo=datetime.timezone.utc),
        )
    )
    with pytest.raises(ls.UnsupportedError):
        await request.fetch()


async def test_query_values_reject_non_canonical_typed_input(laser):
    request = laser.query("readings")
    with pytest.raises(ls.InvalidError):
        request.where_eq("load", decimal.Decimal("NaN"))
    with pytest.raises(ls.InvalidError):
        request.where_eq("load", decimal.Decimal(10) ** 40)
    with pytest.raises(ls.InvalidError):
        request.where_eq("at", datetime.time(1, 2, 3, tzinfo=datetime.timezone.utc))
    with pytest.raises(ls.InvalidError):
        request.where_eq("value", {"nested": 1})


async def test_kv_against_raw_iggy_is_unsupported(open_laser):
    laser = open_laser
    with pytest.raises(ls.UnsupportedError):
        await laser.kv("sessions").get("user:1")


async def test_fork_against_raw_iggy_is_unsupported(open_laser):
    laser = open_laser
    with pytest.raises(ls.UnsupportedError):
        await laser.fork("exp-1").create()


async def test_runs_against_raw_iggy_are_unsupported(open_laser):
    laser = open_laser
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

    with pytest.raises(ls.LaserError, match="cannot build task"):
        await workflow.run()


async def test_graph_against_raw_iggy_is_unsupported(open_laser):
    laser = open_laser
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


async def test_given_a_conversation_when_fetched_under_a_token_budget_then_should_bound_the_turns(
    laser,
):
    await laser.bootstrap(partitions=2)
    conversation = ls.new_conversation_id()
    ctx = laser.context(conversation)
    topics = [ls.Topics.COMMANDS, ls.Topics.RESPONSES]

    await ctx.append(ls.Topics.COMMANDS, b"drain node-7")
    await ctx.append(ls.Topics.RESPONSES, b"drained, 0 connections left")

    generous = await ctx.fetch(topics=topics, last_n=20, token_budget=4_000)
    assert len(generous) == 2

    # One token holds neither turn, so the budget keeps only the newest.
    starved = await ctx.fetch(topics=topics, last_n=20, token_budget=1)
    assert [m.payload for m in starved] == [b"drained, 0 connections left"]

    block = await ctx.block(topics=topics, last_n=20, token_budget=4_000)
    assert "0 connections left" in block


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


@pytest.mark.parametrize("batch", [6000, 20000])
async def test_given_large_batches_when_replaying_then_should_bound_reads_and_resume_without_gaps(
    laser, batch
):
    topic = laser.topic("bounded-replay")
    producer = topic.producer(partition=0, partitions=1, batch_length=1000)
    await producer.init()
    await producer.send_batch([str(index).encode() for index in range(10001)], partition=0)
    cursor = topic.replay(batch=batch)
    messages = await cursor.poll()
    assert len(messages) == 10000
    assert cursor.offsets == [10000]
    remaining = await cursor.poll()
    assert [bytes(message.payload) for message in remaining] == [b"10000"]
    assert cursor.offsets == [10001]


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
        body=b"rotate the storage credentials",
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
    assert bytes(decoded_intent.body) == b"rotate the storage credentials"
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
    # Avro encoding is client-side, so the publish path works on Apache Iggy
    # (only the managed projection of the body needs laser-plane). Encode a
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
    # A bare send_agent to a well-known topic should succeed on Iggy.
    await laser.send_agent("agent.audit", b"audit-record", provenance)


async def test_given_agdx_when_status_and_errors_are_sent_then_should_publish_every_terminal(
    laser,
):
    await laser.bootstrap(partitions=1)
    conversation = ls.new_conversation_id()
    correlation = ls.new_correlation_id()
    agdx = laser.agdx("agent.responses", "worker", conversation)

    status = await agdx.status(
        "task",
        correlation=correlation,
        task_state="working",
    )
    failed = await agdx.fail(
        correlation,
        {"code": 6, "message": "tool failed", "retryable": True},
    )
    stream = agdx.stream(ls.new_correlation_id(), "chat")
    await stream.write(b"partial")
    await stream.fail({"code": 7, "message": "stream failed", "retryable": False})

    assert status is not None
    assert failed is not None


@pytest.mark.parametrize("seconds", [-1.0, float("inf"), float("-inf"), float("nan")])
async def test_given_invalid_duration_when_used_then_should_raise_without_panicking(laser, seconds):
    request = laser.agdx(
        "agent.commands",
        "requester",
        ls.new_conversation_id(),
    ).request_input("agent.responses", b"input", timeout_secs=seconds)
    with pytest.raises(ls.InvalidError, match="finite, non-negative"):
        await request

    kv_set = laser.kv("duration-validation").set("key").payload(b"value").ttl(seconds)
    with pytest.raises(ls.InvalidError, match="finite, non-negative"):
        await kv_set.send()


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
    # Log-backed memory is the source-of-truth path, so it works on Apache Iggy.
    await laser.bootstrap(partitions=1)
    conversation = ls.new_conversation_id()
    memory = laser.memory("notes")
    await memory.remember("the database pool was exhausted", conversation=conversation)
    await memory.remember("auth latency spiked at noon", conversation=conversation)

    # Recall folds the topic in process (the opt-in path), since Apache Iggy
    # serves no key-value read view for the default recall to read.
    recalled = None
    for _ in range(40):
        recalled = await memory.recall(conversation=conversation, limit=10, folded=True)
        if len(recalled) >= 2:
            break
        await asyncio.sleep(0.25)
    assert recalled is not None and len(recalled) == 2
    bodies = {item.text for item in recalled}
    assert "auth latency spiked at noon" in bodies
    # A different conversation recalls nothing.
    assert await memory.recall(conversation=ls.new_conversation_id(), folded=True) == []


async def test_vector_memory_ranks_by_semantic_similarity(laser):
    # In-process semantic memory: a deterministic bag-of-words embedder, so recall
    # ranks by overlap with the query. No server round-trip, but built off a Laser.
    vocabulary = ["database", "pool", "auth", "latency", "storage", "rotation", "noon", "spike"]

    async def embed(text: str) -> list[float]:
        words = set(text.lower().split())
        return [1.0 if term in words else 0.0 for term in vocabulary]

    memory = laser.vector_memory(embed)
    conversation = ls.new_conversation_id()
    await memory.remember("database pool exhaustion", conversation=conversation)
    await memory.remember("storage rotation retried twice", conversation=conversation)
    await memory.remember("auth latency spike at noon", conversation=conversation)

    top = await memory.recall(conversation=conversation, semantic="auth latency", limit=1)
    assert len(top) == 1
    assert top[0].text == "auth latency spike at noon"

    rotation = await memory.recall(conversation=conversation, semantic="rotation", limit=1)
    assert rotation[0].text == "storage rotation retried twice"


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
            "node-7 prefers eu-west [skew:fabricate_memory]",
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
    dog = await memory.remember("the dog ran", conversation=conversation)
    await memory.remember("the cat sat", conversation=conversation)

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


@pytest.mark.parametrize("route_at_spawn", [False, True])
async def test_fan_out_gathers_every_capable_agents_reply(laser, iggy_endpoint, route_at_spawn):
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
            capabilities=[
                {"skill_id": "diagnose", "cost_class": 2, "latency_class": 1, "load": 10}
            ],
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
            fixed_inbox=None if route_at_spawn else "agent.commands",
        )

    orchestrator = laser.spawn_agent(
        "orchestrator",
        "agent.tool_calls",
        orchestrate,
        respond_on="agent.responses",
        fixed_inbox="agent.commands" if route_at_spawn else None,
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


async def test_verified_agent_dispatches_only_signed_commands_with_the_principal(laser):
    await laser.bootstrap(partitions=2)
    caller_key = ls.SigningKey(bytes([21]) * 32)
    registry = ls.KeyRegistry()
    registry.enroll("caller", caller_key.verifying_key)

    principals = []

    async def handle(ctx, message):
        principals.append(message.verified_principal)

    agent = laser.spawn_agent(
        "verified-worker", ls.Topics.COMMANDS, handle, poll_interval_ms=10, verifier=registry
    )
    try:
        await agent.ready()
        conversation = ls.Provenance(agent="caller").conversation_id
        unsigned = laser.agdx(ls.Topics.COMMANDS, "caller", conversation)
        signed = laser.agdx(ls.Topics.COMMANDS, "caller", conversation, signing_key=caller_key)
        # Same conversation, so the partition is ordered: the unsigned forgery
        # arrives first and must dead-letter, then the signed command dispatches
        # with the enrolled principal.
        await unsigned.command(conversation, b"forged-unsigned")
        await signed.command(conversation, b"signed")
        for _ in range(50):
            if principals:
                break
            await asyncio.sleep(0.2)
        assert principals == ["caller"]
    finally:
        await agent.shutdown()


async def test_request_input_resumes_only_on_a_signed_response(iggy_endpoint):
    approver_key = ls.SigningKey(bytes([51]) * 32)
    registry = ls.KeyRegistry()
    registry.enroll("approver", approver_key.verifying_key)
    stream = f"t-{uuid.uuid4().hex[:12]}"
    caller = await ls.Laser.connect(iggy_endpoint, stream=stream, verifier=registry)
    await caller.bootstrap(partitions=2)

    async def forge(ctx, message):
        await ctx.respond_input(ls.Topics.RESPONSES, b"forged")

    async def approve(ctx, message):
        await ctx.respond_input(ls.Topics.RESPONSES, b"approved-signed")

    # An unsigned approver answers every interrupt, but its response cannot
    # verify, so the paused caller must keep waiting and time out.
    faker = caller.spawn_agent("faker", ls.Topics.HUMAN_INPUT, forge, poll_interval_ms=10)
    approver = None
    try:
        await faker.ready()
        orchestrator = caller.agdx(
            ls.Topics.HUMAN_INPUT,
            "orchestrator",
            ls.Provenance(agent="orchestrator").conversation_id,
        )
        with pytest.raises(ls.TimeoutError):
            await orchestrator.request_input(ls.Topics.RESPONSES, b"approve?", timeout_secs=2)

        # A signing approver resumes the caller: `respond_input` signs with the
        # agent's key, so the verified reader accepts exactly this decision.
        approver = caller.spawn_agent(
            "approver",
            ls.Topics.HUMAN_INPUT,
            approve,
            poll_interval_ms=10,
            signing_key=approver_key,
        )
        await approver.ready()
        decision = await orchestrator.request_input(
            ls.Topics.RESPONSES, b"approve?", timeout_secs=15
        )
        assert decision == b"approved-signed"
    finally:
        await faker.shutdown()
        if approver is not None:
            await approver.shutdown()


async def test_given_a_session_when_typed_turns_are_appended_then_context_and_checkpoints_read_back(
    laser,
):
    await laser.bootstrap(partitions=2)
    sessions = laser.sessions()
    session = sessions.create("agent-42")
    assert sessions.create("agent-42").conversation == session.conversation
    assert sessions.open(session.conversation).conversation == session.conversation
    assert sessions.start().conversation != sessions.start().conversation

    await session.append("instruction", b"summarize the ticket")
    await session.append("model.response", "it is a login bug")
    turns = None
    for _ in range(100):
        turns = await session.context()
        if len(turns) == 2:
            break
        await asyncio.sleep(0.05)
    assert [(turn.kind, turn.text()) for turn in turns] == [
        ("instruction", "summarize the ticket"),
        ("model.response", "it is a login bug"),
    ]
    assert turns[0].message.topic == ls.Topics.COMMANDS

    checkpoint = await session.checkpoint()
    restored = ls.Checkpoint.from_json(checkpoint.to_json())
    assert len(await session.turns_at(restored)) == 2
    await session.append("tool.result", b"3 comments found")
    since = None
    for _ in range(100):
        since = await session.turns_since(checkpoint)
        if len(since) == 1:
            break
        await asyncio.sleep(0.05)
    assert [turn.text() for turn in since] == ["3 comments found"]
    replayed = await session.replay(checkpoint, [], lambda acc, turn: [*acc, turn.kind])
    assert replayed == ["tool.result"]

    with pytest.raises(ls.InvalidError):
        laser.sessions(topics={"response": ls.Topics.COMMANDS})
    with pytest.raises(ls.InvalidError):
        await session.append("event", b"nope")


async def test_given_a_stream_when_deleted_then_should_report_absence_on_repeat(laser):
    stream = laser.stream(laser.default_stream)
    await stream.ensure()
    assert await stream.delete() is True
    assert await stream.delete() is False


async def test_given_a_closed_laser_when_used_then_should_raise(laser):
    await laser.topic("closing").ensure(partitions=1)
    clone = laser.with_stream(laser.default_stream)
    await laser.close()
    await laser.close()
    with pytest.raises(ls.LaserError):
        await clone.stream(laser.default_stream).ensure()


async def test_given_a_cached_stream_when_deleted_elsewhere_then_should_publish_after_recreation(
    laser, iggy_endpoint
):
    topic = laser.topic("recreated")
    await topic.ensure(partitions=1)
    await topic.publish().json({"generation": 1}).send()
    other = await ls.Laser.connect(iggy_endpoint)
    try:
        assert await other.stream(laser.default_stream).delete() is True
        assert await laser.stream(laser.default_stream).delete() is False
        await topic.ensure(partitions=1)
        result = await topic.publish().json({"generation": 2}).send()
        assert len(result.confirmations) == 1
        assert result.confirmations[0].base_offset == 0
    finally:
        await other.close()
        try:
            await laser.stream(laser.default_stream).delete()
        finally:
            await laser.close()


async def test_given_a_fresh_consumer_when_reading_next_then_should_start_at_zero(laser):
    topic = laser.topic("default-next")
    producer = topic.producer(partition=0, partitions=1)
    await producer.send_batch([b"zero", b"one"])
    first = topic.consumer("fresh", partition=0, batch_length=1, auto_commit="disabled")
    assert (await asyncio.wait_for(first.next(), 10)).offset == 0
    await first.shutdown()
    retried = topic.consumer("fresh", partition=0, batch_length=1, auto_commit="disabled")
    zero = await asyncio.wait_for(retried.next(), 10)
    assert zero.offset == 0
    await retried.commit(zero)
    await retried.shutdown()
    resumed = topic.consumer("fresh", partition=0, batch_length=1, auto_commit="disabled")
    assert (await asyncio.wait_for(resumed.next(), 10)).offset == 1
    await resumed.shutdown()


async def test_given_default_polling_when_shutdown_after_offset_zero_then_should_resume_at_one(
    laser,
):
    topic = laser.topic("default-shutdown-zero")
    producer = topic.producer(partition=0, partitions=1)
    await producer.send_batch([b"zero", b"one", b"two", b"three"])
    first = topic.consumer("partial", partition=0, batch_length=4)
    assert (await asyncio.wait_for(first.next(), 10)).offset == 0
    await first.shutdown()
    resumed = topic.consumer("partial", partition=0, batch_length=4)
    assert (await asyncio.wait_for(resumed.next(), 10)).offset == 1
    await resumed.shutdown()


@pytest.mark.parametrize("group", [False, True])
@pytest.mark.parametrize(
    "mode,interval",
    [
        ("disabled", None),
        ("interval", 60_000),
        ("polling", None),
        ("polling", 60_000),
        ("all", None),
        ("all", 60_000),
        ("each", None),
        ("each", 60_000),
        ("every", None),
        ("every", 60_000),
    ],
)
async def test_given_each_commit_policy_when_shutdown_after_zero_then_should_resume_correctly(
    laser, group, mode, interval
):
    topic = laser.topic("policy-zero")
    producer = topic.producer(partition=0, partitions=1)
    await producer.send(b"zero")
    await producer.send(b"one")
    options = {"auto_commit": mode, "batch_length": 2}
    if interval is not None:
        options["commit_interval_ms"] = interval
    if mode == "every":
        options["commit_every"] = 10
    first = (
        topic.consumer_group("worker").consumer(**options)
        if group
        else topic.consumer("worker", partition=0, **options)
    )
    assert (await asyncio.wait_for(first.next(), 10)).offset == 0
    await first.shutdown()
    resumed = (
        topic.consumer_group("worker").consumer(auto_commit="disabled")
        if group
        else topic.consumer("worker", partition=0, auto_commit="disabled")
    )
    try:
        assert (await asyncio.wait_for(resumed.next(), 10)).offset == (
            0 if mode == "disabled" else 1
        )
    finally:
        await resumed.shutdown()


async def test_given_pending_reads_when_inspected_then_should_allow_cancel_and_shutdown(
    laser,
):
    topic = laser.topic("pending-group-reads")
    await topic.ensure(1)
    producer = topic.producer(partition=0, partitions=1)
    await producer.send(b"initial")
    consumer = topic.consumer_group("pending-read-workers").consumer(
        polling="first", auto_commit="disabled", allow_replay=True
    )
    await consumer.init()
    initial = await asyncio.wait_for(consumer.next(), timeout=10)
    assert initial.payload == b"initial"
    first = asyncio.ensure_future(consumer.next())
    second = asyncio.ensure_future(consumer.next())
    try:
        await asyncio.wait_for(consumer.last_consumed_offset(0), timeout=1)
        await asyncio.wait_for(consumer.commit(initial), timeout=1)
        await producer.send(b"first")
        done, _ = await asyncio.wait(
            {first, second}, timeout=10, return_when=asyncio.FIRST_COMPLETED
        )
        assert len(done) == 1
        completed = done.pop()
        delivered = completed.result()
        assert delivered.payload == b"first"
        await asyncio.wait_for(consumer.last_stored_offset(0), timeout=1)
        remaining = second if completed is first else first
        remaining.cancel()
        await asyncio.gather(remaining, return_exceptions=True)
        await producer.send(b"second")
        delivered = await asyncio.wait_for(consumer.next(), timeout=10)
        assert delivered.payload == b"second"
        waiting = asyncio.ensure_future(consumer.next())
        await asyncio.wait_for(consumer.shutdown(), timeout=1)
        assert await asyncio.wait_for(waiting, timeout=1) is None
    finally:
        first.cancel()
        second.cancel()
        await asyncio.gather(first, second, return_exceptions=True)
        await consumer.shutdown()


async def test_kv_send_refuses_a_precondition(laser):
    request = laser.kv("config").set("service:auth").json({"log_level": "debug"}).expect_absent()
    with pytest.raises(ls.InvalidError):
        await request.send()


async def test_next_within_raises_timeout_when_nothing_arrives(laser):
    topic = laser.topic("quiet")
    await topic.ensure(partitions=1)
    consumer = topic.consumer("quiet-reader")
    try:
        with pytest.raises(ls.TimeoutError):
            await consumer.next_within(0.5)
    finally:
        await consumer.shutdown()


async def test_watch_is_unsupported_when_opened_on_raw_iggy(open_laser):
    laser = open_laser
    with pytest.raises(ls.UnsupportedError):
        laser.watch(index="readings_v1")


async def test_topic_send_batch_and_batching_publish_raw_payloads(laser):
    topic = laser.topic("raw")
    await topic.ensure(partitions=1)
    sent = await topic.send(b"one", headers={"kind": "metric"}, partition_key="node-7")
    assert isinstance(sent, ls.SendMessagesResponse)
    await topic.batch([b"two", b"three"], partition_key="node-7")
    batching = topic.batching(max_records=2, linger_ms=50)
    await batching.send(b"four")
    await batching.send(b"five")
    await batching.close()
    cursor = topic.replay()
    payloads = []
    for _ in range(40):
        payloads.extend(bytes(message.payload) for message in await cursor.poll())
        if len(payloads) >= 5:
            break
        await asyncio.sleep(0.25)
    assert payloads == [b"one", b"two", b"three", b"four", b"five"]


async def test_context_defaults_topics_and_folds_to_a_checkpoint(laser):
    await laser.bootstrap(partitions=1)
    ctx = laser.context(ls.new_conversation_id())
    await ctx.append(ls.Topics.COMMANDS, b"drain node-7")
    await ctx.append(ls.Topics.RESPONSES, b"drained, 0 connections left")

    history = []
    for _ in range(40):
        history = await ctx.fetch()
        if len(history) == 2:
            break
        await asyncio.sleep(0.25)
    assert [m.payload for m in history] == [b"drain node-7", b"drained, 0 connections left"]

    newest = await ctx.fetch_with(
        [ls.Topics.COMMANDS, ls.Topics.RESPONSES], ls.Chain([ls.LastN(5), ls.TokenBudget(1)])
    )
    assert [m.payload for m in newest] == [b"drained, 0 connections left"]

    checkpoint = await ctx.checkpoint()
    await ctx.append(ls.Topics.COMMANDS, b"restore node-7")
    count = await ctx.state(
        [ls.Topics.COMMANDS, ls.Topics.RESPONSES], 0, lambda total, _: total + 1, at=checkpoint
    )
    assert count == 2


async def test_background_producer_flushes_on_shutdown_and_then_refuses_sends(laser):
    topic = laser.topic("background-readings")
    await topic.ensure(partitions=1)
    producer = topic.producer(partition=0, background=True, background_shards=1, linger_ms=5)
    for index in range(5):
        await producer.send(f"reading-{index}".encode())
    await producer.shutdown()
    with pytest.raises(ls.InvalidError):
        await producer.send(b"late")

    cursor = topic.replay()
    seen = []
    for _ in range(50):
        seen.extend(await cursor.poll())
        if len(seen) >= 5:
            break
        await asyncio.sleep(0.1)
    assert [bytes(message.payload) for message in seen] == [
        f"reading-{index}".encode() for index in range(5)
    ]


async def test_cbor_typed_topic_round_trips_a_dataclass(laser):
    from dataclasses import dataclass

    @dataclass
    class Reading:
        host: str
        cpu: int

    topic = laser.topic("cbor-readings").cbor(Reading)
    await laser.topic("cbor-readings").ensure(partitions=1)
    await topic.publish(Reading("node-7", 82)).send()
    reader = topic.records("cbor-reader")
    record = None
    for _ in range(50):
        record = await reader.next()
        if record is not None:
            break
        await asyncio.sleep(0.1)
    assert record is not None
    assert record.value == Reading("node-7", 82)


async def test_custom_memory_backend_receives_every_verb(laser):
    class ListMemory:
        def __init__(self):
            self.items = []

        async def remember(self, scope, payload):
            item_id = ls.new_conversation_id()
            self.items.append(
                {"id": item_id, "payload": payload, "conversation": scope["conversation"]}
            )
            return item_id

        def recall(self, scope, query):
            return [item for item in self.items if item["conversation"] == scope["conversation"]][
                : query["limit"]
            ]

        async def improve(self, scope, feedback):
            return feedback["target"]

        async def forget(self, scope, item_id):
            self.items = [item for item in self.items if item["id"] != item_id]

    backend = ListMemory()
    memory = laser.memory_custom(backend)
    conversation = ls.new_conversation_id()
    assert memory.backend_name == "custom"
    item_id = await memory.remember("auth is slow", conversation=conversation)
    items = await memory.recall(conversation=conversation)
    assert [item.text for item in items] == ["auth is slow"]
    assert items[0].id == item_id
    await memory.forget(item_id, conversation=conversation)
    assert await memory.recall(conversation=conversation) == []


async def test_capabilities_report_open_only_and_take_injected_versions(laser):
    caps = await laser.capabilities()
    assert caps.is_open_only() in (True, False)
    assert caps.serves_consistency("eventual") is True
    with pytest.raises(ls.InvalidError):
        caps.serves_consistency("sometimes")
    scoped = await laser.with_capabilities(
        query=True,
        query_execution=(True, True, True),
        versions=ls.OpVersions(query=3),
    )
    injected = await scoped.capabilities()
    assert injected.versions is not None
    assert injected.versions.query == 3
    assert injected.is_open_only() is False


async def test_agdx_stream_buffers_until_flush(laser):
    await laser.bootstrap(partitions=1)
    conversation = ls.new_conversation_id()
    agdx = laser.agdx("agent.responses", "worker", conversation)
    stream = agdx.stream(ls.new_correlation_id(), "chat")
    stream.buffered(8, 60_000).content_type("json").with_deadline_micros(5_000_000).with_target(
        "reader"
    )
    assert stream.channel
    await stream.write(b'{"token": "auth"}')
    await stream.flush()
    await stream.finish()
    with pytest.raises(ls.LaserError):
        stream.buffered(1, 1)


async def test_session_context_with_takes_a_policy_and_reaches_the_graph(laser):
    await laser.bootstrap(partitions=1)
    session = laser.sessions().start()
    await session.append("instruction", b"drain node-7")
    await session.append("response", b"drained, 0 connections left")
    turns = []
    for _ in range(100):
        turns = await session.context_with(ls.LastN(1))
        if turns:
            break
        await asyncio.sleep(0.05)
    assert [turn.text() for turn in turns] == ["drained, 0 connections left"]
    assert session.graph("kg") is not None


async def test_fork_create_refuses_severed_and_continuous_together(laser):
    with pytest.raises(ls.InvalidError):
        await laser.fork("readings-plan").create(severed=True, continuous=True)


async def test_given_concurrent_first_sends_when_batching_then_should_initialize_once(laser):
    topic = laser.topic("concurrent-batches")
    await topic.ensure(partitions=1)
    producer = topic.batching(max_records=100, linger_ms=60_000)
    await asyncio.gather(*(producer.send(str(i).encode()) for i in range(20)))
    await producer.close()
    with pytest.raises(ls.InvalidError):
        await producer.send(b"closed")
    cursor = topic.replay()
    messages = []
    for _ in range(50):
        messages.extend(await cursor.poll())
        if len(messages) == 20:
            break
        await asyncio.sleep(0.05)
    assert sorted(int(message.payload) for message in messages) == list(range(20))


async def test_given_an_unused_batcher_when_closed_then_should_refuse_initialization(laser):
    producer = laser.topic("closed-batches").batching()
    await producer.close()
    await producer.close()
    with pytest.raises(ls.InvalidError):
        await producer.send(b"closed")
    with pytest.raises(ls.InvalidError):
        await producer.flush()


async def test_given_a_cbor_handle_when_published_without_a_body_then_should_refuse(laser):
    with pytest.raises(ls.InvalidError, match="requires a body"):
        laser.topic("readings").cbor().publish()


async def test_given_an_agent_scope_when_contract_options_are_set_then_should_retain_identity(
    laser,
):
    await laser.bootstrap(1)
    conversation = ls.new_conversation_id()
    seen = []

    async def handle(context, message):
        seen.append((message.conversation_id, message.provenance.fence_token))
        await context.respond(b"done")

    agent = laser.spawn_agent(
        "scoped-worker", "agent.commands", handle, respond_on="agent.responses"
    )
    try:
        await agent.ready()
        result = await laser.agent("scoped-caller").contract(
            "scoped-worker",
            b"work",
            fixed_inbox="agent.commands",
            conversation=conversation,
            fence=7,
            reply_on="agent.responses",
            deadline_ms=2_000,
        )
        assert result["state"] == "completed"
        assert result["body"] == b"done"
        assert seen == [(conversation, 7)]
    finally:
        await agent.shutdown()


@pytest.mark.parametrize("placement", ["namespace", "explicit_stream"])
async def test_given_scoped_log_memory_when_feedback_and_forget_run_then_should_keep_scope(
    laser, placement
):
    await laser.bootstrap(1)
    if placement == "namespace":
        left = laser.memory("left")
        right = laser.memory("right")
    else:
        stream = f"memory-{uuid.uuid4().hex[:12]}"
        await laser.with_stream(stream).bootstrap(1)
        left = laser.memory_on_topic("agent.audit", stream=stream)
        right = laser.memory_on_topic("agent.audit")
    scope = {
        "conversation": ls.new_conversation_id(),
        "agent": "notetaker",
        "user": "reader",
        "application": "diagnostics",
    }
    left_id = await left.remember("left", **scope)
    right_id = await right.remember("right", **scope)
    await left.improve(left_id, 4.0, **scope)
    for _ in range(40):
        improved = await left.recall(folded=True, **scope)
        if len(improved) == 1 and improved[0].score == 4.0:
            break
        await asyncio.sleep(0.05)
    assert len(improved) == 1
    assert improved[0].id == left_id
    assert improved[0].score == 4.0
    assert await left.recall(folded=True, **{**scope, "user": "another-reader"}) == []
    assert await left.recall(folded=True, **{**scope, "application": "another-app"}) == []
    await left.forget(left_id, **scope)
    for _ in range(40):
        remaining = await left.recall(folded=True, **scope)
        if not remaining:
            break
        await asyncio.sleep(0.05)
    assert remaining == []
    for _ in range(40):
        untouched = await right.recall(folded=True, **scope)
        if len(untouched) == 1:
            break
        await asyncio.sleep(0.05)
    assert len(untouched) == 1
    assert untouched[0].id == right_id
    assert untouched[0].score is None
