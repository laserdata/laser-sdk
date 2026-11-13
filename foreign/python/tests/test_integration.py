import asyncio
import datetime
import decimal
import json
import time
import uuid
from urllib.parse import unquote, urlsplit

import laser_sdk as ls
import pytest

pytestmark = pytest.mark.integration


async def test_given_an_absent_stream_when_a_producer_starts_then_should_require_creation_opt_in(
    iggy_endpoint,
):
    stream = f"creation-{uuid.uuid4().hex[:12]}"
    laser = await ls.Laser.connect(iggy_endpoint, stream=stream)
    try:
        with pytest.raises(ls.TransportError, match="was not found"):
            await laser.topic("events").producer().init()
        response = await laser.topic("events").producer(create_stream=True).send(b"created")
        assert len(response.confirmations) == 1
    finally:
        await laser.close()


async def test_given_a_missing_topic_or_stream_when_published_then_should_create_it(laser):
    published = await laser.topic("new-topic").publish().payload(b"created").send()
    assert len(published.confirmations) == 1
    absent = laser.stream(f"{laser.default_stream}-absent")
    try:
        sent = await absent.topic("events").send(b"created")
        assert len(sent.confirmations) == 1
        memory = laser.memory_on_topic("new.memory")
        assert await memory.fetch_folded("host:123") is None
        with pytest.raises(ls.TransportError, match="was not found"):
            await laser.topic("new.memory").producer(create_topic=False).init()
        await memory.set("host:123", b"created")
        assert await memory.fetch_folded("host:123") == b"created"
    finally:
        await absent.delete()


@pytest.mark.parametrize("factor", [0, ls.MAX_REPLICATION_FACTOR + 1])
async def test_given_a_replication_factor_outside_one_to_thirty_two_when_creating_a_topic_then_should_refuse_it_before_sending(  # noqa: E501
    laser, factor
):
    topic = laser.topic("replicated")
    with pytest.raises(ls.InvalidError, match=r"^invalid: replication factor must be 1\.\.=32$"):
        await topic.ensure(3, replication_factor=factor)
    with pytest.raises(ls.InvalidError, match=r"^invalid: replication factor must be 1\.\.=32$"):
        await topic.producer(partitions=3, replication_factor=factor).init()
    with pytest.raises(ls.TransportError, match="was not found"):
        await topic.producer(create_topic=False).init()


def test_given_signing_keys_when_enrolled_then_should_expose_their_identity():
    key = ls.SigningKey(bytes(range(32)))
    operator_key = ls.SigningKey(bytes(reversed(range(32))))
    registry = ls.KeyRegistry()
    registry.enroll("agent-7", key.verifying_key)
    registry.enroll_operator("operator-9", operator_key.verifying_key)
    record = ls.KeyRecord("agent-7", key.verifying_key)

    assert len(key.key_id) == 8
    assert len(key.verifying_key) == 32
    assert record.verifying == key.verifying_key


def test_given_a_short_seed_when_building_a_key_then_should_raise_value_error():
    with pytest.raises(ValueError, match="exactly 32"):
        ls.SigningKey(b"short")

    with pytest.raises(ValueError, match="exactly 32"):
        ls.KeyRecord("agent-7", b"short")


@pytest.mark.parametrize("stop", ["shutdown", "context"])
async def test_given_periodic_consolidation_when_the_agent_stops_then_should_stop_background_passes(
    laser, stop
):
    await laser.bootstrap(1, retention=ls.TopicRetention.expire_after(86_400_000))
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
        "agent.sessions",
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
    assert passes[0] == {
        "stream": None,
        "agent": "periodic-memory",
        "conversation": None,
        "user": None,
        "application": None,
        "lifetime": "session",
    }


async def test_given_an_active_consolidation_pass_when_shutdown_then_should_cancel_the_callback(
    laser,
):
    await laser.bootstrap(1, retention=ls.TopicRetention.expire_after(86_400_000))
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
        "agent.sessions",
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
    await laser.bootstrap(1, retention=ls.TopicRetention.expire_after(86_400_000))
    passes = []

    class Consolidator:
        async def consolidate(self, scope):
            passes.append(scope)

    async def handle(context, message):
        pass

    agent = laser.spawn_agent(
        "join-memory",
        "agent.sessions",
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
    correlation = ls.mint_ulid()
    cause = ls.mint_ulid()
    producer = laser.agdx("agent.audit", "worker", conversation)
    options = {
        "cause": cause,
        "cause_at": ls.LogPosition(10, 20, 3, 42),
        "metadata": {"host": "node-7"},
    }
    # Each verb also carries the send options its envelope kind admits, so the
    # read-back proves the binding forwards them instead of dropping them.
    usage = {"input_tokens": 3, "output_tokens": 5}
    deadline = time.time_ns() // 1_000 + 60_000_000
    options |= {
        "command": {"deadline_at_micros": deadline, "idempotency_key": "audit-1", "tool": "probe"},
        "respond": {"idempotency_key": "audit-1", "tool": "probe", "usage": usage},
        "emit": {"idempotency_key": "audit-1", "tool": "probe", "usage": usage},
        "status": {"usage": usage},
        "fail": {"tool": "probe", "usage": usage},
    }[verb]
    if verb == "emit":
        await producer.emit(b"event", **options)
    elif verb == "status":
        await producer.status(
            "task", correlation=correlation, task_state=ls.TaskState.Working, **options
        )
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
    assert ls.LogPosition.from_bytes(bytes(envelope["cause_at"])) == ls.LogPosition(10, 20, 3, 42)
    assert envelope["metadata"] == {"host": "node-7"}
    if "deadline_at_micros" in options:
        assert envelope["deadline_micros"] == options["deadline_at_micros"]
    for key in ("idempotency_key", "tool", "usage"):
        if key in options:
            assert envelope[key] == options[key]


@pytest.mark.parametrize("verb", ["command", "respond", "emit", "status", "fail"])
async def test_given_cause_at_without_cause_when_sending_then_should_refuse_before_publish(
    laser, verb
):
    producer = laser.agdx("agent.audit", "worker", ls.new_conversation_id())
    options = {"cause_at": ls.LogPosition(10, 20, 3, 42)}
    with pytest.raises(ls.InvalidError, match="cause_at requires cause"):
        if verb == "emit":
            producer.emit(b"event", **options)
        elif verb == "status":
            producer.status("card", **options)
        elif verb == "fail":
            producer.fail(ls.mint_ulid(), {"code": 6}, **options)
        else:
            getattr(producer, verb)(ls.mint_ulid(), b"body", **options)


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
    await laser.bootstrap(1, retention=ls.TopicRetention.expire_after(86_400_000))
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
        "agent.sessions",
        handle,
        dead_letter=dead_letter,
        retry_max_attempts=3,
        retry_base_delay_ms=1,
    )
    try:
        await agent.ready()
        await laser.send_agent("agent.sessions", b"effect", ls.Provenance())
        await asyncio.wait_for(dead_lettered.wait(), 5)
        assert len(calls) == (3 if failure == "transient" else 1)
        assert attempts == [len(calls)]
    finally:
        await agent.shutdown()


async def test_given_open_iggy_when_connecting_then_should_report_open_capabilities(open_laser):
    laser = open_laser
    caps = await laser.capabilities()
    # Without a managed backend nothing managed is advertised.
    assert caps.query.available is False
    assert caps.kv.available is False
    assert caps.forks is False

    refreshed = await laser.refresh_capabilities()
    assert refreshed.managed is False
    assert refreshed.query.available is False
    assert refreshed.kv.available is False
    assert refreshed.forks is False
    assert refreshed.backends == []
    assert refreshed.filters.native == caps.filters.native
    assert refreshed.filters.group_policy_reads == caps.filters.group_policy_reads
    assert refreshed.filters.catalog is False


@pytest.mark.parametrize("allow_replay", [False, True])
async def test_given_native_offsets_when_rewound_and_deleted_then_should_follow_replay(
    laser, allow_replay
):
    topic = laser.topic("native-offset-history")
    await topic.producer(partitions=1).send_batch([b"zero", b"one", b"two"])
    consumer = topic.consumer(
        "native-offset-history",
        partition=0,
        polling="first",
        auto_commit="disabled",
        allow_replay=allow_replay,
    )
    try:
        records = [await asyncio.wait_for(consumer.next(), 10) for _ in range(3)]
        await consumer.commit(records[2])
        await consumer.commit(records[1])
        assert await consumer.last_stored_offset(0) == 2, "a commit never moves the offset back"
        expected = 1 if allow_replay else 2
        await consumer.store_offset(2, partition=0)
        await consumer.store_offset(2, partition=0)
        await consumer.store_offset(1, partition=0)
        stored = await consumer.stored_offset(0)
        assert stored is not None
        assert stored.stored_offset == expected
        await consumer.delete_offset(partition=0)
        assert await consumer.stored_offset(0) is None
        assert await consumer.last_stored_offset(0) is None
        await consumer.store_offset(1, partition=0)
        stored = await consumer.stored_offset(0)
        assert stored is not None
        assert stored.stored_offset == 1
        await consumer.store_offset(0, partition=0)
        rewound = 0 if allow_replay else 1
        stored = await consumer.stored_offset(0)
        assert stored is not None
        assert stored.stored_offset == rewound
        assert await consumer.last_stored_offset(0) == rewound
    finally:
        await consumer.shutdown()


async def test_given_the_default_policy_when_a_consumer_stops_without_shutdown_while_holding_a_record_then_should_deliver_it_again(  # noqa: E501
    laser,
):
    topic = laser.topic("default-crash")
    await topic.producer(partitions=1).send_batch([b"zero", b"one", b"two", b"three"])
    crashed = topic.consumer("crashed", partition=0, batch_length=4)
    held = [(await asyncio.wait_for(crashed.next(), 10)).position.offset for _ in range(2)]
    assert held == [0, 1]
    del crashed

    restarted = topic.consumer("crashed", partition=0, batch_length=4)
    try:
        record = await asyncio.wait_for(restarted.next(), 10)
        assert record.position.offset == 0, "nothing of the poll was stored"
    finally:
        await restarted.shutdown()


async def test_given_a_native_consumer_when_it_seeks_back_then_should_read_from_there_and_store_nothing_for_records_read_before(  # noqa: E501
    laser,
):
    topic = laser.topic("native-seek")
    await topic.producer(partitions=1).send_batch([b"zero", b"one", b"two", b"three"])
    consumer = topic.consumer("seeker", partition=0, batch_length=4, auto_commit="disabled")
    try:
        read_before = [await asyncio.wait_for(consumer.next(), 10) for _ in range(3)]
        await consumer.seek(0, offset=1)
        sought = await asyncio.wait_for(consumer.next(), 10)
        assert sought.position.offset == 1
        await consumer.commit(read_before[2])
        assert await consumer.stored_offset(0) is None
        await consumer.commit(sought)
        stored = await consumer.stored_offset(0)
        assert stored is not None
        assert stored.stored_offset == 1
    finally:
        await consumer.shutdown()


async def test_given_typed_headers_when_replayed_then_should_keep_each_value_with_its_type(laser):
    topic = laser.topic("typed-replay")
    await topic.producer(partitions=1).send(
        b"body", headers={"kind": ("uint16", 7), "source": "python"}
    )
    messages = await topic.replay().poll()
    assert [(message.headers, message.header_kinds) for message in messages] == [
        ({"kind": 7, "source": "python"}, {"kind": "uint16", "source": "string"})
    ]


async def test_given_percent_encoded_credentials_when_connecting_then_should_log_in_with_the_decoded_secret(  # noqa: E501
    iggy_endpoint,
):
    _, address = iggy_endpoint.rsplit("@", 1)
    parsed = urlsplit(iggy_endpoint if "://" in iggy_endpoint else f"iggy://{iggy_endpoint}")
    username = "".join(f"%{byte:02X}" for byte in unquote(parsed.username).encode())
    password = "".join(f"%{byte:02X}" for byte in unquote(parsed.password).encode())
    laser = await ls.Laser.connect(f"{username}:{password}@{address}")
    try:
        await laser.stream(f"t-{uuid.uuid4().hex[:12]}").ensure()
    finally:
        await laser.close()


async def test_given_a_native_consumer_when_skipping_then_should_refuse_it(laser):
    topic = laser.topic("native-skip")
    await topic.producer(partitions=1).send_batch([b"zero"])
    consumer = topic.consumer("native-skip", partition=0, polling="first")
    try:
        with pytest.raises(ls.InvalidError):
            await consumer.skip(0, 0)
    finally:
        await consumer.shutdown()


async def test_given_a_capability_override_when_refreshing_then_should_keep_it(laser):
    original = await laser.capabilities()
    scoped = laser.with_capabilities(query=True, query_consistency="read_your_writes")

    refreshed = await scoped.refresh_capabilities()

    assert refreshed.managed == original.managed
    assert refreshed.query.available is True
    assert refreshed.query.consistency == "read_your_writes"


async def test_given_an_ensured_topic_when_publishing_one_record_then_should_confirm_it(laser):
    await laser.topic("readings").ensure(partitions=2)
    sent = await (
        laser.topic("readings")
        .publish()
        .index("host_id", "node-7")
        .inline_payload()
        .json({"host": "node-7", "cpu": 82})
        .send()
    )
    assert len(sent.confirmations) == 1
    assert sent.confirmations[0].partition_id in (0, 1)


async def test_given_a_json_batch_when_published_then_should_return_one_confirmation(laser):
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
        polling="next",
        auto_commit="disabled",
    )
    try:
        first = await asyncio.wait_for(uncommitted.next(), timeout=10)
    finally:
        await uncommitted.shutdown()
    uncommitted = topic.consumer_group("uncommitted-workers").consumer(
        polling="next",
        auto_commit="disabled",
    )
    try:
        replayed = await asyncio.wait_for(uncommitted.next(), timeout=10)
        assert (replayed.partition_id, replayed.position.offset) == (
            first.partition_id,
            first.position.offset,
        )
        await uncommitted.commit(replayed)
    finally:
        await uncommitted.shutdown()

    consumer = topic.consumer_group("manual-workers").consumer(
        batch_length=32,
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
        assert received[0].position.offset == 0

        last = received[-1]
        await consumer.commit(last)
        assert await consumer.last_consumed_offset(0) == last.position.offset
        assert await consumer.last_stored_offset(0) == last.position.offset
    finally:
        await consumer.shutdown()

    await producer.send(b"manual-resumed")
    resumed = topic.consumer_group("manual-workers").consumer(
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
                    != last.position.offset
                ):
                    await asyncio.sleep(0.01)
        finally:
            waiting.cancel()
            await asyncio.gather(waiting, return_exceptions=True)
    finally:
        await auto_consumer.shutdown()

    await producer.send(b"auto-resumed")
    resumed = topic.consumer_group("auto-workers").consumer(
        polling="next",
        auto_commit="disabled",
    )
    try:
        message = await asyncio.wait_for(resumed.next(), timeout=10)
        assert message.payload == b"auto-resumed"
    finally:
        await resumed.shutdown()


async def test_given_open_iggy_when_querying_then_should_raise_unsupported(open_laser):
    laser = open_laser
    with pytest.raises(ls.UnsupportedError) as caught:
        await laser.query("readings").where_eq("host_id", "node-7").fetch()
    assert caught.value.unsupported is True


async def test_given_every_typed_value_when_filtering_then_should_accept_it_locally(open_laser):
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


async def test_given_non_canonical_values_when_filtering_then_should_raise_invalid(laser):
    request = laser.query("readings")
    with pytest.raises(ls.InvalidError):
        request.where_eq("load", decimal.Decimal("NaN"))
    with pytest.raises(ls.InvalidError):
        request.where_eq("load", decimal.Decimal(10) ** 40)
    with pytest.raises(ls.InvalidError):
        request.where_eq("at", datetime.time(1, 2, 3, tzinfo=datetime.timezone.utc))
    with pytest.raises(ls.InvalidError):
        request.where_eq("value", {"nested": 1})


async def test_given_open_iggy_when_reading_kv_then_should_raise_unsupported(open_laser):
    laser = open_laser
    with pytest.raises(ls.UnsupportedError):
        await laser.kv("sessions").get("user:1")


async def test_given_open_iggy_when_creating_a_fork_then_should_raise_unsupported(open_laser):
    laser = open_laser
    with pytest.raises(ls.UnsupportedError):
        await laser.fork("exp-1").create()


async def test_given_a_failing_step_builder_when_running_then_should_fail_before_dispatch(laser):
    await laser.bootstrap(partitions=1, retention=ls.TopicRetention.expire_after(86_400_000))

    def broken_builder(_outputs):
        raise RuntimeError("cannot build task")

    workflow = laser.workflow("broken-workflow", fixed_inbox="agent.sessions")
    workflow.step("broken", to="worker", build=broken_builder)

    with pytest.raises(ls.LaserError, match="cannot build task"):
        await workflow.run()


async def test_given_open_iggy_when_upserting_a_graph_then_should_raise_unsupported(open_laser):
    laser = open_laser
    alice = ls.graph_node_entity("Person", "Alice")
    acme = ls.graph_node_entity("Company", "Acme")
    edge = ls.graph_edge_relate(alice, "works_at", acme)
    with pytest.raises(ls.UnsupportedError):
        await laser.graph("knowledge").upsert([alice, acme], [edge])


def test_given_a_graph_entity_when_deriving_ids_then_should_match_the_cross_sdk_golden():
    # The same entity yields the same id, a different label or value a different
    # one, pinned to the cross-SDK golden vector the wire crate fixes, so a graph
    # shared across languages converges on one node.
    assert ls.node_id_content("Person", "Alice") != ls.node_id_content("Company", "Alice")
    assert ls.node_id_content("Person", "Alice") == "13NCEPHNVFHHGNK9GD3MT0W1AB"
    alice = ls.graph_node_entity("Person", "Alice")
    assert alice["id"] == "13NCEPHNVFHHGNK9GD3MT0W1AB"
    assert alice["labels"] == ["Person"]
    acme = ls.graph_node_entity("Company", "Acme")
    edge = ls.graph_edge_relate(alice, "works_at", acme)
    assert edge["from"] == alice["id"]
    assert edge["to"] == acme["id"]
    assert edge["edge_type"] == "works_at"
    assert edge["id"] == ls.edge_id_content(alice["id"], "works_at", acme["id"])


async def test_given_an_echo_agent_when_requesting_then_should_reply_in_the_conversation(laser):
    await laser.bootstrap(partitions=2, retention=ls.TopicRetention.expire_after(86_400_000))

    async def handle(ctx, message):
        await ctx.respond(b"echo: " + message.payload)

    agent = laser.spawn_agent(
        "echo",
        "agent.sessions",
        handle,
        respond_on="agent.sessions",
    )
    try:
        await agent.ready()
        provenance = ls.Provenance(agent="caller")
        reply = await laser.request(
            "agent.sessions",
            "agent.sessions",
            b"ping",
            provenance,
            timeout_ms=20_000,
        )
        assert reply.payload == b"echo: ping"
        assert reply.provenance.conversation_id == provenance.conversation_id
    finally:
        await agent.shutdown()


async def test_given_an_agent_context_manager_when_requesting_then_should_reply(laser):
    await laser.bootstrap(partitions=2, retention=ls.TopicRetention.expire_after(86_400_000))

    async def handle(ctx, message):
        await ctx.respond(b"ack")

    spawned = laser.spawn_agent(
        "withagent",
        ls.AgentTopic.Sessions,
        handle,
        respond_on=ls.AgentTopic.Sessions,
    )
    async with spawned:
        reply = await laser.request(
            ls.AgentTopic.Sessions,
            ls.AgentTopic.Sessions,
            b"hi",
            ls.Provenance(agent="caller"),
            timeout_ms=20_000,
        )
        assert reply.payload == b"ack"
    # The context manager shut the agent down on exit.


async def test_given_a_request_reply_when_assembling_context_then_should_replay_both_turns(laser):
    await laser.bootstrap(partitions=2, retention=ls.TopicRetention.expire_after(86_400_000))

    async def handle(ctx, message):
        await ctx.respond(b"pong")

    agent = laser.spawn_agent("ponger", "agent.sessions", handle, respond_on="agent.sessions")
    try:
        await agent.ready()
        provenance = ls.Provenance(agent="caller")
        reply = await laser.request(
            "agent.sessions", "agent.sessions", b"ping", provenance, timeout_ms=20_000
        )
        history = await laser.assemble_context(reply.provenance.conversation_id)
        assert len(history) >= 2
        assert all(
            m.provenance.conversation_id == reply.provenance.conversation_id for m in history
        )
        assert any(m.payload == b"ping" for m in history)
        assert any(m.payload == b"pong" for m in history)
    finally:
        await agent.shutdown()


async def test_given_a_conversation_when_fetched_under_a_token_budget_then_should_bound_the_turns(
    laser,
):
    await laser.bootstrap(partitions=2, retention=ls.TopicRetention.expire_after(86_400_000))
    conversation = ls.new_conversation_id()
    ctx = laser.context(conversation)
    topics = [ls.AgentTopic.Sessions, ls.AgentTopic.Sessions]

    await ctx.append(ls.AgentTopic.Sessions, b"drain node-7")
    await ctx.append(ls.AgentTopic.Sessions, b"drained, 0 connections left")

    generous = await ctx.fetch(topics=topics, n=20, token_budget=4_000)
    assert len(generous) == 2

    # One token holds neither turn, so the budget keeps only the newest.
    starved = await ctx.fetch(topics=topics, n=20, token_budget=1)
    assert [m.payload for m in starved] == [b"drained, 0 connections left"]

    block = await ctx.block(topics=topics, n=20, token_budget=4_000)
    assert "0 connections left" in block


async def test_given_published_records_when_replaying_then_should_read_them_back(laser):
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

    assert len(seen) == 2
    assert seen[0].payload == b"one"
    assert seen[1].json() == {"n": 2}
    # The single partition's cursor advanced past both records.
    assert cursor.offsets == [2]


async def test_given_async_for_on_a_cursor_when_yielding_then_should_expose_offsets_past_it(laser):
    topic = laser.topic("progress-replay")
    await topic.ensure(partitions=1)
    for value in (b"one", b"two", b"three"):
        await topic.send(value)
    cursor = topic.replay()
    seen = []
    async for message in cursor:
        seen.append((bytes(message.payload), cursor.offsets))
    assert seen == [(b"one", [1]), (b"two", [2]), (b"three", [3])]


async def test_given_a_typed_reader_when_yielding_then_should_expose_offsets_past_each(laser):
    await laser.topic("progress-typed").ensure(partitions=1)
    topic = laser.topic("progress-typed").json()
    for cpu in (1, 2, 3):
        await topic.publish({"cpu": cpu}).send()
    reader = topic.records("progress-typed-reader")
    seen = []
    while (record := await reader.next()) is not None:
        seen.append((record.value["cpu"], reader.offsets))
    assert seen == [(1, [1]), (2, [2]), (3, [3])], (
        "a crash after handling a record resumes right after it"
    )


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


async def test_given_a_blocking_governor_when_publishing_then_should_raise_policy_blocked(laser):
    await laser.topic("business.audit").ensure(partitions=1)

    class BlockBusinessWires:
        def __init__(self):
            self.counters = []

        async def decide(self, action):
            self.counters.append(action.counters)
            if action.kind == "publish" and bytes(action.payload).startswith(b"wire-funds"):
                return ls.ActionDecision.block("no wire transfers")
            return ls.ActionDecision.allow()

    governor = BlockBusinessWires()
    governed = laser.with_governor_retention(governor, "enforce", ls.GovernorRetention(capacity=8))
    provenance = ls.Provenance(agent="publisher")
    with pytest.raises(ls.PolicyBlockedError):
        await (
            governed.topic("business.audit")
            .publish()
            .provenance(provenance)
            .payload(b"wire-funds to acct 7")
            .send()
        )
    counters = governor.counters[0]
    assert isinstance(counters, ls.ActionCounters)
    assert (counters.sends, counters.requests, counters.bytes_sent) == (0, 0, 0)


async def test_given_a_mandatory_blocking_voter_when_publishing_then_should_block(laser):
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


async def test_given_a_met_at_least_quorum_when_publishing_then_should_confirm_it(laser):
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
    sent = await (
        governed.topic("business.audit")
        .publish()
        .provenance(provenance)
        .payload(b"ordinary payload")
        .send()
    )
    assert len(sent.confirmations) == 1


async def test_given_intent_records_when_published_then_should_round_trip_through_the_log(laser):
    conversation = ls.new_conversation_id()
    intent = ls.Intent(
        conversation=conversation,
        proposer="planner",
        body=b"rotate the storage credentials",
        eligible_voters=["safety"],
        policy=ls.IntentPolicy.all(),
        policy_version=7,
        deadline_at_micros=time.time_ns() // 1_000 + 10_000_000,
    )
    vote = ls.Vote.cast(intent, "safety", "allow")
    decision = ls.decide(intent, [ls.ObservedVote(vote, vote.at_micros)], time.time_ns() // 1_000)
    assert decision is not None

    records = [
        ("native-intents", ls.Intent, intent),
        ("native-votes", ls.Vote, vote),
        ("native-decisions", ls.Decision, decision),
    ]
    for topic_name, cls, value in records:
        topic = laser.topic(topic_name).json(cls)
        await topic.topic().ensure(partitions=1)
        await topic.publish(value).send()
        reader = topic.records(f"{topic_name}-reader")
        record = await reader.next()
        assert record is not None
        assert record.value.intent_id == intent.intent_id

    decoded_intent = (
        await laser.topic("native-intents")
        .json(ls.Intent)
        .records("native-intents-second-reader")
        .next()
    ).value
    assert bytes(decoded_intent.body) == b"rotate the storage credentials"
    assert decoded_intent.digest == intent.digest

    decoded_decision = (
        await laser.topic("native-decisions")
        .json(ls.Decision)
        .records("native-decisions-second-reader")
        .next()
    ).value
    assert decoded_decision.intent_digest == intent.digest
    assert decoded_decision.policy_version == 7
    assert decoded_decision.outcome == "committed"


async def test_given_a_swapped_governor_when_publishing_then_should_decide_under_the_new_one(laser):
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


async def test_given_policy_evidence_when_folded_then_should_group_activity_by_agent(laser):
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
            provenance.conversation_id, topics=[ls.AgentTopic.Audit]
        )
        for message in messages:
            envelope = message.envelope
            if not envelope or envelope.get("operation") != "policy_decision":
                continue
            swarm.observe(ls.PolicyEvidence.decode(bytes(envelope["body"])))
        if time.monotonic() > deadline:
            pytest.fail("no policy decision landed on the audit topic")
        await asyncio.sleep(0.2)

    activity = swarm.agent("publisher")
    assert activity.count("block") >= 1
    assert activity.last_decision is not None
    assert activity.last_decision.source == "publisher"

    agents = swarm.agents()
    assert agents[0][0] == "publisher"


async def test_given_observe_mode_when_the_governor_fails_then_should_record_and_publish(laser):
    await laser.topic("business.audit").ensure(partitions=1)

    class Offline:
        async def decide(self, action):
            raise RuntimeError("policy engine offline")

    provenance = ls.Provenance(agent="publisher")
    await (
        laser.with_governor(Offline(), mode="observe")
        .topic("business.audit")
        .publish()
        .provenance(provenance)
        .payload(b"shadowed payload")
        .send()
    )
    with pytest.raises(ls.LaserError):
        await (
            laser.with_governor(Offline(), mode="enforce")
            .topic("business.audit")
            .publish()
            .provenance(provenance)
            .payload(b"enforced payload")
            .send()
        )

    deadline = time.monotonic() + 10
    while True:
        messages = await laser.assemble_context(
            provenance.conversation_id, topics=[ls.AgentTopic.Audit]
        )
        evidence = [
            ls.PolicyEvidence.decode(bytes(message.envelope["body"]))
            for message in messages
            if message.envelope and message.envelope.get("operation") == "policy_decision"
        ]
        failed = [item for item in evidence if item.decision == ls.GOVERNOR_ERROR_DECISION]
        if failed:
            break
        if time.monotonic() > deadline:
            pytest.fail("no governor failure landed on the audit topic")
        await asyncio.sleep(0.2)
    assert failed[0].outcome == "effected"
    assert "policy engine offline" in failed[0].reason


async def test_given_a_blocked_send_when_summarizing_a_crash_then_should_report_the_decision(laser):
    class BlockWires:
        async def decide(self, action):
            if action.kind == "send" and bytes(action.payload).startswith(b"wire-funds"):
                return ls.ActionDecision.block("no wire transfers")
            return ls.ActionDecision.allow()

    governed = laser.with_governor(BlockWires(), mode="enforce")
    provenance = ls.Provenance(agent="publisher")

    # A normal send lands in the journal.
    await governed.send_agent(ls.AgentTopic.Sessions, b"do the thing", provenance)

    # A blocked send is recorded as a decision on the audit topic.
    with pytest.raises(ls.PolicyBlockedError):
        await governed.send_agent(ls.AgentTopic.Sessions, b"wire-funds to acct 9", provenance)

    journal = await laser.assemble_context(
        provenance.conversation_id, topics=[ls.AgentTopic.Sessions]
    )
    # The governor refused the second send before its effect, so only the first landed.
    assert [message.payload for message in journal] == [b"do the thing"]

    last_decision = None
    deadline = time.monotonic() + 10
    while last_decision is None:
        audit = await laser.assemble_context(
            provenance.conversation_id, topics=[ls.AgentTopic.Audit]
        )
        for message in audit:
            envelope = message.envelope
            if envelope and envelope.get("operation") == "policy_decision":
                last_decision = ls.PolicyEvidence.decode(bytes(envelope["body"]))
        if time.monotonic() > deadline:
            pytest.fail("no policy decision landed on the audit topic")
        await asyncio.sleep(0.2)

    context = ls.CrashContext(journal=journal, dead_letter=None, last_decision=last_decision)
    summary = context.summarize()
    assert "do the thing" in summary
    assert "last decision: block (blocked)" in summary
    assert "dead letter: none" in summary


async def test_given_an_avro_record_when_published_then_should_decode_from_the_log(laser):
    # Avro encoding is client-side, so the publish path works on Apache Iggy
    # (only the managed projection of the body needs the managed backend). Encode a
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

    assert len(seen) == 1
    assert compiled.decode(bytes(seen[0].payload)) == {"symbol": "AAPL", "qty": 7}


async def test_given_a_provenance_when_sending_to_an_agent_then_should_land_in_its_conversation(
    laser,
):
    await laser.bootstrap(partitions=2, retention=ls.TopicRetention.expire_after(86_400_000))
    provenance = ls.Provenance(agent="producer")
    await laser.send_agent("agent.audit", b"audit-record", provenance)
    records = await laser.assemble_context(provenance.conversation_id, topics=["agent.audit"])
    assert [record.payload for record in records] == [b"audit-record"]
    assert records[0].provenance.conversation_id == provenance.conversation_id


async def test_given_agdx_when_status_and_errors_are_sent_then_should_publish_every_terminal(
    laser,
):
    await laser.bootstrap(partitions=1, retention=ls.TopicRetention.expire_after(86_400_000))
    conversation = ls.new_conversation_id()
    correlation = ls.mint_ulid()
    agdx = laser.agdx("agent.sessions", "worker", conversation)

    status = await agdx.status(
        "task",
        correlation=correlation,
        task_state=ls.TaskState.Working,
    )
    failed = await agdx.fail(
        correlation,
        {"code": 6, "message": "tool failed", "retryable": True},
    )
    stream = agdx.stream(ls.mint_ulid(), "chat")
    await stream.write(b"partial")
    await stream.fail({"code": 7, "message": "stream failed", "retryable": False})

    records = await laser.assemble_context(conversation, topics=["agent.sessions"])
    kinds = {record.envelope.get("record"): record.envelope["kind"] for record in records}
    assert kinds[status] == "status"
    assert kinds[failed] == "error"
    # The stream's terminal failure is an error record of its own.
    assert list(kinds.values()).count("error") == 2


@pytest.mark.parametrize("millis", [-1.0, float("inf"), float("-inf"), float("nan")])
async def test_given_invalid_duration_when_used_then_should_raise_without_panicking(laser, millis):
    request = laser.agdx(
        "agent.sessions",
        "requester",
        ls.new_conversation_id(),
    ).request_input("agent.sessions", b"input", timeout_ms=millis)
    with pytest.raises(ls.InvalidError, match="finite, non-negative"):
        await request

    kv_set = laser.kv("duration-validation").set("key").bytes(b"value").ttl(millis)
    with pytest.raises(ls.InvalidError, match="finite, non-negative"):
        await kv_set.send()


async def test_given_a_state_snapshot_and_delta_when_reconstructing_then_should_apply_both(laser):
    await laser.bootstrap(partitions=1, retention=ls.TopicRetention.expire_after(86_400_000))
    conversation = ls.new_conversation_id()
    await laser.publish_state_snapshot("ui", conversation, {"count": 1})
    await laser.publish_state_delta(
        "ui",
        conversation,
        [{"op": "replace", "path": "/count", "value": 2}],
    )
    state = None
    for _ in range(20):
        state = await laser.reconstruct_state(conversation)
        if state == {"count": 2}:
            break
        await asyncio.sleep(0.2)
    assert state == {"count": 2}


@pytest.mark.parametrize(
    "tool",
    [{"name": "ask"}, {"name": "ask", "input_schema": None}, {"name": "ask", "input_schema": []}],
)
async def test_given_a_tool_without_an_object_schema_when_bridged_then_should_raise_invalid(
    laser, tool
):
    with pytest.raises(ls.InvalidError):
        ls.McpBridge(laser, "mcp-gw", "agent.sessions", "agent.sessions", "laser-mcp", tools=[tool])


async def test_given_an_mcp_bridge_when_listing_then_should_serve_tools_and_prompts(laser):
    bridge = ls.McpBridge(
        laser,
        "mcp-gw",
        "agent.sessions",
        "agent.sessions",
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
    assert init["capabilities"] == {"tools": {}, "prompts": {}}
    names = [tool["name"] for tool in bridge.list_tools()["tools"]]
    assert names == ["ask"]
    prompt_names = [prompt["name"] for prompt in bridge.list_prompts()["prompts"]]
    assert prompt_names == ["greet"]
    rendered = bridge.get_prompt("greet")
    assert rendered == {
        "description": "a greeting",
        "messages": [{"role": "user", "content": {"type": "text", "text": "say hello"}}],
    }


async def test_given_an_a2a_task_when_a_python_agent_answers_then_should_leave_working(laser):
    await laser.bootstrap(partitions=2, retention=ls.TopicRetention.expire_after(86_400_000))

    async def worker(ctx, message):
        await ctx.respond_input("agent.sessions", b"answered")

    agent = laser.spawn_agent("a2a-worker", "agent.sessions", worker)
    try:
        await agent.ready()
        bridge = ls.A2aBridge(laser, "a2a-gw", "agent.sessions", "agent.sessions")
        task = await bridge.submit(
            {"message": {"role": "user", "parts": [{"kind": "text", "text": "hi"}]}}
        )
        assert task["id"]
        resolved = None
        for _ in range(40):
            current = await bridge.task(task["id"])
            if current["status"]["state"] not in ("TASK_STATE_WORKING", "TASK_STATE_SUBMITTED"):
                resolved = current
                break
            await asyncio.sleep(0.3)
        assert resolved is not None, "the A2A task never left the working state"
    finally:
        await agent.shutdown()


async def test_given_a_custom_deduplicator_when_a_key_repeats_then_should_drop_the_duplicate(laser):
    await laser.bootstrap(partitions=1, retention=ls.TopicRetention.expire_after(86_400_000))
    handled: list[str] = []
    seen: set[str] = set()

    async def dedup(key: str) -> bool:
        first_time = key not in seen
        seen.add(key)
        return first_time

    async def handle(ctx, message):
        handled.append(message.payload.decode())

    agent = laser.spawn_agent("dedup-worker", "agent.sessions", handle, dedup=dedup)
    try:
        await agent.ready()
        provenance = ls.Provenance(agent="caller", idempotency_key="dupe-key")
        await laser.send_agent("agent.sessions", b"once", provenance)
        await laser.send_agent("agent.sessions", b"twice", provenance)
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


async def test_given_log_memory_when_remembering_on_open_iggy_then_should_recall_folded(laser):
    # Log-backed memory is the source-of-truth path, so it works on Apache Iggy.
    await laser.bootstrap(partitions=1, retention=ls.TopicRetention.expire_after(86_400_000))
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
    bodies = {item.text() for item in recalled}
    assert "auth latency spiked at noon" in bodies
    # A different conversation recalls nothing.
    assert await memory.recall(conversation=ls.new_conversation_id(), folded=True) == []


async def test_given_vector_memory_when_recalling_semantically_then_should_rank_by_similarity(
    laser,
):
    # In-process semantic memory: a deterministic bag-of-words embedder, so recall
    # ranks by overlap with the query. No server round-trip, but built off a Laser.
    vocabulary = ["database", "pool", "auth", "latency", "storage", "rotation", "noon", "spike"]

    async def embed(text: str) -> list[float]:
        words = set(text.lower().split())
        return [1.0 if term in words else 0.0 for term in vocabulary]

    memory = ls.VectorMemory.governed(laser, embed)
    conversation = ls.new_conversation_id()
    await memory.remember("database pool exhaustion", conversation=conversation)
    await memory.remember("storage rotation retried twice", conversation=conversation)
    await memory.remember("auth latency spike at noon", conversation=conversation)

    top = await memory.recall(conversation=conversation, semantic="auth latency", limit=1)
    assert len(top) == 1
    assert top[0].text() == "auth latency spike at noon"

    rotation = await memory.recall(conversation=conversation, semantic="rotation", limit=1)
    assert rotation[0].text() == "storage rotation retried twice"


async def test_given_a_blocking_governor_when_writing_vector_memory_then_should_store_nothing(
    laser,
):
    await laser.bootstrap(partitions=1, retention=ls.TopicRetention.expire_after(86_400_000))

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
    memory = ls.VectorMemory.governed(governed, embed)
    conversation = ls.new_conversation_id()
    with pytest.raises(ls.PolicyBlockedError):
        await memory.remember(
            "node-7 prefers eu-west [skew:fabricate_memory]",
            conversation=conversation,
        )
    assert await memory.recall(conversation=conversation) == []


async def test_given_feedback_when_improving_vector_memory_then_should_promote_the_item(laser):
    # Feedback re-ranks recall: a promoted item floats to the front on the next
    # recall, mirroring the Rust feedback contract.
    async def embed(text: str) -> list[float]:
        return [1.0 if term in set(text.lower().split()) else 0.0 for term in ("cat", "dog")]

    memory = ls.VectorMemory.governed(laser, embed)
    conversation = ls.new_conversation_id()
    dog = await memory.remember("the dog ran", conversation=conversation)
    await memory.remember("the cat sat", conversation=conversation)

    before = await memory.recall(conversation=conversation, limit=2)
    assert before[0].text() == "the cat sat"

    await memory.improve(dog, 5.0, conversation=conversation)
    after = await memory.recall(conversation=conversation, limit=2)
    assert after[0].text() == "the dog ran"
    assert after[0].score == 5.0


async def test_given_an_unknown_strategy_when_recalling_then_should_raise_codec_error(laser):
    memory = laser.memory("notes")
    with pytest.raises(ls.CodecError):
        await memory.recall(conversation=ls.new_conversation_id(), strategy="nonsense")


async def test_given_a_built_message_and_ctx_when_calling_a_handler_then_should_need_no_consumer(
    laser,
):
    # The handler unit-test seam: build a message and a ctx directly, then call
    # the handler function like a plain callable, no spawn_agent/consumer group
    # needed at all.
    provenance = ls.Provenance(agent="tester")
    message = ls.agent_message(b"hello", provenance)
    assert message.payload == b"hello"
    assert message.provenance.conversation_id == provenance.conversation_id

    ctx = ls.agent_ctx(laser, message, agent="tester")
    assert ctx.message.payload == b"hello"

    handled = []

    async def handle(ctx, message):
        handled.append(message.payload)

    await handle(ctx, message)
    assert handled == [b"hello"]


async def test_given_capable_agents_when_fanning_out_then_should_gather_every_reply(
    laser, iggy_endpoint
):
    await laser.bootstrap(partitions=2, retention=ls.TopicRetention.expire_after(86_400_000))

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
            "agent.sessions",
            make_worker(name),
            respond_on="agent.sessions",
            capabilities=[
                {"skill_id": "diagnose", "cost_class": 2, "latency_class": 1, "load": 10}
            ],
        )
        await agent.ready()
        workers.append(agent)

    gathered = {}

    async def orchestrate(ctx, message):
        gathered["result"] = await ctx.fan_out("diagnose", b"scan", deadline_ms=10_000)

    orchestrator = laser.spawn_agent(
        "orchestrator",
        "agent.sessions",
        orchestrate,
        respond_on="agent.sessions",
        fixed_inbox="agent.sessions",
    )
    try:
        await orchestrator.ready()
        await laser.send_agent("agent.sessions", b"go", ls.Provenance(agent="trigger"))

        for _ in range(50):
            if "result" in gathered:
                break
            await asyncio.sleep(0.2)

        result = gathered["result"]
        assert len(result.failures) == 0
        assert {agent for agent, _ in result.ok} == {"worker-a", "worker-b"}
        assert {reply.body() for reply in result.replies()} == {b"worker-a:scan", b"worker-b:scan"}
    finally:
        for worker in workers:
            await worker.shutdown()
        await orchestrator.shutdown()


async def test_given_an_approval_gate_when_a_human_answers_then_should_resume_the_handler(laser):
    await laser.bootstrap(partitions=2, retention=ls.TopicRetention.expire_after(86_400_000))

    async def approve(ctx, message):
        await ctx.respond_input("agent.sessions", b"approved")

    approver = laser.spawn_agent(
        "approver", "agent.sessions", approve, operations=["input_request"]
    )
    handled = []

    async def gatekeeper_handle(ctx, message):
        handled.append(message.payload)
        grant = await ctx.approval_gate(
            "approver", "agent.sessions", b"approve a $500 credit?", timeout_ms=10_000
        )
        assert grant.approver == "approver"
        await ctx.reply_on("agent.audit", grant.decision)

    gatekeeper = laser.spawn_agent(
        "gatekeeper",
        "agent.sessions",
        gatekeeper_handle,
        respond_on="agent.sessions",
    )
    try:
        await approver.ready()
        await gatekeeper.ready()
        provenance = ls.Provenance(agent="trigger")
        await laser.send_agent("agent.sessions", b"go", provenance)

        decision = None
        for _ in range(50):
            audit = await laser.assemble_context(provenance.conversation_id, topics=["agent.audit"])
            match = next((m for m in audit if m.payload == b"approved"), None)
            if match is not None:
                decision = match.payload
                break
            await asyncio.sleep(0.2)

        assert decision == b"approved"
        assert len(handled) == 1, "the gatekeeper's own prompt is not its work"
    finally:
        await gatekeeper.shutdown()
        await approver.shutdown()


async def test_given_a_verifying_agent_when_commands_arrive_then_should_dispatch_only_signed(laser):
    await laser.bootstrap(partitions=2, retention=ls.TopicRetention.expire_after(86_400_000))
    caller_key = ls.SigningKey(bytes([21]) * 32)
    registry = ls.KeyRegistry()
    registry.enroll("caller", caller_key.verifying_key)

    principals = []

    async def handle(ctx, message):
        principals.append(message.verified_principal)

    agent = laser.spawn_agent("verified-worker", ls.AgentTopic.Sessions, handle, verifier=registry)
    try:
        await agent.ready()
        conversation = ls.Provenance(agent="caller").conversation_id
        unsigned = laser.agdx(ls.AgentTopic.Sessions, "caller", conversation)
        signed = laser.agdx(ls.AgentTopic.Sessions, "caller", conversation, signing_key=caller_key)
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


async def test_given_a_signed_unquarantine_past_its_deadline_when_folded_then_should_keep_the_quarantine(  # noqa: E501
    iggy_endpoint,
):
    operator_key = ls.SigningKey(bytes([73]) * 32)
    registry = ls.KeyRegistry()
    registry.enroll_operator("operator", operator_key.verifying_key)
    stream = f"t-{uuid.uuid4().hex[:12]}"
    laser = await ls.Laser.connect(iggy_endpoint, stream=stream, verifier=registry)
    try:
        await laser.stream(stream).ensure()
        await laser.quarantine_signed("operator", "q-target", operator_key)
        # A captured unquarantine re-appended after its signed deadline.
        replay = laser.agdx(
            ls.AgentTopic.Registry, "operator", ls.new_conversation_id(), signing_key=operator_key
        )
        await replay.status("unquarantine", body=b"q-target", deadline_at_micros=1)
        await laser.quarantine_signed("operator", "q-sentinel", operator_key)
        folded = laser.agent_registry()
        for _ in range(75):
            await folded.refresh()
            if folded.is_quarantined("q-sentinel"):
                break
            await asyncio.sleep(0.2)
        assert folded.is_quarantined("q-sentinel")
        assert folded.is_quarantined("q-target")
    finally:
        await laser.close()


async def test_given_a_running_verifier_when_its_operator_key_is_revoked_then_should_refuse_later_facts(  # noqa: E501
    iggy_endpoint,
):
    revoked_key = ls.SigningKey(bytes([74]) * 32)
    live_key = ls.SigningKey(bytes([75]) * 32)
    registry = ls.KeyRegistry()
    registry.enroll_operator("operator", revoked_key.verifying_key)
    registry.enroll_operator("operator", live_key.verifying_key)
    stream = f"t-{uuid.uuid4().hex[:12]}"
    laser = await ls.Laser.connect(iggy_endpoint, stream=stream, verifier=registry)
    try:
        await laser.stream(stream).ensure()
        registry.enroll_record(
            ls.KeyRecord("operator", revoked_key.verifying_key, kind="operator", revoked=True)
        )
        await laser.quarantine_signed("operator", "q-after-revocation", revoked_key)
        await laser.quarantine_signed("operator", "q-revocation-sentinel", live_key)
        folded = laser.agent_registry()
        for _ in range(75):
            await folded.refresh()
            if folded.is_quarantined("q-revocation-sentinel"):
                break
            await asyncio.sleep(0.2)
        assert folded.is_quarantined("q-revocation-sentinel")
        assert not folded.is_quarantined("q-after-revocation")
    finally:
        await laser.close()


async def test_given_a_verifying_caller_when_requesting_input_then_should_resume_only_if_signed(
    iggy_endpoint,
):
    approver_key = ls.SigningKey(bytes([51]) * 32)
    registry = ls.KeyRegistry()
    registry.enroll("approver", approver_key.verifying_key)
    stream = f"t-{uuid.uuid4().hex[:12]}"
    caller = await ls.Laser.connect(iggy_endpoint, stream=stream, verifier=registry)
    await caller.bootstrap(partitions=2, retention=ls.TopicRetention.expire_after(86_400_000))

    async def forge(ctx, message):
        await ctx.respond_input(ls.AgentTopic.Sessions, b"forged")

    async def approve(ctx, message):
        await ctx.respond_input(ls.AgentTopic.Sessions, b"approved-signed")

    # An unsigned approver answers every interrupt, but its response cannot
    # verify, so the paused caller must keep waiting and time out.
    faker = caller.spawn_agent("faker", ls.AgentTopic.Sessions, forge, operations=["input_request"])
    approver = None
    try:
        await faker.ready()
        orchestrator = caller.agdx(
            ls.AgentTopic.Sessions,
            "orchestrator",
            ls.Provenance(agent="orchestrator").conversation_id,
        )
        with pytest.raises(ls.TimeoutError):
            await orchestrator.request_input(ls.AgentTopic.Sessions, b"approve?", timeout_ms=2_000)

        # A signing approver resumes the caller: `respond_input` signs with the
        # agent's key, so the verified reader accepts exactly this decision.
        approver = caller.spawn_agent(
            "approver",
            ls.AgentTopic.Sessions,
            approve,
            signing_key=approver_key,
            operations=["input_request"],
        )
        await approver.ready()
        decision = await orchestrator.request_input(
            ls.AgentTopic.Sessions, b"approve?", timeout_ms=15_000
        )
        assert decision == b"approved-signed"
    finally:
        await faker.shutdown()
        if approver is not None:
            await approver.shutdown()


async def _lane(session, count):
    turns = []
    for _ in range(100):
        turns = await session.context()
        if len(turns) >= count:
            break
        await asyncio.sleep(0.05)
    return turns


async def test_given_a_started_session_when_ended_then_should_write_start_and_completed_on_the_lane(
    laser,
):
    await laser.bootstrap(partitions=2, retention=ls.TopicRetention.expire_after(86_400_000))
    sessions = laser.sessions()
    builder = sessions.create("incident").agent("planner").namespace("ops").tag("vip")
    assert builder.id() == ls.derive_session_id(laser.default_stream, "ops", "incident")
    session, lease = await builder.begin()
    assert lease.session == session.conversation
    await session.end()
    await session.end()
    with pytest.raises(ls.InvalidError):
        await session.cancel()
    lease.release()
    turns = await _lane(sessions.open(session.conversation), 2)
    assert [turn.display for turn in turns[:2]] == ["session.started", "session.completed"]

    checkpoint = await session.checkpoint()
    restored = ls.Checkpoint.from_json(checkpoint.to_json())
    # The repeated end wrote the same terminal record again.
    assert len(await session.turns_at(restored)) == 3
    record = ls.new_conversation_id()
    envelope = ls.event_envelope(record, session.conversation, "planner", b"{}", operation="note")
    await session.append(envelope)
    since = []
    for _ in range(100):
        since = await session.turns_since(checkpoint)
        if since:
            break
        await asyncio.sleep(0.05)
    assert [turn.display for turn in since] == ["agent.message"]
    replayed = await session.replay(checkpoint, [], lambda acc, turn: [*acc, turn.display])
    assert replayed == ["agent.message"]
    other = ls.event_envelope(record, ls.new_conversation_id(), "planner", b"{}")
    with pytest.raises(ls.InvalidError):
        await session.append(other)


async def test_given_a_session_context_when_the_block_raises_then_should_fail_with_the_traceback(
    laser,
):
    await laser.bootstrap(partitions=1, retention=ls.TopicRetention.expire_after(86_400_000))
    sessions = laser.sessions()
    conversation = None
    with pytest.raises(RuntimeError, match="provider exploded"):
        async with sessions.start().agent("planner") as session:
            conversation = session.conversation
            raise RuntimeError("provider exploded")
    turns = await _lane(sessions.open(conversation), 2)
    assert [turn.display for turn in turns[:2]] == ["session.started", "session.failed"]
    async with sessions.start().agent("planner") as session:
        clean = session.conversation
    turns = await _lane(sessions.open(clean), 2)
    assert turns[1].display == "session.completed"


async def test_given_a_full_error_body_when_a_session_fails_then_should_record_it(laser):
    await laser.bootstrap(partitions=1, retention=ls.TopicRetention.expire_after(86_400_000))
    sessions = laser.sessions()
    session, _lease = await sessions.start().agent("planner").begin()
    await session.fail({"code": 4, "message": "deadline passed", "retryable": True})
    turns = await _lane(sessions.open(session.conversation), 2)
    assert turns[1].display == "session.failed"
    end = ls.decode_session_end(bytes(turns[1].message.envelope["body"]))
    assert end["error"]["code"] == 4
    assert end["error"]["message"] == "deadline passed"
    assert end["error"]["retryable"] is True


async def test_given_a_stream_when_deleted_then_should_report_absence_on_repeat(laser):
    stream = laser.stream(laser.default_stream)
    await stream.ensure()
    assert await stream.delete() is True
    assert await stream.delete() is False


async def test_given_a_closed_laser_when_used_then_should_raise(laser):
    await laser.topic("closing").ensure(partitions=1)
    clone = laser.with_default_stream(laser.default_stream)
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
    assert (await asyncio.wait_for(first.next(), 10)).position.offset == 0
    await first.shutdown()
    retried = topic.consumer("fresh", partition=0, batch_length=1, auto_commit="disabled")
    zero = await asyncio.wait_for(retried.next(), 10)
    assert zero.position.offset == 0
    await retried.commit(zero)
    await retried.shutdown()
    resumed = topic.consumer("fresh", partition=0, batch_length=1, auto_commit="disabled")
    assert (await asyncio.wait_for(resumed.next(), 10)).position.offset == 1
    await resumed.shutdown()


async def test_given_default_polling_when_shutdown_after_offset_zero_then_should_resume_at_one(
    laser,
):
    topic = laser.topic("default-shutdown-zero")
    producer = topic.producer(partition=0, partitions=1)
    await producer.send_batch([b"zero", b"one", b"two", b"three"])
    first = topic.consumer("partial", partition=0, batch_length=4)
    assert (await asyncio.wait_for(first.next(), 10)).position.offset == 0
    await first.shutdown()
    resumed = topic.consumer("partial", partition=0, batch_length=4)
    assert (await asyncio.wait_for(resumed.next(), 10)).position.offset == 1
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
    assert (await asyncio.wait_for(first.next(), 10)).position.offset == 0
    await first.shutdown()
    resumed = (
        topic.consumer_group("worker").consumer(auto_commit="disabled")
        if group
        else topic.consumer("worker", partition=0, auto_commit="disabled")
    )
    try:
        assert (await asyncio.wait_for(resumed.next(), 10)).position.offset == (
            0 if mode == "disabled" else 1
        )
    finally:
        await resumed.shutdown()


@pytest.mark.parametrize("native", [False, True], ids=["policy_aware", "native"])
async def test_given_a_stored_offset_when_a_group_starts_at_last_then_should_resume_after_it(
    laser, native
):
    if native:
        laser = laser.with_capabilities(filters=False, filters_group_policy_reads=False)
    topic = laser.topic("group-last-start")
    await topic.ensure(1)
    producer = topic.producer(partition=0, partitions=1)
    for value in range(10):
        await producer.send(str(value).encode())

    handled = topic.consumer_group("tail-workers").consumer(auto_commit="disabled")
    try:
        received = [await asyncio.wait_for(handled.next(), timeout=10) for _ in range(5)]
        await handled.commit(received[-1])
    finally:
        await handled.shutdown()

    resumed = topic.consumer_group("tail-workers").consumer(
        polling="last", batch_length=2, auto_commit="disabled"
    )
    try:
        offsets = [
            (await asyncio.wait_for(resumed.next(), timeout=10)).position.offset for _ in range(5)
        ]
        assert offsets == [5, 6, 7, 8, 9]
    finally:
        await resumed.shutdown()

    fresh = topic.consumer_group("tail-fresh").consumer(polling="last", auto_commit="disabled")
    try:
        await fresh.init()
        message = await asyncio.wait_for(fresh.next(), timeout=10)
        assert message.position.offset == 9
        stored = await fresh.stored_offset(0)
        assert stored is not None and stored.stored_offset == 8
    finally:
        await fresh.shutdown()


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


def local_projection_cap(laser):
    return ls.MAX_PROJECTION_ID_BYTES - len(f"stream:{laser.default_stream}/")


# Control commands check the managed capability before their local name rules.
def with_managed_backend(laser):
    return laser.with_capabilities(managed=True)


@pytest.mark.parametrize(
    ("rule", "send"),
    [
        (
            "projection id is",
            lambda laser: (
                with_managed_backend(laser)
                .projections()
                .drop("p" * (local_projection_cap(laser) + 1))
            ),
        ),
        (
            "projection id contains a control character",
            lambda laser: (
                with_managed_backend(laser)
                .bindings()
                .apply(
                    ls.ProjectionBindingBuilder()
                    .source("acme", "readings")
                    .allow("reading\n.v1")
                    .build()
                )
            ),
        ),
        (
            "projection name is",
            lambda laser: (
                with_managed_backend(laser)
                .projections()
                .register(
                    ls.ProjectionBuilder("readings")
                    .name("n" * (ls.MAX_PROJECTION_NAME_BYTES + 1))
                    .build()
                )
            ),
        ),
        (
            "index field name is",
            lambda laser: (
                with_managed_backend(laser)
                .projections()
                .register(
                    ls.ProjectionBuilder("readings")
                    .field("f" * (ls.MAX_INDEX_FIELD_BYTES + 1))
                    .build()
                )
            ),
        ),
        (
            "graph node label is",
            lambda laser: (
                with_managed_backend(laser)
                .projections()
                .register_graph(
                    ls.ProjectionBuilder("fleet")
                    .graph(
                        {
                            "nodes": [
                                {
                                    "label": "l" * (ls.MAX_GRAPH_LABEL_BYTES + 1),
                                    "value_pointer": "/host",
                                }
                            ]
                        }
                    )
                    .build()
                )
            ),
        ),
        (
            "schema name is",
            lambda laser: laser.schemas().register(
                {"kind": "json_schema", "schema": "{}"}, name="s" * (ls.MAX_SCHEMA_NAME_BYTES + 1)
            ),
        ),
        (
            "memory item id is",
            lambda laser: ls.LogMemory.in_namespace(laser, "notes").set_named(
                "k" * (ls.MAX_MEMORY_ID_BYTES + 1 - len("notes/")), b"v"
            ),
        ),
        (
            "memory target is",
            lambda laser: ls.LogMemory.in_namespace(laser, "notes").forget_named(
                "k" * (ls.MAX_MEMORY_ID_BYTES + 1 - len("notes/"))
            ),
        ),
        (
            "session list text is",
            lambda laser: laser.sessions().list(text="t" * (ls.MAX_SESSION_TEXT_QUERY_BYTES + 1)),
        ),
        (
            "session list label prefix is",
            lambda laser: laser.sessions().list(label_prefix="l" * 257),
        ),
        (
            "session list stream must be",
            lambda laser: laser.sessions(stream="s" * 256).list(),
        ),
        (
            "as_of and series name one bound",
            lambda laser: laser.sessions().list(as_of=4405),
        ),
        (
            "as_of and series name one bound",
            lambda laser: laser.sessions().events("00000000000000000000000001", series=77),
        ),
    ],
)
async def test_given_a_name_over_its_wire_rule_when_sent_then_should_refuse_it_before_the_backend(
    laser, rule, send
):
    with pytest.raises(ls.InvalidError, match=rule):
        await send(laser)


async def test_given_a_zero_poll_interval_when_watching_sessions_then_should_raise_invalid(laser):
    with pytest.raises(ls.InvalidError, match="poll"):
        await laser.sessions().watch(0)


async def test_given_an_agent_id_at_the_wire_cap_when_spawned_then_should_need_an_explicit_group(
    laser,
):
    longest = "a" * 256
    assert laser.agent(longest).id == longest
    with pytest.raises(ls.IdError):
        laser.agent(longest + "a")
    with pytest.raises(ls.IdError):
        laser.spawn_agent(longest, ls.AgentTopic.Sessions, lambda ctx, message: None)


async def test_given_a_quiet_topic_when_reading_next_within_then_should_raise_timeout(laser):
    topic = laser.topic("quiet")
    await topic.ensure(partitions=1)
    consumer = topic.consumer("quiet-reader", partition=0)
    try:
        with pytest.raises(ls.TimeoutError):
            await consumer.next_within(500)
    finally:
        await consumer.shutdown()


@pytest.mark.parametrize(
    ("knobs", "reason"),
    [
        ({"max_wait_ms": 600_001}, "max_wait must be at most 600 s"),
        ({"min_count": 0}, r"min_count must be 1\.\.=batch_length"),
        ({"batch_length": 4, "min_count": 5}, r"min_count must be 1\.\.=batch_length"),
        ({"max_bytes": 0}, "max_bytes must be at least"),
    ],
)
@pytest.mark.parametrize("group", [False, True])
async def test_given_an_out_of_range_poll_knob_when_a_consumer_starts_then_should_refuse_it(
    laser, knobs, reason, group
):
    topic = laser.topic("poll-knobs")
    await topic.ensure(partitions=1)
    consumer = (
        topic.consumer_group("poll-knobs").consumer(**knobs)
        if group
        else topic.consumer("poll-knobs", partition=0, **knobs)
    )
    with pytest.raises(ls.InvalidError, match=reason):
        await consumer.init()


async def test_given_the_widest_poll_knobs_when_a_consumer_starts_then_should_accept_them(laser):
    topic = laser.topic("poll-knob-bounds")
    await topic.ensure(partitions=1)
    consumer = topic.consumer(
        "poll-knob-bounds",
        partition=0,
        batch_length=8,
        max_wait_ms=600_000,
        min_count=8,
        max_bytes=2**32 - 1,
    )
    try:
        await consumer.init()
    finally:
        await consumer.shutdown()


async def test_given_a_held_poll_when_a_record_lands_then_should_deliver_it_before_the_wait_ends(
    laser,
):
    topic = laser.topic("held-poll")
    await topic.ensure(partitions=1)
    consumer = topic.consumer("held-reader", partition=0, polling="first", max_wait_ms=30_000)
    try:
        await consumer.init()
        pending = asyncio.ensure_future(consumer.next())
        await asyncio.sleep(0.2)
        assert not pending.done(), "nothing was published yet"
        started = time.monotonic()
        await topic.send(b"landed")
        record = await asyncio.wait_for(pending, 10)
        assert record.payload == b"landed"
        assert time.monotonic() - started < 10, "the server answered the held poll on arrival"
    finally:
        await consumer.shutdown()


async def test_given_open_iggy_when_opening_watch_then_should_raise_unsupported(open_laser):
    laser = open_laser
    opening = laser.watch(index="readings_v1", from_end=True)
    with pytest.raises(ls.UnsupportedError) as refused:
        await opening
    assert (refused.value.surface, refused.value.feature) == ("watch", "watch")


async def test_given_raw_sends_and_batches_when_published_then_should_replay_in_order(laser):
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


async def test_given_a_context_when_fetching_with_defaults_then_should_fold_to_a_checkpoint(laser):
    await laser.bootstrap(partitions=1, retention=ls.TopicRetention.expire_after(86_400_000))
    conversation = ls.new_conversation_id()
    ctx = laser.context(conversation)
    await ctx.append(ls.AgentTopic.Sessions, b"drain node-7")
    await ctx.append(ls.AgentTopic.Sessions, b"drained, 0 connections left")

    history = []
    for _ in range(40):
        history = await ctx.fetch()
        if len(history) == 2:
            break
        await asyncio.sleep(0.25)
    assert [m.payload for m in history] == [b"drain node-7", b"drained, 0 connections left"]

    newest = await ctx.fetch_with(
        [ls.AgentTopic.Sessions, ls.AgentTopic.Sessions],
        ls.Chain([ls.LastN(5), ls.TokenBudget(1)]),
    )
    assert [m.payload for m in newest] == [b"drained, 0 connections left"]

    checkpoint = await ctx.checkpoint()
    await ctx.append(ls.AgentTopic.Sessions, b"restore node-7")
    count = await ctx.state(
        [ls.AgentTopic.Sessions, ls.AgentTopic.Sessions],
        0,
        lambda total, _: total + 1,
        at=checkpoint,
    )
    assert count == 2
    loaded = await ls.ConversationState.load(
        laser,
        conversation,
        [ls.AgentTopic.Sessions, ls.AgentTopic.Sessions],
        0,
        lambda total, _: total + 1,
        at=checkpoint,
    )
    assert loaded == count


async def test_given_a_background_producer_when_shut_down_then_should_flush_and_refuse_sends(laser):
    topic = laser.topic("background-readings")
    await topic.ensure(partitions=1)
    producer = topic.producer(partition=0, background=ls.BackgroundConfig(linger_ms=5))
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


async def test_given_a_cbor_topic_when_publishing_a_dataclass_then_should_read_it_back(laser):
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


async def test_given_log_memory_when_writing_named_items_then_should_fold_them_back(laser):
    await laser.bootstrap(partitions=1, retention=ls.TopicRetention.expire_after(86_400_000))
    memory = ls.LogMemory.in_namespace(laser, "named-notes")
    assert isinstance(memory, ls.MemoryHandle)
    assert memory.backend == "log"
    await memory.set_named("plan", b'{"step": 1}')
    await memory.update_named("plan", b'{"owner": "ops"}')
    value = None
    for _ in range(40):
        value = await memory.fetch_named_folded("plan")
        if value is not None and b"owner" in bytes(value):
            break
        await asyncio.sleep(0.25)
    assert value is not None and json.loads(bytes(value)) == {"step": 1, "owner": "ops"}
    await memory.forget_named("plan")
    for _ in range(40):
        if await memory.fetch_named_folded("plan") is None:
            break
        await asyncio.sleep(0.25)
    assert await memory.fetch_named_folded("plan") is None


async def test_given_a_custom_memory_backend_when_used_then_should_receive_every_verb(laser):
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
    assert memory.backend == "custom"
    item_id = await memory.remember("auth is slow", conversation=conversation)
    items = await memory.recall(conversation=conversation)
    assert [item.text() for item in items] == ["auth is slow"]
    assert items[0].id == item_id
    await memory.forget(item_id, conversation=conversation)
    assert await memory.recall(conversation=conversation) == []


async def test_given_injected_versions_when_reading_capabilities_then_should_report_them(laser):
    caps = await laser.capabilities()
    assert caps.serves_consistency("eventual") is True
    with pytest.raises(ls.InvalidError):
        caps.serves_consistency("sometimes")
    scoped = laser.with_capabilities(
        query=True,
        sessions=True,
        versions=ls.OpVersions(query=3),
    )
    injected = await scoped.capabilities()
    assert injected.versions is not None
    assert injected.versions.query == 3
    assert injected.sessions is True
    assert injected.is_open_only() is False
    unchanged = await laser.capabilities()
    assert unchanged.filters.native == caps.filters.native
    assert unchanged.filters.group_policy_reads == caps.filters.group_policy_reads
    assert unchanged.is_open_only() == caps.is_open_only()


async def test_given_a_buffered_agdx_stream_when_flushed_then_should_publish_its_chunks(laser):
    await laser.bootstrap(partitions=1, retention=ls.TopicRetention.expire_after(86_400_000))
    conversation = ls.new_conversation_id()
    agdx = laser.agdx("agent.sessions", "worker", conversation)
    stream = agdx.stream(ls.mint_ulid(), "chat")
    stream.buffered(8, 60_000).content_type("json").with_deadline_at_micros(5_000_000).with_target(
        "reader"
    )
    channel = stream.channel
    await stream.write(b'{"token": "auth"}')
    await stream.flush()
    await stream.finish()
    with pytest.raises(ls.LaserError):
        stream.buffered(1, 1)

    records = await laser.assemble_context(conversation, topics=["agent.sessions"])
    chunks = [record.envelope for record in records if record.envelope["kind"] == "chunk"]
    assert [chunk["sequence"] for chunk in chunks] == [0, 1]
    assert [chunk.get("last", False) for chunk in chunks] == [False, True]
    assert {chunk["channel"] for chunk in chunks} == {channel}
    assert {chunk["target"] for chunk in chunks} == {"reader"}
    assert chunks[0]["deadline_micros"] == 5_000_000


async def test_given_a_session_when_reading_with_a_policy_then_should_apply_it_and_reach_the_graph(
    laser,
):
    await laser.bootstrap(partitions=1, retention=ls.TopicRetention.expire_after(86_400_000))
    session, lease = await laser.sessions().start().agent("planner").begin()
    await session.end()
    lease.release()
    turns = []
    for _ in range(100):
        turns = await session.context_with(ls.LastN(1))
        if turns:
            break
        await asyncio.sleep(0.05)
    assert [turn.display for turn in turns] == ["session.completed"]
    graph = session.graph("kg")
    assert isinstance(graph, ls.GraphHandle)
    if not (await laser.capabilities()).graph:
        with pytest.raises(ls.UnsupportedError):
            await graph.upsert([ls.graph_node_entity("Host", "node-7")], [])


async def test_given_severed_and_continuous_when_creating_a_fork_then_should_raise_invalid(laser):
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
    with pytest.raises(TypeError):
        laser.topic("readings").cbor().publish()


async def test_given_an_agent_scope_when_contract_options_are_set_then_should_retain_identity(
    laser,
):
    await laser.bootstrap(1, retention=ls.TopicRetention.expire_after(86_400_000))
    conversation = ls.new_conversation_id()
    seen = []

    async def handle(context, message):
        seen.append((message.provenance.conversation_id, message.provenance.fence_token))
        await context.respond(b"done")

    agent = laser.spawn_agent(
        "scoped-worker", "agent.sessions", handle, respond_on="agent.sessions"
    )
    try:
        await agent.ready()
        result = await laser.agent("scoped-caller").contract(
            None,
            b"work",
            agent="scoped-worker",
            fixed_inbox="agent.sessions",
            conversation=conversation,
            fence=7,
            reply_on="agent.sessions",
            deadline_ms=2_000,
        )
        assert isinstance(result, ls.Contract.Completed)
        assert result.reply.body() == b"done"
        assert seen == [(conversation, 7)]
    finally:
        await agent.shutdown()


@pytest.mark.parametrize("placement", ["namespace", "explicit_stream"])
async def test_given_scoped_log_memory_when_feedback_and_forget_run_then_should_keep_scope(
    laser, placement
):
    await laser.bootstrap(1, retention=ls.TopicRetention.expire_after(86_400_000))
    if placement == "namespace":
        left = laser.memory("left")
        right = laser.memory("right")
    else:
        stream = f"memory-{uuid.uuid4().hex[:12]}"
        await laser.with_default_stream(stream).bootstrap(
            1, retention=ls.TopicRetention.expire_after(86_400_000)
        )
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


async def test_given_log_memory_when_another_conversation_forgets_then_should_keep_the_item(laser):
    await laser.bootstrap(1, retention=ls.TopicRetention.expire_after(86_400_000))
    memory = laser.memory("owned")
    owner = ls.new_conversation_id()
    item_id = await memory.remember("kept", conversation=owner)
    await memory.improve(item_id, 3.0, conversation=ls.new_conversation_id())
    await memory.forget(item_id, conversation=ls.new_conversation_id())
    await memory.remember("marker", conversation=owner)
    for _ in range(40):
        kept = await memory.recall(folded=True, conversation=owner)
        if len(kept) == 2:
            break
        await asyncio.sleep(0.05)
    owned = [item for item in kept if item.id == item_id]
    assert len(owned) == 1
    assert owned[0].score is None
    await memory.forget(item_id, conversation=owner)
    for _ in range(40):
        remaining = await memory.recall(folded=True, conversation=owner)
        if len(remaining) == 1:
            break
        await asyncio.sleep(0.05)
    assert item_id not in [item.id for item in remaining]


async def test_given_json_rpc_bridges_when_dispatched_then_should_return_catalog_and_public_errors(
    laser,
):
    a2a = ls.A2aBridge(laser, "edge", "agent.sessions", "agent.sessions")
    mcp = ls.McpBridge(laser, "edge", "agent.sessions", "agent.sessions", "tools")
    for bridge in (a2a, mcp):
        response = await bridge.handle_rpc({"id": "request", "method": "unknown"})
        assert response == {
            "jsonrpc": "2.0",
            "id": "request",
            "error": {"code": -32601, "message": "method not found"},
        }
        response = await bridge.handle_rpc(None)
        assert response["id"] is None
        assert response["error"] == {"code": -32600, "message": "invalid request"}
    response = await mcp.handle_rpc({"id": 7, "method": "initialize"})
    assert response["result"]["protocolVersion"] == "2025-11-25"
    response = await mcp.handle_rpc({"id": None, "method": "tools/list", "params": None})
    assert response["result"] == {"tools": []}
    assert await mcp.handle_rpc({"method": "notifications/initialized"}) is None


async def test_given_managed_key_enrollment_when_called_then_should_accept_principal_and_key(laser):
    if not (await laser.capabilities()).kv.available:
        pytest.skip("managed KV is unavailable")
    # Key-value namespaces are stream scoped, so the default stream must exist
    # before the managed backend authorizes reads and writes under it.
    await laser.bootstrap(1, ls.TopicRetention.expire_after(86_400_000))
    registry = ls.KvKeyRegistry(laser, "keys")
    key = ls.SigningKey(bytes(range(32)))
    assert await registry.enroll("agent-7", key.verifying_key) is None
    record = ls.KeyRecord("agent-7", key.verifying_key)
    assert await registry.enroll_record(record) >= 1
    keys = await registry.registry()
    assert keys is not None


async def test_given_session_run_when_the_work_raises_then_should_fail_and_reraise(laser):
    await laser.bootstrap(1, ls.TopicRetention.expire_after(86_400_000))
    sessions = laser.sessions()
    session, lease = await sessions.start().agent("planner").begin()

    async def work(current):
        assert current.conversation == session.conversation
        raise ValueError("tool timed out")

    with pytest.raises(ValueError, match="tool timed out"):
        await session.run(lease, work)
    turns = await _lane(sessions.open(session.conversation), 2)
    assert [turn.display for turn in turns[:2]] == ["session.started", "session.failed"]
    ok, ok_lease = await sessions.start().agent("planner").begin()
    assert await ok.run(ok_lease, lambda current: 42) == 42
    turns = await _lane(sessions.open(ok.conversation), 2)
    assert turns[1].display == "session.completed"


async def test_given_model_and_tool_calls_when_recorded_then_should_correlate_with_manifest(
    laser,
):
    await laser.bootstrap(1, ls.TopicRetention.expire_after(86_400_000))
    sessions = laser.sessions()
    session, lease = await sessions.start().agent("planner").begin()
    assembled = await session.assemble(ls.LastN(10))
    assert assembled.manifest["fragments"] == [], "the start record never reaches a prompt"
    call = await session.model(
        ls.ModelRequest("gpt-test", b'{"prompt":"hi","api_key":"k"}', provider="test"),
        assembled,
    )
    await call.complete(
        ls.ModelResponse(
            b"hello",
            model="gpt-test-1",
            finish_reason="stop",
            usage={"input_tokens": 3, "output_tokens": 1, "cost_micros": 20},
        )
    )
    tool = await session.tool("lookup", {"id": 7, "token": "secret"})
    await tool.complete(b"found")
    lease.release()
    turns = await _lane(sessions.open(session.conversation), 6)
    assert [turn.display for turn in turns[:6]] == [
        "session.started",
        "model.request",
        "context.assembled",
        "model.response",
        "tool.call",
        "tool.result",
    ]
    request = turns[1].message.envelope
    assert b'"k"' not in bytes(request["body"])
    assert request["metadata"]["gen_ai.request.model"] == "gpt-test"
    tool_call = turns[4].message.envelope
    assert b"secret" not in bytes(tool_call["body"])
    assert turns[3].message.envelope["correlation"] == request["correlation"]
    assert turns[5].message.envelope["correlation"] == tool_call["correlation"]


async def test_given_session_state_when_patched_then_should_fold_and_snapshot_on_end(laser):
    await laser.bootstrap(1, ls.TopicRetention.expire_after(86_400_000))
    sessions = laser.sessions()
    session, lease = await sessions.start().agent("planner").begin()
    state = session.state()
    await state.set("step", 1)
    await state.patch([{"op": "add", "path": "/done", "value": False}])
    await session.end()
    lease.release()
    view = None
    for _ in range(100):
        view = await state.get()
        if view["revision"] == 2:
            break
        await asyncio.sleep(0.05)
    assert view["document"] == {"step": 1, "done": False}
    assert view["complete"] is True
    turns = await _lane(sessions.open(session.conversation), 5)
    # Two deltas, then the snapshot `end` writes.
    assert [turn.display for turn in turns].count("state.updated") == 3


async def test_given_a_submitted_session_when_picked_up_then_should_complete_from_the_handler(
    laser,
):
    await laser.bootstrap(1, ls.TopicRetention.expire_after(86_400_000))

    async def handle(ctx, message):
        await ctx.session().end()

    worker = laser.spawn_agent("worker", ls.AgentTopic.Sessions, handle)
    await worker.ready()
    try:
        submitted = (
            await laser.sessions().submit("worker", b"{}").from_("client").label("ticket-7").send()
        )
        displays = []
        for _ in range(200):
            turns = await laser.sessions().open(submitted.session).context()
            displays = [turn.display for turn in turns]
            if "session.completed" in displays:
                break
            await asyncio.sleep(0.05)
        assert displays[0] == "session.submitted"
        assert "session.resumed" in displays
        assert "session.completed" in displays
    finally:
        await worker.shutdown()


async def test_given_operator_control_when_sent_then_should_ride_the_control_topic(laser):
    await laser.topic(ls.AgentTopic.Control).ensure(partitions=4)
    session = ls.new_conversation_id()
    control = laser.sessions().control(laser.default_stream, session).as_operator("operator")
    await control.cancel()
    await control.force_cancel()
    records = []
    for _ in range(100):
        records = await laser.context(session).fetch(topics=[ls.AgentTopic.Control], n=10)
        if len(records) == 2:
            break
        await asyncio.sleep(0.05)
    assert records[0].envelope["operation"] == "session_cancel"
    assert ls.TaskState.from_code(records[1].envelope["task_state"]) == ls.TaskState.Canceled


def test_given_secret_keys_when_redacted_by_default_then_should_drop_only_their_values():
    redacted = ls.default_redact(
        {"query": "q", "Authorization": "x", "nested": [{"password": "p"}]}
    )
    assert redacted == {
        "query": "q",
        "Authorization": "[redacted]",
        "nested": [{"password": "[redacted]"}],
    }


async def test_given_a_retrieval_and_a_compaction_when_recorded_then_should_show_on_the_lane(laser):
    await laser.bootstrap(1, ls.TopicRetention.expire_after(86_400_000))
    sessions = laser.sessions()
    session, lease = await sessions.start().agent("planner").begin()
    await session.memory().remember("the deploy key rotates on fridays")
    items = []
    for _ in range(100):
        items = await session.memory().recall(semantic="deploy", strategy="keyword", folded=True)
        if items:
            break
        await asyncio.sleep(0.05)
    assert len(items) == 1
    await session.record_retrieval("deploy", items)
    await session.record_compaction(
        {
            "summary_at": {
                "Message": {
                    "stream": 1,
                    "topic": 1,
                    "partition": 0,
                    "offset": 0,
                    "conversation": session.conversation,
                }
            },
            "covered": [[1, 0, 0, 3]],
            "summarizer": {"name": "summarizer", "version": "1"},
        }
    )
    lease.release()
    displays = []
    turns = []
    for _ in range(100):
        turns = await sessions.open(session.conversation).context_with(ls.LastN(20))
        displays = [turn.display for turn in turns]
        if "context.compacted" in displays:
            break
        await asyncio.sleep(0.05)
    assert "context.retrieved" in displays
    assert "context.compacted" in displays
    first = turns[0].message
    assert first.timestamp_micros > 0
    assert all(turn.message.topic_id == first.topic_id for turn in turns)


async def test_given_an_a2a_task_in_a_parent_session_when_submitted_then_should_start_a_child(
    laser,
):
    await laser.bootstrap(partitions=2, retention=ls.TopicRetention.expire_after(86_400_000))
    bridge = ls.A2aBridge(laser, "a2a-gw", "agent.sessions", "agent.sessions")
    parent = ls.new_conversation_id()
    task = await bridge.submit_in(
        parent, {"message": {"role": "user", "parts": [{"kind": "text", "text": "hi"}]}}
    )
    turns = []
    for _ in range(100):
        turns = await laser.sessions().open(task["id"]).context()
        if turns:
            break
        await asyncio.sleep(0.05)
    assert turns[0].display == "session.submitted"
    start = ls.decode_session_start(bytes(turns[0].message.envelope["body"]))
    assert start["parent"] == parent
    assert start["root"] == parent


async def test_given_open_iggy_when_reading_the_session_index_then_should_be_unsupported(
    open_laser,
):
    laser = open_laser
    await laser.bootstrap(1, ls.TopicRetention.expire_after(86_400_000))
    sessions = laser.sessions()
    with pytest.raises(ls.UnsupportedError):
        await sessions.get(ls.new_conversation_id())
    with pytest.raises(ls.UnsupportedError):
        await sessions.list(results=10)
    with pytest.raises(ls.UnsupportedError):
        await sessions.changes()


async def test_given_a_token_budget_when_usage_passes_it_then_should_fold_the_lane_over_budget(
    open_laser,
):
    laser = open_laser
    await laser.bootstrap(1, ls.TopicRetention.expire_after(86_400_000))
    sessions = laser.sessions()
    session, lease = await sessions.start().agent("planner").budget(ls.Budget(tokens=100)).begin()

    async def record(input_tokens, output_tokens):
        await session.record_model_call(
            ls.ModelRequest("gpt-test", b"q"),
            ls.ModelResponse(
                b"a",
                usage={"input_tokens": input_tokens, "output_tokens": output_tokens},
            ),
        )

    await record(60, 40)
    assert await session.over_budget() is False
    await record(1, 0)
    assert await session.over_budget() is True
    unbounded, unbounded_lease = await sessions.start().agent("planner").begin()
    await unbounded.record_model_call(
        ls.ModelRequest("gpt-test", b"q"),
        ls.ModelResponse(b"a", usage={"input_tokens": 10_000, "output_tokens": 10_000}),
    )
    assert await unbounded.over_budget() is False
    lease.release()
    unbounded_lease.release()


async def test_given_a_message_reference_when_read_at_then_should_return_that_record(laser):
    await laser.bootstrap(1, ls.TopicRetention.expire_after(86_400_000))
    session, lease = await laser.sessions().start().agent("planner").begin()
    lease.release()
    turns = []
    for _ in range(100):
        turns = await session.context()
        if turns:
            break
        await asyncio.sleep(0.05)
    first = turns[0].message
    at = {
        "Message": {
            "stream": first.stream_id,
            "topic": first.topic_id,
            "partition": first.id.partition_id,
            "offset": first.id.offset,
        }
    }
    record = await laser.read_at(at)
    assert record is not None
    assert record.envelope == first.envelope
    at["Message"]["generation"] = 1
    assert await laser.read_at(at) is None


async def test_given_an_operator_cancel_when_reading_pending_control_then_should_report_it(laser):
    await laser.bootstrap(1, ls.TopicRetention.expire_after(86_400_000))
    await laser.topic(ls.AgentTopic.Control).ensure(partitions=1)
    session, lease = await laser.sessions().start().agent("planner").begin()
    control = laser.sessions().control(laser.default_stream, session.conversation)
    await control.as_operator("operator").cancel()
    pending = None
    for _ in range(100):
        pending = await session.pending_control()
        if pending.cancel_requested:
            break
        await asyncio.sleep(0.05)
    assert pending.cancel_requested
    assert not pending.pause_requested
    assert await session.cancel_requested()
    lease.release()


async def test_given_a_scope_on_another_stream_when_writing_log_memory_then_should_refuse_it(laser):
    memory = laser.memory("notes")
    with pytest.raises(ls.InvalidError, match="another-stream"):
        await memory.remember("fact", stream="another-stream")
    with pytest.raises(ls.InvalidError):
        await memory.forget(ls.new_conversation_id(), stream="another-stream")
    with pytest.raises(ls.InvalidError):
        await memory.improve(ls.new_conversation_id(), 1.0, stream="another-stream")


async def test_given_a_backlog_past_one_drain_when_recalled_folded_then_should_fold_every_record(
    laser,
):
    await laser.bootstrap(1, retention=ls.TopicRetention.expire_after(86_400_000))
    topic = laser.topic("agent.memory")
    for _ in range(10):
        await topic.batch([b"foreign traffic"] * 1_000)
    conversation = ls.new_conversation_id()
    remembered = await laser.memory("notes").remember(
        "after the backlog", conversation=conversation
    )
    # A probe handle waits until the record is readable, then a fresh handle
    # must fold past the 10,000 record drain in its one recall.
    probe = laser.memory("notes")
    for _ in range(40):
        if await probe.recall(conversation=conversation, folded=True):
            break
        await asyncio.sleep(0.05)
    recalled = await laser.memory("notes").recall(conversation=conversation, folded=True)
    assert [item.id for item in recalled] == [remembered]
