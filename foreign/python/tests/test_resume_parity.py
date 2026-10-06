import asyncio
import json

import laser_sdk as ls
import pytest


async def test_given_a_handled_message_when_remembered_then_should_retain_its_scope():
    memory = ls.MemoryHandle.vector(lambda _: [1.0])
    conversation = ls.new_conversation_id()
    message = ls.agent_message(
        b"auth restarted", ls.Provenance(conversation_id=conversation, agent="planner")
    )
    handled = []

    async def handle(context, incoming):
        handled.append(incoming.payload)

    wrapped = ls.MemoryHandler(handle, memory).auto_remember("message")
    await wrapped(None, message)
    assert handled == [b"auth restarted"]
    items = await memory.recall(conversation=conversation, agent="planner")
    assert [item.text() for item in items] == ["auth restarted"]
    assert items[0].kind == "message"


async def test_given_a_handler_failure_when_remembering_then_should_skip_memory():
    memory = ls.MemoryHandle.vector(lambda _: [1.0])
    conversation = ls.new_conversation_id()
    message = ls.agent_message(b"auth restarted", ls.Provenance(conversation_id=conversation))

    async def handle(context, incoming):
        raise ValueError("handling failed")

    wrapped = ls.MemoryHandler(handle, memory).auto_remember("message")
    with pytest.raises(ValueError, match="handling failed"):
        await wrapped(None, message)
    assert await memory.recall(conversation=conversation) == []


async def test_given_failed_memory_when_the_handler_succeeds_then_should_not_retry():
    def fail_embed(_):
        raise ValueError("embedding failed")

    memory = ls.MemoryHandle.vector(fail_embed)
    message = ls.agent_message(b"auth restarted", ls.Provenance())
    handled = []

    async def handle(context, incoming):
        handled.append(incoming.payload)

    await ls.MemoryHandler(handle, memory).auto_remember("message")(None, message)
    assert handled == [b"auth restarted"]


def test_given_content_and_kind_when_reading_memory_helpers_then_should_match_the_reference():
    assert ls.memory_id_content(
        "fact", b"auth", stream="fleet", agent="planner"
    ) == ls.memory_id_content("fact", b"auth", stream="fleet", agent="planner")
    assert (
        ls.memory_id_content(
            "fact", b"x", stream="laser", agent="agent", user="alice", application="console"
        )
        == "28429K3B8MPSM8WHDQR474827P"
    )
    assert ls.memory_id_content("procedure", b"x", user="alice") == "3E6HJGHKFDBMQ91TCJEHV28X40"
    assert ls.memory_kind_class("fact") == "semantic"
    assert ls.Sessions.turn_kind(ls.Sessions.turn_topic("instruction")) == "instruction"


@pytest.mark.integration
async def test_given_a_schema_handle_when_publishing_then_should_share_its_cached_codec(laser):
    if not (await laser.capabilities()).managed:
        pytest.skip("schema registration requires a managed backend")
    source = {
        "kind": "avro",
        "schema": json.dumps(
            {
                "type": "record",
                "name": "Reading",
                "fields": [{"name": "host", "type": "string"}, {"name": "cpu", "type": "int"}],
            }
        ),
    }
    schema_id = await laser.schemas().register(source, name=f"python-{laser.default_stream}")
    try:
        deadline = asyncio.get_running_loop().time() + 5
        while await laser.schemas().get(schema_id) is None:
            assert asyncio.get_running_loop().time() < deadline
            await asyncio.sleep(0.05)
        topic = await laser.topic("schema-readings").schema(schema_id)
        with pytest.raises(TypeError):
            topic.publish()
        with pytest.raises(ls.CodecError):
            topic.publish({"host": "node-7", "cpu": "bad"})
        await laser.schemas().drop(schema_id)
        await topic.publish({"host": "node-7", "cpu": 82}).send()
        record = await topic.records("schema-reader").next()
        assert record.value == {"host": "node-7", "cpu": 82}
    finally:
        await laser.schemas().drop(schema_id)


def test_given_provenance_when_a_fence_is_set_then_should_expose_the_token():
    assert ls.Provenance(fence_token=7).fence_token == 7
    assert ls.Provenance().fence_token is None


@pytest.mark.integration
async def test_given_custom_memory_when_scope_and_query_are_set_then_should_pass_every_field(laser):
    calls = []

    class Hooks:
        async def remember(self, scope, payload):
            return ls.new_conversation_id()

        async def recall(self, scope, query):
            calls.append((scope, query))
            return []

        async def forget(self, scope, target):
            calls.append((scope, target))

        async def improve(self, scope, feedback):
            calls.append((scope, feedback))
            return ls.new_conversation_id()

    memory = laser.memory_custom(Hooks())
    await memory.recall(
        stream="metrics",
        durable=True,
        token_budget=24,
        user="reader",
        application="monitor",
        agent="planner",
    )
    scope, query = calls[-1]
    assert scope == {
        "stream": "metrics",
        "agent": "planner",
        "conversation": None,
        "user": "reader",
        "application": "monitor",
        "lifetime": "durable",
    }
    assert query["token_budget"] == 24
    assert query["agent"] == "planner"
    conversation = ls.new_conversation_id()
    await (
        laser.context(conversation)
        .memory(memory)
        .recall(stream="metrics", durable=True, token_budget=12)
    )
    assert calls[-1][0]["stream"] == "metrics"
    assert calls[-1][0]["lifetime"] == "durable"
    assert calls[-1][0]["conversation"] == conversation
    assert calls[-1][1]["token_budget"] == 12
    target = ls.new_conversation_id()
    await memory.improve(
        target,
        2.0,
        stream="metrics",
        user="reader",
        application="monitor",
        durable=True,
        note="operator feedback",
    )
    assert calls[-1][0]["lifetime"] == "durable"
    assert calls[-1][1]["note"] == "operator feedback"
    await memory.forget(
        target, stream="metrics", user="reader", application="monitor", durable=True
    )
    assert calls[-1][0]["stream"] == "metrics"
    assert calls[-1][0]["user"] == "reader"
    assert calls[-1][0]["application"] == "monitor"


@pytest.mark.integration
async def test_given_context_history_when_bounded_then_should_honor_children_offsets_and_policies(
    laser,
):
    await laser.bootstrap(1)
    parent = ls.Provenance(agent="planner")
    child = laser.spawn_subconversation(parent)
    await laser.send_agent("agent.audit", b"parent", parent)
    await laser.send_agent("agent.audit", b"child", child)
    checkpoint = await laser.context(parent.conversation_id).checkpoint(["agent.audit"])
    await laser.send_agent("agent.audit", b"tail", parent)

    async def read(**options):
        return [
            message.payload
            for message in await laser.assemble_context(
                parent.conversation_id, topics=["agent.audit"], **options
            )
        ]

    assert await read() == [b"parent", b"tail"]
    assert await read(across_subconversations=True) == [b"parent", b"child", b"tail"]
    assert await read(from_checkpoint=checkpoint) == [b"tail"]
    assert await read(across_subconversations=True, to_checkpoint=checkpoint) == [
        b"parent",
        b"child",
    ]
    assert await read(across_subconversations=True, from_offsets={0: 1}) == [b"child", b"tail"]
    assert await read(across_subconversations=True, policy=ls.LastN(1)) == [b"tail"]
    assert await read(policy=lambda messages: list(reversed(messages))) == [b"tail", b"parent"]

    class TailPolicy:
        def select(self, messages):
            return messages[-1:]

    assert await read(policy=TailPolicy()) == [b"tail"]
    assert await read(roles=["planner"], last_n=1) == [b"tail"]
    with pytest.raises(ls.InvalidError, match="policy cannot be combined"):
        await read(policy=ls.LastN(1), last_n=2)


@pytest.mark.integration
async def test_given_a_failing_estimator_when_assembling_context_then_should_raise_its_error(laser):
    provenance = ls.Provenance()
    await laser.send_agent("agent.audit", b"record", provenance)

    def fail(message):
        raise ValueError("tokenizer failed")

    with pytest.raises(ValueError, match="tokenizer failed"):
        await laser.assemble_context(
            provenance.conversation_id,
            topics=["agent.audit"],
            policy=ls.TokenBudget(100, estimator=fail),
        )


@pytest.mark.integration
async def test_given_a_failing_custom_policy_when_assembling_then_should_raise_its_error(laser):
    provenance = ls.Provenance()
    await laser.send_agent("agent.audit", b"record", provenance)

    def fail(messages):
        raise ValueError("policy failed")

    with pytest.raises(ValueError, match="policy failed"):
        await laser.assemble_context(
            provenance.conversation_id, topics=["agent.audit"], policy=fail
        )

    async def async_policy(messages):
        return messages

    with pytest.raises(ls.InvalidError, match="synchronously"):
        await laser.assemble_context(
            provenance.conversation_id, topics=["agent.audit"], policy=async_policy
        )


@pytest.mark.integration
async def test_given_an_unscoped_consumer_when_records_arrive_then_should_handle_without_identity(
    laser,
):
    await laser.bootstrap(1)
    handled = []
    delivered = asyncio.Event()

    async def handle(context, message):
        handled.append(message.payload)
        delivered.set()

    with pytest.raises(ls.InvalidError, match="requires consumer_group"):
        laser.spawn_agent(None, "agent.commands", handle)
    with pytest.raises(ls.InvalidError, match="requires an agent identity"):
        laser.spawn_agent(
            None, "agent.commands", handle, consumer_group="unscoped", capabilities=["diagnose"]
        )
    consumer = laser.spawn_agent(None, "agent.commands", handle, consumer_group="unscoped")
    try:
        await consumer.ready()
        await laser.send_agent("agent.commands", b"record", ls.Provenance())
        await asyncio.wait_for(delivered.wait(), 2)
        assert handled == [b"record"]
    finally:
        await consumer.shutdown()


async def test_given_recalled_items_when_rendered_then_should_preserve_order_and_omissions():
    memory = ls.MemoryHandle.vector(lambda _: [1.0])
    for body in ("alpha", "beta", "gamma"):
        await memory.remember(body)
    items = await memory.recall()
    assert ls.to_context_block(items) == "\n\n".join(item.text() for item in items)
    assert (
        ls.to_context_block(items, token_budget=0)
        == items[0].text() + "\n\n[... 2 more recalled item(s) omitted ...]"
    )
    assert ls.to_context_block([], token_budget=0) == ""


@pytest.mark.integration
async def test_given_an_arrow_record_when_a_fingerprint_is_set_then_should_stamp_exact_bytes(laser):
    fingerprint = bytes(range(32))
    topic = laser.topic("fingerprint-records")
    await (
        topic.publish_batch()
        .add_record(b"raw payload", content_type="arrow", logical_schema_fingerprint=fingerprint)
        .send()
    )
    consumer = topic.consumer(
        "fingerprint-reader", polling="first", auto_commit="disabled", allow_replay=True
    )
    try:
        message = await consumer.next_within(2)
        assert message.payload == b"raw payload"
        assert message.headers["agdx.sfp"] == fingerprint
        assert message.headers["agdx.ct"] == 7
    finally:
        await consumer.shutdown()


@pytest.mark.integration
@pytest.mark.parametrize(
    ("content_type", "fingerprint"), [("json", bytes(32)), ("arrow", bytes(31))]
)
async def test_given_invalid_fingerprint_metadata_when_sending_then_should_refuse(
    laser, content_type, fingerprint
):
    request = (
        laser.topic("fingerprint-invalid")
        .publish_batch()
        .add_record(b"body", content_type=content_type, logical_schema_fingerprint=fingerprint)
    )
    with pytest.raises(ls.InvalidError):
        await request.send()


async def test_given_an_active_embedder_when_the_call_is_cancelled_then_should_stop_the_callback():
    active = asyncio.Event()
    stopped = asyncio.Event()

    async def embed(text):
        active.set()
        try:
            await asyncio.Event().wait()
        finally:
            stopped.set()

    memory = ls.MemoryHandle.vector(embed)
    pending = asyncio.ensure_future(memory.remember("record"))
    await asyncio.wait_for(active.wait(), 2)
    pending.cancel()
    await asyncio.gather(pending, return_exceptions=True)
    await asyncio.wait_for(stopped.wait(), 2)
    assert await memory.recall() == []


@pytest.mark.integration
@pytest.mark.parametrize("stop", ["abort", "shutdown"])
async def test_given_an_active_handler_when_stopped_then_should_cancel_its_callback(laser, stop):
    await laser.bootstrap(1)
    active = asyncio.Event()
    stopped = asyncio.Event()
    effects = []

    async def handle(context, message):
        active.set()
        try:
            await asyncio.Event().wait()
            effects.append("late effect")
        finally:
            stopped.set()

    agent = laser.spawn_agent("cancellable-worker", "agent.commands", handle, shutdown_grace_ms=5)
    try:
        await agent.ready()
        await laser.send_agent("agent.commands", b"record", ls.Provenance())
        await asyncio.wait_for(active.wait(), 2)
        if stop == "abort":
            agent.abort()
        else:
            with pytest.raises(ls.TimeoutError, match="agent shutdown drain"):
                await agent.shutdown()
        await asyncio.wait_for(stopped.wait(), 2)
        assert effects == []
    finally:
        agent.abort()
        try:
            await agent.join()
        except ls.ConfigError:
            pass


@pytest.mark.integration
async def test_given_a_typed_custom_backend_when_remembered_then_should_keep_kind_and_id(laser):
    calls = []
    entries = {}

    class Hooks:
        async def remember(self, scope, payload):
            raise AssertionError("typed append was not used")

        async def append(self, scope, memory_id, kind, payload):
            calls.append((scope, memory_id, kind, payload))
            entries[memory_id] = {"id": memory_id, "payload": payload, "kind": kind}
            return memory_id

        async def recall(self, scope, query):
            return list(entries.values())

        async def improve(self, scope, feedback):
            return ls.new_conversation_id()

        async def forget(self, scope, target):
            entries.pop(target, None)

    memory = laser.memory_custom(Hooks())
    options = {"kind": "message", "stream": "fleet", "agent": "planner", "dedup": True}
    first = await memory.remember(b"source", **options)
    second = await memory.remember(b"source", **options)
    expected = ls.memory_id_content("message", b"source", stream="fleet", agent="planner")
    assert first == second == expected
    assert len(entries) == 1
    assert [call[2] for call in calls] == ["message", "message"]
    explicit_id = ls.new_conversation_id()
    assert await memory.append(explicit_id, b"entity", kind="entity") == explicit_id
    assert calls[-1][1:4] == (explicit_id, "entity", b"entity")
    report = await memory.consolidate(50, summarizer=lambda bodies: b"summary")
    assert report.summarized == 1
    assert calls[-1][2] == "summary"
    assert calls[-1][0]["lifetime"] == "durable"
    assert calls[-1][3] == b"summary"


async def test_given_an_explicit_memory_id_when_appended_then_should_keep_the_id_kind_and_scope():
    memory = ls.MemoryHandle.vector(lambda _: [1.0])
    memory_id = ls.new_conversation_id()
    scope = {
        "conversation": ls.new_conversation_id(),
        "agent": "planner",
        "user": "reader",
        "application": "notes",
        "stream": "fleet",
        "durable": True,
    }
    assert await memory.append(memory_id, b"typed", kind="entity", **scope) == memory_id
    items = await memory.recall(strategy="recent", **scope)
    assert len(items) == 1
    assert items[0].id == memory_id
    assert items[0].kind == "entity"
    assert items[0].text() == "typed"
    assert await memory.recall(strategy="recent", **{**scope, "stream": "another-fleet"}) == []
