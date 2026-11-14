import asyncio

import laser_sdk as ls
import pytest

pytestmark = pytest.mark.integration


async def _eventually(read, description):
    deadline = asyncio.get_running_loop().time() + 15
    while True:
        value = await read()
        if value is not None:
            return value
        assert asyncio.get_running_loop().time() < deadline, description
        await asyncio.sleep(0.05)


async def _bootstrap(laser):
    await laser.bootstrap(4, ls.TopicRetention.expire_after(86_400_000))
    return laser.sessions()


async def _heartbeat_bodies(laser):
    # Every heartbeat body on the default stream, in log order. Python has no
    # typed heartbeat, so a body decodes to a dict through the CBOR codec.
    cursor = laser.topic(ls.AgentTopic.Heartbeats).replay()
    bodies = []
    while True:
        batch = await cursor.poll()
        if not batch:
            return bodies
        for message in batch:
            bodies.append(bytes(ls.Cbor.decode(bytes(message.payload))["body"]))


async def _statuses(laser, session_id):
    turns = await laser.sessions().open(session_id).context_with(ls.LastN(20))
    return [
        ls.TaskState.from_code(turn.message.envelope["task_state"])
        for turn in turns
        if turn.message.envelope is not None and turn.message.envelope.get("operation") == "session"
    ]


async def test_given_recorded_calls_on_its_lane_when_the_agent_runs_then_should_get_only_real_work(
    laser,
):
    sessions = await _bootstrap(laser)
    handled = []

    async def handle(ctx, message):
        handled.append(bytes(message.envelope["body"]))

    agent = laser.spawn_agent("planner", ls.AgentTopic.Sessions, handle)
    await agent.ready()
    session, lease = await sessions.start().agent("planner").begin()
    await session.model(ls.ModelRequest("m", b"prompt"))
    await session.tool("lookup", {"id": 7})
    await laser.agdx(ls.AgentTopic.Sessions, "client", session.conversation).command(
        ls.mint_ulid(), b"real", operation="invoke_agent", target="planner"
    )

    async def real():
        return True if b"real" in handled else None

    await _eventually(real, "the real work reaches the handler")
    await agent.shutdown()
    lease.release()
    assert handled == [b"real"]


async def test_given_an_ended_labeled_session_when_begun_then_should_raise_and_resume_should_reopen(
    laser,
):
    sessions = await _bootstrap(laser)
    session, lease = await sessions.create("ticket-7").agent("desk").begin()
    await session.end()
    lease.release()
    with pytest.raises(ls.SessionEndedError) as refused:
        await sessions.create("ticket-7").agent("desk").begin()
    assert refused.value.status == "completed"
    assert refused.value.session == session.conversation
    resumed, lease = await sessions.create("ticket-7").agent("desk").resume()
    assert resumed.conversation == session.conversation
    await resumed.cancel()
    lease.release()
    with pytest.raises(ls.SessionEndedError) as canceled:
        await sessions.create("ticket-7").agent("desk").resume()
    assert canceled.value.status == "canceled"


async def test_given_a_submission_when_built_then_should_name_its_session_before_sending(laser):
    sessions = await _bootstrap(laser)
    builder = sessions.submit("worker", b"ticket").from_("intake")
    session_id = builder.id()
    assert builder.id() == session_id
    submitted = await builder.send()
    assert submitted.session == session_id
    labeled = sessions.submit("worker", b"ticket").label("t-1")
    assert labeled.id() == ls.derive_session_id(laser.default_stream, "", "t-1")


async def test_given_session_records_when_assembled_then_should_hold_the_conversation_bodies(laser):
    sessions = await _bootstrap(laser)
    session, lease = await sessions.start().agent("planner").begin()
    await laser.agdx(ls.AgentTopic.Sessions, "user", session.conversation).command(
        ls.mint_ulid(), b"hi", operation="chat", target="planner"
    )
    call = await session.model(ls.ModelRequest("m", b"hi"))
    await call.complete(ls.ModelResponse(b"hello"))
    await session.state().set("k", 1)

    async def assembled():
        context = await session.assemble(ls.LastN(10))
        return context if len(context.fragments) >= 2 else None

    context = await _eventually(assembled, "the conversation records are read")
    assert context.text() == "hi\nhello"
    assert [fragment.body for fragment in context.fragments] == [b"hi", b"hello"]
    assert all(len(fragment.payload) > len(fragment.body) for fragment in context.fragments)
    assert context.manifest["bytes"] == 7
    lease.release()


async def test_given_a_cancelled_task_in_async_with_when_it_exits_then_should_end_canceled(
    laser,
):
    sessions = await _bootstrap(laser)
    entered = asyncio.Event()
    holder = {}

    async def work():
        async with sessions.start().agent("planner") as session:
            holder["id"] = session.conversation
            entered.set()
            await asyncio.sleep(60)

    task = asyncio.create_task(work())
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    async def ended():
        statuses = await _statuses(laser, holder["id"])
        return statuses if len(statuses) >= 2 else None

    statuses = await _eventually(ended, "the terminal record lands")
    assert statuses[-1] == ls.TaskState.Canceled


async def test_given_a_zero_idle_timeout_when_beginning_then_should_raise_invalid(laser):
    sessions = await _bootstrap(laser)
    with pytest.raises(ls.InvalidError):
        await sessions.start().agent("planner").idle_timeout(0).begin()


async def test_given_a_heartbeat_of_a_deleted_stream_when_read_then_should_name_another_generation(
    laser,
):
    await _bootstrap(laser)
    _, lease = await laser.sessions(heartbeat_ms=50).start().agent("planner").begin()

    async def first():
        bodies = await _heartbeat_bodies(laser)
        return bodies[-1] if bodies else None

    late = await _eventually(first, "a heartbeat of the first stream")
    lease.release()
    await asyncio.sleep(0.2)
    assert await laser.stream(laser.default_stream).delete()
    await _bootstrap(laser)
    # The server appends a send it processes after the stream was created
    # again, since the replacement reuses the numeric ids.
    await laser.agdx(ls.AgentTopic.Heartbeats, "planner", ls.mint_ulid()).status(
        "progress", body=late, content_type="cbor"
    )
    _, fresh = await laser.sessions(heartbeat_ms=50).start().agent("planner").begin()

    async def replacement():
        beats = [ls.Cbor.decode(body) for body in await _heartbeat_bodies(laser)]
        return beats if len(beats) > 1 else None

    beats = await _eventually(replacement, "a heartbeat of the replacement's own lease")
    fresh.release()
    generation = beats[-1].get("stream_generation")
    assert isinstance(generation, int), beats[-1]
    assert ls.Cbor.decode(late).get("stream_generation") != generation, (
        "a heartbeat of the deleted stream names the replacement's generation"
    )
