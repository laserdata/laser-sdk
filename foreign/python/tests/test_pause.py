import asyncio
import json

import laser_sdk as ls
import pytest

pytestmark = pytest.mark.integration

WORKER = "pauser"


async def _eventually(read, description):
    deadline = asyncio.get_running_loop().time() + 15
    while True:
        value = await read()
        if value is not None:
            return value
        assert asyncio.get_running_loop().time() < deadline, description
        await asyncio.sleep(0.05)


async def _setup(laser):
    await laser.bootstrap(4, ls.TopicRetention.expire_after(86_400_000))
    await laser.topic(ls.AgentTopic.Control).ensure(partitions=4)
    return laser.sessions().open(ls.new_conversation_id())


def _worker(laser, seen, max_partitions=None):
    async def handle(ctx, message):
        seen.append(bytes(message.envelope["body"]).decode())

    return laser.spawn_agent(
        WORKER,
        ls.AgentTopic.Sessions,
        handle,
        poll_interval_ms=10,
        max_partitions=max_partitions,
    )


def _control(laser, session):
    return (
        laser.sessions().control(laser.default_stream, session.conversation).as_operator("operator")
    )


def _position(receipt):
    assert isinstance(receipt, ls.AgdxReceipt)
    assert receipt.partition_id is not None
    assert receipt.offset is not None
    return receipt.partition_id, receipt.offset


async def _send(laser, session, body):
    return _position(
        await laser.agdx(ls.AgentTopic.Sessions, "client", session.conversation).command(
            ls.mint_ulid(), body.encode(), target=WORKER, receipt=True
        )
    )


async def _envelopes(laser, session, topic=ls.AgentTopic.Sessions):
    records = await laser.context(session.conversation).fetch_with([topic], ls.LastN(1000))
    return [record.envelope for record in records if record.envelope is not None]


async def _acknowledged(laser, session, state, request):
    for envelope in await _envelopes(laser, session):
        if envelope.get("operation") != "session" or envelope.get("task_state") != state.code():
            continue
        transition = ls.decode_session_transition(bytes(envelope["body"]))
        at = transition.get("acknowledges")
        if at is not None and transition.get("actor") == WORKER:
            position = ls.LogPosition.from_bytes(bytes(at))
            if (position.partition_id, position.offset) == request:
                return True
    return False


async def _wait_ack(laser, session, state, request):
    async def read():
        return True if await _acknowledged(laser, session, state, request) else None

    await _eventually(read, f"worker did not acknowledge {state} for {request}")


async def _wait_parked(session, count):
    async def read():
        held = await session.parked()
        assert isinstance(held, ls.ParkedRecords)
        return held if len(held.records) == count else None

    held = await _eventually(read, f"session did not retain {count} parked records")
    assert held.complete
    return held


async def _wait_seen(seen, body):
    async def read():
        return True if body in seen else None

    await _eventually(read, f"worker did not handle {body}")
    assert seen.count(body) == 1


@pytest.mark.parametrize("max_partitions", [None, 4], ids=["serial", "serial-per-partition"])
async def test_given_paused_work_when_resumed_repeatedly_then_should_dispatch_once_in_order(
    laser, max_partitions
):
    session = await _setup(laser)
    seen = []
    worker = _worker(laser, seen, max_partitions)
    control = _control(laser, session).participants([WORKER])
    try:
        await worker.ready()
        await _send(laser, session, "before")
        await _wait_seen(seen, "before")
        for cycle in range(3):
            pause = _position(await control.pause())
            await _wait_ack(laser, session, ls.TaskState.Paused, pause)
            assert (await session.pending_control()).pause_requested
            body = f"held-{cycle}"
            source = await _send(laser, session, body)
            held = await _wait_parked(session, 1)
            record = held.records[0]
            assert record["role"] == WORKER
            request = ls.LogPosition.from_bytes(bytes(record["request"]))
            assert (request.partition_id, request.offset) == pause
            assert (
                record["source"]["Message"]["partition"],
                record["source"]["Message"]["offset"],
            ) == source
            assert record["source"]["Message"]["generation"] > 0
            assert body not in seen

            resume = _position(await control.resume())
            await _wait_ack(laser, session, ls.TaskState.Working, resume)
            await _wait_seen(seen, body)
            await _wait_parked(session, 0)
            assert not (await session.pending_control()).pause_requested

        await _send(laser, session, "after")
        await _wait_seen(seen, "after")
        assert seen == ["before", "held-0", "held-1", "held-2", "after"]
        operations = [envelope.get("operation") for envelope in await _envelopes(laser, session)]
        assert operations.count("session_parked") == 3
        assert operations.count("session_unparked") == 3
    finally:
        await worker.shutdown()


async def test_given_prior_work_when_pausing_without_participants_then_should_infer_the_worker(
    laser,
):
    session = await _setup(laser)
    seen = []
    worker = _worker(laser, seen)
    try:
        await worker.ready()
        await _send(laser, session, "first")
        await _wait_seen(seen, "first")
        pause = _position(await _control(laser, session).pause())
        requests = await _envelopes(laser, session, ls.AgentTopic.Control)
        request = next(
            envelope for envelope in requests if envelope["operation"] == "session_pause"
        )
        assert json.loads(bytes(request["body"]))["participants"] == [WORKER]
        await _wait_ack(laser, session, ls.TaskState.Paused, pause)
    finally:
        await worker.shutdown()


async def test_given_an_empty_lane_when_paused_then_should_hold_work_from_a_later_participant(
    laser,
):
    session = await _setup(laser)
    control = _control(laser, session)
    pause = _position(await control.pause())
    requests = await _envelopes(laser, session, ls.AgentTopic.Control)
    assert json.loads(bytes(requests[0]["body"])).get("participants", []) == []
    assert (await session.pending_control()).pause_requested
    seen = []
    worker = _worker(laser, seen)
    try:
        await worker.ready()
        await _send(laser, session, "held")
        await _wait_parked(session, 1)
        await _wait_ack(laser, session, ls.TaskState.Paused, pause)
        assert not seen
        resume = _position(await control.resume())
        await _wait_ack(laser, session, ls.TaskState.Working, resume)
        await _wait_seen(seen, "held")
        await _wait_parked(session, 0)
    finally:
        await worker.shutdown()


async def test_given_retained_parked_work_when_restarted_then_should_dispatch_it_after_resume(
    laser,
):
    session = await _setup(laser)
    control = _control(laser, session).participants([WORKER])
    seen = []
    worker = _worker(laser, seen)
    try:
        await worker.ready()
        pause = _position(await control.pause())
        await _wait_ack(laser, session, ls.TaskState.Paused, pause)
        await _send(laser, session, "retained")
        held = await _wait_parked(session, 1)
        assert not seen
    finally:
        await worker.shutdown()

    assert (await session.parked()).records == held.records
    restarted = _worker(laser, seen)
    try:
        await restarted.ready()
        resume = _position(await control.resume())
        await _wait_ack(laser, session, ls.TaskState.Working, resume)
        await _wait_seen(seen, "retained")
        await _wait_parked(session, 0)
        await _send(laser, session, "after")
        await _wait_seen(seen, "after")
        assert seen == ["retained", "after"]
    finally:
        await restarted.shutdown()


async def test_given_canceled_parked_work_when_resumed_then_should_keep_it_unprocessed_and_listed(
    laser,
):
    session = await _setup(laser)
    control = _control(laser, session).participants([WORKER])
    seen = []
    worker = _worker(laser, seen)
    try:
        await worker.ready()
        pause = _position(await control.pause())
        await _wait_ack(laser, session, ls.TaskState.Paused, pause)
        source = await _send(laser, session, "held")
        await _wait_parked(session, 1)
        await control.cancel()

        async def canceled():
            envelopes = await _envelopes(laser, session)
            return (
                True
                if any(
                    envelope.get("task_state") == ls.TaskState.Canceled.code()
                    and envelope["source"] == WORKER
                    for envelope in envelopes
                )
                else None
            )

        await _eventually(canceled, "worker did not acknowledge cancellation")
        resume = _position(await control.resume())
        await _send(laser, session, "late")

        async def decided():
            held = await session.parked()
            return held if "late" in seen or len(held.records) == 2 else None

        held = await _eventually(decided, "worker did not decide on work after the resume")
        assert "held" not in seen
        assert held.complete
        assert any(
            (record["source"]["Message"]["partition"], record["source"]["Message"]["offset"])
            == source
            for record in held.records
        )
        assert not await _acknowledged(laser, session, ls.TaskState.Working, resume)
    finally:
        await worker.shutdown()
