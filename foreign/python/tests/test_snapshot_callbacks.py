import asyncio

import laser_sdk as ls
import pytest


@pytest.mark.parametrize("awaitable", [False, True])
async def test_given_custom_snapshots_when_saved_and_read_then_should_keep_all_fields(awaitable):
    stored = {}
    calls = []

    def result(value):
        return asyncio.sleep(0, result=value) if awaitable else value

    class Backend:
        def latest(self, conversation):
            calls.append(("latest", conversation))
            return result(stored.get(conversation))

        def save(self, snapshot):
            calls.append(("save", snapshot))
            stored[snapshot["conversation"]] = snapshot
            return result(None)

    store = ls.SnapshotStore(Backend())
    conversation = ls.new_conversation_id()
    assert await store.latest(conversation) is None
    await store.save(conversation, {0: 7, 4: 0}, b"opaque state")
    expected = {"conversation": conversation, "as_of": {0: 7, 4: 0}, "state": b"opaque state"}
    assert await store.latest(conversation) == expected
    assert calls[1] == ("save", expected)


async def test_given_sdk_snapshot_callback_errors_when_called_then_should_keep_their_class():
    class Backend:
        async def latest(self, conversation):
            await asyncio.sleep(0)
            raise ls.InvalidError("snapshot read refused")

        async def save(self, snapshot):
            await asyncio.sleep(0)
            raise ls.UnsupportedError("snapshot writes unavailable")

    store = ls.SnapshotStore(Backend())
    conversation = ls.new_conversation_id()
    with pytest.raises(ls.InvalidError, match="snapshot read refused"):
        await store.latest(conversation)
    with pytest.raises(ls.UnsupportedError, match="snapshot writes unavailable"):
        await store.save(conversation, {}, b"state")


async def test_given_a_pending_snapshot_callback_when_cancelled_then_should_retire_its_task():
    started = asyncio.Event()
    stopped = asyncio.Event()

    class Backend:
        async def latest(self, conversation):
            started.set()
            try:
                await asyncio.Future()
            finally:
                stopped.set()

        def save(self, snapshot):
            pass

    store = ls.SnapshotStore(Backend())
    pending = asyncio.ensure_future(store.latest(ls.new_conversation_id()))
    await asyncio.wait_for(started.wait(), 2)
    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pending
    await asyncio.wait_for(stopped.wait(), 2)


@pytest.mark.integration
async def test_given_a_custom_snapshot_when_state_resumes_then_should_skip_inclusive_offsets(laser):
    await laser.bootstrap(1)
    conversation = ls.new_conversation_id()
    context = laser.context(conversation)
    await context.append(ls.AgentTopic.Commands, b"1")
    checkpoint = await context.checkpoint([ls.AgentTopic.Commands])
    as_of = {
        partition: offset - 1
        for partition, offset in checkpoint.topic_offsets(ls.AgentTopic.Commands).items()
        if offset > 0
    }
    assert as_of
    await context.append(ls.AgentTopic.Commands, b"2")
    seen = []

    class Backend:
        async def latest(self, requested):
            seen.append(requested)
            await asyncio.sleep(0)
            return {"conversation": requested, "as_of": as_of, "state": b"10"}

        async def save(self, snapshot):
            pass

    total = await context.state_with(
        Backend(), [ls.AgentTopic.Commands], 0, lambda state, message: state + int(message.payload)
    )
    assert total == 12
    assert seen == [conversation]
