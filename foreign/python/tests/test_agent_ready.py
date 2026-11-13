import laser_sdk as ls
import pytest

pytestmark = pytest.mark.integration


async def handle(ctx, message):
    return None


def advertising(laser, name):
    return laser.spawn_agent(name, ls.AgentTopic.Sessions, handle, capabilities=[name])


async def test_given_a_presence_conflict_when_waiting_for_ready_then_should_raise_it(
    laser,
):
    await laser.bootstrap(partitions=1, retention=ls.TopicRetention.expire_after(3_600_000))
    first = advertising(laser, "first-worker")
    await first.ready()
    try:
        second = advertising(laser, "second-worker")
        with pytest.raises(ls.PresenceConflictError) as raised:
            await second.ready()
        assert raised.value.advertised == "first-worker"
        assert raised.value.requested == "second-worker"
        with pytest.raises(ls.PresenceConflictError):
            await second.ready()
        with pytest.raises(ls.PresenceConflictError):
            await second.shutdown()
    finally:
        await first.shutdown()


async def test_given_a_presence_conflict_when_entering_async_with_then_should_raise_it(
    laser,
):
    await laser.bootstrap(partitions=1, retention=ls.TopicRetention.expire_after(3_600_000))
    async with advertising(laser, "first-worker"):
        with pytest.raises(ls.PresenceConflictError):
            async with advertising(laser, "second-worker"):
                pytest.fail("the second agent never becomes ready")
