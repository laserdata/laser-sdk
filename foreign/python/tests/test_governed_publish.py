import asyncio

import laser_sdk as ls
import pytest

pytestmark = pytest.mark.integration

REASON = "the governor blocked this action: wire transfers need review"


class BlockWires:
    async def decide(self, action):
        if bytes(action.payload).startswith(b"wire"):
            return ls.ActionDecision.block("wire transfers need review")
        return ls.ActionDecision.allow()


class Hung:
    async def decide(self, action):
        await asyncio.Event().wait()


class AllowAll:
    async def decide(self, action):
        return ls.ActionDecision.allow()


@pytest.mark.asyncio
async def test_given_a_governor_when_publishing_on_every_raw_path_then_should_govern_each_record(
    laser,
):
    topic = laser.with_governor(BlockWires(), mode="enforce").topic("governed")
    await laser.topic("governed").ensure(1)
    with pytest.raises(ls.PolicyBlockedError, match=REASON):
        await topic.publish_batch().add_payload(b"fine").add_payload(b"wire 1").send()
    with pytest.raises(ls.PolicyBlockedError, match=REASON):
        await topic.send(b"wire 2")
    with pytest.raises(ls.PolicyBlockedError, match=REASON):
        await topic.batch([b"wire 3"])
    producer = topic.producer(create_topic=False)
    with pytest.raises(ls.PolicyBlockedError, match=REASON):
        await producer.send(b"wire 4")
    batching = topic.batching()
    with pytest.raises(ls.PolicyBlockedError, match=REASON):
        await batching.send(b"wire 5")
    await batching.close()
    await topic.send(b"allowed")
    await producer.send(b"allowed")


@pytest.mark.asyncio
async def test_given_a_hung_quorum_voter_when_a_governed_publish_runs_then_should_time_it_out(
    laser,
):
    quorum = (
        ls.QuorumGovernor(ls.QuorumPolicy.any())
        .voter("hung", Hung(), False)
        .voter("ok", AllowAll(), False)
        .voter_timeout(50)
    )
    topic = laser.with_governor(quorum, mode="enforce").topic("quorum")
    await laser.topic("quorum").ensure(1)
    await asyncio.wait_for(topic.send(b"proceeds"), timeout=10)
    with pytest.raises(ls.InvalidError):
        ls.QuorumGovernor(ls.QuorumPolicy.any()).voter_timeout(-1)
