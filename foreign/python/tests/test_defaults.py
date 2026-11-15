import laser_sdk as ls
import pytest

pytestmark = pytest.mark.integration


async def test_given_no_arguments_when_laser_bootstraps_then_should_keep_one_partition_a_day(
    laser,
):
    await laser.bootstrap()
    topic = await laser.stream(laser.default_stream).topic("agent.sessions").get()
    assert (topic.partitions_count, topic.message_expiry_ms) == (1, 86_400_000)


async def test_given_no_arguments_when_sessions_bootstrap_then_should_keep_one_partition_a_day(
    laser,
):
    await laser.sessions().bootstrap()
    topic = await laser.stream(laser.default_stream).topic("agent.sessions").get()
    assert (topic.partitions_count, topic.message_expiry_ms) == (1, 86_400_000)


async def test_given_no_default_stream_when_publishing_then_should_name_the_calls_that_set_one(
    iggy_endpoint,
):
    async with await ls.Laser.connect(iggy_endpoint) as laser:
        with pytest.raises(ls.NoStreamError) as raised:
            await laser.topic("orders").send(b"hello")
    assert str(raised.value) == (
        "no default stream set: pass a stream, or set a default stream when connecting "
        "(stream option) or with with_default_stream"
    )


async def test_given_an_agent_without_an_inbox_when_a_step_routes_to_it_then_should_name_both_fixes(
    laser,
):
    workflow = laser.workflow("lost").step("ping", to="ghost", build=lambda outputs: "ping")
    with pytest.raises(ls.NoInboxError) as raised:
        await workflow.run()
    assert str(raised.value) == (
        "agent has no advertised inbox: ghost: start the agent with capabilities so it "
        "advertises its inbox, or give the workflow a fixed inbox topic (fixed_inbox)"
    )
