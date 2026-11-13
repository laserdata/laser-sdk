import laser_sdk as ls
import pytest

pytestmark = pytest.mark.integration


async def handle(ctx, message):
    return None


@pytest.mark.parametrize("policy", ["dead_letters", "unexpired", "all"])
async def test_given_a_warm_dedup_policy_when_spawning_an_agent_then_should_run_and_stop(
    laser, policy
):
    await laser.bootstrap(partitions=1, retention=ls.TopicRetention.expire_after(3_600_000))
    agent = laser.spawn_agent(
        "warm-policy",
        ls.AgentTopic.Sessions,
        handle,
        warm_dedup=True,
        warm_dedup_policy=policy,
    )
    await agent.ready()
    await agent.shutdown()


async def test_given_an_unknown_warm_dedup_policy_when_spawning_an_agent_then_should_raise_invalid(
    laser,
):
    with pytest.raises(ls.InvalidError, match="warm_dedup_policy"):
        laser.spawn_agent(
            "warm-policy",
            ls.AgentTopic.Sessions,
            handle,
            warm_dedup=True,
            warm_dedup_policy="dead-letters",
        )
