import laser_sdk as ls
import pytest

pytestmark = pytest.mark.integration

REFUSED_DURATIONS = [-1.0, float("nan"), float("inf")]


async def test_given_fractional_milliseconds_when_building_a_group_consumer_then_should_accept_them(
    laser,
):
    consumer = (
        laser.topic("durations")
        .consumer_group("fractional")
        .consumer(
            max_wait_ms=1.5,
            commit_interval_ms=0.5,
            polling_retry_interval_ms=1.5,
            init_retries=1,
            init_retry_interval_ms=2.5,
        )
    )
    assert isinstance(consumer, ls.Consumer)


@pytest.mark.parametrize("value", REFUSED_DURATIONS)
async def test_given_a_refused_max_wait_when_building_a_consumer_then_should_raise_invalid(
    laser, value
):
    topic = laser.topic("durations")
    with pytest.raises(ls.InvalidError):
        topic.consumer_group("refused").consumer(max_wait_ms=value)
    with pytest.raises(ls.InvalidError):
        topic.consumer("refused", partition=0, max_wait_ms=value)


async def test_given_fractional_milliseconds_when_spawning_an_agent_then_should_run_and_stop(laser):
    await laser.bootstrap(partitions=1, retention=ls.TopicRetention.expire_after(3_600_000))

    async def handle(ctx, message):
        return None

    agent = laser.spawn_agent(
        "fractional",
        ls.AgentTopic.Sessions,
        handle,
        max_wait_ms=1.5,
        shutdown_grace_ms=0.5,
        retry_base_delay_ms=0.5,
    )
    await agent.ready()
    await agent.shutdown()
    with pytest.raises(ls.InvalidError):
        laser.spawn_agent("refused", ls.AgentTopic.Sessions, handle, max_wait_ms=-1.0)


async def test_given_a_fractional_contract_deadline_when_nobody_answers_then_should_time_out(
    laser,
):
    await laser.bootstrap(partitions=1, retention=ls.TopicRetention.expire_after(3_600_000))
    outcome = await laser.contract(
        None,
        b"ping",
        source="planner",
        agent="nobody",
        fixed_inbox=ls.AgentTopic.Sessions,
        deadline_ms=250.5,
        expire_if_not_consumed_ms=100.5,
    )
    assert isinstance(outcome, ls.Contract.TimedOut)


@pytest.mark.parametrize("value", REFUSED_DURATIONS)
async def test_given_a_refused_contract_deadline_when_contracting_then_should_raise_invalid(
    laser, value
):
    with pytest.raises(ls.InvalidError):
        await laser.contract(None, b"ping", source="planner", agent="nobody", deadline_ms=value)
    with pytest.raises(ls.InvalidError):
        await laser.agent("planner").contract(None, b"ping", agent="nobody", deadline_ms=value)


async def test_given_a_fractional_window_when_bucketing_a_query_then_should_send_its_micros(laser):
    query = laser.query("readings").window("ts", 1.5).into_query()
    assert query["aggregate"]["window"]["every_micros"] == 1_500
    with pytest.raises(ls.InvalidError):
        laser.query("readings").window("ts", -1.0)
