import time

import laser_sdk as ls
import pytest

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_given_a_relative_deadline_when_the_query_is_taken_later_then_should_count_from_then(
    laser,
):
    request = laser.query("readings").deadline(5_000)
    time.sleep(0.002)
    started = time.time_ns() // 1_000
    assert request.into_query()["deadline_micros"] >= started + 5_000_000


@pytest.mark.asyncio
async def test_given_a_request_when_fetched_then_should_move_to_a_fresh_execution_id(open_laser):
    request = open_laser.query("readings").limit(20)
    first = request.execution_id
    with pytest.raises(ls.UnsupportedError):
        await request.fetch()
    assert request.execution_id != first
    assert request.into_query()["page"]["limit"] == 20
