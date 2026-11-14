import asyncio

import pytest

pytestmark = pytest.mark.integration


async def test_given_a_watched_connection_when_closed_then_should_yield_shutdown_and_stop(laser):
    events = laser.connection_events()
    await asyncio.sleep(0.1)
    await laser.close()

    seen = await asyncio.wait_for(_collect(events), timeout=10)

    assert seen[-1] == "shutdown"


async def _collect(events):
    return [event async for event in events]
