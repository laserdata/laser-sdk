import asyncio
import contextlib
import time
import uuid

import laser_sdk as ls
import pytest
from iggy_test_server import IggyTestServer, StandInCatalog

# A read that every node refuses waits up to the 30 s managed deadline.
pytestmark = [
    pytest.mark.integration,
    pytest.mark.timeout(120),
]

TOPIC = "fleet_changes"
GROUP = "anomaly-desk"
MANAGED_DEADLINE = 30
QUERY_REPLY_GRACE = 2
NAMESPACE = "profiles"


async def test_given_one_node_that_refuses_then_serves_when_reading_then_should_return_the_served_reply():  # noqa: E501
    async with single_node(unanswered_binding_reads=3) as server:
        laser = await connected(server)
        try:
            group = laser.topic(TOPIC).consumer_group(GROUP)
            await group.create()
            assert await group.filter().get() is None
        finally:
            await laser.close()


async def test_given_every_node_refusing_past_the_deadline_when_reading_then_should_return_the_last_refusal():  # noqa: E501
    async with single_node() as server:
        laser = await connected(server)
        try:
            started = time.monotonic()
            with pytest.raises(ls.LaserError) as refused:
                await laser.kv(NAMESPACE).get("user-1")
            waited = time.monotonic() - started
            assert refused.value.code == "BackendUnavailable"
            assert MANAGED_DEADLINE - 5 <= waited <= MANAGED_DEADLINE + 15, waited
        finally:
            await laser.close()


async def test_given_a_query_every_node_refuses_when_fetched_then_should_walk_again_until_the_query_deadline():  # noqa: E501
    async with single_node() as server:
        laser = (await connected(server)).with_capabilities(managed=True, query=True)
        try:
            deadline = 1.0
            started = time.monotonic()
            with pytest.raises(ls.LaserError) as refused:
                await laser.query("readings").deadline(deadline * 1000).fetch()
            waited = time.monotonic() - started
            assert refused.value.code == "BackendUnavailable"
            assert 0.3 <= waited < deadline + QUERY_REPLY_GRACE, waited
        finally:
            await laser.close()


async def test_given_a_read_waiting_out_refusals_when_its_task_is_cancelled_then_should_stop_at_once():  # noqa: E501
    async with single_node() as server:
        laser = await connected(server)
        try:
            reading = asyncio.ensure_future(laser.kv(NAMESPACE).get("user-1"))
            await asyncio.sleep(1)
            assert not reading.done(), "the read waits out the refusals"
            cancelled = time.monotonic()
            reading.cancel()
            with pytest.raises(asyncio.CancelledError):
                await reading
            assert time.monotonic() - cancelled < 1
            # The connection serves the next call, so the cancelled wait held nothing.
            await laser.stream(laser.default_stream).ensure()
        finally:
            await laser.close()


@contextlib.asynccontextmanager
async def single_node(unanswered_binding_reads=0):
    """One node whose managed backend is the stand-in catalog, which answers no key-value read
    and leaves the first `unanswered_binding_reads` binding reads unanswered."""
    catalog = await asyncio.to_thread(
        lambda: StandInCatalog(nodes=1, unanswered_binding_reads=unanswered_binding_reads).start()
    )
    server = IggyTestServer(environment=catalog.node_environment(0))
    try:
        await asyncio.to_thread(server.start)
        yield server
    finally:
        await asyncio.to_thread(server.stop)
        await asyncio.to_thread(catalog.stop)


async def connected(server):
    stream = f"read-wait-{uuid.uuid4().hex[:8]}"
    laser = await ls.Laser.connect(server.endpoint, stream=stream)
    await laser.stream(stream).ensure()
    await laser.topic(TOPIC).ensure(1)
    return laser
