import asyncio

import laser_sdk as ls
import pytest
from iggy_test_server import IggyTestCluster

pytestmark = pytest.mark.integration

PRESENCE = {"v": 1, "agent": "risk", "inbox": "risk.inbox"}


async def _advertised(laser):
    page = await laser.client_metadata_all()
    return [client for client in page if client.get("metadata")]


async def test_given_a_presence_when_advertised_and_cleared_then_should_withdraw_it_from_the_node(  # noqa: E501
    laser,
):
    await laser.advertise_presence(PRESENCE)
    assert any(client["metadata"] for client in await _advertised(laser))
    await laser.clear_presence()
    await laser.advertise_presence(PRESENCE)
    await laser.clear_presence()


async def test_given_a_presence_pinned_to_a_node_when_that_node_is_down_then_should_report_unavailable_keep_the_pin_and_clear_once_it_returns():  # noqa: E501
    cluster = await asyncio.to_thread(lambda: IggyTestCluster().start())
    laser = None
    try:
        laser = await ls.Laser.connect(cluster.endpoint)
        await laser.advertise_presence(PRESENCE)
        pinned = None
        for node in range(3):
            await asyncio.to_thread(cluster.kill_node, node)
            try:
                await laser.clear_presence()
            except ls.LaserError as error:
                assert error.unavailable, str(error)
                pinned = node
                break
            await asyncio.to_thread(cluster.start_node, node)
            await laser.advertise_presence(PRESENCE)
        assert pinned is not None, "one of the nodes holds the presence"

        with pytest.raises(ls.LaserError) as again:
            await laser.advertise_presence(PRESENCE)
        assert again.value.unavailable, "the presence is never advertised elsewhere"

        await asyncio.to_thread(cluster.start_node, pinned)
        deadline = asyncio.get_running_loop().time() + 30
        while True:
            try:
                await laser.clear_presence()
                break
            except ls.LaserError:
                assert asyncio.get_running_loop().time() < deadline, "the returned node clears"
                await asyncio.sleep(0.5)
        await laser.advertise_presence(PRESENCE)
        await laser.clear_presence()
    finally:
        if laser is not None:
            await laser.close()
        await asyncio.to_thread(cluster.stop)
