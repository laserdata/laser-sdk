import asyncio

import laser_sdk as ls
import pytest
from iggy_test_server import IggyTestCluster

pytestmark = [
    pytest.mark.integration,
]


async def test_given_cluster_when_nodes_restart_then_same_handle_should_stream():
    cluster = await asyncio.to_thread(lambda: IggyTestCluster().start())
    laser = None
    worker = None
    stop = asyncio.Event()
    sent = 0
    observed = 0
    try:
        leader, follower = await asyncio.to_thread(cluster.leader_and_follower)
        cluster.route_endpoint_to(leader)
        endpoint = f"{cluster.endpoint}?reconnection_retries=unlimited&reconnection_interval=100ms"
        laser = await ls.Laser.connect(endpoint, stream="rolling_restart")
        topic = laser.topic("pulse")
        await topic.ensure(partitions=1)

        async def stream():
            nonlocal sent, observed
            while not stop.is_set():
                try:
                    await topic.publish().payload((sent + 1).to_bytes(8, "little")).send()
                    sent += 1
                    cursor = topic.replay()
                    observed += len(await asyncio.wait_for(cursor.poll(), timeout=2))
                except (ls.LaserError, TimeoutError):
                    pass
                await asyncio.sleep(0.025)

        worker = asyncio.create_task(stream())
        await _wait_for_progress(lambda: sent, 1)
        await asyncio.to_thread(cluster.restart_node, follower)
        await _wait_for_progress(lambda: sent, sent + 1)
        await asyncio.to_thread(cluster.restart_node, leader)
        await _wait_for_progress(lambda: sent, sent + 1)
        assert observed > 0
    finally:
        stop.set()
        if worker is not None:
            await worker
        await asyncio.to_thread(cluster.stop)


async def _wait_for_progress(current, expected):
    deadline = asyncio.get_running_loop().time() + 30
    while current() < expected:
        assert asyncio.get_running_loop().time() < deadline
        await asyncio.sleep(0.05)
