import asyncio
import contextlib
import json
import time
import uuid

import laser_sdk as ls
import pytest
from iggy_test_server import IggyTestCluster, StandInCatalog

pytestmark = [
    pytest.mark.integration,
]

RECONNECT = "?reconnection_retries=unlimited&reconnection_interval=100ms"
TOPIC = "fleet_changes"
GROUP = "anomaly-desk"
POLICY_NAME = "safe-mode"
PARTITIONS = 3
READ_TIMEOUT = 15
WAIT_MAX_MS = 10_000
# A held wait answers when a record lands, so a page slower than this one came
# from a reader that slept instead of waiting.
PROMPT_WAKE = 4
RECOVERY = 90
FAILOVER = 10
LONG_WAIT_MS = 20_000
# The assignment is read again every 2 s, so a gained partition is read within
# a few seconds, not when a 20 s wait ends.
PROMPT_REBALANCE = 8

SATELLITE = {
    "id": "sat-042",
    "name": "Kestrel-42",
    "mode": "safe",
    "orbit": "leo",
    "battery_pct": 61,
}
SAFE_MODE = json.dumps(
    {"op": "u", "table": "satellites", "changed": ["mode"], "after": SATELLITE},
    separators=(",", ":"),
).encode()
DECOMMISSION = json.dumps(
    {"op": "d", "table": "satellites", "before": {"id": "sat-042"}}, separators=(",", ":")
).encode()
GROUND_STATION = json.dumps(
    {
        "op": "u",
        "table": "ground_stations",
        "changed": ["status"],
        "after": {"id": "svalbard", "status": "online"},
    },
    separators=(",", ":"),
).encode()


async def test_given_a_three_node_cluster_when_an_idle_group_reader_waits_then_should_hold_one_filtered_wait_per_serving_node_and_wake_on_any_partition():  # noqa: E501
    # Each node takes one filtered wait here, so a second wait on a node is
    # refused and the reader would sleep its 30 s idle interval.
    async with catalog_cluster(max_waiting_reads=1) as cluster:
        stream = f"cluster_waits_{uuid.uuid4().hex[:12]}"
        laser = await cluster_laser(cluster, stream)
        try:
            capabilities = await laser.capabilities()
            assert capabilities.filters.group_policy_reads
            assert capabilities.filters.catalog, "the nodes reach the stand-in catalog"
            assert capabilities.filters.max_wait_ms > 0, "the cluster holds filtered waits"
            primaries = await asyncio.to_thread(
                cluster.partition_primaries, stream, TOPIC, PARTITIONS
            )
            assert len(set(primaries)) > 1, (
                f"the partitions are led by different nodes: {primaries}"
            )
            group = await bound_group(laser)
            reader = await group.reader(
                start="first", idle_interval_ms=30_000, max_wait_ms=WAIT_MAX_MS
            )
            try:
                await assigned(reader)
                for round_ in range(2):
                    for partition in range(PARTITIONS):
                        page = await wake_on(laser, reader, partition, PROMPT_WAKE)
                        assert (page.partition_id, len(page.records)) == (partition, 1), (
                            f"round {round_}: the wait of partition {partition}'s primary "
                            "ended with its one match"
                        )
                        assert bytes(page.records[0].message.payload) == SAFE_MODE
                        await reader.ack_page(page)
            finally:
                await reader.close()
        finally:
            await laser.close()


async def test_given_a_policy_aware_group_consumer_on_a_cluster_when_a_partition_primary_fails_over_then_should_keep_delivering_matches_and_store_its_offsets():  # noqa: E501
    async with catalog_cluster() as cluster:
        stream = f"cluster_failover_{uuid.uuid4().hex[:12]}"
        laser = await cluster_laser(cluster, stream)
        try:
            primaries = await asyncio.to_thread(
                cluster.partition_primaries, stream, TOPIC, PARTITIONS
            )
            group = await bound_group(laser)
            consumer = group.consumer(polling="first", auto_commit="each", max_wait_ms=WAIT_MAX_MS)
            delivered = {}
            try:
                for partition in range(PARTITIONS):
                    await deliver_one(laser, consumer, delivered, partition, PROMPT_WAKE)

                leader, _ = await asyncio.to_thread(cluster.leader_and_follower)
                stopped = primaries[0]
                await asyncio.to_thread(cluster.stop_node, stopped)
                if stopped == leader:
                    new_leader, _ = await asyncio.to_thread(
                        cluster.leader_and_follower, (stopped + 1) % 3
                    )
                    cluster.route_endpoint_to(new_leader)
                # The successor leads within milliseconds of a graceful stop, so a
                # read still held by the stopped node must move on long before the
                # client's 30 s operation deadline.
                for partition in range(PARTITIONS):
                    await deliver_one(laser, consumer, delivered, partition, FAILOVER)
                await asyncio.to_thread(cluster.start_node, stopped)
                for partition in range(PARTITIONS):
                    await deliver_one(laser, consumer, delivered, partition, RECOVERY)
                for partition in range(PARTITIONS):
                    await deliver_one(laser, consumer, delivered, partition, PROMPT_WAKE)
            finally:
                await consumer.shutdown()
            await wait_for_group_offsets(group, delivered)
        finally:
            await laser.close()


async def test_given_a_page_read_before_its_primary_crashes_on_a_cluster_when_acknowledged_then_should_store_it_through_the_new_primary():  # noqa: E501
    async with catalog_cluster() as cluster:
        stream = f"cluster_ack_{uuid.uuid4().hex[:12]}"
        laser = await cluster_laser(cluster, stream)
        try:
            primaries = await asyncio.to_thread(
                cluster.partition_primaries, stream, TOPIC, PARTITIONS
            )
            leader, _ = await asyncio.to_thread(cluster.leader_and_follower)
            partition = next(
                partition for partition in range(PARTITIONS) if primaries[partition] != leader
            )
            crashed = primaries[partition]
            await publish(laser, [SAFE_MODE, GROUND_STATION, DECOMMISSION], partition)
            group = await bound_group(laser)
            reader = await group.reader(start="first")
            try:
                page = await asyncio.wait_for(reader.next_page(), READ_TIMEOUT)
                assert (page.partition_id, offsets(page)) == (partition, [0, 2])

                await asyncio.to_thread(cluster.kill_node, crashed)
                started = time.monotonic()
                await reader.ack_page(page)
                stored = time.monotonic() - started
                assert stored < FAILOVER, (
                    f"the acknowledgment took {stored:.2f} s after the primary crashed"
                )
            finally:
                await reader.close()
            await wait_for_group_offsets(group, {partition: page.safe_ack_offset})
        finally:
            await laser.close()


async def test_given_a_group_reader_holding_its_filtered_waits_on_a_cluster_when_another_member_leaves_then_should_read_a_partition_it_gains_before_the_waits_end():  # noqa: E501
    async with catalog_cluster() as cluster:
        stream = f"cluster_rebalance_{uuid.uuid4().hex[:12]}"
        laser = await cluster_laser(cluster, stream)
        try:
            group = await bound_group(laser)
            first = await group.reader(start="first", max_wait_ms=LONG_WAIT_MS)
            second = None
            try:
                await assigned(first)
                second = await group.reader(start="first", max_wait_ms=LONG_WAIT_MS)
                deadline = time.monotonic() + READ_TIMEOUT
                while True:
                    assert await first.try_next_page() is None
                    assert await second.try_next_page() is None
                    if len(await first.partitions()) < PARTITIONS and await second.partitions():
                        break
                    assert time.monotonic() < deadline, "the two members split the partitions"
                    await asyncio.sleep(0.02)
                gained = (await second.partitions())[0]

                # The first member holds its waits on its own partitions when the
                # second one leaves, and the record lands on a partition it gains.
                waiting = asyncio.create_task(asyncio.wait_for(first.next_page(), PROMPT_REBALANCE))
                await asyncio.sleep(0.5)
                await second.close()
                second = None
                await publish(laser, [SAFE_MODE], gained)
                try:
                    page = await waiting
                except TimeoutError:
                    pytest.fail("the gained partition is read long before the 20 s waits end")
                assert (page.partition_id, offsets(page)) == (gained, [0])
            finally:
                if second is not None:
                    await second.close()
                await first.close()
        finally:
            await laser.close()


@contextlib.asynccontextmanager
async def catalog_cluster(max_waiting_reads=None):
    """Three nodes, each reaching one stand-in catalog over a socket of its own."""
    catalog = await asyncio.to_thread(lambda: StandInCatalog().start())
    cluster = IggyTestCluster()
    try:
        for node in range(3):
            cluster.set_node_environment(
                node, catalog.node_environment(node, max_waiting_reads=max_waiting_reads)
            )
        await asyncio.to_thread(cluster.start)
        leader, _ = await asyncio.to_thread(cluster.leader_and_follower)
        cluster.route_endpoint_to(leader)
        yield cluster
    finally:
        await asyncio.to_thread(cluster.stop)
        await asyncio.to_thread(catalog.stop)


async def cluster_laser(cluster, stream):
    """A `Laser` on the catalog cluster with `TOPIC` over every partition."""
    laser = await ls.Laser.connect(
        f"{cluster.endpoint}{RECONNECT}", stream=stream, publish_timeout_ms=5_000
    )
    await laser.stream(stream).ensure()
    await laser.topic(TOPIC).ensure(PARTITIONS)
    return laser


async def bound_group(laser):
    group = laser.topic(TOPIC).consumer_group(GROUP)
    created = await group.create(filter=bound_filter(), filter_name=POLICY_NAME)
    assert created.filter["revision"] == 1
    return group


def bound_filter():
    return ls.ConsumerFilter.json(
        ls.FilterExpr.any(
            [
                ls.FilterExpr.pred("op", "eq", "d"),
                ls.FilterExpr.pred("changed", "contains", "mode"),
            ]
        )
    )


async def publish(laser, payloads, partition):
    producer = laser.topic(TOPIC).producer(partition=partition, partitions=partition + 1)
    try:
        await producer.send_batch(payloads)
    finally:
        await producer.shutdown()


async def assigned(reader):
    """Read rounds without the idle wait until the reader holds every partition."""
    deadline = time.monotonic() + READ_TIMEOUT
    while len(await reader.partitions()) < PARTITIONS:
        assert await reader.try_next_page() is None, "nothing is published yet"
        assert time.monotonic() < deadline, "the reader is assigned every partition"
        await asyncio.sleep(0.02)


async def wake_on(laser, reader, partition, bound):
    """Publish a miss and a match on `partition` during the held waits, take the page."""

    async def late():
        await asyncio.sleep(0.3)
        await publish(laser, [GROUND_STATION, SAFE_MODE], partition)

    publisher = asyncio.create_task(late())
    try:
        page = await asyncio.wait_for(reader.next_page(), bound)
    except TimeoutError:
        pytest.fail(
            f"the held wait answers when a record lands on partition {partition}, "
            "long before the 30 s idle interval"
        )
    await publisher
    return page


async def deliver_one(laser, consumer, delivered, partition, bound):
    """Publish a miss and a match on `partition` until it takes them, take the match."""
    started = time.monotonic()
    while True:
        try:
            await publish(laser, [GROUND_STATION, SAFE_MODE], partition)
            break
        except ls.LaserError as error:
            assert time.monotonic() - started < bound, (
                f"partition {partition} took no record within {bound} s: {error}"
            )
            await asyncio.sleep(0.2)
    while True:
        remaining_ms = max(1, int((bound - (time.monotonic() - started)) * 1000))
        try:
            message = await consumer.next_within(remaining_ms)
        except ls.LaserError as error:
            pytest.fail(f"the match on partition {partition} arrives within {bound} s: {error!r}")
        # A record delivered again after the failover is read past.
        last = delivered.get(message.partition_id)
        if last is not None and last >= message.position.offset:
            continue
        assert (message.partition_id, bytes(message.payload)) == (partition, SAFE_MODE), (
            "only the match of the published partition arrives"
        )
        delivered[partition] = message.position.offset
        return


async def wait_for_group_offsets(group, expected):
    """Read the group's stored offsets until they are `expected`, across a failover."""
    probe = group.consumer(auto_commit="disabled")
    try:
        for partition, offset in expected.items():
            deadline = time.monotonic() + READ_TIMEOUT
            while True:
                try:
                    stored = await probe.stored_offset(partition)
                except ls.LaserError:
                    stored = None
                stored_offset = None if stored is None else stored.stored_offset
                if stored_offset == offset:
                    break
                assert time.monotonic() < deadline, (
                    f"partition {partition} stored {stored_offset}, expected {offset}"
                )
                await asyncio.sleep(0.1)
    finally:
        await probe.shutdown()


def offsets(page):
    return [record.offset for record in page.records]
