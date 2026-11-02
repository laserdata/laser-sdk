import asyncio
import time
import uuid

import laser_sdk as ls
import pytest
from iggy_test_server import IggyTestCluster

# A test settles the primaries, restarts nodes and fails over, so the 60 s
# default is too short.
pytestmark = [
    pytest.mark.integration,
    pytest.mark.timeout(300),
]

RECONNECT = "?reconnection_retries=unlimited&reconnection_interval=100ms"
MAX_WAIT_MS = 5_000
# The reply hub holds its reads as long, so a reply that lands well before
# this was delivered by a held read, not by the next read after the wait.
REPLY_WAIT_MS = 5_000
# The server's refusals of a replication factor it cannot place.
INVALID_REPLICATION_FACTOR = 2018
INVALID_OPTION_VALUE = 4042


async def test_given_node_restarts_when_held_reads_consume_then_should_deliver_and_commit_all():
    cluster = await asyncio.to_thread(lambda: IggyTestCluster().start())
    laser = None
    consumers = []
    loops = []
    running = asyncio.Event()
    running.set()
    try:
        leader, follower = await asyncio.to_thread(cluster.leader_and_follower)
        cluster.route_endpoint_to(leader)
        laser = await ls.Laser.connect(f"{cluster.endpoint}{RECONNECT}", stream="held_reads")
        topic = laser.topic("tape")
        await topic.ensure(partitions=3)
        native = topic.consumer(
            "tape-reader", partition=0, polling="first", batch_length=100, max_wait_ms=MAX_WAIT_MS
        )
        group = topic.consumer_group("tape-desk").consumer(
            polling="first", auto_commit="disabled", batch_length=100, max_wait_ms=MAX_WAIT_MS
        )
        consumers.extend([native, group])
        native_seen = {}
        group_seen = {}
        group_highest = {}
        commit_failures = []

        async def handle_native(message):
            native_seen[_sequence(message)] = time.monotonic()

        async def handle_group(message):
            group_seen[_sequence(message)] = time.monotonic()
            # The reader follows a moved primary and stores again while a
            # partition has none, so a commit never needs the caller's retry.
            try:
                await group.commit(message)
            except ls.LaserError as error:
                commit_failures.append(error)
                return
            highest = group_highest.get(message.partition_id)
            if highest is None or message.position.offset > highest:
                group_highest[message.partition_id] = message.position.offset

        loops.append(asyncio.create_task(_drive(native, running, handle_native)))
        loops.append(asyncio.create_task(_drive(group, running, handle_group)))

        producer = topic.producer(partitions=3)
        await producer.init()
        sent = {}
        sequence = 1
        publishing = asyncio.Event()
        publishing.set()

        async def publish():
            nonlocal sequence
            while publishing.is_set():
                value = sequence
                partition = value % 3
                sequence += 1
                try:
                    await asyncio.wait_for(
                        producer.send(value.to_bytes(8, "little"), partition=partition),
                        timeout=10,
                    )
                    sent[value] = partition
                except (ls.LaserError, TimeoutError):
                    # An unconfirmed record may or may not be stored, so only
                    # confirmed ones are required.
                    pass
                await asyncio.sleep(0.025)

        publisher = asyncio.create_task(publish())
        await _wait_until(lambda: len(sent) >= 20, "publishing did not start")
        await asyncio.to_thread(cluster.restart_node, follower)
        after_follower = len(sent)
        await _wait_until(
            lambda: len(sent) > after_follower + 20, "publishing stalled after the restart"
        )
        await asyncio.to_thread(cluster.stop_node, leader)
        new_leader, _ = await asyncio.to_thread(cluster.leader_and_follower, follower)
        cluster.route_endpoint_to(new_leader)
        await asyncio.to_thread(cluster.start_node, leader)
        after_failover = len(sent)
        await _wait_until(
            lambda: len(sent) > after_failover + 20, "publishing stalled after the failover"
        )
        publishing.clear()
        await publisher

        native_expected = [value for value, partition in sent.items() if partition == 0]
        await _wait_until(
            lambda: (
                all(value in native_seen for value in native_expected)
                and all(value in group_seen for value in sent)
            ),
            "a confirmed record was never delivered",
            timeout=60,
        )

        # Both readers now sit in held reads. One record wakes them before the
        # wait elapses: the native read at once, the group read within its pass.
        await asyncio.sleep(MAX_WAIT_MS / 1000 + 1)
        probe = sequence
        probe_sent = time.monotonic()
        await asyncio.wait_for(producer.send(probe.to_bytes(8, "little"), partition=0), timeout=10)
        await _wait_until(
            lambda: probe in native_seen and probe in group_seen,
            "the probe record was never delivered",
            timeout=30,
        )
        native_latency = (native_seen[probe] - probe_sent) * 1000
        group_latency = (group_seen[probe] - probe_sent) * 1000
        assert native_latency < MAX_WAIT_MS / 2, f"native latency {native_latency:.0f} ms"
        assert group_latency < MAX_WAIT_MS, f"group latency {group_latency:.0f} ms"

        running.clear()
        await asyncio.gather(*loops)
        loops.clear()
        assert commit_failures == []
        # The polling policy stores each read on the server, the explicit
        # policy stores the highest handled offset of each partition.
        native_stored = await native.stored_offset(0)
        assert native_stored.stored_offset == native_stored.current_offset
        for partition, highest in group_highest.items():
            stored = await group.stored_offset(partition)
            assert stored.stored_offset == highest, f"group offset of partition {partition}"
        await producer.shutdown()
    finally:
        running.clear()
        await asyncio.gather(*loops, return_exceptions=True)
        for consumer in consumers:
            try:
                await consumer.shutdown()
            except ls.LaserError:
                pass
        if laser is not None:
            await laser.close()
        await asyncio.to_thread(cluster.stop)


async def test_given_replies_across_nodes_and_a_restart_when_asking_then_should_beat_the_wait():
    cluster = await asyncio.to_thread(lambda: IggyTestCluster().start())
    requester = None
    worker = None
    agent = None
    try:
        leader, follower = await asyncio.to_thread(cluster.leader_and_follower)
        cluster.route_endpoint_to(leader)
        stream = f"cluster_replies_{uuid.uuid4().hex[:12]}"
        requester = await ls.Laser.connect(f"{cluster.endpoint}{RECONNECT}", stream=stream)
        worker = await ls.Laser.connect(f"{cluster.endpoint}{RECONNECT}", stream=stream)
        await requester.bootstrap(6, ls.TopicRetention.expire_after(86_400_000))
        # When the worker's reply was published, by question.
        replied = {}

        async def handle(ctx, message):
            await ctx.respond(bytes(message.payload))
            replied[bytes(message.payload)] = time.monotonic()

        agent = worker.spawn_agent(
            "echo",
            ls.AgentTopic.Sessions,
            handle,
            respond_on=ls.AgentTopic.Sessions,
            max_wait_ms=200,
        )
        await agent.ready()
        asker = requester.agent("asker")

        # Each round trip and the part of it the reply hub took: from the
        # worker's published reply to the requester holding it.
        async def round_trips(count, timeout_ms, *, until_partitions=0):
            trips = []
            while len(trips) < count or len({trip[0] for trip in trips}) < until_partitions:
                assert len(trips) < 120, "the replies never covered every partition"
                question = f"question-{uuid.uuid4().hex}".encode()
                reply = await asker.ask(
                    ls.AgentTopic.Sessions,
                    ls.AgentTopic.Sessions,
                    question,
                    ls.Provenance(conversation_id=ls.new_conversation_id()),
                    timeout_ms=timeout_ms,
                )
                received = time.monotonic()
                assert bytes(reply.payload) == question
                # A held read can hand the reply over before the worker's
                # respond call returns.
                deadline = time.monotonic() + 10
                while question not in replied:
                    assert time.monotonic() < deadline, "the worker never recorded its reply"
                    await asyncio.sleep(0.05)
                published = replied[question]
                trips.append((reply.id.partition_id, max(0.0, (received - published) * 1000)))
            return trips

        # The cluster spreads partition primaries over its nodes. Python reads
        # no route, so a reply from every partition stands for every node.
        # The agent is ready before its group reader owns every partition of a
        # cluster group, so the first command may wait for the next assignment.
        await round_trips(1, 60_000)
        first = await round_trips(12, 15_000, until_partitions=6)
        assert {partition for partition, _ in first} == set(range(6))
        for partition, ms in first:
            assert ms < REPLY_WAIT_MS / 2, (
                f"a reply on partition {partition} reached the requester after {ms:.0f} ms"
            )

        await asyncio.to_thread(cluster.restart_node, follower)
        # The first replies after the restart may wait for the node's links.
        await round_trips(4, 30_000)
        second = await round_trips(12, 15_000)
        for partition, ms in second:
            assert ms < REPLY_WAIT_MS / 2, (
                f"after the restart a reply on partition {partition} reached the requester "
                f"after {ms:.0f} ms"
            )
    finally:
        if agent is not None:
            await agent.shutdown()
        if worker is not None:
            await worker.close()
        if requester is not None:
            await requester.close()
        await asyncio.to_thread(cluster.stop)


async def test_given_an_idle_group_reader_when_any_node_gets_a_record_then_should_wake_at_once():
    cluster = await asyncio.to_thread(lambda: IggyTestCluster().start())
    laser = None
    try:
        leader, _ = await asyncio.to_thread(cluster.leader_and_follower)
        cluster.route_endpoint_to(leader)
        stream = f"cluster_waits_{uuid.uuid4().hex[:12]}"
        laser = await ls.Laser.connect(f"{cluster.endpoint}{RECONNECT}", stream=stream)
        capabilities = await laser.capabilities()
        assert capabilities.filters.group_policy_reads, "the cluster serves group reads"
        assert capabilities.filters.max_wait_ms > 0, "the cluster holds filtered waits"
        await laser.stream(stream).ensure()
        topic = laser.topic("fleet")
        await topic.ensure(partitions=6)
        # The cluster moves new primaries one at a time, and a wait refused
        # twice in a row by moving primaries is paced by the idle interval.
        await asyncio.to_thread(cluster.settled_primaries, stream, "fleet", 6)
        producer = topic.producer(partitions=6)
        await producer.init()
        group = topic.consumer_group("fleet-desk")
        await group.create()
        # An idle round that slept instead of holding a wait would sleep 30 s.
        reader = await group.reader(max_wait_ms=MAX_WAIT_MS, idle_interval_ms=30_000)
        try:
            # A member that just joined a cluster group can own some partitions
            # before the others, so the wait is measured once it owns all of them.
            deadline = time.monotonic() + 90
            while len(await reader.partitions()) < 6:
                assert time.monotonic() < deadline, "the reader never owned every partition"
                await reader.try_next_page()
                await asyncio.sleep(0.05)
            # A primary that has not applied the new member's ownership yet
            # refuses its wait, and a second refusal in a row is paced by the
            # idle interval, so one pass over the partitions goes untimed.
            for partition in range(6):
                page, _ = await _wake_on(reader, producer, partition, timeout=60)
                await reader.ack_page(page)
            for partition in [*range(6), *range(6)]:
                page, latency = await _wake_on(reader, producer, partition, timeout=30)
                assert latency < MAX_WAIT_MS / 2, (
                    f"a record on partition {partition} woke the reader after {latency:.0f} ms"
                )
                await reader.ack_page(page)
        finally:
            await reader.close()
            await producer.shutdown()
    finally:
        if laser is not None:
            await laser.close()
        await asyncio.to_thread(cluster.stop)


async def test_given_a_replication_factor_of_two_when_a_topic_is_ensured_on_a_three_node_cluster_then_should_store_each_partition_on_two_nodes():  # noqa: E501
    cluster = await asyncio.to_thread(lambda: IggyTestCluster().start())
    laser = None
    try:
        leader, _ = await asyncio.to_thread(cluster.leader_and_follower)
        cluster.route_endpoint_to(leader)
        stream = f"cluster_factor_{uuid.uuid4().hex[:12]}"
        laser = await ls.Laser.connect(f"{cluster.endpoint}{RECONNECT}", stream=stream)
        await laser.stream(stream).ensure()
        await laser.topic("fleet").ensure(3, replication_factor=2)
        members = await _partition_members(cluster, stream, "fleet", 3)
        assert all(len(nodes) == 2 for nodes in members), f"members by partition: {members}"
        # Placement skips a node the cluster does not count as ready yet, so
        # the members need not cover every node right after the start.
        primaries = await asyncio.to_thread(cluster.partition_primaries, stream, "fleet", 3)
        assert all(primary in nodes for primary, nodes in zip(primaries, members, strict=True)), (
            f"each partition is led by one of its members: {primaries} over {members}"
        )
    finally:
        if laser is not None:
            await laser.close()
        await asyncio.to_thread(cluster.stop)


async def test_given_a_producer_with_a_replication_factor_of_two_on_a_three_node_cluster_when_records_round_trip_then_should_create_the_topic_on_two_nodes_and_deliver_every_record():  # noqa: E501
    cluster = await asyncio.to_thread(lambda: IggyTestCluster().start())
    laser = None
    consumer = None
    producer = None
    try:
        leader, _ = await asyncio.to_thread(cluster.leader_and_follower)
        cluster.route_endpoint_to(leader)
        stream = f"cluster_factor_{uuid.uuid4().hex[:12]}"
        laser = await ls.Laser.connect(f"{cluster.endpoint}{RECONNECT}", stream=stream)
        await laser.stream(stream).ensure()
        topic = laser.topic("tape")
        producer = topic.producer(partitions=3, replication_factor=2)
        await producer.init()
        members = await _partition_members(cluster, stream, "tape", 3)
        assert all(len(nodes) == 2 for nodes in members), f"members by partition: {members}"

        consumer = topic.consumer_group("tape-desk").consumer(
            polling="first", auto_commit="disabled", max_wait_ms=MAX_WAIT_MS
        )
        sent = {}
        for value in range(30):
            partition = value % 3
            await producer.send(value.to_bytes(8, "little"), partition=partition)
            sent[value] = partition
        received = {}
        deadline = time.monotonic() + 60
        while len(received) < len(sent):
            assert time.monotonic() < deadline, f"received {sorted(received)} of {len(sent)}"
            try:
                message = await consumer.next_within(1_000)
            except ls.TimeoutError:
                continue
            received[_sequence(message)] = message.partition_id
            await consumer.commit(message)
        assert received == sent
    finally:
        if consumer is not None:
            await consumer.shutdown()
        if producer is not None:
            await producer.shutdown()
        if laser is not None:
            await laser.close()
        await asyncio.to_thread(cluster.stop)


async def test_given_a_replication_factor_above_the_node_count_when_creating_a_topic_on_a_three_node_cluster_then_should_raise_the_server_refusal():  # noqa: E501
    cluster = await asyncio.to_thread(lambda: IggyTestCluster().start())
    laser = None
    try:
        leader, _ = await asyncio.to_thread(cluster.leader_and_follower)
        cluster.route_endpoint_to(leader)
        stream = f"cluster_factor_{uuid.uuid4().hex[:12]}"
        laser = await ls.Laser.connect(f"{cluster.endpoint}{RECONNECT}", stream=stream)
        await laser.stream(stream).ensure()
        with pytest.raises(ls.TransportError) as ensured:
            await laser.topic("fleet").ensure(3, replication_factor=4)
        producer = laser.topic("tape").producer(partitions=3, replication_factor=4)
        with pytest.raises(ls.TransportError) as produced:
            await producer.init()
        for refusal in (ensured.value, produced.value):
            assert refusal.iggy_error_code in (INVALID_REPLICATION_FACTOR, INVALID_OPTION_VALUE), (
                f"the server refusal: {refusal!r}"
            )
        for name in ("fleet", "tape"):
            details = await asyncio.to_thread(cluster.topic_details, stream, name)
            assert details is None, f"topic {name} was not created"
    finally:
        if laser is not None:
            await laser.close()
        await asyncio.to_thread(cluster.stop)


async def _drive(consumer, running, handle):
    while running.is_set():
        try:
            message = await consumer.next_within(1_000)
        except ls.TimeoutError:
            continue
        except ls.LaserError:
            await asyncio.sleep(0.05)
            continue
        await handle(message)


def _sequence(message):
    return int.from_bytes(message.payload[:8], "little")


async def _wake_on(reader, producer, partition, timeout):
    """The page a record on `partition` ends while the reader waits, and its latency in ms."""
    pending = asyncio.create_task(reader.next_page())
    # The reader finds nothing and holds one filtered wait per node. A refusal
    # it reports while a primary still applies the member's ownership is read past.
    await asyncio.sleep(1.5)
    if _refused_membership(pending):
        pending = asyncio.create_task(reader.next_page())
        await asyncio.sleep(1.5)
    assert not pending.done()
    sent = time.monotonic()
    await producer.send(f"record-{partition}".encode(), partition=partition)
    deadline = time.monotonic() + timeout
    while True:
        try:
            page = await asyncio.wait_for(pending, timeout=deadline - time.monotonic())
            break
        except ls.FilterError as error:
            if error.reason != "membership_stale":
                raise
            pending = asyncio.create_task(reader.next_page())
    assert [record.partition_id for record in page.records] == [partition]
    return page, (time.monotonic() - sent) * 1000


def _refused_membership(task):
    """Whether a read ended with the refusal a primary gives before it applies the ownership."""
    if not task.done() or task.cancelled():
        return False
    error = task.exception()
    return isinstance(error, ls.FilterError) and error.reason == "membership_stale"


async def _partition_members(cluster, stream, topic, partitions):
    """Each partition's member nodes as the server reports them, once every partition is listed."""
    deadline = time.monotonic() + 30
    while True:
        details = await asyncio.to_thread(cluster.topic_details, stream, topic)
        listed = sorted(
            (details or {}).get("partitions", []), key=lambda partition: partition["id"]
        )
        members = [sorted(partition["members"]) for partition in listed]
        if len(members) == partitions and all(members):
            return members
        assert time.monotonic() < deadline, f"topic {topic} reported {details}"
        await asyncio.sleep(0.2)


async def _wait_until(condition, failure, timeout=90):
    deadline = asyncio.get_running_loop().time() + timeout
    while not condition():
        assert asyncio.get_running_loop().time() < deadline, failure
        await asyncio.sleep(0.05)
