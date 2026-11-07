import asyncio

import laser_sdk as ls
import pytest

pytestmark = pytest.mark.integration

READ_TIMEOUT = 15


async def test_given_a_connected_laser_when_its_block_exits_then_should_close_the_connection(
    iggy_endpoint,
):
    async with await ls.Laser.connect(iggy_endpoint, stream="cleanup-owner") as laser:
        await laser.stream("cleanup-owner").ensure()
    with pytest.raises(ls.LaserError):
        await laser.stream("cleanup-owner").ensure()


async def test_given_a_borrowing_handle_when_its_block_exits_then_should_keep_the_connection(
    laser,
):
    async with laser.with_default_stream(laser.default_stream) as view:
        await view.stream(view.default_stream).ensure()
    await laser.stream(laser.default_stream).ensure()


async def test_given_handles_in_async_with_blocks_when_the_blocks_exit_then_should_close_each_one(
    laser,
):
    topic = laser.topic("cleanup-handles")
    async with topic.producer(partition=0, partitions=1) as producer:
        await producer.send(b"direct")
    with pytest.raises(ls.InvalidError):
        await producer.send(b"late")

    async with topic.batching(max_records=100, linger_ms=60_000) as batching:
        await batching.send(b"batched")
    with pytest.raises(ls.InvalidError):
        await batching.send(b"late")

    async with topic.consumer(
        "cleanup-reader", partition=0, polling="first", auto_commit="disabled"
    ) as consumer:
        first = await asyncio.wait_for(consumer.next(), READ_TIMEOUT)
        assert bytes(first.payload) == b"direct"
    assert [message async for message in consumer] == []


async def test_given_a_filtered_reader_in_a_block_when_the_block_exits_then_should_close_it(
    laser,
):
    capabilities = await laser.capabilities()
    if not capabilities.filters.group_policy_reads:
        pytest.skip("a group reader needs group policy reads")
    topic = laser.topic("cleanup-filtered")
    await topic.producer(partition=0, partitions=1).send(b'{"mode":"safe"}')
    group = topic.consumer_group("cleanup-desk")
    await group.create()
    async with await group.reader(start="first") as reader:
        record = await asyncio.wait_for(reader.next_record(), READ_TIMEOUT)
        await reader.ack(record)
    assert [record async for record in reader] == []


async def test_given_a_background_producer_when_flushed_then_should_write_the_queued_records(
    laser,
):
    topic = laser.topic("cleanup-background")
    await topic.ensure(partitions=1)
    producer = topic.producer(
        partition=0,
        background=ls.BackgroundConfig(linger_ms=600_000, batch_length=0, batch_size=0),
    )
    for index in range(3):
        await producer.send(f"reading-{index}".encode())
    await producer.flush()

    cursor = topic.replay()
    seen = []
    for _ in range(50):
        seen.extend(await cursor.poll())
        if len(seen) >= 3:
            break
        await asyncio.sleep(0.1)
    assert [bytes(message.payload) for message in seen] == [
        f"reading-{index}".encode() for index in range(3)
    ]
    await producer.shutdown()
