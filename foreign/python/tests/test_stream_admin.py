import uuid

import laser_sdk as ls
import pytest

pytestmark = pytest.mark.integration


async def test_given_a_unique_stream_when_listed_and_deleted_then_should_report_it_until_gone(
    laser,
):
    name = f"admin-{uuid.uuid4().hex[:12]}"
    stream = laser.stream(name)
    assert await stream.get() is None
    assert await stream.exists() is False
    await stream.topic("metrics").ensure(2)

    listed = [info for info in await laser.streams() if info.name == name]
    assert len(listed) == 1
    info = listed[0]
    assert isinstance(info, ls.StreamInfo)
    assert info.topics_count == 1
    assert info.created_at_micros > 0
    assert await stream.get() == info
    assert await stream.exists() is True

    topics = await stream.topics()
    assert [topic.name for topic in topics] == ["metrics"]
    assert isinstance(topics[0], ls.TopicInfo)
    assert topics[0].partitions_count == 2
    assert topics[0].message_expiry_ms is None

    assert await stream.delete() is True
    assert await stream.exists() is False
    assert all(info.name != name for info in await laser.streams())


async def test_given_a_topic_when_partitions_changed_and_deleted_then_should_report_each_state(
    laser,
):
    topic = laser.topic("hosts")
    assert await topic.get() is None
    assert await topic.delete() is False
    await topic.ensure(1)
    await topic.add_partitions(2)
    info = await topic.get()
    assert isinstance(info, ls.TopicInfo)
    assert info.partitions_count == 3
    partitions = await topic.partitions()
    assert len(partitions) == 3
    assert all(isinstance(partition, ls.PartitionInfo) for partition in partitions)
    await topic.delete_partitions(1)
    assert len(await topic.partitions()) == 2
    assert await topic.delete() is True
    assert await topic.exists() is False
    with pytest.raises(ls.LaserError) as missing:
        await topic.partitions()
    assert missing.value.stream_or_topic_not_found


async def test_given_a_missing_stream_when_topics_or_groups_listed_then_should_raise_not_found(
    laser,
):
    stream = laser.stream(f"missing-{uuid.uuid4().hex[:12]}")
    with pytest.raises(ls.LaserError) as missing:
        await stream.topics()
    assert missing.value.stream_or_topic_not_found
    with pytest.raises(ls.LaserError) as missing:
        await stream.topic("jobs").consumer_groups()
    assert missing.value.stream_or_topic_not_found
    assert await stream.delete() is False


async def test_given_a_zero_count_when_partitions_or_segments_changed_then_should_raise_invalid(
    laser,
):
    topic = laser.topic("hosts")
    for change in (
        topic.add_partitions(0),
        topic.delete_partitions(0),
        topic.delete_segments(1, 0),
    ):
        with pytest.raises(ls.InvalidError, match="count must be greater than zero"):
            await change


async def test_given_a_consumer_group_when_listed_and_deleted_then_should_report_it_until_gone(
    laser,
):
    topic = laser.topic("jobs")
    await topic.ensure(1)
    await topic.ensure_consumer_group("workers")
    groups = await topic.consumer_groups()
    assert [group.name for group in groups] == ["workers"]
    assert isinstance(groups[0], ls.ConsumerGroupSummary)
    assert groups[0].members_count == 0
    group = topic.consumer_group("workers")
    assert await group.delete() is True
    assert await group.delete() is False
    assert await topic.consumer_groups() == []
