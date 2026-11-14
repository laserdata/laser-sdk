from pathlib import Path

from pytest_bdd import parsers, scenarios, then, when

SCENARIOS = Path(__file__).parent.parent / "scenarios"
scenarios(str(SCENARIOS / "stream_admin.feature"))


def unique_stream(world):
    return f"{world.laser.default_stream}-admin"


def topic(world, name):
    return world.laser.stream(unique_stream(world)).topic(name)


@when(parsers.parse('I create the unique stream with topic "{name}" of {partitions:d} partitions'))
def create_unique_stream(world, name, partitions):
    world.run(lambda: topic(world, name).ensure(partitions))


@then("the stream listing includes the unique stream")
def listing_includes(world):
    streams = world.run(world.laser.streams)
    assert any(stream.name == unique_stream(world) for stream in streams)


@then("the stream listing omits the unique stream")
def listing_omits(world):
    streams = world.run(world.laser.streams)
    assert all(stream.name != unique_stream(world) for stream in streams)


@then(parsers.re(r"the unique stream exists with (?P<topics>\d+) topics?"))
def stream_exists(world, topics):
    stream = world.laser.stream(unique_stream(world))
    assert world.run(stream.exists)
    info = world.run(stream.get)
    assert info is not None
    assert info.topics_count == int(topics)


@then(parsers.parse('the unique stream lists topic "{name}" with {partitions:d} partitions'))
def stream_lists_topic(world, name, partitions):
    topics = world.run(world.laser.stream(unique_stream(world)).topics)
    info = next(info for info in topics if info.name == name)
    assert info.partitions_count == partitions


@when("I delete the unique stream")
def delete_stream(world):
    assert world.run(world.laser.stream(unique_stream(world)).delete)


@then("the unique stream no longer exists")
def stream_gone(world):
    stream = world.laser.stream(unique_stream(world))
    assert not world.run(stream.exists)
    assert world.run(stream.get) is None


@when(parsers.parse('I add {count:d} partitions to topic "{name}"'))
def add_partitions(world, count, name):
    world.run(lambda: topic(world, name).add_partitions(count))


@then(parsers.parse('topic "{name}" has {count:d} partitions'))
def topic_partitions(world, name, count):
    handle = topic(world, name)
    info = world.run(handle.get)
    assert info is not None
    assert info.partitions_count == count
    assert len(world.run(handle.partitions)) == count


@when(parsers.parse('I delete topic "{name}"'))
def delete_topic(world, name):
    assert world.run(topic(world, name).delete)


@then(parsers.parse('topic "{name}" no longer exists'))
def topic_gone(world, name):
    handle = topic(world, name)
    assert not world.run(handle.exists)
    assert world.run(handle.get) is None


@then(parsers.parse('a second delete of topic "{name}" reports it missing'))
def second_delete(world, name):
    assert not world.run(topic(world, name).delete)


@when(parsers.parse('I create consumer group "{group}" on topic "{name}"'))
def create_group(world, group, name):
    world.run(lambda: topic(world, name).ensure_consumer_group(group))


@then(parsers.parse('topic "{name}" lists consumer group "{group}"'))
def lists_group(world, name, group):
    groups = world.run(topic(world, name).consumer_groups)
    assert any(summary.name == group for summary in groups)


@when(parsers.parse('I delete consumer group "{group}" on topic "{name}"'))
def delete_group(world, group, name):
    assert world.run(topic(world, name).consumer_group(group).delete)


@then(parsers.parse('topic "{name}" lists no consumer groups'))
def lists_no_groups(world, name):
    assert world.run(topic(world, name).consumer_groups) == []
