from pathlib import Path

from pytest_bdd import parsers, scenarios, then, when

# The Background steps ("a running data platform", "a fresh stream") are shared
# across every feature and live in conftest.py.
SCENARIOS = Path(__file__).parent.parent / "scenarios"
scenarios(str(SCENARIOS / "streaming.feature"))


@when(parsers.parse("I bootstrap the stream with {partitions:d} partitions"))
def bootstrap(world, partitions):
    world.run(lambda: world.laser.bootstrap(partitions))


@then("the stream is ready")
def stream_ready(world):
    assert world.laser is not None


@when(parsers.parse('I publish a JSON event to topic "{topic}"'))
def publish_json(world, topic):
    world.run(lambda: world.laser.publish(topic).json({"hello": "world"}).send())
    world.published = True


@then("the publish succeeds")
def publish_succeeds(world):
    assert world.published is True


@when(parsers.parse('I publish a batch of {count:d} JSON events to topic "{topic}"'))
def publish_batch(world, count, topic):
    items = [{"i": index} for index in range(count)]
    world.count = world.run(lambda: world.laser.publish_batch(topic).extend_json(items).send())


@then(parsers.parse("all {count:d} events are published"))
def all_published(world, count):
    assert world.count == count
