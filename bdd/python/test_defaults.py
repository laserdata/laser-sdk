from pathlib import Path

import laser_sdk as ls
from pytest_bdd import given, parsers, scenarios, then, when

SCENARIOS = Path(__file__).parent.parent / "scenarios"
scenarios(str(SCENARIOS / "defaults.feature"))


@when("I bootstrap the stream with the defaults")
def bootstrap_defaults(world):
    world.run(world.laser.bootstrap)


@then(
    parsers.parse(
        'topic "{name}" has {partitions:d} partition and expires records after {seconds:d} seconds'
    )
)
def topic_defaults(world, name, partitions, seconds):
    info = world.run(world.laser.stream(world.laser.default_stream).topic(name).get)
    assert info.partitions_count == partitions
    assert info.message_expiry_ms == seconds * 1000


@given("a client without a default stream")
def client_without_default_stream(world):
    world.laser = world.run(lambda: ls.Laser.connect(world.endpoint))


@when(parsers.parse('I publish "{payload}" to topic "{topic}" of the default stream'))
def publish_to_default_stream(world, payload, topic):
    world.capture(lambda: world.laser.topic(topic).send(payload.encode()))


@when(
    parsers.parse(
        'the workflow "{name}" runs the step "{label}" on "{agent}" '
        "through the advertised inbox route"
    )
)
def advertised_step(world, name, label, agent):
    workflow = world.laser.workflow(name).step(label, to=agent, build=lambda outputs: label)
    world.capture(workflow.run)


@then("the error names the call that sets a default stream")
def names_default_stream_call(world):
    assert isinstance(world.error, ls.NoStreamError), world.error
    assert "scratch_stream(prefix)" in str(world.error)
    assert "with_default_stream(name)" in str(world.error)


@then("the error names the agent capabilities and the fixed inbox route")
def names_inbox_fixes(world):
    assert isinstance(world.error, ls.NoInboxError), world.error
    assert "capabilities" in str(world.error)
    assert "fixed_inbox" in str(world.error)
