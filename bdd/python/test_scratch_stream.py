import re
from pathlib import Path

import pytest
from pytest_bdd import parsers, scenarios, then, when

SCENARIOS = Path(__file__).parent.parent / "scenarios"
scenarios(str(SCENARIOS / "scratch_stream.feature"))


@pytest.fixture(autouse=True)
def delete_scratch_streams(world):
    """A scenario that fails before its delete step still leaves no stream."""
    yield
    for scratch in [getattr(world, "scratch", None), *getattr(world, "scratches", [])]:
        if scratch is not None:
            world.run(scratch.delete)


@when(parsers.parse('I open a scratch stream with the prefix "{prefix}"'))
def open_scratch(world, prefix):
    world.scratch = world.laser.scratch_stream(prefix)
    world.run(world.scratch.__aenter__)


@then(parsers.parse('the scratch stream is named "{prefix}" and 8 lowercase hex characters'))
def scratch_named(world, prefix):
    assert re.fullmatch(re.escape(prefix) + r"[0-9a-f]{8}", world.scratch.name)


@then("the scratch stream exists")
def scratch_exists(world):
    assert world.run(world.laser.stream(world.scratch.name).exists)


@then("the scratch handle's default stream is the scratch stream")
def scratch_default_stream(world):
    assert world.scratch.laser.default_stream == world.scratch.name


@when("I delete the scratch stream")
def delete_scratch(world):
    world.run(world.scratch.delete)


@then("the scratch stream no longer exists")
def scratch_gone(world):
    assert not world.run(world.laser.stream(world.scratch.name).exists)


@then("deleting the scratch stream again succeeds")
def delete_scratch_again(world):
    world.run(world.scratch.delete)


@when(parsers.parse('I open two scratch streams with the prefix "{prefix}"'))
def open_two_scratch(world, prefix):
    world.scratches = [world.laser.scratch_stream(prefix) for _ in range(2)]
    for scratch in world.scratches:
        world.run(scratch.__aenter__)


@then("the two scratch streams have different names")
def different_names(world):
    first, second = world.scratches
    assert first.name != second.name
    for scratch in world.scratches:
        world.run(scratch.delete)
