from pathlib import Path

from pytest_bdd import parsers, scenarios, then, when

SCENARIOS = Path(__file__).parent.parent / "scenarios"
scenarios(str(SCENARIOS / "consistency.feature"))


def _kv_position(world):
    return world.laser.seen_positions().kv


@when(parsers.parse('I write "{value}" to the managed key "{key}" in namespace "{namespace}"'))
def write_managed_key(world, value, key, namespace):
    kv = world.laser.kv(namespace)
    world.run(lambda: kv.set(key).bytes(value).send())


@then("the client has seen a key-value position")
def seen_kv_position(world):
    assert _kv_position(world) is not None, "the write reply carried the fold's position"


@when("I note the client's key-value position")
def note_kv_position(world):
    world.noted_position = _kv_position(world)


@then("the client's key-value position moved past the noted one")
def kv_position_moved(world):
    noted = world.noted_position
    seen = _kv_position(world)
    assert noted is not None and seen is not None
    assert (seen.topic_generation, seen.offset) > (noted.topic_generation, noted.offset)


@then(parsers.parse('the managed key "{key}" in namespace "{namespace}" reads "{value}"'))
def managed_key_reads(world, key, namespace, value):
    kv = world.laser.kv(namespace)
    assert world.run(lambda: kv.get(key)) == value.encode()
