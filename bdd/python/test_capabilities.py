from pathlib import Path

import laser_sdk as ls
from pytest_bdd import parsers, scenarios, then, when

# The Background steps live in conftest.py (shared across features).
SCENARIOS = Path(__file__).parent.parent / "scenarios"
scenarios(str(SCENARIOS / "capabilities.feature"))


@when("I read the negotiated capabilities")
def read_capabilities(world):
    world.caps = world.run(lambda: world.laser.capabilities())


@then("managed query is unavailable")
def query_unavailable(world):
    assert world.caps.query is False


@then("managed key-value is unavailable")
def kv_unavailable(world):
    assert world.caps.kv is False


@then("forks are unavailable")
def forks_unavailable(world):
    assert world.caps.forks is False


@then("the coordination features are unavailable")
def coordination_unavailable(world):
    assert world.caps.kv_cas is False
    # Consistency is one ordered level. Raw Apache Iggy serves only the weakest.
    assert world.caps.query_consistency == "eventual"


@when(parsers.parse('I run a query against topic "{topic}"'))
def run_query(world, topic):
    world.capture(lambda: world.laser.query(topic).fetch())


@when(parsers.parse('I run a read-your-writes query against topic "{topic}"'))
def run_ryw_query(world, topic):
    world.capture(lambda: world.laser.query(topic).read_your_writes().fetch())


@when(
    parsers.parse('I compare-and-swap key "{key}" in namespace "{namespace}" expecting it absent')
)
def compare_and_swap(world, key, namespace):
    world.capture(lambda: world.laser.kv(namespace).set(key).payload(b"x").expect_absent().commit())


@then("the call fails as unsupported")
def fails_unsupported(world):
    assert isinstance(world.error, ls.UnsupportedError)


@then("the unified result code is unsupported")
def unified_code_unsupported(world):
    assert world.error is not None
    assert world.error.code == "Unsupported"
