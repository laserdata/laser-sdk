from pathlib import Path

import laser_sdk as ls
from pytest_bdd import parsers, scenarios, then, when

# The Background steps live in conftest.py (shared across features), and the
# shared unsupported assertions live in test_capabilities.py's step library.
SCENARIOS = Path(__file__).parent.parent / "scenarios"
scenarios(str(SCENARIOS / "runs.feature"))


@then("the run registry is unavailable")
def run_registry_unavailable(world):
    caps = world.run(lambda: world.laser.capabilities())
    assert caps.agent_workflow is False


@when(parsers.parse('I submit a run to agent "{agent}"'))
def submit_run(world, agent):
    world.capture(lambda: world.laser.runs().submit(agent, b"input"))


@when(parsers.parse('I read the status of run "{run_id}"'))
def run_status(world, run_id):
    world.capture(lambda: world.laser.runs().status(run_id))


@when(parsers.parse('I cancel run "{run_id}"'))
def cancel_run(world, run_id):
    world.capture(lambda: world.laser.runs().cancel(run_id))


@when("I list runs")
def list_runs(world):
    world.capture(lambda: world.laser.runs().list())


@then("the call fails as unsupported")
def fails_unsupported(world):
    assert isinstance(world.error, ls.UnsupportedError)


@then("the unified result code is unsupported")
def unified_code_unsupported(world):
    assert world.error is not None
    assert world.error.code == "Unsupported"
