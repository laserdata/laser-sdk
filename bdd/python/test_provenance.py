from pathlib import Path

from pytest_bdd import parsers, scenarios, then

# Background and the shared command / assemble / payload steps live in conftest.py.
SCENARIOS = Path(__file__).parent.parent / "scenarios"
scenarios(str(SCENARIOS / "provenance.feature"))


@then(parsers.parse('the assembled message agent is "{agent}"'))
def assembled_agent_is(world, agent):
    assert world.assembled[0].agent == agent


@then(parsers.parse('the assembled message idempotency key is "{key}"'))
def assembled_idempotency_key_is(world, key):
    assert world.assembled[0].idempotency_key == key


@then(parsers.parse('the assembled message correlation id is "{correlation}"'))
def assembled_correlation_id_is(world, correlation):
    assert world.assembled[0].correlation_id == correlation


@then("the assembled message belongs to the conversation")
def assembled_belongs_to_conversation(world):
    assert world.assembled[0].conversation_id == world.conversation
