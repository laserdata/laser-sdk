from pathlib import Path

import laser_sdk as ls
from pytest_bdd import parsers, scenarios, then, when

# Background and the shared command / assemble / payload steps live in conftest.py.
SCENARIOS = Path(__file__).parent.parent / "scenarios"
scenarios(str(SCENARIOS / "agent.feature"))


@when(parsers.parse('I send agent commands "{first}", "{second}", "{third}"'))
def send_three_commands(world, first, second, third):
    for payload in (first, second, third):
        world.send_command(payload)


@then(parsers.parse('the assembled payloads are "{first}", "{second}", "{third}" in order'))
def assembled_payloads_in_order(world, first, second, third):
    payloads = [message.payload.decode() for message in world.assembled]
    assert payloads == [first, second, third]


@when("I start another conversation")
def start_another_conversation(world):
    world.new_conversation()


@when(parsers.parse('I publish an AGDX command "{body}" via the typed producer'))
def publish_agdx_command(world, body):
    correlation = ls.new_correlation_id()
    world.run(
        lambda: world.laser.agdx(ls.Topics.COMMANDS, "producer", world.conversation).command(
            correlation, body.encode()
        )
    )


@then(parsers.parse('the AGDX command body is "{body}"'))
def agdx_command_body_is(world, body):
    assert any(message.agdx_body == body.encode() for message in world.assembled)
