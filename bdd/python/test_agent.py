from pathlib import Path

import laser_sdk as ls
from pytest_bdd import given, parsers, scenarios, then, when

# Background and the shared command / assemble / payload steps live in conftest.py.
SCENARIOS = Path(__file__).parent.parent / "scenarios"
scenarios(str(SCENARIOS / "agent.feature"))
scenarios(str(SCENARIOS / "agent_must_understand.feature"))


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
    correlation = ls.mint_ulid()
    world.run(
        lambda: world.laser.agdx(ls.AgentTopic.Sessions, "producer", world.conversation).command(
            correlation, body.encode()
        )
    )


@then(parsers.parse('the AGDX command body is "{body}"'))
def agdx_command_body_is(world, body):
    assert any(
        message.envelope is not None and bytes(message.envelope["body"]) == body.encode()
        for message in world.assembled
    )


def understood_by_bare_receiver(requiring):
    envelope = ls.event_envelope(
        ls.mint_ulid(), ls.new_conversation_id(), "sender", b"event", requiring=requiring
    )
    return ls.unmet_requirements(envelope, 0) == 0


@when("I build an agent event requiring feature bits the receiver lacks")
def build_required_event(world):
    world.agent_event_understood = understood_by_bare_receiver(1 << 8)


@when("I build a plain agent event")
def build_plain_event(world):
    world.agent_event_understood = understood_by_bare_receiver(0)


@then("the receiver rejects it as not understood")
def receiver_rejects(world):
    assert world.agent_event_understood is False


@then("the receiver understands it")
def receiver_understands(world):
    assert world.agent_event_understood is True


async def _idle(_ctx, _message):
    return None


@given(parsers.parse('agent "{name}" advertising the skill "{skill}" is ready on this connection'))
def advertising_agent_ready(world, name, skill):
    async def spawn():
        # The agent runs on the scenario's loop, so it is spawned inside it.
        worker = world.laser.spawn_agent(name, ls.AgentTopic.Sessions, _idle, capabilities=[skill])
        await worker.ready()
        return worker

    world.first_worker = world.run(spawn)


@when(parsers.parse('agent "{name}" advertising the skill "{skill}" starts on the same connection'))
def second_advertising_agent(world, name, skill):
    async def spawn():
        return world.laser.spawn_agent(name, ls.AgentTopic.Sessions, _idle, capabilities=[skill])

    world.second_worker = world.run(spawn)


@then("waiting for the second agent to be ready fails with a presence conflict")
def second_ready_conflict(world):
    try:
        world.capture(lambda: world.second_worker.ready())
        assert isinstance(world.error, ls.PresenceConflictError), repr(world.error)
    finally:
        world.run(lambda: world.first_worker.shutdown())
