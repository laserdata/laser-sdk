import time
from pathlib import Path

from pytest_bdd import parsers, scenarios, then, when

SCENARIOS = Path(__file__).parent.parent / "scenarios"
scenarios(str(SCENARIOS / "session.feature"))


def _eventually(read, description):
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        value = read()
        if value is not None:
            return value
        time.sleep(0.05)
    raise AssertionError(f"{description} did not converge within 10s")


def _labelled(turn):
    return f"{turn.kind}:{turn.text()}"


@when(parsers.parse('I open the session "{session_id}"'))
def open_session(world, session_id):
    world.session = world.laser.sessions().create(session_id)


@when(parsers.re(r'I append an? "(?P<kind>[^"]+)" turn "(?P<text>[^"]+)" to the session'))
def append_turn(world, kind, text):
    world.run(lambda: world.session.append(kind, text.encode()))


@when(parsers.re(r"I take a session checkpoint after (?P<turns>\d+) turns?"))
def take_checkpoint(world, turns):
    def read():
        checkpoint = world.run(lambda: world.session.checkpoint())
        seen = world.run(lambda: world.session.turns_at(checkpoint))
        return checkpoint if len(seen) == int(turns) else None

    world.checkpoint = _eventually(read, "the session checkpoint")


@then(parsers.parse('the session context is "{first}", "{second}", "{third}" in order'))
def context_is(world, first, second, third):
    def read():
        turns = world.run(lambda: world.session.context())
        return turns if len(turns) == 3 else None

    turns = _eventually(read, "the session context")
    assert [_labelled(turn) for turn in turns] == [first, second, third]


@then(parsers.parse('opening the session "{session_id}" again reaches the same conversation'))
def same_conversation(world, session_id):
    assert world.laser.sessions().create(session_id).conversation == world.session.conversation


@then(parsers.parse('opening the session "{session_id}" reaches a different conversation'))
def different_conversation(world, session_id):
    assert world.laser.sessions().create(session_id).conversation != world.session.conversation


@then(parsers.parse('the turns since the checkpoint are "{text}"'))
def turns_since(world, text):
    def read():
        turns = world.run(lambda: world.session.turns_since(world.checkpoint))
        return turns if len(turns) == 1 else None

    turns = _eventually(read, "the turns since the checkpoint")
    assert turns[0].text() == text


@then(parsers.parse('the turns at the checkpoint are "{text}"'))
def turns_at(world, text):
    turns = world.run(lambda: world.session.turns_at(world.checkpoint))
    assert [turn.text() for turn in turns] == [text]
