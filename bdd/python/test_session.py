import asyncio
import json
import time
from pathlib import Path

import laser_sdk as ls
from pytest_bdd import given, parsers, scenarios, then, when

STATE_SNAPSHOT_OPERATION = "state_snapshot"
SCENARIOS = Path(__file__).parent.parent / "scenarios"
scenarios(str(SCENARIOS / "session.feature"))
scenarios(str(SCENARIOS / "context_window.feature"))


def _eventually(world, read, description):
    # Waiting runs the loop, so an agent spawned on it keeps handling records.
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        value = read()
        if value is not None:
            return value
        world.run(lambda: asyncio.sleep(0.05))
    raise AssertionError(f"{description} did not converge within 10s")


def _names(listing):
    return [name.strip('"') for name in listing.split(", ")]


def _lane(world, session, done):
    def read():
        turns = world.run(lambda: world.laser.sessions().open(session).context_with(ls.LastN(200)))
        return turns if done(turns) else None

    return _eventually(world, read, "the session lane")


def _sessions(world):
    # The scenario's configured session factory, or the default one.
    config = getattr(world, "session_config", None)
    return world.laser.sessions(**config) if config else world.laser.sessions()


@when(parsers.parse('agent "{owner}" starts the session "{label}"'))
def start_session(world, owner, label):
    world.session, world.lease = world.run(
        lambda: _sessions(world).create(label).agent(owner).begin()
    )


@when("the session ends")
def end_session(world):
    world.run(lambda: world.session.end())


@when("I cancel the session")
def cancel_session(world):
    world.capture(lambda: world.session.cancel())


@when(
    parsers.parse(
        'agent "{owner}" runs the session "{label}" with work that fails with "{message}"'
    )
)
def run_failing(world, owner, label, message):
    session, lease = world.run(lambda: world.laser.sessions().create(label).agent(owner).begin())
    world.session = session

    async def work(_session):
        raise RuntimeError(message)

    try:
        world.run(lambda: session.run(lease, work))
    except RuntimeError:
        pass
    else:
        raise AssertionError("the work error is raised")


@then(parsers.re(r"the session lane shows (?P<listing>\".+\")$"))
def lane_shows(world, listing):
    expected = _names(listing)
    turns = _lane(world, world.session.conversation, lambda turns: len(turns) >= len(expected))
    assert [turn.display for turn in turns] == expected


@then("the call fails as invalid")
def fails_invalid(world):
    assert isinstance(world.error, ls.InvalidError)


@then(parsers.parse('the session failure message is "{message}"'))
def failure_message(world, message):
    turns = _lane(
        world,
        world.session.conversation,
        lambda turns: any(turn.display == "session.failed" for turn in turns),
    )
    failed = next(turn for turn in turns if turn.display == "session.failed")
    end = ls.decode_session_end(bytes(failed.message.envelope["body"]))
    assert message in end["error"]["message"]


@then(parsers.parse('the session "{label}" has the same id when created twice'))
def same_id(world, label):
    sessions = world.laser.sessions()
    first = sessions.create(label).id()
    assert first == sessions.create(label).id()
    assert first == ls.derive_session_id(sessions.stream, "", label)


@then(parsers.parse('the sessions "{first}" and "{second}" have different ids'))
def different_ids(world, first, second):
    sessions = world.laser.sessions()
    assert sessions.create(first).id() != sessions.create(second).id()


@when(parsers.parse('the session records the message "{text}"'))
def record_message(world, text):
    envelope = ls.event_envelope(
        ls.new_conversation_id(),
        world.session.conversation,
        world.session.agent,
        text.encode(),
        operation="note",
    )
    world.run(lambda: world.session.append(envelope))


@when(parsers.re(r"I take a session checkpoint after (?P<records>\d+) records?"))
def take_checkpoint(world, records):
    def read():
        checkpoint = world.run(lambda: world.session.checkpoint())
        seen = world.run(lambda: world.session.turns_at(checkpoint))
        return checkpoint if len(seen) == int(records) else None

    world.checkpoint = _eventually(world, read, "the session checkpoint")


@then(parsers.parse('the records since the checkpoint are "{text}"'))
def records_since(world, text):
    def read():
        turns = world.run(lambda: world.session.turns_since(world.checkpoint))
        return turns if len(turns) == 1 else None

    turns = _eventually(world, read, "the records since the checkpoint")
    assert turns[0].text() == text


@then(parsers.parse('the last record at the checkpoint is "{text}"'))
def last_at_checkpoint(world, text):
    turns = world.run(lambda: world.session.turns_at(world.checkpoint))
    assert turns[-1].text() == text


@when(
    parsers.parse('the session records a model call to "{model}" whose request carries an api key')
)
def record_model(world, model):
    request = ls.ModelRequest(model, b'{"prompt":"hi","api_key":"k-secret"}')
    response = ls.ModelResponse(b"hello")
    world.run(lambda: world.session.record_model_call(request, response))


@when(parsers.parse('the session records a call of tool "{tool}" whose arguments carry a token'))
def record_tool(world, tool):
    call = world.run(lambda: world.session.tool(tool, {"id": 7, "token": "t-secret"}))
    world.run(lambda: call.complete(b"found"))


@then("no recorded call carries a secret value")
def no_secret(world):
    turns = _lane(world, world.session.conversation, lambda turns: len(turns) >= 5)
    for turn in turns:
        text = turn.text()
        assert "k-secret" not in text and "t-secret" not in text, text


@when(parsers.re(r'the session state sets "(?P<key>[^"]+)" to (?P<value>.+)$'))
def state_set(world, key, value):
    state = world.session.state()
    world.run(lambda: state.set(key, json.loads(value)))


@then(
    parsers.re(
        r'the folded session state has "(?P<first>[^"]+)" (?P<first_value>\S+) and '
        r'"(?P<second>[^"]+)" (?P<second_value>\S+) at revision (?P<revision>\d+)$'
    )
)
def folded_state(world, first, first_value, second, second_value, revision):
    state = world.session.state()

    def read():
        view = world.run(lambda: state.get())
        return view if view["revision"] == int(revision) else None

    view = _eventually(world, read, "the folded state")
    assert view["document"][first] == json.loads(first_value)
    assert view["document"][second] == json.loads(second_value)
    assert view["complete"] is True


@then(parsers.re(r'the session lane shows (?P<count>\d+) "(?P<display>[^"]+)" records$'))
def lane_counts(world, count, display):
    def enough(turns):
        return sum(turn.display == display for turn in turns) >= int(count)

    turns = _lane(world, world.session.conversation, enough)
    assert sum(turn.display == display for turn in turns) == int(count)


@given(parsers.parse('agent "{name}" ends every session it handles'))
def ending_worker(world, name):
    async def handle(ctx, _message):
        await ctx.session().end()

    async def spawn():
        # The agent runs on the scenario's loop, so it is spawned inside it.
        worker = world.laser.spawn_agent(
            name, ls.AgentTopic.Sessions, handle, sessions=_sessions(world)
        )
        await worker.ready()
        return worker

    world.worker = world.run(spawn)


@when(parsers.parse('"{submitter}" submits a session to agent "{target}"'))
def submit_session(world, submitter, target):
    submitted = world.run(
        lambda: world.laser.sessions().submit(target, b"{}").from_(submitter).send()
    )
    world.other_session = submitted.session


@then(parsers.parse('the submitted session starts as "{first}" and reaches "{last}"'))
def submitted_reaches(world, first, last):
    try:
        turns = _lane(
            world,
            world.other_session,
            lambda turns: any(turn.display == last for turn in turns),
        )
        assert turns[0].display == first
    finally:
        world.run(lambda: world.worker.shutdown())


@when(parsers.parse('operator "{operator}" asks to cancel the session "{label}"'))
def operator_cancel(world, operator, label):
    world.run(lambda: world.laser.topic(ls.AgentTopic.Control).ensure(partitions=1))
    stream = world.laser.default_stream
    session = ls.derive_session_id(stream, "", label)
    control = world.laser.sessions().control(stream, session).as_operator(operator)
    world.run(lambda: control.cancel())
    world.other_session = session


@then(parsers.parse('the control topic holds a "{operation}" request for the session'))
def control_holds(world, operation):
    def read():
        records = world.run(
            lambda: world.laser.context(world.other_session).fetch(
                topics=[ls.AgentTopic.Control], n=10
            )
        )
        return records or None

    records = _eventually(world, read, "the control records")
    assert records[0].envelope["operation"] == operation


@given(
    parsers.parse(
        'the agents use a declared partition layout with "{first}" on {first_partition:d} '
        'and "{second}" on {second_partition:d}'
    )
)
def declared_layout(world, first, first_partition, second, second_partition):
    world.laser.sessions(
        layout=ls.SessionLayout.PerAgentPartition(
            partitions={first: first_partition, second: second_partition}
        )
    )


@when(
    parsers.parse(
        '"{requester}" sends a command to "{target}" in a new session and "{responder}" replies'
    )
)
def routed_exchange(world, requester, target, responder):
    session = ls.new_conversation_id()
    correlation = ls.new_conversation_id()
    command = world.run(
        lambda: world.laser.agdx(ls.AgentTopic.Sessions, requester, session).command(
            correlation, b"{}", target=target, receipt=True
        )
    )
    reply = world.run(
        lambda: world.laser.agdx(ls.AgentTopic.Sessions, responder, session).respond(
            correlation, b"{}", target=requester, receipt=True
        )
    )
    world.routed = (command.partition_id, reply.partition_id)
    world.routed_session = session


@given(
    parsers.parse(
        'the agents use a declared topic layout with "{first}" on "{first_topic}" '
        'and "{second}" on "{second_topic}"'
    )
)
def declared_topic_layout(world, first, first_topic, second, second_topic):
    sessions = world.laser.sessions(
        layout=ls.SessionLayout.PerAgentTopic(topics={first: first_topic, second: second_topic})
    )
    world.run(lambda: sessions.bootstrap(1, ls.TopicRetention.expire_after(86_400_000)))


def _routed_kinds(world, topic):
    messages = world.run(
        lambda: world.laser.context(world.routed_session).fetch(topics=[topic], n=10)
    )
    return [message.envelope["kind"] for message in messages if message.envelope]


@then(parsers.parse('the topic "{work}" holds the command and the topic "{reply}" holds the reply'))
def routed_topics(world, work, reply):
    commands = _eventually(world, lambda: _routed_kinds(world, work) or None, "the routed command")
    assert commands == ["command"]
    replies = _eventually(world, lambda: _routed_kinds(world, reply) or None, "the routed reply")
    assert replies == ["response"]


@then("the session lane holds neither")
def lane_holds_neither(world):
    assert _routed_kinds(world, "agent.sessions") == []


@then(
    parsers.parse(
        "the command landed on partition {command:d} and the reply on partition {reply:d}"
    )
)
def routed_partitions(world, command, reply):
    assert world.routed == (command, reply)


@when(parsers.parse('the session remembers "{text}" through its linked memory'))
def remember_linked(world, text):
    memory = world.session.linked_memory()
    world.run(lambda: memory.remember(text))


@then(parsers.parse('the remembered item names producer "{producer}"'))
def remembered_producer(world, producer):
    memory = world.session.memory()

    def read():
        items = world.run(lambda: memory.recall(strategy="recent", folded=True))
        return items or None

    items = _eventually(world, read, "the remembered items")
    assert items[0].producer["name"] == producer


@when(parsers.parse('I publish {count:d} unrelated records to topic "{topic}"'))
def publish_unrelated(world, count, topic):
    batch = world.laser.topic(topic).publish_batch()
    for index in range(count):
        batch = batch.add_payload(f"unrelated-{index}".encode())
    world.run(lambda: batch.send())


@then(parsers.parse('the session context holds only the message "{text}"'))
def context_only(world, text):
    def read():
        texts = [turn.text() for turn in world.run(lambda: world.session.context())]
        return texts if texts == [text] else None

    assert _eventually(world, read, "the session context") == [text]


TERMINAL = ("session.completed", "session.failed", "session.canceled")
CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def _terminal(turns):
    return [turn for turn in turns if turn.display in TERMINAL]


def _record_ids(turns):
    return {turn.message.envelope["record"] for turn in _terminal(turns)}


def _sleep(world, seconds):
    world.run(lambda: asyncio.sleep(seconds))


class RefuseSessionStatus:
    """Blocks the next `remaining` session status publishes, then allows everything."""

    def __init__(self):
        self.remaining = 0
        self.snapshots = 0

    async def decide(self, action):
        if action.kind == "status" and action.operation == "session" and self.remaining > 0:
            self.remaining -= 1
            return ls.ActionDecision.block("injected publish refusal")
        if action.operation == STATE_SNAPSHOT_OPERATION and self.snapshots > 0:
            self.snapshots -= 1
            return ls.ActionDecision.block("injected publish refusal")
        return ls.ActionDecision.allow()


@given("session publishes pass through a policy that can refuse them")
def refusing_policy(world):
    world.refusals = RefuseSessionStatus()
    world.laser = world.laser.with_governor(world.refusals)


@when("the policy refuses the next session status publish")
def refuse_next(world):
    world.refusals.remaining = 1


@when("I end the session")
def end_captured(world):
    world.capture(lambda: world.session.end())


@then("the send is rejected by policy")
def rejected_by_policy(world):
    assert isinstance(world.error, ls.PolicyBlockedError)


@then("every terminal record on the lane carries one record id")
def one_terminal_record(world):
    turns = _lane(world, world.session.conversation, lambda turns: bool(_terminal(turns)))
    records = _record_ids(turns)
    assert len(records) == 1 and None not in records, records


@when(
    parsers.parse(
        "{enders:d} clones of the session end it while {cancelers:d} other clones cancel it at once"
    )
)
def racing_clones(world, enders, cancelers):
    session = world.session

    async def call(verb):
        clone = session.as_agent(session.agent)
        try:
            await (clone.end() if verb == "end" else clone.cancel())
            return verb, None
        except ls.LaserError as error:
            return verb, error

    async def race():
        # Alternate the verbs so neither side gets a head start.
        verbs = []
        for index in range(max(enders, cancelers)):
            if index < enders:
                verbs.append("end")
            if index < cancelers:
                verbs.append("cancel")
        return await asyncio.gather(*(call(verb) for verb in verbs))

    world.outcomes = world.run(race)


@then("the calls of one verb all succeed and the calls of the other all fail as invalid")
def one_verb_wins(world):
    winners = {verb for verb, error in world.outcomes if error is None}
    assert len(winners) == 1, world.outcomes
    world.winner = winners.pop()
    for verb, error in world.outcomes:
        if verb == world.winner:
            assert error is None, error
        else:
            assert isinstance(error, ls.InvalidError), error


@then(
    parsers.parse(
        "the lane holds {count:d} terminal records of the winning verb under one record id"
    )
)
def winner_on_lane(world, count):
    display = "session.completed" if world.winner == "end" else "session.canceled"
    turns = _lane(world, world.session.conversation, lambda turns: len(_terminal(turns)) >= count)
    terminal = _terminal(turns)
    assert len(terminal) == count
    assert all(turn.display == display for turn in terminal), [turn.display for turn in turns]
    records = _record_ids(turns)
    assert len(records) == 1 and None not in records, records


@given("a fresh stream bootstrapped for sessions in the single-partition layout")
def single_partition_stream(world):
    world.connect()
    sessions = world.laser.sessions(
        layout=ls.SessionLayout.SinglePartition(), register_source=False
    )
    world.run(lambda: sessions.bootstrap(4, ls.TopicRetention.expire_after(86_400_000)))


@given(parsers.parse('agents "{first}" and "{second}" each record the commands they handle'))
def recording_agents(world, first, second):
    world.handled = {}
    world.agents = []

    def recorder(seen):
        async def handle(_ctx, message):
            seen.append(bytes(message.body()).decode())

        return handle

    async def spawn(name):
        seen = []
        agent = world.laser.spawn_agent(name, ls.AgentTopic.Sessions, recorder(seen))
        await agent.ready()
        world.handled[name] = seen
        world.agents.append(agent)

    for name in (first, second):
        world.run(lambda name=name: spawn(name))


@when(parsers.re(r'"(?P<sender>[^"]+)" sends (?P<listing>.+) in separate sessions$'))
def send_separately(world, sender, listing):
    world.partitions = []
    for pair in listing.split(", "):
        body, target = (part.strip('"') for part in pair.split(" to "))
        receipt = world.run(
            lambda body=body, target=target: world.laser.agdx(
                ls.AgentTopic.Sessions, sender, ls.new_conversation_id()
            ).command(ls.new_conversation_id(), body.encode(), target=target, receipt=True)
        )
        world.partitions.append(receipt.partition_id)


@then("every command landed on the same partition")
def same_partition(world):
    assert len(set(world.partitions)) == 1, world.partitions
    assert world.partitions[0] is not None


@then(parsers.re(r'agent "(?P<name>[^"]+)" handled exactly (?P<listing>".+")$'))
def handled_exactly(world, name, listing):
    expected = _names(listing)
    seen = world.handled[name]
    try:
        _eventually(world, lambda: True if seen == expected else None, f"agent {name}")
        # The other agent's records share the partition, so give a wrongly
        # handled one time to show up before the final check.
        _sleep(world, 0.5)
        assert seen == expected, seen
    finally:
        if name == list(world.handled)[-1]:
            for agent in world.agents:
                world.run(lambda agent=agent: agent.shutdown())


@when(
    parsers.re(
        r'"(?P<sender>[^"]+)" writes the control requests (?P<listing>".+") '
        r'for agent "(?P<target>[^"]+)" on the session lane$'
    )
)
def misplaced_control(world, sender, listing, target):
    lane = world.laser.agdx(ls.AgentTopic.Sessions, sender, world.session.conversation)
    for operation in _names(listing):
        world.run(
            lambda operation=operation: lane.command(
                ls.new_conversation_id(), b"{}", operation=operation, target=target
            )
        )


@when(parsers.parse('"{sender}" sends work to agent "{target}" in the session'))
def send_work(world, sender, target):
    lane = world.laser.agdx(ls.AgentTopic.Sessions, sender, world.session.conversation)
    world.run(lambda: lane.command(ls.new_conversation_id(), b"work", target=target))


@then(parsers.parse('agent "{name}" sees no pending pause or cancel for the session'))
def no_pending_control(world, name):
    try:
        lens = _sessions(world).open(world.session.conversation).as_agent(name)
        pending = world.run(lambda: lens.pending_control())
        assert not pending.pause_requested, "a pause was applied"
        assert not pending.cancel_requested, "a cancel was applied"
        assert not world.run(lambda: lens.cancel_requested()), "a cancel was applied"
    finally:
        world.run(lambda: world.worker.shutdown())


@when(
    parsers.parse(
        'the session records a model call to "{model}" '
        "with the context assembled from its {records:d} records"
    )
)
def record_model_assembled(world, model, records):
    session = world.session

    def read():
        assembled = world.run(lambda: session.assemble(ls.LastN(50)))
        return assembled if len(assembled.fragments) == records else None

    world.assembled_context = _eventually(world, read, "the assembled context")
    request = ls.ModelRequest(model, b'{"prompt":"hi"}')
    world.run(
        lambda: session.record_model_call(
            request, ls.ModelResponse(b"hello"), world.assembled_context
        )
    )


@when(parsers.parse('the session records a model call to "{model}" without an assembled context'))
def record_model_bare(world, model):
    request = ls.ModelRequest(model, b'{"prompt":"hi"}')
    world.run(lambda: world.session.record_model_call(request, ls.ModelResponse(b"hello")))


@then("the context manifest lists the address of every assembled fragment")
def manifest_addresses(world):
    session = world.session.conversation
    turns = _lane(
        world, session, lambda turns: any(turn.display == "context.assembled" for turn in turns)
    )
    envelope = next(turn for turn in turns if turn.display == "context.assembled").message.envelope
    request = next(turn for turn in turns if turn.display == "model.request").message.envelope
    manifest = ls.decode_context_manifest(bytes(envelope["body"]))
    assert manifest["correlation"] == request["correlation"]
    listed = []
    for fragment in manifest["fragments"]:
        assert list(fragment) == ["Message"], fragment
        assert list(fragment["Message"]["at"]) == ["Message"], fragment
        address = fragment["Message"]["at"]["Message"]
        assert address["generation"] is not None, fragment
        assert address["conversation"] == session, fragment
        listed.append((address["partition"], address["offset"]))
    assembled = [
        (message.id.partition_id, message.id.offset)
        for message in world.assembled_context.fragments
    ]
    earlier = [
        (turn.message.id.partition_id, turn.message.id.offset) for turn in turns[: len(assembled)]
    ]
    assert listed == assembled
    assert listed == earlier


@given(parsers.parse("sessions publish heartbeats every {millis:d} milliseconds"))
def heartbeat_every(world, millis):
    world.session_config = {"heartbeat_ms": millis}


@when(parsers.parse('agent "{owner}" opens the session "{label}"'))
def open_lens(world, owner, label):
    sessions = _sessions(world)
    world.session = sessions.open(sessions.create(label).id()).as_agent(owner)


def _crockford(raw):
    if isinstance(raw, str):
        return raw
    value = int.from_bytes(bytes(raw), "big")
    return "".join(CROCKFORD[(value >> (5 * (25 - index))) & 31] for index in range(26))


def _heartbeats(world, stream):
    # Every heartbeat on `stream`, in log order. Python has no typed heartbeat
    # decoder, so the envelope and its body go through the generic CBOR codec.
    cursor = world.laser.stream(stream).topic(ls.AgentTopic.Heartbeats).replay()
    beats = []
    while True:
        batch = world.run(lambda: cursor.poll())
        if not batch:
            return beats
        for message in batch:
            envelope = ls.Cbor.decode(bytes(message.payload))
            beat = ls.Cbor.decode(bytes(envelope["body"]))
            beat["sessions"] = {_crockford(session) for session in beat["sessions"]}
            beats.append(beat)


@then(parsers.re(r"no heartbeat is published within (?P<seconds>\d+) seconds?$"))
def no_heartbeat(world, seconds):
    try:
        _sleep(world, int(seconds))
        beats = _heartbeats(world, world.laser.default_stream)
        assert beats == [], beats
    finally:
        world.run(lambda: world.worker.shutdown())


def _start_leased(world, sessions, owner, label):
    session, lease = world.run(lambda: sessions.create(label).agent(owner).begin())
    if not hasattr(world, "leases"):
        world.leases = {}
        world.labels = {}
    world.labels[label] = (sessions.stream, session.conversation)
    world.leases[label] = lease


@when(parsers.parse('agent "{owner}" starts the sessions "{first}" and "{second}"'))
def start_two(world, owner, first, second):
    for label in (first, second):
        _start_leased(world, _sessions(world), owner, label)


@then(parsers.re(r"a heartbeat lists the sessions (?P<listing>\".+\")$"))
def heartbeat_lists(world, listing):
    expected = {world.labels[label][1] for label in _names(listing)}
    stream = world.laser.default_stream

    def read():
        beats = _heartbeats(world, stream)
        return (
            True
            if any(beat["stream"] == stream and beat["sessions"] == expected for beat in beats)
            else None
        )

    _eventually(world, read, "a heartbeat listing the sessions")


@when(parsers.parse('the lease on the session "{label}" is released'))
def release_lease(world, label):
    world.leases.pop(label).release()


@then("the heartbeats stop")
def heartbeats_stop(world):
    stream = world.laser.default_stream
    # A beat already in flight when the last lease dropped may still land.
    _sleep(world, 0.5)
    settled = len(_heartbeats(world, stream))
    assert settled > 0, "no heartbeat was ever published"
    _sleep(world, 1)
    assert len(_heartbeats(world, stream)) == settled


@given("a second stream bootstrapped for sessions on the same connection")
def second_stream(world):
    world.second_stream = f"{world.laser.default_stream}-second"
    config = dict(getattr(world, "session_config", None) or {})
    sessions = world.laser.sessions(stream=world.second_stream, register_source=False, **config)
    world.run(lambda: sessions.bootstrap(4, ls.TopicRetention.expire_after(86_400_000)))


@when(
    parsers.parse(
        'agent "{owner}" starts the session "{here}" on this stream '
        'and the session "{there}" on the second stream'
    )
)
def start_on_two_streams(world, owner, here, there):
    _start_leased(world, _sessions(world), owner, here)
    config = dict(getattr(world, "session_config", None) or {})
    _start_leased(world, world.laser.sessions(stream=world.second_stream, **config), owner, there)


@then("the heartbeats of each stream list only that stream's session")
def heartbeats_per_stream(world):
    streams = dict(world.labels.values())
    assert len(streams) == 2, "one session on each of two streams"
    try:
        # The later session's stream beats only while both leases are held.
        _eventually(
            world,
            lambda: (
                True if all(len(_heartbeats(world, stream)) >= 2 for stream in streams) else None
            ),
            "heartbeats on both streams",
        )
        for stream, session in streams.items():
            for beat in _heartbeats(world, stream):
                assert beat["stream"] == stream, beat
                assert beat["sessions"] == {session}, beat
    finally:
        for lease in world.leases.values():
            lease.release()
        world.leases.clear()


@when("the policy refuses the next state snapshot publish")
def refuse_snapshot(world):
    world.refusals.snapshots = 1


@when("I write a state snapshot")
def captured_snapshot(world):
    try:
        world.run(lambda: world.session.state().snapshot())
        world.error = None
    except ls.LaserError as error:
        world.error = error
