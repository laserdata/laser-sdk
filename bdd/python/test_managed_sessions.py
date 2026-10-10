import asyncio
import json
import subprocess
import time
import uuid
from pathlib import Path

import laser_sdk as ls
from pytest_bdd import given, parsers, scenarios, then, when

SCENARIOS = Path(__file__).parent.parent / "scenarios"
scenarios(str(SCENARIOS / "managed_sessions.feature"))

# Folds run on their own schedule, so every index read waits for its answer.
CONVERGE_SECS = 30
ONE_DAY_MS = 86_400_000


class Managed:
    """The streams, sessions, and reads of one managed-session scenario."""

    def __init__(self):
        self.laser = None
        self.sessions = None
        self.layout = "shared"
        self.second_laser = None
        self.second = None
        self.second_session = None
        self.labels = {}
        self.children = []
        self.workflow_run = None
        self.linked_error = None
        self.remembered = None
        self.worker = None
        self.lease = None
        self.watch = None
        self.handled = []
        self.retained_source = None


def _managed(world):
    if not hasattr(world, "managed"):
        world.managed = Managed()
    return world.managed


def _eventually(world, read, description, show=lambda: ""):
    # Waiting runs the loop, so an agent spawned on it keeps handling records.
    deadline = time.monotonic() + CONVERGE_SECS
    while time.monotonic() < deadline:
        try:
            value = read()
        except ls.LaserError:
            value = None
        if value is not None:
            return value
        world.run(lambda: asyncio.sleep(0.1))
    raise AssertionError(f"{description} did not converge within {CONVERGE_SECS}s: {show()}")


def _index(world, read):
    try:
        return world.run(read)
    except ls.LaserError:
        return None


def _stream(world, layout=None, heartbeat_ms=None):
    stream = f"bdd-{uuid.uuid4().hex[:10]}"
    laser = world.run(lambda: world._connect(stream))
    options = {}
    if layout is not None:
        options["layout"] = layout
    if heartbeat_ms is not None:
        options["heartbeat_ms"] = heartbeat_ms
    sessions = laser.sessions(**options)
    registered = world.run(
        lambda: sessions.bootstrap(4, ls.TopicRetention.expire_after(ONE_DAY_MS))
    )
    assert registered, "a deployment that serves sessions registers the stream"
    return laser, sessions


LAYOUTS = {
    "shared": lambda: ls.SessionLayout.Shared(),
    "per-agent topic": lambda: ls.SessionLayout.PerAgentTopic(
        topics={"planner": "planner.inbox", "worker": "worker.inbox"}
    ),
    "declared partitions": lambda: ls.SessionLayout.PerAgentPartition(
        partitions={"planner": 0, "worker": 2}
    ),
    "single partition": lambda: ls.SessionLayout.SinglePartition(),
}


@given(parsers.re(r"a managed session stream in the (?P<layout>.+) layout$"))
def managed_layout(world, layout):
    managed = _managed(world)
    managed.laser, managed.sessions = _stream(world, layout=LAYOUTS[layout]())
    managed.layout = layout


@given(
    parsers.re(r"a managed session stream whose agents heartbeat every (?P<seconds>\d+) seconds?$")
)
def managed_heartbeat(world, seconds):
    managed = _managed(world)
    managed.laser, managed.sessions = _stream(world, heartbeat_ms=int(seconds) * 1000)


@given("a second managed session stream")
def second_stream(world):
    managed = _managed(world)
    managed.second_laser, managed.second = _stream(world)


def _begin(world, sessions, owner, label, idle_timeout=None):
    builder = sessions.create(label).agent(owner)
    if idle_timeout is not None:
        builder = builder.idle_timeout(idle_timeout)
    return world.run(lambda: builder.begin())


@when(
    parsers.parse(
        'agent "{owner}" runs the session "{label}" with one command answered by "{worker}"'
    )
)
def run_exchange(world, owner, label, worker):
    managed = _managed(world)
    session, lease = _begin(world, managed.sessions, owner, label)
    # Every layout sends on the lane. A per-agent topic layout moves the
    # addressed command and reply onto the declared topics.
    work, reply = ls.AgentTopic.Sessions, ls.AgentTopic.Sessions
    conversation = session.conversation
    correlation = ls.new_conversation_id()
    laser = managed.laser
    world.run(
        lambda: laser.agdx(work, owner, conversation).command(correlation, b"{}", target=worker)
    )
    world.run(
        lambda: laser.agdx(reply, worker, conversation).respond(correlation, b"{}", target=owner)
    )
    world.run(lambda: session.end())
    lease.release()
    managed.labels[label] = conversation


def _get(world, sessions, session_id):
    return _index(world, lambda: sessions.get(session_id))


@then(
    parsers.parse('the indexed session "{label}" is completed by "{owner}" with {events:d} events')
)
def indexed_completed(world, label, owner, events):
    managed = _managed(world)
    session_id = managed.labels[label]

    def read():
        info = _get(world, managed.sessions, session_id)
        done = info is not None and info["status"] == "completed" and info["events"] >= events
        return info if done else None

    info = _eventually(world, read, "the completed session row")
    assert info["events"] == events, info
    assert info["label"] == label
    assert info["agent"] == owner
    assert not info.get("flags", {}).get("lane_conflict", False), info


@then(parsers.re(r"the stream counts (?P<count>\d+) completed sessions?$"))
def counts_completed(world, count):
    managed = _managed(world)

    def read():
        page = _index(world, lambda: managed.sessions.list(status="completed", total=True))
        return page if page is not None and page.get("total") == int(count) else None

    page = _eventually(world, read, "the completed count")
    assert len(page["items"]) == int(count)


@given(parsers.parse('agent "{name}" answers every command it receives'))
def answering_worker(world, name):
    managed = _managed(world)

    async def handle(ctx, message):
        envelope = message.envelope
        if envelope is None or envelope.get("correlation") is None:
            await ctx.respond(b'{"ok":true}')
            return
        # A typed request gets a typed, correlated response addressed to its
        # requester, which is what `AgentCtx::respond` writes in Rust.
        await managed.laser.agdx(ls.AgentTopic.Sessions, name, envelope["conversation"]).respond(
            envelope["correlation"],
            b'{"ok":true}',
            target=envelope["source"],
            parent=envelope.get("parent"),
            root=envelope.get("root"),
        )

    async def spawn():
        worker = managed.laser.spawn_agent(
            name, ls.AgentTopic.Sessions, handle, respond_on=ls.AgentTopic.Sessions
        )
        await worker.ready()
        return worker

    managed.worker = world.run(spawn)


@when(parsers.parse('agent "{owner}" starts the session "{label}"'))
def start_session(world, owner, label):
    managed = _managed(world)
    session, lease = _begin(world, managed.sessions, owner, label)
    managed.labels[label] = session.conversation
    managed.lease = lease
    world.session = session


@when(parsers.parse('agent "{owner}" starts the session "{label}" on the second stream'))
def start_on_second(world, owner, label):
    managed = _managed(world)
    session, lease = _begin(world, managed.second, owner, label)
    managed.second_session = session.conversation
    world.run(lambda: session.end())
    lease.release()


@when(
    parsers.parse(
        'agent "{owner}" starts the session "{label}" with an idle timeout of {seconds:d} seconds'
    )
)
def start_with_idle(world, owner, label, seconds):
    managed = _managed(world)
    session, lease = _begin(world, managed.sessions, owner, label, idle_timeout=seconds * 1000)
    managed.labels[label] = session.conversation
    managed.lease = lease
    world.session = session


@when("the session ends")
def end_session(world):
    world.run(lambda: world.session.end())


@when(parsers.parse('the session fans out one branch to "{target}"'))
def fan_out_branch(world, target):
    managed = _managed(world)
    author = world.session.agent
    parent = ls.Provenance(conversation_id=world.session.conversation, agent=author)
    # One fan-out branch: a subconversation of the session addressed to its
    # target, the request the gather awaits.
    spawned = managed.laser.spawn_subconversation(parent, author)
    branch = ls.Provenance(
        conversation_id=spawned.conversation_id,
        parent_conversation_id=spawned.parent_conversation_id,
        root_conversation_id=spawned.root_conversation_id,
        agent=author,
        target_agent_id=target,
        correlation_id=f"branch-{spawned.conversation_id}",
    )
    world.run(
        lambda: managed.laser.request(
            ls.AgentTopic.Sessions, ls.AgentTopic.Sessions, b"branch", branch, timeout_ms=20_000
        )
    )
    managed.children.append(branch.conversation_id)


@when("the session submits an A2A task as a child")
def a2a_child(world):
    managed = _managed(world)
    session = world.session.conversation
    bridge = ls.A2aBridge(
        managed.laser, "a2a-gateway", ls.AgentTopic.Sessions, ls.AgentTopic.Sessions
    )
    task = world.run(
        lambda: bridge.submit_in(session, {"message": {"role": "user", "text": "look"}})
    )
    managed.children.append(task["id"])


@given(parsers.parse('agent "{name}" has a bound addressee filter'))
def bound_addressee(world, name):
    managed = _managed(world)
    assert world.run(lambda: managed.laser.capabilities()).filters.group_policy_reads
    group = managed.laser.topic(ls.AgentTopic.Sessions).consumer_group(name)
    assert world.run(lambda: group.filter().get()) is not None


@when(
    parsers.parse('agent "{owner}" sends untargeted low-altitude work with send_agent and request')
)
def untargeted_work(world, owner):
    managed = _managed(world)
    provenance = ls.Provenance(conversation_id=world.session.conversation, agent=owner)
    assert provenance.target_agent_id is None
    world.run(lambda: managed.laser.send_agent(ls.AgentTopic.Sessions, b"send", provenance))
    reply = world.run(
        lambda: managed.laser.request(
            ls.AgentTopic.Sessions,
            ls.AgentTopic.Sessions,
            b"request",
            provenance,
            timeout_ms=20_000,
        )
    )
    assert reply.payload == b"{}"


@then(parsers.parse('agent "{name}" handles both untargeted records for the session "{label}"'))
def untargeted_delivered(world, name, label):
    managed = _managed(world)
    _eventually(
        world,
        lambda: True if len(managed.handled) >= 2 else None,
        "both untargeted records are handled",
    )
    assert managed.handled == [managed.labels[label], managed.labels[label]]


@when(parsers.parse('the session calls the MCP tool "{tool}" as a child'))
def mcp_child(world, tool):
    managed = _managed(world)
    session = world.session.conversation
    bridge = ls.McpBridge(
        managed.laser,
        "mcp-gateway",
        ls.AgentTopic.Sessions,
        ls.AgentTopic.Sessions,
        "bdd",
        timeout_ms=20_000,
    )
    world.run(lambda: bridge.call_tool_in(session, tool, {"q": "auth"}))


def _tree(world, sessions, root):
    page = _index(world, lambda: sessions.list(root=root))
    if page is None:
        return []
    return [item for item in page["items"] if item["id"] != root]


@then(parsers.re(r'the session tree of "(?P<label>[^"]+)" holds (?P<count>\d+) child(?:ren)?$'))
def tree_holds(world, label, count):
    managed = _managed(world)
    root = managed.labels[label]

    def read():
        children = _tree(world, managed.sessions, root)
        return children if len(children) >= int(count) else None

    def show():
        return f"tree {_tree(world, managed.sessions, root)}, stream " + str(
            _index(world, lambda: managed.sessions.list())
        )

    children = _eventually(world, read, "the session tree", show)
    assert len(children) == int(count), children
    indexed = [child["id"] for child in children]
    for child in managed.children:
        assert child in indexed, f"child {child} is in the tree"


@then(parsers.parse('every child names "{label}" as its parent and root'))
def children_name_root(world, label):
    managed = _managed(world)
    root = managed.labels[label]
    for child in _tree(world, managed.sessions, root):
        assert child.get("parent") == root
        assert child.get("root") == root


def _run_workflow(world, laser, name, worker, run=None):
    async def run_it():
        workflow = laser.workflow(name, fixed_inbox=ls.AgentTopic.Sessions)
        if run is not None:
            workflow.run_id(run)
        workflow.step("check", build=lambda _outputs: b"incident", to=worker)
        return await workflow.run()

    return world.run(run_it).run_id


@when(parsers.parse('the workflow "{name}" runs its one step on "{worker}"'))
def workflow_runs(world, name, worker):
    managed = _managed(world)
    managed.workflow_run = _run_workflow(world, managed.laser, name, worker)


@when(parsers.parse('the workflow "{name}" resumes the same run'))
def workflow_resumes(world, name):
    managed = _managed(world)
    _run_workflow(world, managed.laser, name, "worker", managed.workflow_run)


@then(parsers.re(r"the session tree of the workflow run holds (?P<count>\d+) child(?:ren)?$"))
def workflow_tree(world, count):
    managed = _managed(world)
    run = managed.workflow_run

    def read():
        children = _tree(world, managed.sessions, run)
        return children if len(children) >= int(count) else None

    _eventually(world, read, "the workflow tree")

    def finished():
        info = _get(world, managed.sessions, run)
        terminal = info is not None and info["status"] in ("completed", "failed", "canceled")
        return info if terminal else None

    info = _eventually(world, finished, "the workflow run row")
    assert info["status"] == "completed", info
    children = _tree(world, managed.sessions, run)
    assert len(children) == int(count), children


@when(parsers.parse('the session remembers "{text}" through its linked memory'))
def remember_linked(world, text):
    memory = world.session.linked_memory()
    world.run(lambda: memory.remember(text))


@when("the session records that it recalled the remembered item")
def record_recall(world):
    managed = _managed(world)
    memory = world.session.memory()

    def read():
        items = world.run(lambda: memory.recall(strategy="recent", folded=True))
        return items or None

    items = _eventually(world, read, "the remembered item")
    world.run(lambda: world.session.record_retrieval("deploy key", items[:1]))
    managed.remembered = items[0]


@then("the session links the remembered item as written and recalled")
def memory_links(world):
    managed = _managed(world)
    session_id = world.session.conversation
    item = str(managed.remembered.id)

    def read():
        view = _index(world, lambda: managed.sessions.links(session_id, "memory"))
        if view is None:
            return None
        relations = {link["relation"] for link in view["links"] if link["item"] == item}
        return view if {"wrote", "recalled"} <= relations else None

    view = _eventually(world, read, "the memory links")
    assert all(link["surface"] == "memory" for link in view["links"])


@when(parsers.re(r'the session state sets "(?P<key>[^"]+)" to (?P<value>.+)$'))
def state_set(world, key, value):
    state = world.session.state()
    world.run(lambda: state.set(key, json.loads(value)))


@when(parsers.re(r"the session state is replaced by (?P<document>.+)$"))
def state_replace(world, document):
    state = world.session.state()
    world.run(lambda: state.replace(json.loads(document)))


@when("the session writes a state snapshot")
def state_snapshot(world):
    state = world.session.state()
    world.run(lambda: state.snapshot())


@then(parsers.re(r"the indexed state of the session is (?P<document>.+)$"))
def indexed_state(world, document):
    managed = _managed(world)
    expected = json.loads(document)
    session_id = world.session.conversation

    def read():
        view = _index(world, lambda: managed.sessions.state(session_id))
        return view if view is not None and view["document"] == expected else None

    view = _eventually(world, read, "the indexed state")
    assert view["complete"] is True, view


@then(
    parsers.re(
        r"the indexed state history chains its digests over (?P<deltas>\d+) deltas "
        r"and the snapshot$"
    )
)
def state_history(world, deltas):
    managed = _managed(world)
    deltas = int(deltas)
    session_id = world.session.conversation

    # The snapshot folds after the document already matched, so wait for it.
    def read():
        view = _index(world, lambda: managed.sessions.state(session_id))
        return view if view is not None and len(view["history"]) == deltas + 1 else None

    view = _eventually(world, read, "the indexed state history")
    history = view["history"]
    for change in history:
        assert change["outcome"] == "applied", change
        assert change.get("old_digest") is not None and change.get("new_digest") is not None, change
    for earlier, later in zip(history, history[1:], strict=False):
        assert later.get("old_digest") == earlier.get("new_digest"), (earlier, later)
    for index, delta in enumerate(history[:deltas]):
        assert delta.get("op_id") is not None, delta
        assert delta["revision"] == index + 1, delta
        assert delta.get("old_digest") != delta.get("new_digest"), delta
    # A snapshot of the document the deltas built keeps the revision and the
    # digest.
    snapshot = history[deltas]
    assert snapshot.get("op_id") is None, snapshot
    assert snapshot["revision"] == deltas, snapshot
    assert snapshot.get("old_digest") == snapshot.get("new_digest"), snapshot


@then(parsers.parse('the indexed session "{label}" is active'))
def indexed_active(world, label):
    managed = _managed(world)
    session_id = managed.labels[label]

    def read():
        info = _get(world, managed.sessions, session_id)
        return info if info is not None and info["status"] == "active" else None

    _eventually(world, read, "the active session row")


@when(parsers.parse('the session links "{source}" to "{target}" in graph "{graph}"'))
def graph_link(world, source, target, graph):
    handle = world.session.linked_graph(graph)
    world.run(lambda: handle.link(source, "depends_on", target))


def _graph_nodes(world, sessions, session_id):
    view = _index(world, lambda: sessions.links(session_id, "graph_node"))
    return {link["item"] for link in view["links"]} if view is not None else set()


@then(parsers.parse('the sessions "{first}" and "{second}" both link the graph node "{node}"'))
def both_link_node(world, first, second, node):
    managed = _managed(world)

    def read():
        firsts = _graph_nodes(world, managed.sessions, managed.labels[first])
        seconds = _graph_nodes(world, managed.sessions, managed.labels[second])
        ready = len(firsts) == 2 and len(seconds) == 2
        return (firsts, seconds) if ready else None

    firsts, seconds = _eventually(world, read, "the graph node links")
    # Each session linked its own pair, and only the re-observed node is shared.
    assert len(firsts & seconds) == 1, (firsts, seconds, node)


@when(parsers.parse('the session sets key "{key}" to "{value}" in namespace "{namespace}"'))
def session_kv_set(world, key, value, namespace):
    kv = world.session.kv(namespace)
    world.run(lambda: kv.set(key).bytes(value).send())


@then(parsers.parse('the session links key "{key}" in namespace "{namespace}"'))
def kv_link(world, key, namespace):
    managed = _managed(world)
    session_id = world.session.conversation

    def read():
        view = _index(world, lambda: managed.sessions.links(session_id, "kv"))
        if view is None:
            return None
        found = any(
            link["item"] == key and link["resource"].endswith(namespace) for link in view["links"]
        )
        return view if found else None

    _eventually(world, read, "the key-value link")


@then(parsers.parse('the key-value lens of the session in namespace "{namespace}" lists "{key}"'))
def kv_lens(world, namespace, key):
    managed = _managed(world)
    session_id = world.session.conversation
    scan = managed.laser.kv(namespace).scan().conversation(session_id)

    def read():
        entries = _index(world, lambda: scan.entries())
        return entries or None

    entries = _eventually(world, read, "the key-value lens")
    assert [bytes(entry.key).decode() for entry in entries] == [key]


@when(
    parsers.parse(
        'I set key "{key}" in namespace "{namespace}" linked to the second stream\'s session'
    )
)
def forged_write(world, key, namespace):
    managed = _managed(world)
    victim = managed.second.open(managed.second_session).reference()
    kv = managed.laser.kv(namespace).in_session(victim)
    try:
        world.run(lambda: kv.set(key).bytes("stolen").send())
        managed.linked_error = None
    except ls.LaserError as error:
        managed.linked_error = error


@then("the linked write is refused")
def write_refused(world):
    assert _managed(world).linked_error is not None, (
        "a write linked to another stream's session is refused"
    )


@then("the second stream's session links no key-value item")
def victim_unlinked(world):
    managed = _managed(world)
    session_id = managed.second_session
    _eventually(world, lambda: _get(world, managed.second, session_id), "the second session row")
    view = world.run(lambda: managed.second.links(session_id, "kv"))
    assert view["links"] == [], view


@when(parsers.parse("the session stays quiet for {seconds:d} seconds"))
def stay_quiet(world, seconds):
    world.run(lambda: asyncio.sleep(seconds))


@then("the indexed session is live with a recent heartbeat")
def live_with_heartbeat(world):
    managed = _managed(world)
    session_id = world.session.conversation

    def read():
        info = _get(world, managed.sessions, session_id)
        live = (
            info is not None
            and not info.get("flags", {}).get("liveness_unknown", False)
            and info.get("last_heartbeat_at") is not None
        )
        return info if live else None

    info = _eventually(world, read, "the heartbeat liveness")
    assert info["status"] == "active", info
    assert not info["idle"], info
    assert info["last_heartbeat_at"] > info["last_event_at"], info


@then("no indexed event of the session is a heartbeat")
def no_heartbeat_events(world):
    managed = _managed(world)
    page = world.run(lambda: managed.sessions.events(world.session.conversation))
    assert page["items"]
    for event in page["items"]:
        assert "heartbeat" not in event["display"] and "heartbeat" not in event["kind"], event


@when("the session lease is released")
def release_lease(world):
    managed = _managed(world)
    managed.lease.release()
    managed.lease = None


@then("the indexed session becomes idle")
def becomes_idle(world):
    managed = _managed(world)
    session_id = world.session.conversation

    def read():
        info = _get(world, managed.sessions, session_id)
        return info if info is not None and info["idle"] else None

    info = _eventually(world, read, "the idle session")
    assert info["status"] == "active", "idle never overwrites a status"


@when(parsers.parse("the session streams {chunks:d} chunks in one batch"))
def stream_chunks(world, chunks):
    managed = _managed(world)
    session = world.session
    stream = (
        managed.laser.agdx(ls.AgentTopic.Sessions, session.agent, session.conversation)
        .stream(ls.new_conversation_id(), "chat")
        .buffered(chunks, 60_000)
    )
    for index in range(chunks):
        world.run(lambda index=index: stream.write(f"chunk-{index}"))
    world.run(lambda: stream.flush())


@then(parsers.parse("the index counts {records:d} records for the session"))
def counts_records(world, records):
    managed = _managed(world)
    session_id = world.session.conversation

    def read():
        info = _get(world, managed.sessions, session_id)
        return info if info is not None and info["events"] >= records else None

    info = _eventually(world, read, "the session counters")
    assert info["events"] == records, info


@then(parsers.parse("the change feed names the session in at most {rows:d} rows"))
def feed_rows(world, rows):
    managed = _managed(world)
    session_id = world.session.conversation
    changes = world.run(lambda: managed.sessions.changes(0))
    naming = [row for row in changes["rows"] if session_id in row["sessions"]]
    assert naming, changes
    assert len(naming) <= rows, f"one row per fold batch, not per record: {changes}"


@then("the change feed never names the second stream's session")
def feed_isolated(world):
    managed = _managed(world)
    other = managed.second_session

    def read():
        changes = _index(world, lambda: managed.second.changes(0))
        named = changes is not None and any(other in row["sessions"] for row in changes["rows"])
        return changes if named else None

    # The second stream's own feed names it, so both folds have run.
    _eventually(world, read, "the second stream's feed")
    changes = world.run(lambda: managed.sessions.changes(0))
    assert all(other not in row["sessions"] for row in changes["rows"]), changes


@when("a watch follows the stream's session changes")
def watch_changes(world):
    managed = _managed(world)
    watch = world.run(lambda: managed.sessions.watch(250))

    async def follow():
        return await watch.next()

    # A follower awaits its next change from the start, as a console would.
    managed.watch = world.loop.create_task(follow())


@when(parsers.parse('the session records a call of tool "{tool}" whose arguments carry a token'))
def record_tool(world, tool):
    call = world.run(lambda: world.session.tool(tool, {"id": 7, "token": "t-secret"}))
    world.run(lambda: call.complete(b"found"))


@then(parsers.parse('listing the stream finds "{label}" as completed'))
def listing_finds(world, label):
    managed = _managed(world)
    session_id = managed.labels[label]

    def read():
        page = _index(world, lambda: managed.sessions.list())
        found = page is not None and any(
            item["id"] == session_id and item["status"] == "completed" for item in page["items"]
        )
        return page if found else None

    page = _eventually(world, read, "the listed session")
    assert len(page["items"]) == 1, page


@then(parsers.re(r"the indexed events of the session are (?P<listing>\".+\")$"))
def indexed_events(world, listing):
    managed = _managed(world)
    expected = [name.strip('"') for name in listing.split(", ")]
    session_id = world.session.conversation

    def read():
        page = _index(world, lambda: managed.sessions.events(session_id))
        if page is None:
            return None
        displays = [event["display"] for event in page["items"]]
        return displays if len(displays) >= len(expected) else None

    assert _eventually(world, read, "the indexed events") == expected


@then("the session sources show folded offsets within their heads")
def sources_cover(world):
    managed = _managed(world)
    sources = world.run(lambda: managed.sessions.sources(world.session.conversation))
    assert any(source.get("folded") is not None for source in sources["sources"]), sources
    for source in sources["sources"]:
        if source.get("folded") is not None and source.get("head") is not None:
            assert source.get("folded") <= source.get("head"), source


@then("the session has no links")
def no_links(world):
    managed = _managed(world)
    view = world.run(lambda: managed.sessions.links(world.session.conversation))
    assert view["links"] == [], view


@then("the watch reports the session")
def watch_reports(world):
    managed = _managed(world)
    task = managed.watch

    async def wait():
        return await asyncio.wait_for(task, CONVERGE_SECS)

    change = world.run(wait)
    assert isinstance(change, ls.SessionChange.Changed), change
    assert world.session.conversation in change.sessions, change


@given(parsers.parse('agent "{name}" counts the work it handles'))
def counting_worker(world, name):
    managed = _managed(world)

    async def handle(ctx, message):
        managed.handled.append(message.provenance.conversation_id)
        await ctx.respond(b"{}")

    async def spawn():
        worker = managed.laser.spawn_agent(
            name, ls.AgentTopic.Sessions, handle, respond_on=ls.AgentTopic.Sessions
        )
        await worker.ready()
        return worker

    managed.worker = world.run(spawn)


@when(
    parsers.parse('agent "{owner}" starts the session "{label}" with a budget of {tokens:d} tokens')
)
def start_with_budget(world, owner, label, tokens):
    managed = _managed(world)
    builder = managed.sessions.create(label).agent(owner).budget(ls.Budget(tokens=tokens))
    session, lease = world.run(lambda: builder.begin())
    managed.labels[label] = session.conversation
    managed.lease = lease
    world.session = session


@when(parsers.parse("the session records a model call that used {tokens:d} tokens"))
def record_spend(world, tokens):
    world.run(
        lambda: world.session.record_model_call(
            ls.ModelRequest("gpt-test", b"q"),
            ls.ModelResponse(b"a", usage={"input_tokens": tokens, "output_tokens": 0}),
        )
    )


@then(parsers.parse('the indexed session "{label}" is over its budget'))
def indexed_over_budget(world, label):
    managed = _managed(world)
    session_id = managed.labels[label]

    def read():
        info = _get(world, managed.sessions, session_id)
        return True if info is not None and info["over_budget"] else None

    _eventually(world, read, "the over-budget flag")
    assert world.run(lambda: managed.sessions.open(session_id).over_budget()) is True


@when(
    parsers.re(
        r'agent "(?P<owner>[^"]+)" sends the session (?P<count>\d+) work records? '
        r'for "(?P<target>[^"]+)"$'
    )
)
def send_work(world, owner, count, target):
    managed = _managed(world)
    conversation = world.session.conversation
    lane = managed.laser.agdx(ls.AgentTopic.Sessions, owner, conversation)
    for _ in range(int(count)):
        correlation = ls.new_conversation_id()
        world.run(lambda correlation=correlation: lane.command(correlation, b"{}", target=target))


@then(parsers.parse('agent "{name}" handles only the work of the session "{label}"'))
def handles_only(world, name, label):
    managed = _managed(world)
    session_id = managed.labels[label]

    def read():
        return list(managed.handled) if session_id in managed.handled else None

    seen = _eventually(world, read, "the handled work", show=lambda: str(managed.handled))
    assert all(session == session_id for session in seen), seen


@then(parsers.parse('the session "{label}" ends failed once with reason "{reason}"'))
def ends_failed_once(world, label, reason):
    managed = _managed(world)
    lens = managed.sessions.open(managed.labels[label])

    def read():
        turns = world.run(lambda: lens.context())
        ends = [
            ls.decode_session_end(bytes(turn.message.envelope["body"]))
            for turn in turns
            if turn.display == "session.failed"
        ]
        return ends or None

    ends = _eventually(world, read, "the failed terminal")
    assert len(ends) == 1, ends
    assert ends[0]["reason"] == reason
    assert "budget" in ends[0]["error"]["message"]


@when("I retain the session and state handles for lane identity checks")
def retain_lane_handles(world):
    managed = _managed(world)
    world.run(lambda: world.session.state().set("before", 1))
    managed.retained = world.session.as_agent("planner")
    managed.retained_state = world.session.state()
    if managed.lease is not None:
        managed.lease.release()
        managed.lease = None


@when(parsers.re(r"the session source changes its (?P<change>.+)$"))
def change_lane_source(world, change):
    managed = _managed(world)
    count = 1 if managed.layout == "single partition" else 4
    helper = SCENARIOS.parent / "session-lane-admin.mjs"
    result = subprocess.run(
        [
            "rtk",
            "proxy",
            "node",
            str(helper),
            world.endpoint,
            world.session.stream,
            change,
            str(count),
        ],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def _first_source_reference(world, sessions, session):
    def first_record():
        turns = world.run(lambda: session.context())
        return turns[0].message if turns else None

    first = _eventually(world, first_record, "the session's first source record")

    def lane_identity():
        sources = _index(world, lambda: sessions.sources(session.conversation))
        return sources.get("lane") if sources else None

    topic, generation, _ = _eventually(world, lane_identity, "the registered lane identity")
    assert topic == first.topic_id
    return {
        "Message": {
            "stream": first.stream_id,
            "topic": topic,
            "partition": first.id.partition_id,
            "offset": first.id.offset,
            "generation": generation,
        }
    }


@when("I retain a generation-bearing reference to its first record")
def retain_source_reference(world):
    managed = _managed(world)
    at = _first_source_reference(world, managed.sessions, world.session)
    assert world.run(lambda: managed.laser.read_at(at)) is not None
    managed.retained_source = at
    if managed.lease is not None:
        managed.lease.release()
        managed.lease = None


@when("a replacement session records its start at the retained source address")
def replacement_source_record(world):
    managed = _managed(world)
    replacement, lease = world.run(
        lambda: managed.second.start().with_id(world.session.conversation).agent("planner").begin()
    )
    lease.release()
    at = _first_source_reference(world, managed.second, replacement)
    original = managed.retained_source["Message"]
    current = at["Message"]
    for key in ("stream", "topic", "partition", "offset"):
        assert original[key] == current[key], "the recreated source reuses the record address"
    assert original["generation"] != current["generation"]
    assert world.run(lambda: managed.laser.read_at(at)) is not None


@then("reading the retained source reference returns no replacement record")
def refuse_replacement_source_record(world):
    managed = _managed(world)
    assert world.run(lambda: managed.laser.read_at(managed.retained_source)) is None


def _assert_stale_lane(world, write):
    try:
        world.run(write)
    except ls.SessionError as error:
        assert "stale session generation" in str(error), str(error)
    else:
        raise AssertionError("the SDK must refuse the changed lane before appending")


@then("retained session handles refuse lifecycle and state writes as stale")
def refuse_retained_lane(world):
    managed = _managed(world)
    before = len(world.run(lambda: world.session.context()))
    _assert_stale_lane(world, lambda: world.session.cancel())
    _assert_stale_lane(world, lambda: managed.retained.cancel())
    _assert_stale_lane(world, lambda: managed.retained_state.set("after", 2))
    _assert_stale_lane(world, lambda: managed.retained_state.snapshot())
    envelope = ls.event_envelope(
        ls.new_conversation_id(), world.session.conversation, "planner", b"{}"
    )
    _assert_stale_lane(world, lambda: world.session.append(envelope))
    _assert_stale_lane(world, lambda: managed.sessions.start().agent("planner").begin())
    _assert_stale_lane(
        world, lambda: managed.sessions.submit("worker", b"{}").from_("planner").send()
    )
    assert len(world.run(lambda: world.session.context())) == before


@then("a fresh handle refuses the stale lane registration")
def refuse_stale_registration(world):
    managed = _managed(world)
    before = len(world.run(lambda: world.session.context()))
    fresh = managed.laser.sessions().open(world.session.conversation).as_agent("planner")
    _assert_stale_lane(world, lambda: fresh.cancel())
    assert len(world.run(lambda: world.session.context())) == before


@when("the session source is explicitly removed and registered again")
def recover_lane_registration(world):
    managed = _managed(world)
    laser = managed.laser
    command = ls.Cbor.encode(
        {
            "v": 1,
            "timestamp_micros": time.time_ns() // 1000,
            "command": {"RemoveSessionSource": {"stream": world.session.stream}},
        }
    )
    world.run(
        lambda: (
            laser.stream(laser.ops_stream)
            .topic(laser.control_topic)
            .send(command, partition_key="control")
        )
    )

    def removed():
        try:
            world.run(lambda: laser.sessions().sources(world.session.conversation))
        except ls.SessionError as error:
            if "stream is not registered for sessions" in str(error):
                return True
        return None

    _eventually(world, removed, "source registration removal")
    managed.second = laser.sessions(layout=LAYOUTS[managed.layout]())
    assert world.run(
        lambda: managed.second.bootstrap(4, ls.TopicRetention.expire_after(ONE_DAY_MS))
    )


@then("a fresh session can write lifecycle and state on the recovered lane")
def recovered_lane_writes(world):
    sessions = _managed(world).second
    session, lease = world.run(lambda: sessions.start().agent("planner").begin())
    world.run(lambda: session.state().set("after", 2))
    world.run(lambda: session.end())
    lease.release()

    def state_applied():
        view = _index(world, lambda: sessions.state(session.conversation, history_limit=10))
        return view if view and view["document"] == {"after": 2} else None

    _eventually(world, state_applied, "the recovered session state")

    def completed():
        info = _index(world, lambda: sessions.get(session.conversation))
        return info if info and info["status"] == "completed" else None

    info = _eventually(world, completed, "the recovered session completion")
    assert not info.get("flags", {}).get("lane_conflict", False)


@then("retained session handles still refuse the recovered lane")
def old_lane_stays_stale(world):
    managed = _managed(world)
    _assert_stale_lane(world, lambda: managed.retained.cancel())
    _assert_stale_lane(world, lambda: managed.retained_state.set("after", 3))


@then("the old lease publishes no heartbeat into the recreated stream")
def old_generation_never_heartbeats(world):
    managed = _managed(world)
    world.run(lambda: asyncio.sleep(3))
    cursor = managed.laser.topic(ls.AgentTopic.Heartbeats).replay()
    assert not world.run(lambda: cursor.poll()), "an old lease must not heartbeat into a new stream"
    managed.lease.release()
    managed.lease = None


@when("a native lane writer without session read grants writes lifecycle and state")
def native_lane_writer(world):
    managed = _managed(world)
    stream = managed.laser.default_stream
    username = f"writer-{stream}"
    result = subprocess.run(
        [
            "rtk",
            "proxy",
            "node",
            str(SCENARIOS.parent / "session-lane-admin.mjs"),
            world.endpoint,
            stream,
            "writer credentials",
            "4",
            username,
        ],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    address = world.endpoint.rsplit("@", 1)[1]
    writer = world.run(
        lambda: ls.Laser.connect(f"{username}:lane-writer-test@{address}", stream=stream)
    )
    session, lease = world.run(lambda: writer.sessions().create("writer").agent("planner").begin())
    world.run(lambda: session.state().set("step", 1))
    world.run(lambda: session.end())
    lease.release()
    managed.labels["writer"] = session.conversation
    managed.writer = writer


@then("aggregate session reads remain refused for that writer")
def native_writer_cannot_read_aggregate(world):
    managed = _managed(world)
    writer = managed.writer
    session_id = managed.labels["writer"]
    for read in (
        lambda: writer.sessions().sources(session_id),
        lambda: writer.sessions().get(session_id),
    ):
        try:
            world.run(read)
        except ls.LaserError as error:
            assert error.permission_denied, str(error)
        else:
            raise AssertionError("aggregate reads require session read authority")
    world.run(lambda: writer.close())
    managed.writer = None
