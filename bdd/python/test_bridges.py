import asyncio
from pathlib import Path

import laser_sdk as ls
from pytest_bdd import given, parsers, scenarios, then, when

SCENARIOS = Path(__file__).parent.parent / "scenarios"
scenarios(str(SCENARIOS / "bridges.feature"))


@when(parsers.parse('bridge "{bridge}" enters after hops "{hops}"'))
def bridge_enters_after(world, bridge, hops):
    world.bridge_hops = ls.enter_bridge(bridge, hops.split(","))


@then(parsers.parse('the bridge hops are "{hops}"'))
def bridge_hops_are(world, hops):
    assert world.bridge_hops == hops.split(",")


@when(parsers.parse('bridge "{bridge}" enters the same route'))
def bridge_enters_same_route(world, bridge):
    try:
        ls.enter_bridge(bridge, world.bridge_hops)
        world.bridge_loop_rejected = False
    except ls.LaserError:
        world.bridge_loop_rejected = True


@then("the bridge route is rejected as a loop")
def bridge_route_rejected(world):
    assert world.bridge_loop_rejected


@when("I submit and cancel an A2A task")
def submit_and_cancel_a2a(world):
    bridge = ls.A2aBridge(
        world.laser, "a2a-gateway", ls.AgentTopic.Sessions, ls.AgentTopic.Sessions
    )
    task = world.run(lambda: bridge.submit({"message": {"role": "user", "text": "cancel me"}}))
    canceled = world.run(lambda: bridge.cancel(task["id"]))
    replayed = canceled
    for _ in range(80):
        replayed = world.run(lambda: bridge.task(task["id"]))
        if replayed["status"]["state"].casefold() == "canceled":
            world.bridge_task_state = "Canceled"
            return
        world.run(lambda: asyncio.sleep(0.025))
    raise AssertionError(f"canceled task did not replay: {replayed!r}")


@then(parsers.parse('the replayed A2A task state is "{state}"'))
def replayed_a2a_state(world, state):
    assert world.bridge_task_state == state


@when("I publish an AG-UI count snapshot of 1 and replace it with 2")
def publish_state(world):
    world.run(
        lambda: world.laser.publish_state_snapshot("agui-gateway", world.conversation, {"count": 1})
    )
    world.run(
        lambda: world.laser.publish_state_delta(
            "agui-gateway",
            world.conversation,
            [{"op": "replace", "path": "/count", "value": 2}],
        )
    )
    for _ in range(80):
        state = world.run(lambda: world.laser.reconstruct_state(world.conversation))
        if state == {"count": 2}:
            world.reconstructed_state = state
            return
        world.run(lambda: asyncio.sleep(0.025))
    raise AssertionError("state delta did not become visible")


@then(parsers.parse("the reconstructed AG-UI count is {count:d}"))
def reconstructed_count(world, count):
    assert world.reconstructed_state == {"count": count}


@when(parsers.parse('I stream chat chunks "{first}" and "{second}"'))
def stream_chat(world, first, second):
    stream = world.laser.agdx(ls.AgentTopic.Sessions, "assistant", world.conversation).stream(
        ls.mint_ulid(), "chat"
    )
    world.run(lambda: stream.write(first.encode()))
    world.run(lambda: stream.write(second.encode()))
    world.run(lambda: stream.finish(finish_reason="stop"))
    for _ in range(80):
        events = world.run(
            lambda: world.laser.agui_events(world.conversation, ls.AgentTopic.Sessions)
        )
        if len(events) >= 4:
            world.agui_event_types = [event["type"] for event in events]
            return
        world.run(lambda: asyncio.sleep(0.025))
    raise AssertionError("chat events did not become visible")


@then("AG-UI renders the chat lifecycle in order")
def chat_lifecycle(world):
    assert world.agui_event_types == [
        "TEXT_MESSAGE_START",
        "TEXT_MESSAGE_CONTENT",
        "TEXT_MESSAGE_CONTENT",
        "TEXT_MESSAGE_END",
    ]


@given(
    parsers.parse('responders "{first}" and "{second}" answer every command with their own name')
)
def responders(world, first, second):
    world.responders = []
    world.answered = {}

    def responder(name, answered):
        async def handle(ctx, message):
            envelope = message.envelope
            if envelope is None or envelope["kind"] != "command":
                return
            if envelope.get("tool") is not None:
                answered.append("tool")
            elif envelope.get("operation") == "chat":
                answered.append("task")
            else:
                answered.append("input")
            await ctx.respond_input(ls.AgentTopic.Sessions, name.encode())

        return handle

    async def spawn(name):
        answered = []
        agent = world.laser.spawn_agent(
            name, ls.AgentTopic.Sessions, responder(name, answered), poll_interval_ms=10
        )
        await agent.ready()
        world.answered[name] = answered
        world.responders.append(agent)

    for name in (first, second):
        world.run(lambda name=name: spawn(name))


@when(parsers.parse('I submit an A2A task to "{target}"'))
def submit_to(world, target):
    bridge = ls.A2aBridge(
        world.laser, "a2a-gateway", ls.AgentTopic.Sessions, ls.AgentTopic.Sessions
    )
    task = world.run(
        lambda: bridge.submit({"message": {"role": "user", "text": "addressed"}}, target=target)
    )
    world.bridge_task = task["id"]


@when(parsers.parse('I call the MCP tool "{tool}" on "{target}"'))
def call_tool_on(world, tool, target):
    bridge = ls.McpBridge(
        world.laser,
        "mcp-gateway",
        ls.AgentTopic.Sessions,
        ls.AgentTopic.Sessions,
        "tools",
        timeout_ms=15_000,
    )
    result = world.run(lambda: bridge.call_tool(tool, {"q": "addressed"}, target=target))
    world.tool_result = result["content"][0]["text"]


@when(parsers.parse('I request input from "{target}"'))
def request_input_from(world, target):
    gate = world.laser.agdx(ls.AgentTopic.Sessions, "orchestrator", ls.new_conversation_id())
    decision = world.run(
        lambda: gate.request_input(
            ls.AgentTopic.Sessions, b"approve?", timeout_ms=15_000, target=target
        )
    )
    world.input_decision = bytes(decision).decode()


@then(parsers.parse('the A2A task completes with "{text}"'))
def task_completes_with(world, text):
    bridge = ls.A2aBridge(
        world.laser, "a2a-gateway", ls.AgentTopic.Sessions, ls.AgentTopic.Sessions
    )
    current = None
    for _ in range(600):
        current = world.run(lambda: bridge.task(world.bridge_task))
        if current["status"]["state"].casefold() == "completed":
            assert current["artifacts"][0]["text"] == text
            return
        world.run(lambda: asyncio.sleep(0.025))
    raise AssertionError(f"the addressed A2A task did not complete: {current!r}")


@then(parsers.parse('the MCP tool result is "{text}"'))
def tool_result_is(world, text):
    assert world.tool_result == text


@then(parsers.parse('the input decision is "{text}"'))
def input_decision_is(world, text):
    assert world.input_decision == text


@then(parsers.re(r'responder "(?P<name>[^"]+)" answered exactly (?P<listing>".+")$'))
def answered_exactly(world, name, listing):
    expected = [label.strip('"') for label in listing.split(", ")]
    assert world.answered[name] == expected, (name, world.answered[name])
