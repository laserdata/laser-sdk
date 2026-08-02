import asyncio
from pathlib import Path

import laser_sdk as ls
from pytest_bdd import parsers, scenarios, then, when

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
    bridge = world.laser.a2a_bridge("a2a-gateway", ls.Topics.COMMANDS, ls.Topics.RESPONSES)
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
        lambda: world.laser.publish_state_snapshot(
            ls.Topics.AUDIT, "agui-gateway", world.conversation, {"count": 1}
        )
    )
    world.run(
        lambda: world.laser.publish_state_delta(
            ls.Topics.AUDIT,
            "agui-gateway",
            world.conversation,
            [{"op": "replace", "path": "/count", "value": 2}],
        )
    )
    for _ in range(80):
        state = world.run(
            lambda: world.laser.reconstruct_state(world.conversation, ls.Topics.AUDIT)
        )
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
    stream = world.laser.agdx(ls.Topics.LLM_IO, "assistant", world.conversation).stream(
        ls.new_correlation_id(), "chat"
    )
    world.run(lambda: stream.write(first.encode()))
    world.run(lambda: stream.write(second.encode()))
    world.run(lambda: stream.finish(finish_reason="stop"))
    for _ in range(80):
        events = world.run(lambda: world.laser.agui_events(world.conversation, ls.Topics.LLM_IO))
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
