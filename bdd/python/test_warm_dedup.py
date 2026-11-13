import asyncio
import time
from pathlib import Path

import laser_sdk as ls
from pytest_bdd import given, parsers, scenarios, then, when

SCENARIOS = Path(__file__).parent.parent / "scenarios"
scenarios(str(SCENARIOS / "warm_dedup.feature"))

# How far ahead a resend's deadline lies, far enough that it never expires in
# the scenario.
RESEND_DEADLINE_MICROS = 60_000_000


@given(parsers.parse('a warm-dedup worker "{name}" with policy "{policy}"'))
def warm_worker(world, name, policy):
    world.warm_agent = name
    world.warm_policy = policy
    world.warm_handled = []
    world.warm_deadlines = {}
    _start(world)


@when(parsers.parse('the worker is sent a command under key "{key}" due in {seconds:d} seconds'))
def send_due(world, key, seconds):
    _send(world, key, _now_micros() + seconds * 1_000_000)


@when(parsers.parse('the worker is sent a command under key "{key}" already past its deadline'))
def send_expired(world, key):
    _send(world, key, _now_micros() - 1_000_000)


@when(parsers.parse('the worker is sent a command under key "{key}" without a deadline'))
def send_without_deadline(world, key):
    _send(world, key, None)


@then(parsers.parse('the worker handled the keys "{keys}"'))
def worker_handled(world, keys):
    _assert_handled(world, keys)


@when(parsers.parse('the worker stops and the deadline of key "{key}" passes'))
def stop_after_deadline(world, key):
    world.run(world.warm_worker.shutdown)
    world.warm_worker = None
    deadline = world.warm_deadlines[key]
    while (now := _now_micros()) <= deadline:
        time.sleep((deadline + 1 - now) / 1_000_000)


@when(parsers.parse('resends arrive under the keys "{keys}" and then "{last}"'))
def resends(world, keys, last):
    due = _now_micros() + RESEND_DEADLINE_MICROS
    for key in [*keys.split(", "), last]:
        _send(world, key, due)


@when("the worker restarts")
def restart(world):
    world.warm_handled.clear()
    _start(world)


@then(parsers.parse('the restarted worker handled the keys "{keys}"'))
def restarted_handled(world, keys):
    try:
        _assert_handled(world, keys)
    finally:
        world.run(world.warm_worker.shutdown)
        world.warm_worker = None


def _start(world):
    async def handle(_ctx, message):
        world.warm_handled.append(bytes(message.payload).decode())

    async def spawn():
        # The agent runs on the scenario's loop, so it is spawned inside it.
        worker = world.laser.spawn_agent(
            world.warm_agent,
            ls.AgentTopic.Sessions,
            handle,
            warm_dedup=True,
            warm_dedup_policy=world.warm_policy,
        )
        await worker.ready()
        return worker

    world.warm_worker = world.run(spawn)


def _send(world, key, deadline):
    provenance = ls.Provenance(
        conversation_id=world.conversation,
        agent="warm-sender",
        target_agent_id=world.warm_agent,
        idempotency_key=key,
        deadline_at_micros=deadline,
    )
    world.run(lambda: world.laser.send_agent(ls.AgentTopic.Sessions, key.encode(), provenance))
    if deadline is not None:
        world.warm_deadlines[key] = deadline


# Waits until the worker handled the last listed key, then compares the whole
# list. Every record rides one partition and the worker is serial, so the
# records before the last one were decided before it.
def _assert_handled(world, keys):
    expected = keys.split(", ")

    async def wait():
        for _ in range(200):
            if expected[-1] in world.warm_handled:
                return
            await asyncio.sleep(0.05)

    world.run(wait)
    assert world.warm_handled == expected


def _now_micros():
    return time.time_ns() // 1_000
