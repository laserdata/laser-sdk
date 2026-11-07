import asyncio
import time
from pathlib import Path

import laser_sdk as ls
from pytest_bdd import given, parsers, scenarios, then, when

SCENARIOS = Path(__file__).parent.parent / "scenarios"
scenarios(str(SCENARIOS / "kv_leases.feature"))

COORDINATION = "coordination"
STATE = "state"
TAKEOVER_DEADLINE_SECONDS = 15


# Every Python client connects through a connection string, the client the
# fenced-lease verbs need, and the stream the namespaces are scoped to must exist.
@given("a fresh stream bootstrapped on a connection-string client")
def connected_stream(world):
    world.connect()
    world.run(
        lambda: world.laser.bootstrap(1, retention=ls.TopicRetention.expire_after(86_400_000))
    )


@when(parsers.parse('"{holder}" acquires the lease "{key}" for {ttl_ms:d} ms'))
def acquire(world, holder, key, ttl_ms):
    lease = world.run(lambda: world.laser.kv(COORDINATION).lease(key.encode(), holder, ttl_ms))
    leases(world)[holder] = (key, lease)


# The earlier grant has to run out, so the step polls until the store grants
# the key rather than sleeping a fixed time.
@when(parsers.parse('"{holder}" acquires the lease "{key}" once it is free'))
def acquire_when_free(world, holder, key):
    async def poll():
        deadline = time.monotonic() + TAKEOVER_DEADLINE_SECONDS
        while True:
            try:
                return await world.laser.kv(COORDINATION).lease(key.encode(), holder, 5000)
            except ls.LaserError:
                if time.monotonic() >= deadline:
                    raise
                await asyncio.sleep(0.1)

    leases(world)[holder] = (key, world.run(poll))


@when(parsers.parse('"{holder}" renews the lease "{key}"'))
def renew(world, holder, key):
    token = leases(world)[holder][1].token
    world.renewed_lease = world.run(
        lambda: world.laser.kv(COORDINATION).renew_lease(key.encode(), holder, token, 5000)
    )


@then(parsers.parse('the renewal of "{holder}" keeps its lease token'))
def renewal_keeps_token(world, holder):
    assert world.renewed_lease.token == leases(world)[holder][1].token


@when(parsers.parse('"{holder}" releases the lease "{key}"'))
def release(world, holder, key):
    token = leases(world)[holder][1].token
    world.lease_released = world.run(
        lambda: world.laser.kv(COORDINATION).release(key.encode(), holder, token)
    )


@then("the release reports the lease was held")
def release_was_held(world):
    assert world.lease_released is True


@when(parsers.parse('"{holder}" writes "{value}" to "{key}" fenced by its lease'))
def fenced_write(world, holder, value, key):
    fence_key, lease = leases(world)[holder]
    request = (
        world.laser.kv(STATE)
        .cas_fenced(key.encode(), COORDINATION, fence_key.encode(), lease.token)
        .bytes(value.encode())
    )
    entry = getattr(world, "last_entry", None)
    request = request.expect_absent() if entry is None else request.expect_version(entry.version)
    world.capture(request.commit)


@then("the fenced write fails as lease lost")
def fenced_write_lost(world):
    assert isinstance(world.error, ls.KvError), world.error
    assert world.error.lease_lost, world.error


@then(parsers.parse('a read of "{key}" at the lease grant of "{holder}" returns "{value}"'))
def read_at_grant(world, key, holder, value):
    position = leases(world)[holder][1].position
    entry = world.run(lambda: world.laser.kv(STATE).get_entry_at_least(key.encode(), position))
    assert entry is not None, "the earlier holder's write is visible at the new grant"
    assert bytes(entry.value) == value.encode()
    world.last_entry = entry


@then(parsers.parse('reading "{key}" returns "{value}"'))
def read_back(world, key, value):
    assert world.run(lambda: world.laser.kv(STATE).get(key.encode())) == value.encode()


def leases(world):
    if not hasattr(world, "leases"):
        world.leases = {}
    return world.leases
