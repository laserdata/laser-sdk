import asyncio

import laser_sdk as ls
import pytest

RELEASED = bytes.fromhex("a1624f6ba16852656c6561736564f5")
RELEASE = {"v": 1, "namespace": "coord", "key": b"run", "lease_token": 7, "holder_id": "worker"}
ACQUIRE = {
    "v": 1,
    "namespace": "coord",
    "key": b"run",
    "lease_ttl_micros": 1_000_000,
    "holder_id": "worker",
}


class Transport:
    def __init__(self):
        self.frames = []
        self.resets = 0

    async def send(self, code, frame):
        self.frames.append(frame)
        if len(self.frames) == 1:
            raise TimeoutError("transport")
        return RELEASED

    async def reset(self):
        self.resets += 1


async def test_given_release_when_repeated_after_ambiguity_then_should_keep_the_frame():
    transport = Transport()
    client = ls.FencedLeaseClient(transport)
    operation = client.prepare_release(RELEASE)
    assert operation.operation_id > 0
    assert operation.ambiguous_recovery == ls.AmbiguousMutationRecovery.repeat_prepared()
    assert operation.ambiguous_recovery.kind == "repeat_prepared"
    with pytest.raises(ls.LaserError) as failure:
        await client.release(operation)
    assert failure.value.ambiguous_mutation
    assert not failure.value.retryable
    assert await client.release(operation)
    assert transport.frames[0] == transport.frames[1]
    assert transport.resets == 1


async def test_given_prepared_mutations_when_mismatched_then_should_refuse_before_send():
    transport = Transport()
    first = ls.FencedLeaseClient(transport)
    second = ls.FencedLeaseClient(transport)
    operation = first.prepare_release(RELEASE)
    with pytest.raises(ls.InvalidError):
        await second.release(operation)
    with pytest.raises(ls.InvalidError):
        await first.acquire(operation)
    assert transport.frames == []


async def test_given_a_stalled_acquisition_when_timed_out_then_should_report_recovery():
    class Stalled:
        def __init__(self):
            self.stop = asyncio.Event()
            self.resets = 0

        async def send(self, code, frame):
            await self.stop.wait()
            return RELEASED

        async def reset(self):
            self.resets += 1
            self.stop.set()
            await asyncio.sleep(0)

    transport = Stalled()
    client = ls.FencedLeaseClient(transport).with_attempt_timeout(5)
    operation = client.prepare_acquire(ACQUIRE)
    assert operation.ambiguous_recovery == ls.AmbiguousMutationRecovery.wait_for_lease_expiry(1_000)
    assert operation.ambiguous_recovery != ls.AmbiguousMutationRecovery.wait_for_lease_expiry(2_000)
    assert operation.ambiguous_recovery.kind == "wait_for_lease_expiry"
    with pytest.raises(ls.LaserError) as failure:
        await client.acquire(operation)
    assert failure.value.ambiguous_mutation
    assert transport.resets == 1


async def test_given_a_zero_timeout_when_executed_then_should_refuse_before_connection_or_send():
    transport = Transport()
    client = ls.FencedLeaseClient(transport).with_attempt_timeout(0)
    with pytest.raises(ls.InvalidError):
        await client.release(client.prepare_release(RELEASE))
    assert transport.frames == []


@pytest.mark.parametrize(
    "error_type",
    [ls.InvalidError, ls.UnsupportedError, ls.ConfigError, ls.ProtocolError, PermissionError],
)
async def test_given_a_permanent_transport_error_when_mutating_then_should_not_mark_it_ambiguous(
    error_type,
):
    class Permanent:
        def __init__(self):
            self.resets = 0

        async def send(self, code, frame):
            raise error_type("refused")

        async def reset(self):
            self.resets += 1

    transport = Permanent()
    client = ls.FencedLeaseClient(transport)
    with pytest.raises(ls.LaserError) as failure:
        await client.release(client.prepare_release(RELEASE))
    assert not failure.value.ambiguous_mutation
    assert not failure.value.retryable
    assert transport.resets == 1
    if error_type is PermissionError:
        assert failure.value.permission_denied


async def test_given_a_client_when_closed_twice_then_should_retire_once_and_refuse_sends():
    transport = Transport()
    client = ls.FencedLeaseClient(transport)
    operation = client.prepare_release(RELEASE)
    await client.close()
    await client.close()
    with pytest.raises(ls.LaserError):
        await client.release(operation)
    assert transport.resets == 1
    assert transport.frames == []


async def test_given_a_client_context_when_exited_then_should_close_without_hiding_errors():
    transport = Transport()
    client = ls.FencedLeaseClient(transport)
    with pytest.raises(ValueError, match="body failed"):
        async with client as entered:
            assert entered is client
            raise ValueError("body failed")
    assert transport.resets == 1


async def test_given_a_dedicated_transport_when_closed_then_should_refuse_readiness():
    transport = ls.DedicatedKvTransport("iggy:iggy@127.0.0.1:1")
    async with transport as entered:
        assert entered is transport
    await transport.close()
    await transport.reset()
    with pytest.raises(ls.LaserError):
        await transport.ready()


async def test_given_a_custom_close_when_the_client_closes_then_should_use_terminal_retirement():
    class Closing(Transport):
        def __init__(self):
            super().__init__()
            self.closed = 0

        async def close(self):
            self.closed += 1

    transport = Closing()
    client = ls.FencedLeaseClient(transport)
    await client.close()
    await client.close()
    assert transport.closed == 1
    assert transport.resets == 0


async def test_given_a_dedicated_transport_when_its_client_closes_then_should_close_the_transport():
    transport = ls.DedicatedKvTransport("iggy:iggy@127.0.0.1:1")
    client = ls.FencedLeaseClient(transport)
    await client.close()
    with pytest.raises(ls.LaserError):
        await transport.ready()
