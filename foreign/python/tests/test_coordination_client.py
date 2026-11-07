import asyncio
import time

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


def _text(value):
    encoded = value.encode()
    header = bytes([0x60 | len(encoded)]) if len(encoded) < 24 else bytes([0x78, len(encoded)])
    return header + encoded


def _command_error(code):
    message = _text("message") + _text("the outcome is not known yet")
    return b"\xa2" + _text("code") + _text(code) + message


@pytest.mark.parametrize("code", ["pending", "backend_unavailable", "unavailable"])
async def test_given_an_unknown_outcome_reply_when_acquiring_then_should_keep_the_transport_and_raise_ambiguity_with_the_identity(  # noqa: E501
    code,
):
    reply = _command_error(code)

    class Unknown:
        def __init__(self):
            self.resets = 0

        async def send(self, code, frame):
            return reply

        async def reset(self):
            self.resets += 1

    transport = Unknown()
    client = ls.FencedLeaseClient(transport)
    operation = client.prepare_acquire(ACQUIRE)
    with pytest.raises(ls.LaserError) as failure:
        await client.acquire(operation)
    assert failure.value.ambiguous_mutation
    assert not failure.value.retryable
    assert f"{operation.operation_id:032x}" in str(failure.value)
    assert transport.resets == 0
    assert operation.ambiguous_recovery == ls.AmbiguousMutationRecovery.wait_for_lease_expiry(1_000)


def _uint(value):
    if value < 24:
        return bytes([value])
    if value < 0x100:
        return bytes([0x18, value])
    if value < 0x10000:
        return b"\x19" + value.to_bytes(2, "big")
    return b"\x1a" + value.to_bytes(4, "big")


def _map(entries):
    return bytes([0xA0 | len(entries)]) + b"".join(_text(key) + value for key, value in entries)


LEASED = _map(
    [
        (
            "Ok",
            _map(
                [
                    (
                        "Leased",
                        _map(
                            [
                                ("lease_token", _uint(4)),
                                ("granted_ttl_micros", _uint(1_000_000)),
                                (
                                    "position",
                                    _map(
                                        [
                                            ("topic_generation", _uint(1)),
                                            ("partition", _uint(0)),
                                            ("offset", _uint(512)),
                                        ]
                                    ),
                                ),
                            ]
                        ),
                    )
                ]
            ),
        )
    ]
)


async def test_given_a_grant_answered_late_when_acquired_then_should_be_valid_from_the_first_send():
    class Late:
        async def send(self, code, frame):
            await asyncio.sleep(0.2)
            return LEASED

        async def reset(self):
            pass

    client = ls.FencedLeaseClient(Late())
    before = time.monotonic()
    lease = await client.acquire(client.prepare_acquire(ACQUIRE))
    after = time.monotonic()
    assert lease.token == 4
    assert lease.valid_until >= before + lease.granted_ttl_ms / 1_000 - 0.005
    assert lease.valid_until + 0.19 <= after + lease.granted_ttl_ms / 1_000, (
        "the reply delay is spent from the grant, not added to it"
    )


def test_given_a_holder_id_with_a_control_character_when_preparing_a_lease_then_should_refuse_it():
    client = ls.FencedLeaseClient(Transport())
    with pytest.raises(ls.InvalidError, match="control character"):
        client.prepare_acquire({**ACQUIRE, "holder_id": "work\ter"})
    assert client.prepare_acquire(ACQUIRE).operation_id > 0


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


async def test_given_a_transport_that_wrote_the_frame_and_lost_its_reply_when_mutating_then_should_raise_ambiguity_with_the_identity():  # noqa: E501
    class Written:
        def __init__(self):
            self.resets = 0

        async def send(self, code, frame):
            raise ls.AmbiguousMutationError("the frame was written and its reply was lost")

        async def reset(self):
            self.resets += 1

    transport = Written()
    client = ls.FencedLeaseClient(transport)
    operation = client.prepare_release(RELEASE)
    with pytest.raises(ls.AmbiguousMutationError) as failure:
        await client.release(operation)
    assert failure.value.operation_id == operation.operation_id
    assert transport.resets == 1


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
