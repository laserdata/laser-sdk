import asyncio

import laser_sdk as ls
import pytest


async def _silent_server():
    """Accept connections and read forever, like a server that never answers login."""

    writers = []

    async def hold(reader, writer):
        writers.append(writer)
        try:
            while await reader.read(1024):
                pass
        finally:
            writer.close()

    return await asyncio.start_server(hold, "127.0.0.1", 0), writers


@pytest.mark.parametrize(("tls", "stage"), [(False, "login"), (True, "accept")])
async def test_given_a_silent_server_when_connecting_then_should_raise_a_named_timeout(
    monkeypatch, tls, stage
):
    monkeypatch.setenv("LASER_CONNECT_TIMEOUT_MS", "invalid")
    server, writers = await _silent_server()
    port = server.sockets[0].getsockname()[1]
    loop = asyncio.get_running_loop()
    started = loop.time()
    try:
        with pytest.raises(ls.TimeoutError, match=stage):
            await ls.Laser.connect(
                f"iggy:iggy@127.0.0.1:{port}?tls={str(tls).lower()}",
                connect_timeout_ms=300,
            )
        assert loop.time() - started < 5
    finally:
        server.close()
        await server.wait_closed()
        for writer in writers:
            writer.close()
            await writer.wait_closed()


async def test_given_a_zero_connect_timeout_when_connecting_then_should_reject_it():
    with pytest.raises(ls.ConfigError):
        await ls.Laser.connect("iggy:iggy@127.0.0.1:8090", connect_timeout_ms=0)


async def test_given_an_unreachable_server_when_connecting_then_should_use_the_environment_budget(
    monkeypatch,
):
    server, _ = await _silent_server()
    port = server.sockets[0].getsockname()[1]
    server.close()
    await server.wait_closed()
    monkeypatch.setenv("LASER_CONNECT_TIMEOUT_MS", "100")
    with pytest.raises(ls.TimeoutError, match="accept"):
        await asyncio.wait_for(ls.Laser.connect(f"iggy:iggy@127.0.0.1:{port}"), timeout=2)
