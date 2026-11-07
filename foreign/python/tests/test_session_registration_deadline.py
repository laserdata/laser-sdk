import asyncio
import time
import uuid
from urllib.parse import urlsplit

import laser_sdk as ls
import pytest
import pytest_asyncio

pytestmark = [
    pytest.mark.integration,
]

# Every consensus frame is a 256-byte header followed by its body. The frame
# size sits at byte 48 and a request's command code at byte 196, both
# little-endian.
HEADER_BYTES = 256
SIZE_AT = 48
REQUEST_CODE_AT = 196
SESSION_CHANGES_CODE = 1_000_716


@pytest_asyncio.fixture(loop_scope="function")
async def held_session_reads(iggy_endpoint):
    """A transparent proxy that holds every session-changes read while
    `holding` is cleared, so the registration readiness read never answers."""
    endpoint = urlsplit(iggy_endpoint if "://" in iggy_endpoint else f"iggy+tcp://{iggy_endpoint}")
    holding = asyncio.Event()
    holding.set()
    held = [0]
    tasks = set()

    async def frames(reader):
        while True:
            try:
                header = await reader.readexactly(HEADER_BYTES)
            except asyncio.IncompleteReadError:
                return
            size = int.from_bytes(header[SIZE_AT : SIZE_AT + 4], "little")
            assert size >= HEADER_BYTES
            yield header + await reader.readexactly(size - HEADER_BYTES)

    async def requests(reader, writer):
        async for frame in frames(reader):
            code = int.from_bytes(frame[REQUEST_CODE_AT : REQUEST_CODE_AT + 4], "little")
            if code == SESSION_CHANGES_CODE and not holding.is_set():
                held[0] += 1
                await holding.wait()
            writer.write(frame)
            await writer.drain()

    async def replies(reader, writer):
        while True:
            data = await reader.read(65536)
            if not data:
                return
            writer.write(data)
            await writer.drain()

    async def connect(reader, writer):
        task = asyncio.current_task()
        tasks.add(task)
        upstream = None
        pumps = []
        try:
            incoming, upstream = await asyncio.open_connection(endpoint.hostname, endpoint.port)
            pumps = [
                asyncio.create_task(requests(reader, upstream)),
                asyncio.create_task(replies(incoming, writer)),
            ]
            await asyncio.wait(pumps, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for pump in pumps:
                pump.cancel()
            await asyncio.gather(*pumps, return_exceptions=True)
            writer.close()
            if upstream is not None:
                upstream.close()
            tasks.discard(task)

    server = await asyncio.start_server(connect, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    credentials = endpoint.netloc.rsplit("@", 1)[0]
    try:
        yield (
            endpoint._replace(netloc=f"{credentials}@127.0.0.1:{port}").geturl(),
            holding,
            held,
        )
    finally:
        holding.set()
        server.close()
        await server.wait_closed()
        pending = list(tasks)
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)


async def test_given_a_registration_read_that_never_answers_when_bootstrapping_then_should_report_unregistered_at_the_publish_deadline(  # noqa: E501
    held_session_reads,
):
    endpoint, holding, held = held_session_reads
    laser = await ls.Laser.connect(
        endpoint,
        stream=f"deadline-{uuid.uuid4().hex[:12]}",
        publish_timeout_ms=1_000,
        publish_max_retries=0,
    )
    try:
        if not (await laser.capabilities()).sessions:
            pytest.skip("registration readiness needs a deployment that serves sessions")
        await laser.stream(laser.default_stream).ensure()
        sessions = laser.sessions()
        holding.clear()
        started = time.monotonic()
        bootstrap = await asyncio.wait_for(
            sessions.bootstrap(1, ls.TopicRetention.expire_after(86_400_000)), 10
        )
        elapsed = time.monotonic() - started
        assert bootstrap.registered is False, (
            "a read that never answers cannot confirm the registration"
        )
        assert held[0] >= 1, "the readiness read must have reached the proxy and been held"
        assert elapsed < 5, f"the publish deadline bounds the wait, waited {elapsed:.2f}s"
        holding.set()
        # The registration itself was published once. A later bootstrap sees it
        # served without any identity change.
        # Each bootstrap waits at most the 1 s publish deadline, so poll until
        # the folded registration is served.
        deadline = time.monotonic() + 30
        while not (
            await sessions.bootstrap(1, ls.TopicRetention.expire_after(86_400_000))
        ).registered:
            assert time.monotonic() < deadline, "the published registration is served"
            await asyncio.sleep(0.2)
    finally:
        holding.set()
        await laser.close()
