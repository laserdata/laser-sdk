import asyncio
import time
import uuid
from urllib.parse import urlsplit

import laser_sdk as ls
import pytest
import pytest_asyncio
from conftest import _connect_with_retry
from iggy_test_server import IggyTestServer

pytestmark = [
    pytest.mark.integration,
]


@pytest_asyncio.fixture(loop_scope="function")
async def paused_transport(iggy_endpoint):
    endpoint = urlsplit(iggy_endpoint if "://" in iggy_endpoint else f"iggy+tcp://{iggy_endpoint}")
    forwarding = asyncio.Event()
    forwarding.set()
    tasks = set()
    pause_after_reply = [None]

    async def relay(reader, writer, reply=False):
        while True:
            if reply:
                try:
                    # Iggy VSR replies use a 256-byte header and a u32-LE size at byte 48.
                    header = await reader.readexactly(256)
                    size = int.from_bytes(header[48:52], "little")
                    assert size >= len(header)
                    data = header + await reader.readexactly(size - len(header))
                except asyncio.IncompleteReadError:
                    return
            else:
                data = await reader.read(65536)
                if not data:
                    return
            await forwarding.wait()
            writer.write(data)
            await writer.drain()
            if reply and pause_after_reply[0] is not None:
                pause_after_reply[0] -= 1
                if pause_after_reply[0] == 0:
                    forwarding.clear()

    async def connect(reader, writer):
        task = asyncio.current_task()
        tasks.add(task)
        upstream = None
        pumps = []
        try:
            incoming, upstream = await asyncio.open_connection(endpoint.hostname, endpoint.port)
            pumps = [
                asyncio.create_task(relay(reader, upstream)),
                asyncio.create_task(relay(incoming, writer, reply=True)),
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
            forwarding,
            pause_after_reply,
        )
    finally:
        server.close()
        await server.wait_closed()
        pending = list(tasks)
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)


@pytest.mark.parametrize("initialized", [False, True])
@pytest.mark.parametrize("options", [{}, {"retry_interval_ms": 10}])
async def test_given_a_stalled_connection_when_a_producer_sends_then_should_honor_connection_budget(
    paused_transport, initialized, options
):
    endpoint, forwarding, _ = paused_transport
    laser = await ls.Laser.connect(
        endpoint,
        stream=f"outage-{uuid.uuid4().hex[:12]}",
        publish_timeout_ms=200,
        publish_max_retries=0,
        publish_retry_backoff_ms=10,
    )
    try:
        producer = laser.topic("pulse").producer(**options)
        if initialized:
            await producer.init()
        forwarding.clear()
        with pytest.raises(ls.TimeoutError) as failure:
            await asyncio.wait_for(producer.send(b"pending"), 2)
        assert failure.value.committed == []
        assert failure.value.unconfirmed_count == (1 if initialized else None)
    finally:
        forwarding.set()
        await laser.close()


async def test_given_a_confirmed_chunk_when_the_next_times_out_then_should_preserve_progress(
    paused_transport,
):
    endpoint, forwarding, pause_after_reply = paused_transport
    laser = await ls.Laser.connect(
        endpoint,
        stream=f"partial-{uuid.uuid4().hex[:12]}",
        publish_timeout_ms=200,
        publish_max_retries=0,
    )
    try:
        producer = laser.topic("pulse").producer(batch_length=1, partition=0)
        await producer.init()
        pause_after_reply[0] = 1
        with pytest.raises(ls.TimeoutError) as failure:
            await asyncio.wait_for(producer.send_batch([b"first", b"second", b"third"]), 2)
        assert failure.value.unconfirmed_count == 2
        assert len(failure.value.committed) == 1
        assert failure.value.committed[0].base_offset == 0
    finally:
        forwarding.set()
        await laser.close()


async def test_given_a_temporary_outage_when_retries_are_overridden_then_should_recover(
    paused_transport,
):
    endpoint, forwarding, _ = paused_transport
    separator = "&" if "?" in endpoint else "?"
    laser = await ls.Laser.connect(
        f"{endpoint}{separator}reestablish_after=0",
        stream=f"recovery-{uuid.uuid4().hex[:12]}",
        publish_timeout_ms=200,
        publish_max_retries=0,
    )
    resume = None
    try:
        producer = laser.topic("pulse").producer(retries=2, retry_interval_ms=10, partition=0)
        await producer.init()
        forwarding.clear()
        resume = asyncio.get_running_loop().call_later(0.3, forwarding.set)
        response = await asyncio.wait_for(producer.send(b"recovered"), 2)
        assert len(response.confirmations) == 1
    finally:
        if resume is not None:
            resume.cancel()
        forwarding.set()
        await laser.close()


async def test_given_a_stopped_server_when_a_producer_sends_then_should_time_out_in_budget():
    server = await asyncio.to_thread(lambda: IggyTestServer().start())
    laser = None
    try:
        await (await _connect_with_retry(server.endpoint, "producer_outage")).close()
        laser = await ls.Laser.connect(
            server.endpoint,
            stream="producer_outage",
            publish_timeout_ms=1000,
            publish_max_retries=1,
            publish_retry_backoff_ms=100,
        )
        producer = laser.topic("pulse").producer(retries=1, retry_interval_ms=100)
        await producer.send(b"warm")
        await asyncio.to_thread(server.stop)
        started = time.monotonic()
        with pytest.raises(ls.TimeoutError):
            await asyncio.wait_for(producer.send(b"lost"), 60)
        assert time.monotonic() - started < 30, "the publish budget should bound the failed send"
    finally:
        if laser is not None:
            await laser.close()
        await asyncio.to_thread(server.stop)


async def test_given_a_deleted_stream_when_a_producer_sends_then_should_name_cause_and_failures(
    laser,
):
    stream = laser.default_stream
    producer = laser.topic("pulse").producer(retries=1, retry_interval_ms=100)
    await producer.send(b"first")
    await laser.stream(stream).delete()
    with pytest.raises(ls.TransportError) as failure:
        await producer.send_batch([b"a", b"b", b"c"])
    assert str(failure.value).startswith(f"publish failed to {stream}/pulse: ")
    assert failure.value.unconfirmed_count == 3
    assert failure.value.committed == []
    assert failure.value.not_found
    assert failure.value.code == "NotFound"
    assert not failure.value.retryable
