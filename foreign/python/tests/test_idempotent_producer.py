import ast
import asyncio
import uuid
from urllib.parse import urlsplit

import laser_sdk as ls
import pytest
import pytest_asyncio
from test_stub_types import parameter

READINGS = [b'{"host":"node-1","cpu":41}', b'{"host":"node-2","cpu":57}']


def test_given_the_producer_stub_when_read_then_should_offer_idempotent_off_by_default():
    argument = parameter("Topic", "producer", "idempotent")
    assert ast.unparse(argument.annotation) == "builtins.bool"


@pytest_asyncio.fixture(loop_scope="function")
async def withheld_replies(iggy_endpoint):
    endpoint = urlsplit(iggy_endpoint if "://" in iggy_endpoint else f"iggy+tcp://{iggy_endpoint}")
    replies = asyncio.Event()
    replies.set()
    tasks = set()

    async def relay(reader, writer, reply=False):
        while data := await reader.read(65536):
            if reply:
                await replies.wait()
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
                asyncio.create_task(relay(reader, upstream)),
                asyncio.create_task(relay(incoming, writer, reply=True)),
            ]
            await asyncio.wait(pumps, return_when=asyncio.FIRST_COMPLETED)
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            for pump in pumps:
                pump.cancel()
            await asyncio.gather(*pumps, return_exceptions=True)
            writer.close()
            if upstream is not None:
                upstream.close()
            tasks.discard(task)

    def withhold(seconds):
        replies.clear()
        release = asyncio.get_running_loop().call_later(seconds, replies.set)
        tasks.add(release)

    server = await asyncio.start_server(connect, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    credentials = endpoint.netloc.rsplit("@", 1)[0]
    try:
        yield endpoint._replace(netloc=f"{credentials}@127.0.0.1:{port}").geturl(), withhold
    finally:
        replies.set()
        server.close()
        await server.wait_closed()
        pending = [task for task in tasks if isinstance(task, asyncio.Task)]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)


@pytest.mark.integration
async def test_given_a_lost_reply_when_the_idempotent_producer_resends_then_should_write_the_batch_once(  # noqa: E501
    withheld_replies,
):
    endpoint, withhold = withheld_replies
    laser = await ls.Laser.connect(
        endpoint,
        stream=f"idempotent-{uuid.uuid4().hex[:12]}",
        publish_timeout_ms=1500,
        publish_max_retries=3,
        publish_retry_backoff_ms=50,
    )
    try:
        await laser.stream(laser.default_stream).ensure()
        topic = laser.topic("readings")
        producer = topic.producer(partition=0, partitions=1, idempotent=True)
        await producer.init()
        withhold(2.5)
        resent = await asyncio.wait_for(producer.send_batch(READINGS), 30)
        assert [confirmation.base_offset for confirmation in resent.confirmations] == [0]
        following = await producer.send(b'{"host":"node-3","cpu":82}')
        assert following.confirmations[0].base_offset == len(READINGS)

        reader = topic.consumer("idempotent-reader", partition=0, polling="first")
        try:
            stored = [await asyncio.wait_for(reader.next(), 10) for _ in range(3)]
        finally:
            await reader.shutdown()
        assert [(record.position.offset, bytes(record.payload)) for record in stored] == [
            (0, READINGS[0]),
            (1, READINGS[1]),
            (2, b'{"host":"node-3","cpu":82}'),
        ]
        await producer.shutdown()
    finally:
        await laser.close()
