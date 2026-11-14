import asyncio
import uuid

import laser_sdk as ls
import pytest
from iggy_test_server import IggyTestServer, free_tcp_port, free_udp_port

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def listeners():
    quic, websocket = free_udp_port(), free_tcp_port()
    server = IggyTestServer(
        {
            "IGGY_QUIC_ENABLED": "true",
            "IGGY_QUIC_ADDRESS": f"127.0.0.1:{quic}",
            "IGGY_WEBSOCKET_ENABLED": "true",
            "IGGY_WEBSOCKET_ADDRESS": f"127.0.0.1:{websocket}",
        }
    ).start()
    try:
        yield {
            "quic": f"iggy+quic://iggy:iggy@127.0.0.1:{quic}",
            "ws": f"iggy+ws://iggy:iggy@127.0.0.1:{websocket}",
        }
    finally:
        server.stop()


@pytest.mark.parametrize("transport", ["quic", "ws"])
async def test_given_a_server_on_quic_and_websocket_when_connected_by_scheme_then_should_publish_and_poll(  # noqa: E501
    listeners, transport
):
    stream = f"t-{uuid.uuid4().hex[:12]}"
    laser = await _connect_with_retry(listeners[transport], stream)
    try:
        assert (await laser.capabilities()).hello != "failed"
        await laser.stream(stream).ensure()
        topic = laser.topic("records")
        await topic.producer(partitions=1).send_batch([transport.encode()])
        consumer = topic.consumer("transport-reader", partition=0, polling="first")
        try:
            record = await asyncio.wait_for(consumer.next(), 15)
            assert bytes(record.payload) == transport.encode()
        finally:
            await consumer.shutdown()
    finally:
        await laser.close()


async def _connect_with_retry(connection_string, stream, attempts=40, delay=0.5):
    """The listeners bind next to TCP, so retry until they accept."""
    last = None
    for _ in range(attempts):
        try:
            return await ls.Laser.connect(connection_string, stream=stream, connect_timeout_ms=2000)
        except ls.LaserError as error:
            last = error
            await asyncio.sleep(delay)
    raise AssertionError(f"could not connect over {connection_string.split('@')[-1]}: {last}")
