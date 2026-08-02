import asyncio
import os
import sys
import uuid
from pathlib import Path

import laser_sdk as ls
import pytest
import pytest_asyncio

PYTHON_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PYTHON_ROOT))
from iggy_test_server import IggyTestServer  # noqa: E402


@pytest.fixture(scope="session")
def iggy_endpoint():
    """Use LASER_BDD_URL or start the pinned VSR Iggy test server."""
    if endpoint := os.environ.get("LASER_BDD_URL"):
        yield endpoint
        return

    server = IggyTestServer().start()
    try:
        yield server.endpoint
    finally:
        server.stop()


async def _connect_with_retry(connection_string, stream, attempts=40, delay=0.5):
    """The process binds TCP before Iggy is ready for VSR, so retry the connect."""
    last = None
    for _ in range(attempts):
        try:
            return await ls.Laser.connect(connection_string, stream=stream)
        except ls.LaserError as error:
            last = error
            await asyncio.sleep(delay)
    raise AssertionError(f"could not connect to Iggy in time: {last}")


@pytest_asyncio.fixture
async def laser(iggy_endpoint):
    """Create a client with a unique default stream for test isolation."""
    stream = f"t-{uuid.uuid4().hex[:12]}"
    client = await _connect_with_retry(iggy_endpoint, stream)
    return client
