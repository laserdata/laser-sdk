import asyncio
import os
import uuid

import laser_sdk as ls
import pytest
import pytest_asyncio

IGGY_IMAGE = os.environ.get("LASER_TEST_IGGY_IMAGE", "apache/iggy:latest")
IGGY_TCP_PORT = 3000
IGGY_HTTP_PORT = 80


@pytest.fixture(scope="session")
def iggy_endpoint():
    """Start an Apache Iggy container and yield a connection string. Skips the
    whole integration suite when Docker is unavailable."""
    try:
        from testcontainers.core.container import DockerContainer
    except ImportError:
        pytest.skip("testcontainers is not installed")

    container = (
        DockerContainer(IGGY_IMAGE)
        .with_exposed_ports(IGGY_TCP_PORT, IGGY_HTTP_PORT)
        .with_env("IGGY_ROOT_USERNAME", "iggy")
        .with_env("IGGY_ROOT_PASSWORD", "iggy")
        .with_env("IGGY_TCP_ENABLED", "true")
        .with_env("IGGY_TCP_ADDRESS", "0.0.0.0:3000")
        .with_env("IGGY_HTTP_ENABLED", "true")
        .with_env("IGGY_HTTP_ADDRESS", "0.0.0.0:80")
        .with_kwargs(cap_add=["SYS_NICE"], security_opt=["seccomp=unconfined"])
    )
    try:
        container.start()
    except Exception as error:  # docker not running / image pull failed
        pytest.skip(f"could not start the Iggy container: {error}")

    try:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(IGGY_TCP_PORT)
        yield f"iggy://iggy:iggy@{host}:{port}"
    finally:
        container.stop()


async def _connect_with_retry(connection_string, stream, attempts=40, delay=0.5):
    """The container is up before Iggy is accepting TCP, so retry the connect."""
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
    """A connected client pinned to a unique stream per test, so cases stay
    isolated on one shared container."""
    stream = f"t-{uuid.uuid4().hex[:12]}"
    client = await _connect_with_retry(iggy_endpoint, stream)
    return client
