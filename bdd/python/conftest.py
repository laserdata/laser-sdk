import asyncio
import os
import uuid

import laser_sdk as ls
import pytest
from pytest_bdd import given, parsers, then, when

IGGY_IMAGE = os.environ.get("LASER_TEST_IGGY_IMAGE", "apache/iggy:latest")
IGGY_TCP_PORT = 3000
IGGY_HTTP_PORT = 80


@pytest.fixture(scope="session")
def iggy_endpoint():
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
    except Exception as error:
        pytest.skip(f"could not start the Iggy container: {error}")
    try:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(IGGY_TCP_PORT)
        yield f"iggy://iggy:iggy@{host}:{port}"
    finally:
        container.stop()


class World:
    """One per scenario: a dedicated event loop (so the bindings' futures stay on
    one loop), the client under test, and slots for results and captured errors."""

    def __init__(self, endpoint):
        self.endpoint = endpoint
        self.loop = asyncio.new_event_loop()
        self.laser = None
        self.conversation = None
        self.assembled = []
        self.published = False
        self.count = None
        self.caps = None
        self.error = None

    def run(self, factory):
        # `factory` is a zero-arg callable returning a coroutine. The binding's
        # awaitable must be built while the loop is running (future_into_py needs
        # the running loop), so we build and await it inside run_until_complete.
        async def driver():
            return await factory()

        return self.loop.run_until_complete(driver())

    async def _connect(self, stream):
        last = None
        for _ in range(40):
            try:
                return await ls.Laser.connect(self.endpoint, stream=stream)
            except ls.LaserError as error:
                last = error
                await asyncio.sleep(0.5)
        raise AssertionError(f"could not connect to Iggy in time: {last}")

    def connect(self):
        stream = f"bdd-{uuid.uuid4().hex[:10]}"
        self.laser = self.run(lambda: self._connect(stream))

    def new_conversation(self):
        self.conversation = ls.new_conversation_id()

    def send_command(self, payload, agent=None, idempotency_key=None):
        provenance = ls.Provenance(
            conversation_id=self.conversation,
            agent=agent,
            idempotency_key=idempotency_key,
        )
        self.run(lambda: self.laser.send_agent(ls.Topics.COMMANDS, payload.encode(), provenance))

    def assemble(self):
        self.assembled = self.run(lambda: self.laser.assemble_context(self.conversation))

    def capture(self, factory):
        try:
            self.run(factory)
            self.error = None
        except ls.LaserError as error:
            self.error = error

    def close(self):
        self.loop.close()


@pytest.fixture
def world(iggy_endpoint):
    instance = World(iggy_endpoint)
    yield instance
    instance.close()


class Bench:
    """The query and key-value scenarios run against the in-memory reference
    engines, with no Iggy and no client, so they need no container."""

    def __init__(self):
        self.query_engine = None
        self.kv_engine = None
        self.last_query = None
        self.last_cas = None


@pytest.fixture
def bench():
    return Bench()


# Shared Background steps, available to every feature in this directory.


@given("a running data platform")
def running_platform(world):
    assert world.endpoint


@given("a fresh stream")
def fresh_stream(world):
    world.connect()


@given(parsers.parse("a fresh stream bootstrapped with {partitions:d} partitions"))
def fresh_stream_bootstrapped(world, partitions):
    world.connect()
    world.run(lambda: world.laser.bootstrap(partitions))


@given("a new conversation")
def new_conversation(world):
    world.new_conversation()


@given("a managed-query connection that does not advertise read-your-writes")
def managed_without_ryw():
    pytest.skip("capability injection is not exposed in the Python SDK")


# Shared steps used by both the provenance and agent features.


@when(
    parsers.parse(
        'I send an agent command "{payload}" with agent "{agent}" and idempotency key "{key}"'
    )
)
def send_command_with_agent_and_key(world, payload, agent, key):
    world.send_command(payload, agent=agent, idempotency_key=key)


@when("I assemble the conversation")
def assemble_conversation(world):
    world.assemble()


@then(parsers.parse('the assembled message payload is "{payload}"'))
def assembled_payload_is(world, payload):
    assert [message.payload for message in world.assembled] == [payload.encode()]
