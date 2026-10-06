import asyncio

import laser_sdk as ls
import pytest

pytestmark = pytest.mark.integration


async def test_given_custom_routes_when_called_then_should_preserve_candidates_and_errors(
    laser, iggy_endpoint
):
    await laser.bootstrap(1)
    connections = []
    workers = []
    seen = []

    def worker(name):
        async def handle(context, message):
            await context.respond(name.encode() + b":" + bytes(message.body()))

        return handle

    class Scorer:
        def select(self, skill, candidates):
            seen.append((skill, candidates))
            return next(
                index
                for index, candidate in enumerate(candidates)
                if candidate["agent"] == "route-two"
            )

    try:
        for name, cost in [("route-one", 1), ("route-two", 7)]:
            connection = await ls.Laser.connect(iggy_endpoint, stream=laser.default_stream)
            connections.append(connection)
            agent = connection.spawn_agent(
                name,
                "agent.commands",
                worker(name),
                respond_on="agent.responses",
                capabilities=[{"skill_id": "diagnose", "cost_class": cost}],
            )
            workers.append(agent)
            await agent.ready()

        registry = laser.agent_registry()
        for _ in range(100):
            await registry.refresh()
            if len(registry.resolve("diagnose")) == 2:
                break
            await asyncio.sleep(0.1)
        assert len(registry.resolve("diagnose")) == 2

        reply = await laser.contract(
            "diagnose",
            b"work",
            source="caller",
            fixed_inbox="agent.commands",
            deadline_ms=10_000,
            policy=Scorer(),
        )
        assert bytes(reply) == b"route-two:work"
        skill, candidates = seen[0]
        assert skill == "diagnose"
        assert {candidate["agent"] for candidate in candidates} == {"route-one", "route-two"}
        assert all(candidate["card"]["agent"] == candidate["agent"] for candidate in candidates)
        assert all(candidate["capability"]["skill_id"] == "diagnose" for candidate in candidates)

        report = await laser.agent("caller").contract(
            None,
            b"again",
            skill="diagnose",
            fixed_inbox="agent.commands",
            deadline_ms=10_000,
            policy=lambda skill, candidates: next(
                index
                for index, candidate in enumerate(candidates)
                if candidate["agent"] == "route-one"
            ),
        )
        assert report["state"] == "completed"
        assert bytes(report["body"]) == b"route-one:again"

        with pytest.raises(ls.LaserError) as refused:
            await laser.contract(
                "diagnose", b"refuse", source="caller", policy=lambda skill, candidates: None
            )
        assert refused.value.no_capable_agent

        rejection = ls.InvalidError("custom scorer refused")

        def broken(skill, candidates):
            raise rejection

        with pytest.raises(ls.InvalidError) as failure:
            await laser.contract("diagnose", b"invalid", source="caller", policy=broken)
        assert failure.value is rejection

        async def asynchronous(skill, candidates):
            return 0

        with pytest.raises(ls.InvalidError, match="synchronously"):
            await laser.contract("diagnose", b"invalid", source="caller", policy=asynchronous)
    finally:
        for agent in workers:
            await agent.shutdown()
        for connection in connections:
            await connection.close()


async def test_given_middleware_when_a_handler_retries_then_should_receive_typed_errors_and_success(
    laser,
):
    await laser.bootstrap(1)
    outcomes = []
    finished = asyncio.Event()
    attempts = 0

    class Middleware:
        def before_handle(self, message):
            assert message.payload == b"work"

        async def after_handle(self, message, result, attempt):
            outcomes.append((result, attempt))
            if result["ok"]:
                finished.set()

    async def handle(context, message):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("temporary failure")

    agent = laser.spawn_agent(
        "full-outcome",
        "agent.commands",
        handle,
        middleware=[Middleware()],
        retry_max_attempts=2,
        retry_base_delay_ms=1,
    )
    try:
        await agent.ready()
        await laser.send_agent("agent.commands", b"work", ls.Provenance())
        await asyncio.wait_for(finished.wait(), 10)
        assert len(outcomes) == 2
        first, first_attempt = outcomes[0]
        second, second_attempt = outcomes[1]
        assert first_attempt == 1
        assert not first["ok"]
        assert isinstance(first["error"], ls.LaserError)
        assert first["error"].retryable
        assert "temporary failure" in str(first["error"])
        assert second_attempt == 2
        assert second == {"ok": True, "error": None}
    finally:
        await agent.shutdown()


@pytest.mark.parametrize("publish_fails", [False, True])
async def test_given_handler_refusal_when_dead_lettered_then_should_receive_capsule_and_error(
    laser, publish_fails
):
    await laser.bootstrap(1)
    published = asyncio.Event()
    dead_lettered = asyncio.Event()
    observed = {}

    class Middleware:
        async def after_handle(self, message, result, attempt):
            observed["result"] = result

    async def handle(context, message):
        if publish_fails:
            await published.wait()
            await laser.close()
        raise ls.UnsupportedError("handler refuses this operation")

    async def sink(message, capsule, publish_error):
        observed.update(message=message, capsule=capsule, publish_error=publish_error)
        dead_lettered.set()

    agent = laser.spawn_agent(
        "full-dead-letter",
        "agent.commands",
        handle,
        middleware=[Middleware()],
        dead_letter=sink,
    )
    try:
        await agent.ready()
        await laser.send_agent("agent.commands", b"poison", ls.Provenance())
        published.set()
        await asyncio.wait_for(dead_lettered.wait(), 10)
        assert observed["message"].payload == b"poison"
        capsule = observed["capsule"]
        assert bytes(capsule["payload"]) == b"poison"
        assert capsule["attempts"] == 1
        assert capsule["reason"] == 2
        assert "handler refuses" in capsule["detail"]
        source = bytes(capsule["source"])
        assert len(source) == 20
        partition, offset = map(int, observed["message"].message_id.split(":"))
        assert int.from_bytes(source[8:12], "big") == partition
        assert int.from_bytes(source[12:20], "big") == offset
        error = observed["result"]["error"]
        assert isinstance(error, ls.UnsupportedError)
        assert error.unsupported
        assert not error.retryable
        if publish_fails:
            publish_error = observed["publish_error"]
            assert isinstance(publish_error, ls.TransportError)
            assert not publish_error.retryable
            with pytest.raises(ls.TransportError):
                await agent.join()
        else:
            assert observed["publish_error"] is None
    finally:
        await agent.shutdown()


async def test_given_bad_provenance_when_dead_lettered_then_should_keep_the_payload(
    laser,
):
    await laser.bootstrap(1)
    dead_lettered = asyncio.Event()
    observed = {}

    async def handle(context, message):
        raise AssertionError("malformed provenance must not reach the handler")

    loop = asyncio.get_running_loop()

    def sink(message, capsule, publish_error):
        observed.update(message=message, capsule=capsule, publish_error=publish_error)
        loop.call_soon_threadsafe(dead_lettered.set)

    agent = laser.spawn_agent("malformed-dead-letter", "agent.commands", handle, dead_letter=sink)
    try:
        await agent.ready()
        await laser.topic("agent.commands").send(b"malformed")
        await asyncio.wait_for(dead_lettered.wait(), 10)
        assert observed["message"] is None
        assert bytes(observed["capsule"]["payload"]) == b"malformed"
        assert observed["capsule"]["reason"] == 3
        assert observed["publish_error"] is None
    finally:
        await agent.shutdown()
