import asyncio
import contextvars

import laser_sdk as ls
import pytest

pytestmark = pytest.mark.integration

REQUEST = contextvars.ContextVar("request")


async def test_given_custom_routes_when_called_then_should_preserve_candidates_and_errors(
    laser, iggy_endpoint
):
    await laser.bootstrap(1, retention=ls.TopicRetention.expire_after(86_400_000))
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
                if candidate.agent == "route-two"
            )

    try:
        for name, cost in [("route-one", 1), ("route-two", 7)]:
            connection = await ls.Laser.connect(iggy_endpoint, stream=laser.default_stream)
            connections.append(connection)
            agent = connection.spawn_agent(
                name,
                "agent.sessions",
                worker(name),
                respond_on="agent.sessions",
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
        assert set(registry.resolve_targets(all_capable="diagnose")) == {"route-one", "route-two"}
        assert registry.resolve_targets(to="route-one") == ["route-one"]
        assert registry.resolve_targets(broadcast=True) == []

        reply = await laser.contract(
            "diagnose",
            b"work",
            source="caller",
            fixed_inbox="agent.sessions",
            deadline_ms=10_000,
            policy=Scorer(),
        )
        assert isinstance(reply, ls.Contract.Completed)
        assert bytes(reply[0].body()) == b"route-two:work"
        skill, candidates = seen[0]
        assert skill == "diagnose"
        assert {candidate.agent for candidate in candidates} == {"route-one", "route-two"}
        assert all(candidate.card.agent == candidate.agent for candidate in candidates)
        assert all(candidate.capability["skill_id"] == "diagnose" for candidate in candidates)

        report = await laser.agent("caller").contract(
            None,
            b"again",
            skill="diagnose",
            fixed_inbox="agent.sessions",
            deadline_ms=10_000,
            policy=lambda skill, candidates: next(
                index
                for index, candidate in enumerate(candidates)
                if candidate.agent == "route-one"
            ),
        )
        assert isinstance(report, ls.Contract.Completed)
        assert bytes(report[0].body()) == b"route-one:again"

        with pytest.raises(ls.LaserError) as refused:
            await laser.contract(
                "diagnose",
                b"refuse",
                source="caller",
                fixed_inbox="agent.sessions",
                policy=lambda skill, candidates: None,
            )
        assert refused.value.no_capable_agent

        rejection = ls.InvalidError("custom scorer refused")

        def broken(skill, candidates):
            raise rejection

        with pytest.raises(ls.InvalidError) as failure:
            await laser.contract(
                "diagnose", b"invalid", source="caller", fixed_inbox="agent.sessions", policy=broken
            )
        assert failure.value is rejection

        async def asynchronous(skill, candidates):
            return 0

        with pytest.raises(ls.InvalidError, match="synchronously"):
            await laser.contract(
                "diagnose",
                b"invalid",
                source="caller",
                fixed_inbox="agent.sessions",
                policy=asynchronous,
            )
    finally:
        for agent in workers:
            await agent.shutdown()
        for connection in connections:
            await connection.close()


async def test_given_middleware_when_a_handler_retries_then_should_receive_typed_errors_and_success(
    laser,
):
    await laser.bootstrap(1, retention=ls.TopicRetention.expire_after(86_400_000))
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
        "agent.sessions",
        handle,
        middleware=[Middleware()],
        retry_max_attempts=2,
        retry_base_delay_ms=1,
    )
    try:
        await agent.ready()
        await laser.send_agent("agent.sessions", b"work", ls.Provenance())
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
    await laser.bootstrap(1, retention=ls.TopicRetention.expire_after(86_400_000))
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
        "agent.sessions",
        handle,
        middleware=[Middleware()],
        dead_letter=sink,
    )
    try:
        await agent.ready()
        await laser.send_agent("agent.sessions", b"poison", ls.Provenance())
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
        position = observed["message"].id
        partition, offset = position.partition_id, position.offset
        assert int.from_bytes(source[8:12], "big") == partition
        assert int.from_bytes(source[12:20], "big") == offset
        error = observed["result"]["error"]
        assert isinstance(error, ls.UnsupportedError)
        assert error.unsupported
        assert not error.retryable
        if publish_fails:
            publish_error = observed["publish_error"]
            assert isinstance(publish_error, ls.PublishFailedError)
            assert isinstance(publish_error.__cause__, ls.TransportError)
            assert not publish_error.retryable
            with pytest.raises((ls.PublishFailedError, ls.TransportError)) as stopped:
                await agent.join()
            assert not stopped.value.retryable
        else:
            assert observed["publish_error"] is None
    finally:
        await agent.shutdown()


async def test_given_bad_provenance_when_dead_lettered_then_should_keep_the_payload(
    laser,
):
    await laser.bootstrap(1, retention=ls.TopicRetention.expire_after(86_400_000))
    dead_lettered = asyncio.Event()
    observed = {}

    async def handle(context, message):
        raise AssertionError("malformed provenance must not reach the handler")

    loop = asyncio.get_running_loop()

    def sink(message, capsule, publish_error):
        observed.update(message=message, capsule=capsule, publish_error=publish_error)
        loop.call_soon_threadsafe(dead_lettered.set)

    agent = laser.spawn_agent("malformed-dead-letter", "agent.sessions", handle, dead_letter=sink)
    try:
        await agent.ready()
        await laser.topic("agent.sessions").send(b"malformed")
        filters = (await laser.capabilities()).filters
        if filters.group_policy_reads and filters.native and filters.catalog:
            # A record without `agdx.to` never passes the addressee filter the
            # agent's group is bound to, so a server that binds the filter (group
            # reads, filtered reads, and the catalog) keeps it on the log and the
            # agent neither handles nor dead-letters it.
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(dead_lettered.wait(), 2)
            assert not observed
        else:
            await asyncio.wait_for(dead_lettered.wait(), 10)
            assert observed["message"] is None
            assert bytes(observed["capsule"]["payload"]) == b"malformed"
            assert observed["capsule"]["reason"] == 3
            assert observed["publish_error"] is None
    finally:
        await agent.shutdown()


async def test_given_partition_lanes_when_hooks_run_then_should_use_the_spawning_loop_and_context(
    laser,
):
    await laser.bootstrap(1, retention=ls.TopicRetention.expire_after(86_400_000))
    handled = asyncio.Event()
    seen = {}

    class Handler:
        async def handle(self, context, message):
            seen["handler"] = REQUEST.get(None)
            handled.set()

    class Deduplicator:
        def observe(self, key):
            seen["dedup"] = (key, REQUEST.get(None))
            return True

    REQUEST.set("spawner")
    agent = laser.spawn_agent(
        "lane-hooks",
        "agent.sessions",
        Handler(),
        dedup=Deduplicator(),
        max_partitions=2,
    )
    try:
        await agent.ready()
        await laser.send_agent("agent.sessions", b"work", ls.Provenance(idempotency_key="lane-key"))
        await asyncio.wait_for(handled.wait(), 10)
        assert seen == {"handler": "spawner", "dedup": ("lane-key", "spawner")}
    finally:
        await agent.shutdown()


async def test_given_a_plain_consolidator_when_ticking_then_should_pass_the_agent_scope(laser):
    await laser.bootstrap(1, retention=ls.TopicRetention.expire_after(86_400_000))
    scopes = []
    ticked = asyncio.Event()
    loop = asyncio.get_running_loop()

    def consolidate(scope):
        scopes.append(scope)
        loop.call_soon_threadsafe(ticked.set)

    async def handle(context, message):
        pass

    agent = laser.spawn_agent(
        "consolidating-agent",
        "agent.sessions",
        handle,
        consolidate_every_ms=10,
        consolidator=consolidate,
    )
    try:
        await agent.ready()
        await asyncio.wait_for(ticked.wait(), 10)
    finally:
        await agent.shutdown()
    assert scopes[0] == {
        "stream": None,
        "user": None,
        "agent": "consolidating-agent",
        "conversation": None,
        "application": None,
        "lifetime": "session",
    }


async def test_given_hooks_without_their_method_when_spawning_then_should_refuse_them(laser):
    async def handle(context, message):
        pass

    with pytest.raises(ls.InvalidError, match="handle"):
        laser.spawn_agent("refused-handler", "agent.sessions", object())
    with pytest.raises(ls.InvalidError, match="observe"):
        laser.spawn_agent("refused-dedup", "agent.sessions", handle, dedup=object())
    with pytest.raises(ls.InvalidError, match="consolidate"):
        laser.spawn_agent(
            "refused-consolidator",
            "agent.sessions",
            handle,
            consolidate_every_ms=10,
            consolidator=object(),
        )
