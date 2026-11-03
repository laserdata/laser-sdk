"""Record independent sessions, child work, explicit results, and linked resources."""

from __future__ import annotations

import asyncio

import _common
import laser_sdk as ls

EXAMPLE = "sessions"


async def acting_on_last(session, sessions, managed):
    turns = await session.context()
    if not turns:
        raise RuntimeError("the recorded source is unavailable")
    message = turns[-1].message
    lane = (await sessions.sources(session.conversation)).get("lane") if managed else None
    return session.acting_on(
        {
            "Message": {
                "stream": message.stream_id,
                "topic": message.topic_id,
                "partition": message.id.partition_id,
                "offset": message.id.offset,
                "generation": lane[1] if lane else None,
                "conversation": session.conversation,
            }
        }
    )


async def task(sessions, root, label, agent, result):
    child, lease = await (
        sessions.create(label)
        .namespace(f"ops/{root.conversation}")
        .agent(agent)
        .parent(root.conversation, root.conversation)
        .begin()
    )
    try:
        call = await child.tool("inspect_service", {"service": "api", "task": label})
        correlation = call.correlation
        receipt = await call.complete(result.encode())
        await child.state().set("result", result)
        turns = await child.context()
        turn = next(t for t in turns if t.message.envelope.get("record") == receipt.record)
        message = turn.message
        at = ls.LogPosition(
            message.stream_id, message.topic_id, message.id.partition_id, message.id.offset
        )
        await root.append(
            ls.event_envelope(
                ls.new_conversation_id(),
                root.conversation,
                "triage",
                result.encode(),
                correlation=correlation,
                cause=receipt.record,
                cause_at=at,
            )
        )
        await root.state().set(label, result)
        await child.end()
        return child
    finally:
        lease.release()


async def report(session, label, sessions, managed):
    events = len(await session.context())
    links = len((await sessions.links(session.conversation)).get("links", [])) if managed else 0
    print(f"  {label}: {events} events, {links} resource links")


async def main():
    laser = await _common.connect(EXAMPLE)
    try:
        sessions = laser.sessions()
        bootstrap = await sessions.bootstrap(
            _common.PARTITIONS, ls.TopicRetention.expire_after(86_400_000)
        )
        registered = bootstrap.registered
        capabilities = await laser.capabilities()
        if capabilities.sessions and not registered:
            raise RuntimeError("the session source registration is unavailable")
        _common.phase("one root incident, two child tasks, explicit result collection")
        root, root_lease = (
            await sessions.create("incident-42").namespace("ops").agent("triage").begin()
        )
        maintenance_lease = None
        try:
            observer = root.as_agent("specialist")
            call = await observer.tool("read_metrics", {"service": "api"})
            await call.complete(b"latency increased")
            diagnosis = await task(sessions, root, "diagnosis", "specialist", "cache saturation")
            remediation = await task(
                sessions, root, "remediation", "resolver", "reduce cache pressure"
            )
            assembled = await root.assemble(ls.LastN(20))
            call = await root.model(
                ls.ModelRequest("mock", b"summarize the recorded findings"), assembled
            )
            await call.complete(ls.ModelResponse(b"reduce cache pressure and observe latency"))
            _common.phase("a separate maintenance session shares resources in the same stream")
            maintenance, maintenance_lease = (
                await sessions.create("maintenance-7").namespace("ops").agent("resolver").begin()
            )
            await maintenance.state().set("task", "verify cache capacity")
            root_resources = await acting_on_last(root, sessions, capabilities.sessions)
            maintenance_resources = await acting_on_last(
                maintenance, sessions, capabilities.sessions
            )
            if capabilities.kv.available:
                await (
                    root.kv("infra").set("service:api").json({"finding": "cache saturation"}).send()
                )
                await (
                    maintenance.kv("infra")
                    .set("maintenance:api")
                    .json({"task": "verify cache capacity"})
                    .send()
                )
            if capabilities.graph:
                await root_resources.linked_graph("infra").link(
                    "service:api", "depends_on", "service:cache"
                )
                await maintenance_resources.linked_graph("infra").link(
                    "service:api", "observed_by", "agent:resolver"
                )
            await root_resources.linked_memory().remember(
                "api latency increased when the cache saturated"
            )
            await root.end()
            await maintenance.end()
        finally:
            root_lease.release()
            if maintenance_lease is not None:
                maintenance_lease.release()
        if capabilities.sessions:

            async def ready():
                for _ in range(600):
                    info = await sessions.get(root.conversation)
                    links = await sessions.links(root.conversation)
                    if info["status"] == "completed" and any(
                        link["surface"] == "memory" for link in links.get("links", [])
                    ):
                        return
                    await asyncio.sleep(0.1)
                raise RuntimeError("the session example views did not converge")

            await ready()
        for session, label in (
            (root, "incident-42"),
            (diagnosis, "diagnosis"),
            (remediation, "remediation"),
            (maintenance, "maintenance-7"),
        ):
            await report(session, label, sessions, capabilities.sessions)
        print("  2 independent roots, 2 child sessions, explicit parent results")
    finally:
        await laser.close()


if __name__ == "__main__":
    asyncio.run(main())
