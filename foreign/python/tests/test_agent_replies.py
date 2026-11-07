import asyncio
import time

import laser_sdk as ls
import pytest

pytestmark = pytest.mark.integration


async def test_given_a_python_agent_when_it_answers_with_reply_on_then_should_complete_the_request(
    laser,
):
    await laser.bootstrap(partitions=2, retention=ls.TopicRetention.expire_after(86_400_000))

    async def handle(ctx, message):
        await ctx.reply_on(ls.AgentTopic.Sessions, b"answer")

    agent = laser.spawn_agent("replier", ls.AgentTopic.Sessions, handle)
    try:
        await agent.ready()
        reply = await laser.request(
            ls.AgentTopic.Sessions,
            ls.AgentTopic.Sessions,
            b"question",
            ls.Provenance(agent="caller"),
            timeout_ms=10_000,
        )
        assert reply.payload == b"answer"
        assert reply.provenance.target_agent_id == "caller"
    finally:
        await agent.shutdown()


async def test_given_an_uncorrelated_send_when_a_python_agent_responds_then_should_correlate_the_reply(  # noqa: E501
    laser,
):
    await laser.bootstrap(partitions=2, retention=ls.TopicRetention.expire_after(86_400_000))

    async def handle(ctx, message):
        await ctx.respond(b"reply")

    agent = laser.spawn_agent(
        "responder", ls.AgentTopic.Sessions, handle, respond_on="agent.sessions"
    )
    try:
        await agent.ready()
        root = ls.Provenance(agent="caller")
        child = ls.Provenance(
            agent="caller",
            parent_conversation_id=root.conversation_id,
            root_conversation_id=root.conversation_id,
        )
        await laser.send_agent(ls.AgentTopic.Sessions, b"plain", child)
        reply = None
        for _ in range(50):
            turns = await laser.assemble_context(child.conversation_id, topics=["agent.sessions"])
            reply = next((turn for turn in turns if turn.payload == b"reply"), None)
            if reply is not None:
                break
            await asyncio.sleep(0.2)
        assert reply is not None
        assert reply.provenance.correlation_id is not None
        assert reply.provenance.parent_conversation_id == root.conversation_id
        assert reply.provenance.target_agent_id == "caller"
    finally:
        await agent.shutdown()


async def test_given_a_per_agent_topic_layout_when_spawning_on_the_lane_then_should_read_the_declared_topic(  # noqa: E501
    laser,
):
    layout = ls.SessionLayout.PerAgentTopic({"planner": "planner.inbox", "worker": "worker.inbox"})
    sessions = laser.sessions(layout=layout)
    await sessions.bootstrap(1, ls.TopicRetention.expire_after(3_600_000))

    async def handle(ctx, message):
        await ctx.respond(bytes(message.body()))

    worker = laser.spawn_agent(
        "worker",
        ls.AgentTopic.Sessions,
        handle,
        respond_on=ls.AgentTopic.Sessions,
        sessions=sessions,
    )
    try:
        await worker.ready()
        outcome = await laser.contract(
            None,
            b"ping",
            source="planner",
            agent="worker",
            fixed_inbox=ls.AgentTopic.Sessions,
            deadline_ms=10_000,
        )
        assert isinstance(outcome, ls.Contract.Completed)
    finally:
        await worker.shutdown()


async def test_given_ack_on_pickup_without_respond_on_when_spawning_then_should_raise_config_error(
    laser,
):
    async def handle(ctx, message):
        return None

    with pytest.raises(ls.ConfigError):
        laser.spawn_agent("worker", ls.AgentTopic.Sessions, handle, ack_on_pickup=True)


async def test_given_a_python_agent_refusing_a_contract_when_contracted_then_should_fail_at_once(
    laser,
):
    await laser.bootstrap(partitions=2, retention=ls.TopicRetention.expire_after(86_400_000))

    async def handle(ctx, message):
        raise ls.RejectedError("the rotation window is closed")

    agent = laser.spawn_agent(
        "refuser", ls.AgentTopic.Sessions, handle, respond_on="agent.sessions"
    )
    try:
        await agent.ready()
        started = time.monotonic()
        outcome = await laser.contract(
            None,
            b"rotate-42",
            source="orchestrator",
            agent="refuser",
            fixed_inbox=ls.AgentTopic.Sessions,
            deadline_ms=30_000,
        )
        assert isinstance(outcome, ls.Contract.Failed)
        assert time.monotonic() - started < 15
    finally:
        await agent.shutdown()


async def test_given_a_python_handler_waiting_on_an_approval_when_another_session_sends_work_then_should_handle_it_and_resume_once(  # noqa: E501
    laser,
):
    await laser.bootstrap(partitions=2, retention=ls.TopicRetention.expire_after(86_400_000))
    await laser.topic(ls.AgentTopic.Control).ensure(partitions=1)
    gated = ls.Provenance(agent="client")
    other = ls.Provenance(agent="client")
    passes = []

    async def handle(ctx, message):
        session = message.provenance.conversation_id
        passes.append((session, "before"))
        if session == gated.conversation_id:
            grant = await ctx.approval_gate(
                "approver", ls.AgentTopic.Sessions, b"approve?", timeout_ms=60_000
            )
            assert grant.decision == b"approved"
        passes.append((session, "after"))

    async def approve(ctx, message):
        await ctx.respond_input(ls.AgentTopic.Sessions, b"approved")

    gatekeeper = laser.spawn_agent("gatekeeper", ls.AgentTopic.Sessions, handle)
    approver = None
    try:
        await gatekeeper.ready()
        for provenance in (gated, other):
            await laser.agdx(ls.AgentTopic.Sessions, "client", provenance.conversation_id).command(
                ls.new_conversation_id(), b"work", target="gatekeeper"
            )
        for _ in range(75):
            if (other.conversation_id, "after") in passes:
                break
            await asyncio.sleep(0.2)
        assert (other.conversation_id, "after") in passes

        approver = laser.spawn_agent(
            "approver", ls.AgentTopic.Sessions, approve, operations=["input_request"]
        )
        for _ in range(75):
            if (gated.conversation_id, "after") in passes:
                break
            await asyncio.sleep(0.2)
        stages = [stage for session, stage in passes if session == gated.conversation_id]
        assert stages == ["before", "before", "after"]
    finally:
        await gatekeeper.shutdown()
        if approver is not None:
            await approver.shutdown()
