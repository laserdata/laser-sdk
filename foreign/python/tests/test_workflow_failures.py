import asyncio

import laser_sdk as ls
import pytest

pytestmark = pytest.mark.integration


async def test_given_a_rejected_reply_when_verified_then_should_fail_typed_and_compensate_it(laser):
    await laser.bootstrap(1, retention=ls.TopicRetention.expire_after(86_400_000))
    seen = []

    async def handle(context, message):
        seen.append(bytes(message.body()))
        await context.respond(b"done")

    agent = laser.spawn_agent(
        "rejected-worker",
        ls.AgentTopic.Sessions,
        handle,
        respond_on=ls.AgentTopic.Sessions,
    )
    try:
        await agent.ready()
        workflow = laser.workflow("rejected-workflow", fixed_inbox=ls.AgentTopic.Sessions)
        workflow.step(
            "charge",
            to="rejected-worker",
            build=lambda outputs: b"charge",
            verify=lambda output: False,
            compensate=lambda outputs: b"refund",
        )
        with pytest.raises(ls.StepFailedError) as raised:
            await workflow.run()
        assert raised.value.step == "charge"
        assert raised.value.reason == "unverified"
        assert raised.value.retryable is False
        assert seen == [b"charge", b"refund"]
    finally:
        await agent.shutdown()


async def test_given_a_step_nobody_answers_when_run_then_should_fail_retryable_as_timed_out(laser):
    await laser.bootstrap(1, retention=ls.TopicRetention.expire_after(86_400_000))
    workflow = laser.workflow("unanswered-workflow", fixed_inbox=ls.AgentTopic.Sessions)
    workflow.step("triage", to="nobody", build=lambda outputs: b"task", deadline_ms=300)
    with pytest.raises(ls.StepFailedError) as raised:
        await workflow.run()
    assert raised.value.reason == "timed_out"
    assert raised.value.retryable is True
    assert raised.value.run != ""


async def test_given_a_step_command_that_expired_unconsumed_when_a_fresh_process_resumes_the_run_then_should_run_the_resent_step(  # noqa: E501
    laser, iggy_endpoint
):
    await laser.bootstrap(4, retention=ls.TopicRetention.expire_after(86_400_000))
    run = ls.new_conversation_id()
    first = laser.workflow("expiryflow", fixed_inbox=ls.AgentTopic.Sessions)
    first.run_id(run)
    first.step("work", to="expiry-worker", build=lambda outputs: b"go", deadline_ms=1_000)
    with pytest.raises(ls.StepFailedError) as stopped:
        await first.run()
    assert stopped.value.reason == "timed_out"
    calls = []

    async def handle(context, message):
        calls.append(bytes(message.body()))
        await context.respond(b"counted")

    # A fresh process resumes the run and sends the step again before its
    # worker starts, so the worker reads the expired copy first.
    stream = laser.default_stream
    async with (
        await ls.Laser.connect(iggy_endpoint, stream=stream) as resuming_process,
        await ls.Laser.connect(iggy_endpoint, stream=stream) as worker_process,
    ):
        resumed = resuming_process.workflow("expiryflow", fixed_inbox=ls.AgentTopic.Sessions)
        resumed.run_id(run)
        resumed.step("work", to="expiry-worker", build=lambda outputs: b"go", deadline_ms=20_000)
        running = asyncio.ensure_future(resumed.run())
        await _eventually(
            lambda: _envelopes_on_log(laser, f"{run}/work/1", "command", "expiry-worker"), 2
        )
        agent = worker_process.spawn_agent(
            "expiry-worker",
            ls.AgentTopic.Sessions,
            handle,
            respond_on=ls.AgentTopic.Sessions,
            ack_on_pickup=True,
        )
        try:
            outcome = await running
        finally:
            await agent.shutdown()
    assert calls == [b"go"]
    assert bytes(outcome.outputs["work"]) == b"counted"


async def test_given_an_expired_step_command_answered_before_the_resume_when_a_fresh_process_resumes_the_run_then_should_run_the_resent_step(  # noqa: E501
    laser, iggy_endpoint
):
    await laser.bootstrap(4, retention=ls.TopicRetention.expire_after(86_400_000))
    run = ls.new_conversation_id()
    first = laser.workflow("answeredflow", fixed_inbox=ls.AgentTopic.Sessions)
    first.run_id(run)
    first.step("work", to="answered-worker", build=lambda outputs: b"go", deadline_ms=1_000)
    with pytest.raises(ls.StepFailedError) as stopped:
        await first.run()
    assert stopped.value.reason == "timed_out"
    calls = []

    async def handle(context, message):
        calls.append(bytes(message.body()))
        await context.respond(b"counted")

    # The worker starts in its own process, drops the expired copy, and
    # answers that it was not consumed before the run resumes.
    stream = laser.default_stream
    async with (
        await ls.Laser.connect(iggy_endpoint, stream=stream) as worker_process,
        await ls.Laser.connect(iggy_endpoint, stream=stream) as resuming_process,
    ):
        agent = worker_process.spawn_agent(
            "answered-worker",
            ls.AgentTopic.Sessions,
            handle,
            respond_on=ls.AgentTopic.Sessions,
            ack_on_pickup=True,
        )
        try:
            await _eventually(
                lambda: _envelopes_on_log(laser, f"{run}/work/1", "error", "answeredflow"), 1
            )
            resumed = resuming_process.workflow("answeredflow", fixed_inbox=ls.AgentTopic.Sessions)
            resumed.run_id(run)
            resumed.step(
                "work", to="answered-worker", build=lambda outputs: b"go", deadline_ms=20_000
            )
            outcome = await resumed.run()
        finally:
            await agent.shutdown()
    assert calls == [b"go"]
    assert bytes(outcome.outputs["work"]) == b"counted"


async def test_given_duplicate_step_labels_when_run_then_should_refuse_before_any_dispatch(laser):
    await laser.bootstrap(1, retention=ls.TopicRetention.expire_after(86_400_000))
    seen = []

    async def handle(context, message):
        seen.append(bytes(message.body()))
        await context.respond(b"done")

    agent = laser.spawn_agent(
        "duplicate-worker",
        ls.AgentTopic.Sessions,
        handle,
        respond_on=ls.AgentTopic.Sessions,
    )
    try:
        await agent.ready()
        workflow = laser.workflow("duplicate-workflow", fixed_inbox=ls.AgentTopic.Sessions)
        workflow.step("same", to="duplicate-worker", build=lambda outputs: b"first")
        workflow.step("same", to="duplicate-worker", build=lambda outputs: b"second")
        with pytest.raises(ls.InvalidError, match="declared twice"):
            await workflow.run()
        assert seen == []
    finally:
        await agent.shutdown()


async def test_given_a_zero_quorum_when_gathering_then_should_refuse_before_any_branch(laser):
    message = ls.agent_message(b"task", ls.Provenance(agent="caller"))
    context = ls.agent_ctx(laser, message, agent="caller", respond_on="agent.sessions")
    with pytest.raises(ls.InvalidError, match="quorum"):
        await context.fan_out("review", b"check", deadline_ms=100, gather="quorum", quorum=0)
    with pytest.raises(ls.InvalidError, match="quorum"):
        await laser.scatter(
            "review", b"check", source="caller", deadline_ms=100, gather="quorum", quorum=0
        )


def test_given_a_completed_contract_when_read_then_should_name_its_reply():
    reply = ls.agent_message(b"done", ls.Provenance(agent="worker"))
    outcome = ls.Contract.Completed(reply)
    assert outcome.reply.body() == b"done"
    assert ls.Contract.Failed(reply=reply).reply.body() == b"done"


async def _envelopes_on_log(laser, step, kind, target):
    """How many AGDX envelopes of `kind` addressed to `target` the step's session holds."""
    conversation = ls.derive_conversation_id(step)
    messages = await laser.context(conversation).fetch(topics=["agent.sessions"], n=50)
    return sum(
        1
        for message in messages
        if message.envelope is not None
        and message.envelope["kind"] == kind
        and message.envelope.get("target") == target
    )


async def _eventually(count, expected):
    for _ in range(200):
        if await count() == expected:
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"the log never held {expected} matching envelopes")
