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
