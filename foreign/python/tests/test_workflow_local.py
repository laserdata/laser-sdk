import asyncio

import laser_sdk as ls
import pytest

pytestmark = pytest.mark.integration


async def test_given_local_steps_and_no_agent_topics_when_run_then_should_complete_in_order(
    iggy_endpoint,
):
    async with await ls.Laser.connect(iggy_endpoint) as laser:
        async with laser.scratch_stream("local") as scratch:

            async def process(outputs):
                await asyncio.sleep(0.05)
                return outputs["generate"].decode() + " processed"

            outcome = await (
                scratch.laser.workflow("jobs")
                .step("generate", run=lambda outputs: "job-7")
                .step("process", run=process)
                .step("finish", run=lambda outputs: b"completed")
                .run()
            )
            assert outcome.completed_steps == ["generate", "process", "finish"]
            assert outcome.status == "completed"
            assert outcome.text("process") == "job-7 processed"
            assert outcome.outputs["finish"] == b"completed"
            assert outcome.text("missing") is None


async def test_given_a_failed_local_step_when_the_run_resumes_then_should_run_only_that_step(
    laser,
):
    ran = []
    failing = {"finish": True}

    def body(label):
        def run(outputs):
            ran.append(label)
            if failing.pop(label, False):
                raise ValueError(f"{label} failed")
            return f"{label} done in process"

        return run

    def workflow(run_id):
        return (
            laser.workflow("jobs")
            .run_id(run_id)
            .step("generate", run=body("generate"))
            .step("process", run=body("process"))
            .step("finish", run=body("finish"))
        )

    run_id = ls.new_conversation_id()
    with pytest.raises(ValueError, match="finish failed"):
        await workflow(run_id).run()
    outcome = await workflow(run_id).run()
    assert ran == ["generate", "process", "finish", "finish"]
    assert outcome.completed_steps == ["generate", "process", "finish"]


async def test_given_a_local_step_that_fails_when_the_run_compensates_then_should_undo_in_reverse(
    laser,
):
    undone = []

    def undo(label):
        def compensate(outputs):
            undone.append(f"undo {label}")
            return b""

        return compensate

    def ship(outputs):
        raise RuntimeError("ship failed")

    workflow = (
        laser.workflow("billing")
        .step("reserve", run=lambda outputs: b"reserved", compensate=undo("reserve"))
        .step("charge", run=lambda outputs: b"charged", compensate=undo("charge"))
        .step("ship", run=ship)
    )
    with pytest.raises(RuntimeError, match="ship failed"):
        await workflow.run()
    assert undone == ["undo charge", "undo reserve"]


async def test_given_a_local_step_with_a_target_when_declared_then_should_refuse_it(laser):
    workflow = laser.workflow("refused")
    for routed in [{"to": "worker"}, {"build": lambda outputs: b""}, {"to_capable": "billing"}]:
        with pytest.raises(ls.InvalidError, match="run= alone"):
            workflow.step("charge", run=lambda outputs: b"", **routed)
    for exclusive in [
        {"exclusive": True},
        {"fence_namespace": "fences"},
        {"on_timeout": "reassign"},
    ]:
        with pytest.raises(
            ls.InvalidError, match="a local step runs in process, so it cannot be exclusive"
        ):
            workflow.step("charge", run=lambda outputs: b"", **exclusive)
    with pytest.raises(ls.InvalidError, match="run= for a local step"):
        workflow.step("charge")


async def test_given_a_local_step_past_its_deadline_when_run_then_should_fail_timed_out(
    laser,
):
    async def slow(outputs):
        await asyncio.sleep(5)
        return "late"

    workflow = laser.workflow("slow").step("wait", run=slow, deadline_ms=200)
    with pytest.raises(ls.StepFailedError) as raised:
        await workflow.run()
    assert (raised.value.reason, raised.value.target, raised.value.deadline_ms) == (
        "timed_out",
        "local step",
        200,
    )


async def test_given_a_rejected_local_output_when_verified_then_should_fail_unverified(
    laser,
):
    workflow = laser.workflow("checked").step(
        "draft", run=lambda outputs: "draft", verify=lambda output: output == b"final"
    )
    with pytest.raises(ls.StepFailedError) as raised:
        await workflow.run()
    assert (raised.value.step, raised.value.reason, raised.value.target) == (
        "draft",
        "unverified",
        "local step",
    )


async def test_given_a_verifier_that_raises_when_run_then_should_raise_its_exception_unchanged(
    laser,
):
    undone = []
    failure = ValueError("check failed")

    def check(output):
        raise failure

    def undo(outputs):
        undone.append("undo draft")
        return b""

    workflow = (
        laser.workflow("checked")
        .step("draft", run=lambda outputs: "draft", compensate=undo)
        .step("review", run=lambda outputs: "review", verify=check)
    )
    with pytest.raises(ValueError, match="check failed") as raised:
        await workflow.run()
    assert raised.value is failure
    assert undone == ["undo draft"]


async def test_given_a_one_invocation_budget_when_two_local_steps_run_then_should_exceed_it(
    laser,
):
    workflow = (
        laser.workflow("capped")
        .budget(ls.WorkflowBudget.unlimited().invocations(1))
        .step("first", run=lambda outputs: "one")
        .step("second", run=lambda outputs: "two")
    )
    with pytest.raises(ls.BudgetExceededError):
        await workflow.run()


async def test_given_an_unanswered_step_when_it_times_out_then_should_name_its_target_and_deadline(
    laser,
):
    await laser.bootstrap()
    workflow = laser.workflow("silent", fixed_inbox=ls.AgentTopic.Sessions).step(
        "wait", to="nobody", build=lambda outputs: "wait", deadline_ms=300
    )
    with pytest.raises(ls.StepFailedError) as raised:
        await workflow.run()
    assert raised.value.target == "agent `nobody`"
    assert raised.value.deadline_ms == 300
    assert str(raised.value).endswith("failed: timed_out, target agent `nobody`, deadline 300 ms")
