import asyncio

import laser_sdk as ls
import pytest

pytestmark = pytest.mark.integration


async def test_given_async_workflow_callbacks_when_run_then_should_await_build_and_verify(laser):
    await laser.bootstrap(1)
    seen = []

    async def handle(context, message):
        seen.append(bytes(message.body()))
        await context.respond(bytes(message.body()) + b":reply")

    class Builder:
        async def build(self, outputs):
            await asyncio.sleep(0)
            assert outputs == {}
            return b"task"

    class Check:
        async def verify(self, output):
            await asyncio.sleep(0)
            return output == b"task:reply"

    agent = laser.spawn_agent(
        "workflow-worker",
        ls.Topics.COMMANDS,
        handle,
        respond_on=ls.Topics.RESPONSES,
        poll_interval_ms=10,
    )
    try:
        await agent.ready()
        workflow = laser.workflow("async-workflow", fixed_inbox=ls.Topics.COMMANDS)
        workflow.step("first", to="workflow-worker", build=Builder(), verify=Check())
        assert await workflow.run() == {"first": b"task:reply"}
        assert seen == [b"task"]
    finally:
        await agent.shutdown()


async def test_given_async_compensation_when_a_later_build_fails_then_should_await_rollback(laser):
    await laser.bootstrap(1)
    seen = []

    async def handle(context, message):
        seen.append(bytes(message.body()))
        await context.respond(b"done")

    async def compensate(outputs):
        await asyncio.sleep(0)
        assert outputs == {"first": b"done"}
        return b"undo"

    async def refuse(outputs):
        await asyncio.sleep(0)
        raise ls.InvalidError("later build refused")

    agent = laser.spawn_agent(
        "rollback-worker",
        ls.Topics.COMMANDS,
        handle,
        respond_on=ls.Topics.RESPONSES,
        poll_interval_ms=10,
    )
    try:
        await agent.ready()
        workflow = laser.workflow("rollback-workflow", fixed_inbox=ls.Topics.COMMANDS)
        workflow.step(
            "first", to="rollback-worker", build=lambda outputs: b"do", compensate=compensate
        )
        workflow.step("second", to="rollback-worker", after=["first"], build=refuse)
        with pytest.raises(ls.InvalidError, match="later build refused"):
            await workflow.run()
        assert seen == [b"do", b"undo"]
    finally:
        await agent.shutdown()


async def test_given_async_verifier_error_when_a_step_replies_then_should_keep_the_error_class(
    laser,
):
    await laser.bootstrap(1)

    async def handle(context, message):
        await context.respond(b"reply")

    async def verify(output):
        await asyncio.sleep(0)
        raise ls.ProtocolError("verification refused")

    agent = laser.spawn_agent(
        "verified-worker",
        ls.Topics.COMMANDS,
        handle,
        respond_on=ls.Topics.RESPONSES,
        poll_interval_ms=10,
    )
    try:
        await agent.ready()
        workflow = laser.workflow("verified-workflow", fixed_inbox=ls.Topics.COMMANDS)
        workflow.step("first", to="verified-worker", build=lambda outputs: b"task", verify=verify)
        with pytest.raises(ls.ProtocolError, match="verification refused"):
            await workflow.run()
    finally:
        await agent.shutdown()
