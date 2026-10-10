import asyncio

import laser_sdk as ls
import pytest

pytestmark = pytest.mark.integration


async def _workers(laser, names, seen):
    agents = []
    for name in names:

        async def handle(ctx, message, name=name):
            envelope = message.envelope
            if envelope is None or envelope["kind"] != "command":
                return
            seen.append((name, bytes(envelope["body"])))
            await ctx.respond_input(ls.AgentTopic.Sessions, name.encode())

        agent = laser.spawn_agent(name, ls.AgentTopic.Sessions, handle, poll_interval_ms=10)
        await agent.ready()
        agents.append(agent)
    return agents


async def _settled(bridge, task_id):
    for _ in range(50):
        current = await bridge.task(task_id)
        if current["status"]["state"].lower() not in ("working", "submitted"):
            return current
        await asyncio.sleep(0.2)
    raise AssertionError("the task never left the working state")


async def test_given_two_workers_when_submitting_to_one_then_only_that_worker_should_handle_it(
    laser,
):
    await laser.bootstrap(partitions=2, retention=ls.TopicRetention.expire_after(86_400_000))
    seen = []
    agents = await _workers(laser, ["alpha", "beta"], seen)
    try:
        bridge = ls.A2aBridge(laser, "a2a-gw", ls.AgentTopic.Sessions, ls.AgentTopic.Sessions)
        to_beta = await bridge.submit({"message": {"text": "for beta"}}, target="beta")
        parent = ls.Provenance(agent="a2a-gw").conversation_id
        to_alpha = await bridge.submit_in(
            parent, {"message": {"text": "for alpha"}}, target="alpha"
        )
        assert (await _settled(bridge, to_beta["id"]))["artifacts"][0]["text"] == "beta"
        assert (await _settled(bridge, to_alpha["id"]))["artifacts"][0]["text"] == "alpha"
        assert sorted(name for name, _ in seen) == ["alpha", "beta"]
    finally:
        for agent in agents:
            await agent.shutdown()


async def test_given_two_tool_workers_when_calling_one_then_only_that_worker_should_run_it(laser):
    await laser.bootstrap(partitions=2, retention=ls.TopicRetention.expire_after(86_400_000))
    seen = []
    agents = await _workers(laser, ["alpha", "beta"], seen)
    try:
        bridge = ls.McpBridge(
            laser,
            "mcp-gw",
            ls.AgentTopic.Sessions,
            ls.AgentTopic.Sessions,
            "tools",
            timeout_ms=15_000,
        )
        result = await bridge.call_tool("search", {"q": "x"}, target="beta")
        assert result["content"][0]["text"] == "beta"
        parent = ls.Provenance(agent="mcp-gw").conversation_id
        result = await bridge.call_tool_in(parent, "search", {"q": "y"}, target="alpha")
        assert result["content"][0]["text"] == "alpha"
        assert sorted(name for name, _ in seen) == ["alpha", "beta"]
    finally:
        for agent in agents:
            await agent.shutdown()


async def test_given_two_responders_when_asking_one_for_input_then_only_it_should_see_the_prompt(
    laser,
):
    await laser.bootstrap(partitions=2, retention=ls.TopicRetention.expire_after(86_400_000))
    seen = []
    agents = await _workers(laser, ["approver", "bystander"], seen)
    try:
        # One producer keeps both prompts in one session, so each responder
        # reads them in order: once the bystander has seen the broadcast, it
        # has already passed the addressed prompt.
        gate = laser.agdx(
            ls.AgentTopic.Sessions,
            "orchestrator",
            ls.Provenance(agent="orchestrator").conversation_id,
        )
        decision = await gate.request_input(
            ls.AgentTopic.Sessions, b"addressed", timeout_ms=10_000, target="approver"
        )
        assert decision == b"approver"
        await gate.request_input(ls.AgentTopic.Sessions, b"broadcast", timeout_ms=10_000)
        for _ in range(75):
            if any(name == "bystander" for name, _ in seen):
                break
            await asyncio.sleep(0.2)
        assert [body for name, body in seen if name == "bystander"] == [b"broadcast"]
        assert [body for name, body in seen if name == "approver"][0] == b"addressed"
    finally:
        for agent in agents:
            await agent.shutdown()
