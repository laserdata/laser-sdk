import asyncio

import laser_sdk as ls
import pytest

pytestmark = pytest.mark.integration

BRIDGE_KEY = ls.SigningKey.from_bytes(bytes([61] * 32))
PARAMS = {"message": {"messageId": "m-1", "role": "ROLE_USER", "parts": [{"text": "hi"}]}}


async def _bootstrapped(laser):
    await laser.bootstrap(partitions=1, retention=ls.TopicRetention.expire_after(86_400_000))
    return laser


async def _first_command(laser, conversation):
    messages = await laser.context(conversation).fetch(topics=["agent.sessions"], n=10)
    commands = [message for message in messages if message.envelope["kind"] == "command"]
    assert commands, "the bridge published its command"
    return commands[0]


async def test_given_a_signing_key_when_an_a2a_task_is_submitted_then_should_sign_the_command(
    laser,
):
    await _bootstrapped(laser)
    bridge = ls.A2aBridge(
        laser, "a2a-edge", "agent.sessions", "agent.sessions", signing_key=BRIDGE_KEY
    )
    task = await bridge.submit(PARAMS)
    registry = ls.KeyRegistry()
    registry.enroll("a2a-edge", BRIDGE_KEY.verifying_key)
    command = await _first_command(laser, task["id"])
    assert registry.verify(command.envelope) == "a2a-edge"


async def test_given_a_verified_agent_when_mcp_bridges_call_then_should_handle_only_signed_calls(
    laser,
):
    await _bootstrapped(laser)
    registry = ls.KeyRegistry()
    registry.enroll("mcp-edge", BRIDGE_KEY.verifying_key)
    handled = []
    refused = asyncio.Event()

    async def handle(ctx, message):
        handled.append(message.envelope["tool"])

    async def dead_letter(message, capsule, publish_error):
        refused.set()

    agent = laser.spawn_agent(
        "tool-agent", "agent.sessions", handle, verifier=registry, dead_letter=dead_letter
    )
    try:
        await agent.ready()
        for key, tool in [(None, "unsigned"), (BRIDGE_KEY, "signed")]:
            bridge = ls.McpBridge(
                laser,
                "mcp-edge",
                "agent.sessions",
                "agent.sessions",
                "tools",
                timeout_ms=300,
                signing_key=key,
                target="tool-agent",
            )
            with pytest.raises(ls.TimeoutError):
                await bridge.call_tool(tool, {"q": "x"})
        await asyncio.wait_for(refused.wait(), 5)
        for _ in range(50):
            if handled:
                break
            await asyncio.sleep(0.1)
        assert handled == ["signed"]
    finally:
        await agent.shutdown()


async def test_given_mcp_protocol_details_when_dispatched_then_should_follow_the_schema(laser):
    bridge = ls.McpBridge(
        laser,
        "mcp-edge",
        "agent.sessions",
        "agent.sessions",
        "tools",
        tools=[{"name": "search", "input_schema": {"type": "object"}}],
        prompts=[{"prompt": {"name": "greet"}, "messages": [["user", "hello"]]}],
    )
    assert await bridge.handle_rpc({"jsonrpc": "2.0", "id": 1, "method": "ping"}) == {
        "jsonrpc": "2.0",
        "id": 1,
        "result": {},
    }
    assert (
        await bridge.handle_rpc({"jsonrpc": "2.0", "method": "notifications/initialized"}) is None
    )
    init = await bridge.handle_rpc(
        {"id": 2, "method": "initialize", "params": {"protocolVersion": "1999-01-01"}}
    )
    assert init["result"]["protocolVersion"] == "2025-11-25"
    older = await bridge.handle_rpc(
        {"id": 3, "method": "initialize", "params": {"protocolVersion": "2025-06-18"}}
    )
    assert older["result"]["protocolVersion"] == "2025-06-18"
    prompt = await bridge.handle_rpc(
        {"id": 4, "method": "prompts/get", "params": {"name": "greet"}}
    )
    assert "description" not in prompt["result"]
    for request, code in [
        ({"id": 5, "method": "unknown"}, -32601),
        ({"id": 6, "method": "tools/call", "params": {"name": "nope"}}, -32602),
        ({"id": 7, "method": "prompts/get", "params": {"name": "nope"}}, -32602),
        ({"id": 8, "method": "resources/read", "params": {"uri": "file:///nope"}}, -32002),
        (
            {
                "id": 9,
                "method": "tools/call",
                "params": {
                    "name": "search",
                    "_meta": {ls.BRIDGE_HOPS_META_KEY: ["a2a", "mcp-edge"]},
                },
            },
            -32602,
        ),
    ]:
        assert (await bridge.handle_rpc(request))["error"]["code"] == code, request
    assert ls.MCP_SUPPORTED_PROTOCOL_VERSIONS == ["2025-11-25", "2025-06-18", "2025-03-26"]


async def test_given_a2a_requests_when_dispatched_then_should_answer_v1_results_and_codes(laser):
    await _bootstrapped(laser)
    bridge = ls.A2aBridge(laser, "a2a-edge", "agent.sessions", "agent.sessions")
    sent = await bridge.handle_rpc({"id": 1, "method": "SendMessage", "params": PARAMS})
    task = sent["result"]["task"]
    assert task["status"]["state"] == "TASK_STATE_SUBMITTED"
    assert task["contextId"] == task["id"]
    for request, code in [
        ({"id": 2, "method": "ListTasks"}, -32601),
        ({"id": 3, "method": "SendStreamingMessage", "params": PARAMS}, -32004),
        ({"id": 4, "method": "GetTask", "params": {"id": "nope"}}, -32001),
        ({"id": 5, "method": "GetTask", "params": {"id": ls.new_conversation_id()}}, -32001),
        ({"id": 6, "method": "CancelTask", "params": {"id": "nope"}}, -32001),
        ([], -32600),
    ]:
        assert (await bridge.handle_rpc(request))["error"]["code"] == code, request
    current = await bridge.handle_rpc({"id": 7, "method": "GetTask", "params": {"id": task["id"]}})
    assert current["result"]["status"]["state"] == "TASK_STATE_WORKING"
    with pytest.raises(ls.HandlerConfigError):
        await bridge.task(ls.new_conversation_id())


async def test_given_card_details_when_the_card_is_built_then_should_advertise_the_public_url(
    laser,
):
    with pytest.raises(ls.HandlerConfigError):
        ls.A2aBridge(laser, "a2a-edge", "agent.sessions", "agent.sessions").card()
    with pytest.raises(ls.InvalidError):
        ls.A2aBridge(laser, "a2a-edge", "agent.sessions", "agent.sessions", public_url="/a2a")
    card = ls.A2aBridge(
        laser,
        "a2a-edge",
        "agent.sessions",
        "agent.sessions",
        public_url="https://agents.example.com/a2a",
        description="Plans trips",
        version="2.1.0",
    ).card()
    assert card["capabilities"]["streaming"] is False
    assert card["description"] == "Plans trips"
    assert card["version"] == "2.1.0"
    assert card["supportedInterfaces"][0]["url"] == "https://agents.example.com/a2a"


async def test_given_inbound_hops_when_a_bridge_publishes_then_should_extend_or_refuse_the_path(
    laser,
):
    await _bootstrapped(laser)
    bridge = ls.A2aBridge(laser, "a2a-edge", "agent.sessions", "agent.sessions")
    looped = await bridge.handle_rpc(
        {
            "id": 1,
            "method": "SendMessage",
            "params": {**PARAMS, "metadata": {ls.BRIDGE_HOPS_META_KEY: ["mcp", "a2a-edge"]}},
        }
    )
    assert looped["error"]["code"] == -32602
    with pytest.raises(ls.InvalidError):
        await bridge.submit(PARAMS, previous_hops=["a2a-edge"])
    task = await bridge.submit(PARAMS, previous_hops=["mcp-edge"], target="planner")
    command = await _first_command(laser, task["id"])
    assert command.envelope["metadata"]["bridge_hops"] == ["mcp-edge", "a2a-edge"]
    assert command.envelope["target"] == "planner"
