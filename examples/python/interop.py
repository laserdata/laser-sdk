"""interop (agentic): edge interoperability over the durable log.

Reaches one agent four ways, all bridged onto the Agent Data Exchange Protocol
and riding Apache Iggy rather than SSE:

  - A2A     SendMessage publishes a task, the worker answers, GetTask completes.
  - MCP     tools/call reaches the same worker and renders the answer as a result.
  - AG-UI   a chat answer streams onto the log and renders back as AG-UI events.
  - HITL    the orchestrator pauses for a human decision and an approver resolves it.

Every worker only ever speaks AGDX. The bridges produce AGDX. Runs on raw Apache
Iggy.

Run it:
    python3 interop.py
"""

from __future__ import annotations

import asyncio

import _common
import laser_sdk as ls

EXAMPLE = "interop"


async def main() -> None:
    laser = await _common.connect(EXAMPLE)
    try:
        _common.phase("connecting")
        await laser.bootstrap(
            partitions=_common.PARTITIONS, retention=ls.TopicRetention.expire_after(86_400_000)
        )

        # Three responders share `agent.sessions`. Each bridge call and the input
        # request name the agent they are for, so only that agent takes them as
        # work. Every worker reads the decoded AGDX command body and answers with
        # a correlated AGDX response, which is what a bridge's tasks/get and tool
        # result read.
        async def worker(ctx, message):
            prompt = bytes(message.body()).decode(errors="replace")
            await ctx.respond_input(ls.AgentTopic.Sessions, complete(prompt).encode())

        # The human behind the interrupt gate: it resolves every request_input it is
        # handed with a correlated AGDX response. A real deployment routes this to a
        # UI or a person.
        async def approver(ctx, message):
            await ctx.respond_input(ls.AgentTopic.Sessions, b"approved")

        async def spawn(agent_id, handler):
            handle = laser.spawn_agent(
                agent_id, ls.AgentTopic.Sessions, handler, poll_interval_ms=10
            )
            await handle.ready()
            return handle

        assistant = await spawn("assistant", worker)
        tool_runner = await spawn("tool-runner", worker)
        approver_agent = await spawn("approver", approver)

        async with assistant, tool_runner, approver_agent:
            await run_flows(laser)
    finally:
        await laser.close()


async def run_flows(laser) -> None:
    # A2A: SendMessage publishes the task, the worker answers, GetTask completes.
    _common.phase("A2A: SendMessage -> GetTask")
    a2a = ls.A2aBridge(laser, "a2a-gateway", ls.AgentTopic.Sessions, ls.AgentTopic.Sessions)
    params = {
        "message": {
            "role": "user",
            "parts": [{"kind": "text", "text": "summarize the incident"}],
        }
    }
    task = await a2a.submit(params, target="assistant")
    completed = None
    for _ in range(60):
        completed = await a2a.task(task["id"])
        if completed["status"]["state"].lower() not in ("working", "submitted"):
            break
        await asyncio.sleep(0.25)
    artifacts = completed.get("artifacts") or []
    answer = artifacts[0]["text"] if artifacts and "text" in artifacts[0] else "(no artifact)"
    print(f"A2A task {completed['id']} -> {completed['status']['state']}: {answer}")

    # MCP: tools/call reaches a worker and renders the answer as a tool result.
    _common.phase("MCP: initialize / tools/list / tools/call")
    mcp = ls.McpBridge(
        laser,
        "mcp-gateway",
        ls.AgentTopic.Sessions,
        ls.AgentTopic.Sessions,
        "laser-mcp",
        tools=[
            {
                "name": "ask",
                "description": "ask the assistant a question",
                "input_schema": {"type": "object", "properties": {"q": {"type": "string"}}},
            }
        ],
        timeout_ms=15_000,
    )
    names = [tool["name"] for tool in mcp.list_tools()["tools"]]
    print(f"MCP tools/list: [{', '.join(names)}]")
    # The MCP `tools/call` params ride the command body unchanged.
    params = {
        "name": "ask",
        "arguments": {"q": "what is the Agent Data Exchange Protocol?"},
    }
    result = await mcp.call_tool("ask", params, target="tool-runner")
    content = result.get("content") or []
    text = content[0]["text"] if content and "text" in content[0] else "(empty)"
    is_error = str(result.get("isError", False)).lower()
    print(f"MCP tools/call -> isError={is_error}, content: {text}")

    # AG-UI: stream a chat answer with the typed AGDX producer, then render the
    # conversation as AG-UI events straight off the log.
    _common.phase("AG-UI: render a chat stream as events")
    conversation = ls.new_conversation_id()
    correlation = ls.mint_ulid()
    stream = laser.agdx(ls.AgentTopic.Sessions, "assistant", conversation).stream(
        correlation, "chat"
    )
    for token in complete("give a one-line status update").split(" "):
        await stream.write((token + " ").encode())
    await stream.finish(finish_reason="stop")
    events = await laser.agui_events(conversation, ls.AgentTopic.Sessions)
    print(f"AG-UI rendered {len(events)} event(s) from the chat stream")

    # HITL: the orchestrator pauses for a human decision with the typed AGDX
    # producer's request_input, and the approver resolves the interrupt with a
    # correlated response. Built on AGDX command/response, riding the same log.
    _common.phase("Human-in-the-loop: request_input -> respond_input")
    gate = laser.agdx(ls.AgentTopic.Sessions, "orchestrator", ls.new_conversation_id())
    decision = await gate.request_input(
        ls.AgentTopic.Sessions, b"approve draining node-7?", timeout_ms=15_000, target="approver"
    )
    print(f"HITL decision: {bytes(decision).decode(errors='replace')}")


# The model behind every worker: the deterministic mock the Rust and TypeScript
# examples use by default. A real deployment swaps in an LLM client.
def complete(prompt: str) -> str:
    return f"[mock-llm] {prompt}"


if __name__ == "__main__":
    asyncio.run(main())
