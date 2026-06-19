"""interop (agentic): edge interoperability over the durable log.

Reaches one agent four ways, all bridged onto the Agent Data Exchange Protocol
and riding Apache Iggy rather than SSE:

  - A2A     message/send publishes a task, the worker answers, tasks/get completes.
  - MCP     tools/call reaches the same worker and renders the answer as a result.
  - AG-UI   a chat answer streams onto the log and renders back as AG-UI events.
  - HITL    the orchestrator pauses for a human decision and an approver resolves it.

Every worker only ever speaks AGDX; the bridges produce AGDX. Runs on raw Apache
Iggy.

Run it:
    python interop.py
"""

from __future__ import annotations

import asyncio

import _common
import laser_sdk as ls

EXAMPLE = "interop"


# The model behind every worker: a deterministic canned reply, the Python
# analog of the examples' MockLlm. A real deployment swaps in an LLM client.
def complete(prompt: str) -> str:
    return f"summary: {prompt.strip()[:48]}"


async def main() -> None:
    laser = await _common.connect(EXAMPLE)
    await laser.bootstrap(partitions=_common.PARTITIONS)

    # One worker reachable through A2A (Commands -> Responses) and another through
    # MCP (ToolCalls -> ToolResults). Same handler shape, same canned model. Each
    # reads the decoded AGDX command body and answers with a correlated AGDX
    # response, which is what a bridge's tasks/get and tool result read.
    async def a2a_worker(ctx, message):
        prompt = bytes(message.agdx_body or message.payload).decode(errors="replace")
        await ctx.respond_input(ls.Topics.RESPONSES, complete(prompt).encode())

    async def mcp_worker(ctx, message):
        prompt = bytes(message.agdx_body or message.payload).decode(errors="replace")
        await ctx.respond_input(ls.Topics.TOOL_RESULTS, complete(prompt).encode())

    # The human behind the interrupt gate: it resolves every request_input it is
    # handed with a correlated AGDX response. A real deployment routes this to a
    # UI or a person.
    async def approver(ctx, message):
        await ctx.respond_input(ls.Topics.RESPONSES, b"approved")

    def spawn(agent_id, topic, handler):
        return laser.spawn_agent(agent_id, topic, handler, poll_interval_ms=10)

    a2a_agent = spawn("assistant", ls.Topics.COMMANDS, a2a_worker)
    mcp_agent = spawn("tool-runner", ls.Topics.TOOL_CALLS, mcp_worker)
    approver_agent = spawn("approver", ls.Topics.HUMAN_INPUT, approver)

    async with a2a_agent, mcp_agent, approver_agent:
        # A2A: message/send publishes the task, the worker answers, tasks/get completes.
        print("A2A: message/send -> tasks/get")
        a2a = laser.a2a_bridge("a2a-gateway", ls.Topics.COMMANDS, ls.Topics.RESPONSES)
        params = {"message": {"role": "user", "parts": [{"kind": "text", "text": "summarize"}]}}
        task = await a2a.submit(params)
        completed = None
        for _ in range(60):
            completed = await a2a.task(task["id"])
            if completed["status"]["state"].lower() not in ("working", "submitted"):
                break
            await asyncio.sleep(0.25)
        artifacts = completed.get("artifacts") or []
        answer = artifacts[0]["text"] if artifacts and "text" in artifacts[0] else "(no artifact)"
        print(f"A2A task {completed['id']} -> {completed['status']['state']}: {answer}")

        # MCP: tools/call reaches the same worker and renders the answer as a tool result.
        print("MCP: initialize / tools/list / tools/call")
        mcp = laser.mcp_bridge(
            "mcp-gateway",
            ls.Topics.TOOL_CALLS,
            ls.Topics.TOOL_RESULTS,
            "laser-mcp",
            tools=[
                {
                    "name": "ask",
                    "description": "ask the assistant a question",
                    "input_schema": {"type": "object", "properties": {"q": {"type": "string"}}},
                }
            ],
            timeout_secs=15.0,
        )
        print(f"MCP tools/list: {[t['name'] for t in mcp.list_tools()['tools']]}")
        result = await mcp.call_tool("ask", {"q": "what is the Agent Data Exchange Protocol?"})
        content = result.get("content") or []
        text = content[0]["text"] if content and "text" in content[0] else "(empty)"
        print(f"MCP tools/call -> isError={result.get('isError', False)}, content: {text}")

        # HITL: the orchestrator pauses for a human decision with the typed AGDX
        # producer's request_input; the approver resolves the interrupt with a
        # correlated response. Built on AGDX command/response, riding the same log.
        print("Human-in-the-loop: request_input -> respond_input")
        conversation = ls.new_conversation_id()
        gate = laser.agdx(ls.Topics.HUMAN_INPUT, "orchestrator", conversation)
        decision = await gate.request_input(
            ls.Topics.RESPONSES, b"approve a $500 refund?", timeout_secs=15.0
        )
        print(f"HITL decision: {bytes(decision).decode(errors='replace')}")

    # AG-UI: stream a chat answer with the typed AGDX producer, then render the
    # conversation as AG-UI events straight off the log.
    print("AG-UI: render a chat stream as events")
    conversation = ls.new_conversation_id()
    correlation = ls.new_correlation_id()
    stream = laser.agdx(ls.Topics.LLM_IO, "assistant", conversation).stream(correlation, "chat")
    for token in complete("give a one-line status update").split(" "):
        await stream.write((token + " ").encode())
    await stream.finish(finish_reason="stop")
    events = await laser.agui_events(conversation, ls.Topics.LLM_IO)
    print(f"AG-UI rendered {len(events)} event(s) from the chat stream")


if __name__ == "__main__":
    asyncio.run(main())
