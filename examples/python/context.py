"""context (Context primitive): one conversation, fully assembled.

Everything one conversation touched, such as messages, memories, and graph
entries, is scoped by id and assembled on demand under a token budget.

What it shows:
  - open one conversation's scope by id
  - append a couple of turns to it
  - assemble it back under a policy: the last N turns, trimmed to a token budget

Run it:
    just up
    python3 context.py

Docs: https://docs.laserdata.cloud/laser-sdk/context
Full scenario: incident_desk.py (an incident rebuilt from its own conversation as the audit trail)
"""

from __future__ import annotations

import asyncio

import _common
import laser_sdk as ls

EXAMPLE = "context"
LAST_N = 20
TOKEN_BUDGET = 4_000


async def main() -> None:
    laser = await _common.connect(EXAMPLE)
    try:
        # Conversation turns ride the shared session topic, created once here.
        await laser.bootstrap(
            _common.PARTITIONS, retention=ls.TopicRetention.expire_after(86_400_000)
        )
        conversation = ls.new_conversation_id()

        _common.phase("append a conversation, then assemble it under a budget")
        ctx = laser.context(conversation)
        await ctx.append(ls.AgentTopic.Sessions, b"drain node-7")
        await ctx.append(ls.AgentTopic.Sessions, b"drained, 0 connections left")

        # The shape of a prompt's context is a declared policy, not slicing logic
        # spread through the application: cap the turns, then fit the budget.
        turns = await ctx.fetch_with(
            [ls.AgentTopic.Sessions],
            ls.Chain([ls.LastN(LAST_N), ls.TokenBudget(TOKEN_BUDGET)]),
        )

        print(f"  {len(turns)} turn(s) within {TOKEN_BUDGET} tokens:")
        for turn in turns:
            print(f"    {bytes(turn.payload).decode()}")
    finally:
        await laser.close()


if __name__ == "__main__":
    asyncio.run(main())
