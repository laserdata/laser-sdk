"""context (Context primitive): one conversation, fully assembled.

Everything one conversation touched - messages, memories, graph entries -
scoped by id and assembled on demand under a token budget. Stop hand-rolling
context windows.

What it shows:
  - open one conversation's scope by id
  - append a couple of turns to it
  - assemble it back under a bound: the last N turns, trimmed to a token budget

Run it:
    just up
    python3 context.py

Docs: https://docs.laserdata.cloud/laser-sdk/context
Full scenario: concierge.py (an incident rebuilt from its own conversation as the audit trail)
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
    # Conversation turns ride the well-known agent topics, created once here.
    await laser.bootstrap(_common.PARTITIONS)
    conversation = ls.new_conversation_id()

    _common.phase("append a conversation, then assemble it under a budget")
    ctx = laser.context(conversation)
    await ctx.append(ls.Topics.COMMANDS, b"book me an aisle seat")
    await ctx.append(ls.Topics.RESPONSES, b"booked, aisle 12")

    # The shape of a prompt's context is a declared bound, not slicing logic
    # spread through the application: cap the turns, then fit the budget.
    turns = await ctx.fetch(
        topics=[ls.Topics.COMMANDS, ls.Topics.RESPONSES],
        last_n=LAST_N,
        token_budget=TOKEN_BUDGET,
    )

    print(f"  {len(turns)} turn(s) within {TOKEN_BUDGET} tokens:")
    for turn in turns:
        print(f"    {bytes(turn.payload).decode()}")


if __name__ == "__main__":
    asyncio.run(main())
