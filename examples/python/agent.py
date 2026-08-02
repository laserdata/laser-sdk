"""agent (Fabric primitive): agents that survive crashes and find each other.

A reliable runtime for agents on the log: deduplication, retries, dead-letters,
request/reply. Contracts hand out tasks with deadlines. Workflows add budgets
and compensation. Discovery lets agents find each other by capability.

What it shows:
  - spawn a handler agent that advertises a capability and acks on pickup
  - send it a deadline-bounded contract, addressed by capability, not by name
  - read back its reply (or None, had it missed the deadline)

Run it:
    just up
    python3 agent.py

Docs: https://docs.laserdata.cloud/laser-sdk/fabric
Full scenario: orchestra.py (six agents, discovery, workflows, quarantine, deadline recovery)
"""

from __future__ import annotations

import asyncio

import _common
import laser_sdk as ls

EXAMPLE = "agent"
COMMANDS = ls.Topics.COMMANDS
RESPONSES = ls.Topics.RESPONSES
CAPABILITY = "resolve-ticket"
DEADLINE_MS = 60_000


async def handle(ctx, message) -> None:
    print(f'  triage picked up "{message.body().decode()}"')
    await ctx.respond(b"on it")


async def main() -> None:
    laser = await _common.connect(EXAMPLE)
    # The well-known agent topics (commands, responses, registry, ...) must
    # exist before an agent's consumer group joins one.
    await laser.bootstrap(_common.PARTITIONS)

    _common.phase("spawn a handler, then hand it a deadline-bounded task")
    triage = laser.spawn_agent(
        "triage",
        COMMANDS,
        handle,
        respond_on=RESPONSES,
        # The advertised capability is what makes this agent addressable by what
        # it can do rather than by the name it happens to run under.
        capabilities=[CAPABILITY],
        # Acknowledge on pickup, so a crash mid-handler is a retry rather than a
        # silently dropped task.
        ack_on_pickup=True,
    )
    await triage.ready()

    # A contract is a directed task with a deadline and a real answer. Routed by
    # capability, not by name.
    reply = await laser.contract(
        CAPABILITY,
        b"ticket #42 is stuck",
        source="orchestrator",
        fixed_inbox=COMMANDS,
        deadline_ms=DEADLINE_MS,
    )
    if reply is None:
        print("  contract ended without a reply")
    else:
        print(f"  contract completed: {reply.decode()}")

    await triage.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
