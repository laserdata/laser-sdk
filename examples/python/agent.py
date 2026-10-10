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
SESSIONS = ls.AgentTopic.Sessions
CAPABILITY = "resolve-ticket"
DEADLINE_MS = 60_000


async def handle(ctx, message) -> None:
    print(f'  triage picked up "{message.body().decode()}"')
    await ctx.respond(b"on it")


async def main() -> None:
    laser = await _common.connect(EXAMPLE)
    try:
        # The agent topics (the shared session topic and its satellites) must
        # exist before an agent's consumer group joins one.
        await laser.bootstrap(
            _common.PARTITIONS, retention=ls.TopicRetention.expire_after(86_400_000)
        )

        _common.phase("spawn a handler, then hand it a deadline-bounded task")
        triage = laser.spawn_agent(
            "triage",
            SESSIONS,
            handle,
            respond_on=SESSIONS,
            # The advertised capability is what makes this agent addressable by what
            # it can do rather than by the name it happens to run under.
            capabilities=[CAPABILITY],
            # Emit a Working status on pickup, so a contract caller can tell the
            # command was consumed. Redelivery after a crash comes from
            # commit-after-success.
            ack_on_pickup=True,
        )
        await triage.ready()

        # A contract is a directed task with a deadline and a real answer. Routed by
        # capability, not by name.
        reply = await laser.contract(
            CAPABILITY,
            b"ticket #42 is stuck",
            source="orchestrator",
            fixed_inbox=SESSIONS,
            deadline_ms=DEADLINE_MS,
        )
        if isinstance(reply, ls.Contract.Completed):
            print(f"  contract completed: {bytes(reply[0].body()).decode()}")
        else:
            print(f"  contract ended without a reply: {reply!r}")

        await triage.shutdown()
    finally:
        await laser.close()


if __name__ == "__main__":
    asyncio.run(main())
