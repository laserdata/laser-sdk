"""orchestra (orchestration): one orchestrator over a pool of capability agents.

The Python peer of the Rust `orchestra`. One orchestrator coordinates a pool of
long-running capability agents entirely over the log. It is INTERACTIVE and
paced: it stops at each phase and waits for Enter, so you can open the stream-ui
Orchestration console (`/orchestration`) and watch every transition happen live,
presence, the registry, contracts, and the workflow journal.

The agents connect once at the start and stay up for the whole run, so the
console shows a live, populated fabric the entire time. Each phase:

  1. DISCOVERY    six agents connect and advertise a capability card + presence.
  2. CONTRACT     a directed task to one capable agent (to_capable).
  3. FAN-OUT      a panel scattered to every capable agent (scatter); the
                  unavailable agent is routed around by health.
  4. WORKFLOW     a journalled run: triage, then a diagnose panel, then remediate.
  5. QUARANTINE   an operator pulls a misbehaving agent; the panel routes around it.
  6. RECOVERY     the operator reinstates it (un-quarantine); the panel is whole.
  7. EXPIRY       a tight-deadline task to a slow agent times out, and the
                  orchestrator recovers by re-dispatching to a healthy one.

Routing uses a fixed inbox topic so it runs against a stock local Apache Iggy.
Presence advertisement is best-effort: it lights up the console's presence panel
against the LaserData fork, and is a harmless no-op against stock Iggy.

    python orchestra.py
"""

from __future__ import annotations

import asyncio

import _common
import laser_sdk as ls

EXAMPLE = "orchestra"
CLASSIFY = "classify"
DIAGNOSE = "diagnose"
REMEDIATE = "remediate"
SLOW_TASK = "slow-task"
COMMANDS = ls.Topics.COMMANDS
RESPONSES = ls.Topics.RESPONSES
INCIDENT = b"checkout API latency spike"
ORCHESTRATOR = "orchestrator"


def phase(title: str) -> None:
    print(f"\n=== {title} ===")


async def pause(prompt: str) -> None:
    """Print what to watch, then block on Enter so the operator can flip to the
    stream-ui console and observe the phase live. The read runs in an executor, so
    the asyncio loop (and the spawned agents) keep running while it waits."""
    message = f"\n  >>> {prompt}\n      (watch stream-ui /orchestration, then press Enter) "
    await asyncio.get_event_loop().run_in_executor(None, input, message)


def worker(name: str, skill: str, delay: float):
    """A capability agent: reads the task body, waits its handling delay (so the
    in-flight Working state is visible in the console), and replies with the work
    its skill produces."""

    async def handle(ctx, message):
        await asyncio.sleep(delay)
        task = message.body().decode("utf-8", "replace")
        if skill == CLASSIFY:
            reply = f"severity=high ({task})"
        elif skill == DIAGNOSE:
            reply = f"{name}: cache stampede on the hot key [{task}]"
        elif skill == REMEDIATE:
            reply = f"{name}: drained the hot key, scaled the cache [{task}]"
        else:
            reply = f"{name}: {skill} done [{task}]"
        await ctx.respond(reply.encode())

    return handle


async def spawn(agent_id: str, skill: str, health: str, delay: float):
    # Each agent gets its own connection, so it is a distinct live presence in the
    # console (presence is per connection). The handle keeps the connection alive.
    laser = await _common.connect(EXAMPLE)
    handle = laser.spawn_agent(
        agent_id,
        COMMANDS,
        worker(agent_id, skill, delay),
        respond_on=RESPONSES,
        capabilities=[skill],
        ack_on_pickup=True,
        health=health,
        poll_interval_ms=10,
    )
    await handle.ready()
    return handle


async def diagnose_panel(laser) -> int:
    bodies = await laser.scatter(
        DIAGNOSE, INCIDENT, source=ORCHESTRATOR, fixed_inbox=COMMANDS, deadline_ms=10_000
    )
    return len(bodies)


async def main() -> None:
    laser = await _common.connect(EXAMPLE)
    await laser.bootstrap(_common.PARTITIONS)

    phase("Discovery: a pool of long-running capability agents connects")
    # Spawned once, kept alive for the whole run so the console stays populated.
    agents = [
        await spawn("triager", CLASSIFY, "healthy", 0.2),
        await spawn("diag-alpha", DIAGNOSE, "healthy", 0.4),
        await spawn("diag-beta", DIAGNOSE, "healthy", 0.4),
        await spawn("diag-gamma", DIAGNOSE, "unavailable", 0.4),
        await spawn("executor", REMEDIATE, "healthy", 0.3),
        await spawn("laggard", SLOW_TASK, "healthy", 6.0),
    ]
    print("six agents connected and advertised their capability cards")
    await pause("DISCOVERY: six agents are live in the registry (one unavailable)")

    phase("Contract: a directed task to one capable agent, with a deadline")
    reply = await laser.contract(
        CLASSIFY, INCIDENT, source=ORCHESTRATOR, fixed_inbox=COMMANDS, deadline_ms=10_000
    )
    print("classifier replied:", reply.decode() if reply else "<did not complete>")
    await pause("CONTRACT: a directed task completed (see it in the Contracts panel)")

    phase("Fan-out: a panel scattered to every capable agent")
    findings = await diagnose_panel(laser)
    print(f"panel gathered {findings} findings (the unavailable agent was skipped)")
    await pause("FAN-OUT: two healthy diagnosers answered, the unavailable one was skipped")

    phase("Workflow: triage, then a diagnose panel, then remediate (journalled)")
    wf = laser.workflow("incident-response", fixed_inbox=COMMANDS)
    wf.budget(invocations=8, wall_clock_ms=60_000)
    wf.step("triage", to_capable=CLASSIFY, build=lambda outputs: INCIDENT)
    wf.step(
        "diagnose",
        all_capable=DIAGNOSE,
        after=["triage"],
        build=lambda outputs: b"diagnose: " + outputs.get("triage", b""),
        verify=lambda folded: len(folded) > 0,
    )
    wf.step(
        "remediate",
        to_capable=REMEDIATE,
        after=["diagnose"],
        build=lambda outputs: b"remediate: " + outputs.get("diagnose", b""),
    )
    outputs = await wf.run()
    print(f"workflow completed and journalled: {len(outputs)} steps")
    await pause("WORKFLOW: the run journalled triage -> diagnose -> remediate (Workflow panel)")

    phase("Quarantine: an operator pulls a misbehaving agent")
    await laser.quarantine("operator", "diag-alpha")
    after = await diagnose_panel(laser)
    print(f"panel after quarantine: {after} findings (alpha routed around)")
    await pause("QUARANTINE: diag-alpha is quarantined in the registry, the panel routes around it")

    phase("Recovery: the operator reinstates the agent")
    await laser.unquarantine("operator", "diag-alpha")
    reinstated = await diagnose_panel(laser)
    print(f"panel after un-quarantine: {reinstated} findings (alpha is back)")
    await pause("RECOVERY: diag-alpha is reinstated, the panel is whole again")

    phase("Expiry + recovery: a tight deadline times out, the orchestrator recovers")
    # The slow agent acks pickup but cannot finish inside the deadline, so the
    # contract times out, and the orchestrator recovers by re-dispatching to a
    # healthy fast agent.
    slow = await laser.contract(
        SLOW_TASK, INCIDENT, source=ORCHESTRATOR, fixed_inbox=COMMANDS, deadline_ms=1_000
    )
    if slow is None:
        print("the slow agent missed the deadline, recovering on a healthy agent")
        recovered = await laser.contract(
            REMEDIATE, INCIDENT, source=ORCHESTRATOR, fixed_inbox=COMMANDS, deadline_ms=10_000
        )
        print("recovered:", recovered.decode() if recovered else "<did not complete>")
    else:
        print("unexpectedly fast:", slow.decode())
    await pause("EXPIRY: the slow agent timed out, the task recovered on a healthy agent")

    print(
        "\norchestra: discovery, routing, fan-out, a journalled workflow, health,\n"
        "reversible quarantine, and deadline recovery, all coordinated over the log."
    )

    for agent in agents:
        await agent.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
