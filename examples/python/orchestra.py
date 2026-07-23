"""orchestra (orchestration): one orchestrator over a pool of capability agents.

The Python peer of the Rust `orchestra`, 1:1 with it. One orchestrator
coordinates a pool of long-running capability agents entirely over the log, never
a direct call. It is INTERACTIVE and paced: it stops at each phase and waits for
Enter, so you can open the LaserData console's Orchestration view (`/orchestration`) and
watch every transition happen live, presence, the registry, contracts, and the
workflow journal.

The agents connect once at the start and stay up for the whole run, so the
console shows a live, populated fabric the entire time. Each phase:

  1. DISCOVERY    six agents connect and advertise a capability card + presence.
                  The orchestrator resolves them from the fused registry, so it
                  never hard-codes who can do what.
  2. CONTRACT     a directed task to one capable agent (to_capable).
  3. FAN-OUT      a panel scattered to every capable agent (scatter). One agent
                  advertises unavailable, so routing leaves it out.
  4. WORKFLOW     a journalled run: triage, then a diagnose panel, then remediate,
                  each step building its task from the prior steps' outputs.
  5. QUARANTINE   an operator pulls a misbehaving agent, and the panel routes around it.
  6. RECOVERY     the operator reinstates it (un-quarantine), and the panel is whole.
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


async def main() -> None:
    laser = await _common.connect(EXAMPLE)
    await laser.bootstrap(_common.PARTITIONS)

    _common.phase("Discovery: a pool of long-running capability agents connects")
    # Kept alive for the whole run so the console stays populated. Health is a
    # property of the card: diag-gamma advertises unavailable to prove routing
    # reads it, and laggard is deliberately slow to drive the expiry phase.
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

    _common.phase("Contract: a directed task to one capable agent, with a deadline")
    # The orchestrator names a capability, not an agent. Routing resolves the one
    # classifier from the registry and waits for the reply or the deadline.
    reply = await laser.contract(
        CLASSIFY, INCIDENT, source=ORCHESTRATOR, fixed_inbox=COMMANDS, deadline_ms=10_000
    )
    print("classifier replied:", reply.decode() if reply else "<did not complete>")
    await pause("CONTRACT: a directed task completed (see it in the Contracts panel)")

    _common.phase("Fan-out: a panel scattered to every capable agent")
    # Three agents advertise diagnose, but one is unavailable, so the scatter
    # reaches the two healthy ones without the orchestrator knowing their ids.
    findings = await diagnose_panel(laser)
    print(f"panel gathered {findings} findings (the unavailable agent was skipped)")
    await pause("FAN-OUT: two healthy diagnosers answered, the unavailable one was skipped")

    _common.phase("Workflow: triage, then a diagnose panel, then remediate (journalled)")
    wf = laser.workflow("incident-response", fixed_inbox=COMMANDS)
    # Cap the dispatches and wall clock so a runaway fan-out cannot spin.
    wf.budget(invocations=8, wall_clock_ms=60_000)
    # Register the run in the managed run registry when the plane serves it (the
    # Runs panel then shows its lifecycle), and run log-native otherwise.
    caps = await laser.capabilities()
    if caps.agent_workflow:
        wf.registered()
    wf.step("triage", to_capable=CLASSIFY, build=lambda outputs: INCIDENT)
    wf.step(
        "diagnose",
        all_capable=DIAGNOSE,
        after=["triage"],
        # Each step reads the prior steps' outputs from the journal, so the
        # dependency edge is data, not a shared variable.
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

    _common.phase("Quarantine: an operator pulls a misbehaving agent")
    # Quarantine is a registry fact every fused registry folds, so the next panel
    # routes around diag-alpha with no change to the orchestrator.
    await laser.quarantine("operator", "diag-alpha")
    after = await diagnose_panel(laser)
    print(f"panel after quarantine: {after} findings (alpha routed around)")
    await pause("QUARANTINE: diag-alpha is quarantined in the registry, the panel routes around it")

    _common.phase("Recovery: the operator reinstates the agent")
    await laser.unquarantine("operator", "diag-alpha")
    reinstated = await diagnose_panel(laser)
    print(f"panel after un-quarantine: {reinstated} findings (alpha is back)")
    await pause("RECOVERY: diag-alpha is reinstated, the panel is whole again")

    _common.phase("Expiry + recovery: a tight deadline times out, the orchestrator recovers")
    # The slow agent acks pickup but cannot finish inside the one-second deadline,
    # so the contract expires. The orchestrator recovers by re-dispatching to a
    # healthy fast agent, the pattern any real coordinator uses for a stuck task.
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
    """Spawn one long-running capability agent on its own connection, so each is a
    distinct live presence in the console (presence is per connection). It
    advertises its card on start, and the returned handle keeps the connection
    alive until the run ends."""
    laser = await _common.connect(EXAMPLE)
    handle = laser.spawn_agent(
        agent_id,
        COMMANDS,
        worker(agent_id, skill, delay),
        respond_on=RESPONSES,
        capabilities=[skill],
        # Ack on pickup so the orchestrator can tell a consumed task from an
        # expired one, which is what makes the expiry phase legible.
        ack_on_pickup=True,
        health=health,
        poll_interval_ms=10,
    )
    await handle.ready()
    return handle


async def diagnose_panel(laser) -> int:
    """Scatter a diagnose panel to every capable agent and return how many
    answered. Unavailable agents are left out by capability resolution, so the
    count reflects who could actually answer."""
    bodies = await laser.scatter(
        DIAGNOSE, INCIDENT, source=ORCHESTRATOR, fixed_inbox=COMMANDS, deadline_ms=10_000
    )
    return len(bodies)


async def pause(prompt: str) -> None:
    """Print what to watch, then block on Enter so the operator can flip to the
    LaserData console and observe the phase live. The read runs in an executor, so
    the asyncio loop (and the spawned agents) keep running while it waits."""
    message = (
        f"\n  >>> {prompt}\n      (watch the console's /orchestration view, then press Enter) "
    )
    await asyncio.get_event_loop().run_in_executor(None, input, message)


if __name__ == "__main__":
    asyncio.run(main())
