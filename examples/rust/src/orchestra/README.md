# orchestra: one orchestrator over a pool of capability agents

This example coordinates six agents through capability discovery, contracts, fan-out, a workflow, and operator control. Every exchange goes through the log, never a direct call.

Each agent opens its own connection and stays up for the whole run. The example pauses for Enter after each phase, so you can watch the change in the console's Orchestration view. Set `LASER_NON_INTERACTIVE=1` to run it straight through.

## What it does

1. Discovery. Six agents connect and advertise a capability card. The orchestrator resolves them from the fused registry, so it never hard-codes who can do what. `diag-gamma` advertises `Unavailable` and `laggard` is deliberately slow.
2. Contract. The orchestrator sends a task with a deadline to one agent that can `classify`, through `Laser::contract(Router::to_capable("classify", RoutePolicy::Any))`. Acknowledgment on pickup tells a consumed task from an expired one.
3. Fan-out. `Laser::scatter` sends a panel to every agent that can `diagnose`. Capability resolution leaves the unavailable agent out, so two of the three answer.
4. Workflow. `Laser::workflow` runs a journalled `triage`, then a `diagnose` panel under a `verify_with` check, then `remediate`. A `WorkflowBudget` caps the dispatches and the wall clock. Each step builds its task from the earlier steps' outputs. The run is a root session and each step is a child session of it.
5. Quarantine. An operator quarantines `diag-alpha` with `Laser::quarantine`. Every fused registry folds that fact, so the next panel routes around the agent.
6. Recovery. The operator reinstates the agent with `Laser::unquarantine`, and the panel is whole again.
7. Expiry and recovery. A task with a one-second deadline goes to the slow agent and times out. The orchestrator sends it again to a healthy agent.

Routing uses a fixed inbox (`InboxRoute::Fixed(AgentTopic::Sessions)`), so the example runs on Apache Iggy. Each task is addressed to its agent on `agent.sessions`. A managed deployment also records live presence. On Apache Iggy that step is skipped.

## Run it

Run from `examples/rust`:

```sh
just up && cargo run --example orchestra
```

Run it without prompts:

```sh
LASER_NON_INTERACTIVE=1 cargo run --example orchestra
```

## Where to look (LaserData Cloud)

- Orchestration: the contract, the diagnostic panels, quarantine, recovery, and the deadline reroute.
- Sessions: the workflow run and one child session for each step.
- Agent registry: capabilities, health, quarantine state, and presence.

## Highlights

- Capability routing skips unavailable and quarantined agents without a change to the orchestrator.
- `scatter` returns one reply for each selected agent.
- `workflow` journals dependency-ordered steps and enforces one shared budget.
- A contract ends as completed, failed, not consumed, or timed out.

The Python and TypeScript `orchestra` examples run the same phases with the same agents and print the same lines.
