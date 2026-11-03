# orchestra: one orchestrator over a pool of capability agents

This example coordinates six agents through capability discovery, contracts, fan-out, a workflow, and operator control. Every exchange goes through the log, never a direct call.

Each agent opens its own connection and stays up for the whole run. The example pauses for Enter after each phase, so you can watch the change in the console's Orchestration view. Set `LASER_NON_INTERACTIVE=1` to run it straight through.

## What it does

1. Discovery. Six agents connect and advertise a capability card through `AgentBuilder.capabilities(..)`. `diag-gamma` advertises `Unavailable` and `laggard` is deliberately slow.
2. Contract. The orchestrator sends a task with a deadline to one agent that can `classify`, through `laser.contract(routeToCapable("classify", { kind: "any" }))`. Acknowledgment on pickup tells a consumed task from an expired one.
3. Fan-out. `laser.scatter(..)` sends a panel to every agent that can `diagnose`. Capability resolution leaves the unavailable agent out, so two of the three answer.
4. Workflow. `laser.workflow(..)` runs a journalled `triage`, then a `diagnose` panel under a `verifyWith` check, then `remediate`. A `WorkflowBudget` caps the dispatches and the wall clock. Each step builds its task from the earlier steps' outputs. The run is a root session and each step is a child session of it.
5. Quarantine. An operator quarantines `diag-alpha` with `laser.quarantine(..)`, and the next panel routes around it.
6. Recovery. The operator reinstates the agent with `laser.unquarantine(..)`, and the panel is whole again.
7. Expiry and recovery. A task with a one-second deadline goes to the slow agent and times out. The orchestrator sends it again to a healthy agent.

Routing uses a fixed inbox on `agent.sessions`, so the example runs on Apache Iggy. A managed deployment also records live presence.

## Run it

Run `npm run setup` once, then run from `examples/typescript`:

```sh
npm run example:orchestra
```

Run it without prompts:

```sh
LASER_NON_INTERACTIVE=1 npm run example:orchestra
```

Point the same code at LaserData Cloud to inspect presence, the registry, contracts, and the workflow in the console:

```sh
LASER_CONNECTION_STRING=user:pwd@your-laserdata-cloud-host \
  npm run example:orchestra
```

## Where to look (LaserData Cloud)

- Orchestration: the contract, the diagnostic panels, quarantine, recovery, and the deadline reroute.
- Sessions: the workflow run and one child session for each step.
- Agent registry: capabilities, health, quarantine state, and presence.

## Highlights

- Capability routing skips unavailable and quarantined agents without a change to the orchestrator.
- `scatter()` returns one reply for each selected agent.
- `workflow()` journals dependency-ordered steps and enforces one shared budget.
- A contract ends as `completed`, `failed`, `notConsumed`, or `timedOut`.
- One `AsyncResourceGroup` owns every agent and its connection and closes them in reverse order.
