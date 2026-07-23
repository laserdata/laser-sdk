# orchestra - contracts, panels, workflows, and recovery

> One orchestrator coordinates a pool of long-lived capability agents entirely through the durable log.

## What it does

1. Starts six workers for triage, diagnosis, remediation, slow execution, and healthy backup.
2. Publishes capability cards and marks one diagnostic candidate unavailable.
3. Sends a directed contract to triage with a bounded deadline.
4. Scatters one task to every healthy agent advertising `diagnose` and asserts that the unavailable candidate is excluded.
5. Runs a journalled `triage -> diagnose -> remediate` workflow under invocation and wall-clock budgets.
6. Quarantines one diagnostic agent and proves that the next panel routes around it.
7. Reinstates the agent and proves that the full panel returns.
8. Lets a deliberately tight deadline expire, then redispatches the task to the healthy backup.

The example pauses after each phase so the state changes are visible in a console. Set `LASER_NON_INTERACTIVE=1` for CI or unattended runs.

## Run it

```sh
npm run example:orchestra
```

Run without prompts.

```sh
LASER_NON_INTERACTIVE=1 \
  npm run example:orchestra
```

The coordination path works on stock Apache Iggy. Point the same code at LaserData Cloud to inspect presence, registry, contracts, and workflow journals in the console.

```sh
LASER_CONNECTION_STRING=user:pwd@your-laserdata-cloud-host \
  npm run example:orchestra
```

## Where to look (LaserData Cloud)

- **Orchestration**: the directed contract, diagnostic panels, quarantine, recovery, and deadline reroute.
- **Workflows**: the journalled triage, diagnose, and remediate steps with their outputs.
- **Agent registry**: advertised capabilities, health, quarantine state, and live presence.
- **Conversations**: every command, response, and correlation that formed the run.

## Highlights

- Capability routing removes unavailable or quarantined agents without changing orchestrator code.
- `scatter()` reports one reply per selected agent and keeps attribution.
- `workflow()` journals dependency-ordered steps and enforces a shared budget.
- Contracts distinguish completed, failed, unconsumed, and timed-out work.
- One async resource group owns every agent handle and drains them in reverse startup order even when one shutdown fails.
