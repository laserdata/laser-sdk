# agent - agents that survive crashes and find each other

> A reliable runtime for agents on the log: deduplication, retries, dead-letters, request/reply. Contracts hand out tasks with deadlines. Workflows add budgets and compensation. Discovery lets agents find each other by capability.

## What it shows

- Spawns a minimal `triage` handler agent that listens on the commands topic, replies on the responses topic, and advertises the `resolve-ticket` capability (`Agent.builder()...spawn(laser)`), so it is addressable by what it can do rather than by the name it runs under.
- Acknowledges on pickup (`.ackOnPickup()`), so a crash mid-handler is a retry rather than a silently dropped task.
- Sends it a deadline-bounded contract by capability, not by name: `laser.contract(routeToCapable("resolve-ticket", ANY_ROUTE_POLICY)).from(...).payload(...).inboxRoute(...).deadline(60_000).send()`.
- Reads the outcome (`completed` / `failed` / `notConsumed` / `timedOut`) and prints the decoded reply.

Runs against Apache Iggy - no LaserData Cloud needed.

## Run it

Run `npm run setup` once, then run from `examples/typescript`:

```sh
npm run example:agent
```

## Learn more

- Docs: https://docs.laserdata.cloud/laser-sdk/fabric
- Full system built on this primitive: [`orchestra`](../orchestra) - discovery, contracts, scatter/gather, workflows, and quarantine in one durable orchestration.
