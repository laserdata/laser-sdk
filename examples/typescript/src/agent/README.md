# agent - agents that survive crashes and find each other

This example runs an agent that handles tasks from the log. It demonstrates capability-based routing, pickup acknowledgments, deadlines, and contract results.

## What it shows

- Run a `triage` handler on the commands topic with replies on the responses topic. Advertise its capability and enable pickup acknowledgments.
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
