# agent - the Fabric primitive

A reliable runtime for agents on the log: deduplication, retries, dead-letters, request/reply. Contracts hand out tasks with deadlines. Workflows add budgets and compensation. Discovery lets agents find each other by capability.

Runs with no Cloud: the agent runtime is open core.

## What it shows

- Spawn a handler agent (`Agent::builder().id(..).listen_on(AgentTopic::Commands)...handler(Triage).build().spawn(laser)`) advertising the `resolve-ticket` capability, so it is addressable by what it can do rather than by the name it runs under.
- Acknowledge on pickup (`.ack_on_pickup(true)`), so a crash mid-handler is a retry rather than a silently dropped task.
- Hand it a deadline-bounded task by capability, not by name: `laser.contract(Router::to_capable("resolve-ticket", RoutePolicy::Any)).from(..).deadline(Duration::from_secs(60)).send()`.
- Match the outcome (`Contract::Completed` / `Failed` / `NotConsumed` / `TimedOut`) and print the reply.

## Run it

Run from `examples/rust`:

```sh
just up && cargo run --example agent
```

## Learn more

- Docs: https://docs.laserdata.cloud/laser-sdk/fabric
- Full system built on this primitive: [`orchestra`](../orchestra/README.md)
