# agent - the Fabric primitive

This example runs an agent that handles tasks from the log. It demonstrates capability-based routing, pickup acknowledgments, deadlines, and contract results.

The agent runtime runs on Apache Iggy without a managed backend.

## What it shows

- Spawn `Triage` with `Agent::builder().id(..).listen_on(AgentTopic::Commands)...handler(Triage).build().spawn(laser)`. Advertise `resolve-ticket` so callers can select the agent by capability.
- Enable `.ack_on_pickup(true)` to distinguish task pickup from completion.
- Send a task with `laser.contract(Router::to_capable("resolve-ticket", RoutePolicy::Any)).from(..).deadline(Duration::from_secs(60)).send()`.
- Match the outcome (`Contract::Completed` / `Failed` / `NotConsumed` / `TimedOut`) and print the reply.

## Run it

Run from `examples/rust`:

```sh
just up && cargo run --example agent
```

## Learn more

- Docs: https://docs.laserdata.cloud/laser-sdk/fabric
- Full system built on this primitive: [`orchestra`](../orchestra/README.md)
