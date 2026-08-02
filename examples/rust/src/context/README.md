# context - the Context primitive

Everything one conversation touched - messages, memories, graph entries - scoped by id and assembled on demand under a token budget. Stop hand-rolling context windows.

Runs with no Cloud: context rides ordinary log topics.

## What it shows

- Bind a conversation once: `laser.context(conversation)`.
- Append a command and a response to it (`scope.append(AgentTopic::Commands, ..)`).
- Read it back under a composed policy: the last 20 turns, further trimmed to a 4,000-token budget (`scope.fetch_with(topics, Box::new(Chain(vec![LastN(20), TokenBudget::new(4_000)])))`). The shape of a prompt's context is a declared policy, not slicing logic spread through the application.

## Run it

Run from `examples/rust`:

```sh
just up && cargo run --example context
```

## Learn more

- Docs: https://docs.laserdata.cloud/laser-sdk/context
- Full system built on this primitive: [`concierge`](../concierge/README.md)
