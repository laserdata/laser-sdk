# context - the Context primitive

This example groups messages by conversation and reads them under a token budget. A context handle keeps the conversation ID with each operation.

Context reads use ordinary log topics and run on Apache Iggy.

## What it shows

- Bind a conversation once: `laser.context(conversation)`.
- Append a command and a response to it (`scope.append(AgentTopic::Commands, ..)`).
- Use `scope.fetch_with(topics, Box::new(Chain(vec![LastN(20), TokenBudget::new(4_000)])))` to retain the last 20 turns within a 4,000-token budget.

## Run it

Run from `examples/rust`:

```sh
just up && cargo run --example context
```

## Learn more

- Docs: https://docs.laserdata.cloud/laser-sdk/context
- Full system built on this primitive: [`concierge`](../concierge/README.md)
