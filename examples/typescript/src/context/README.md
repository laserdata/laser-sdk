# context: the Context primitive

This example groups messages by conversation and reads them under a token budget. A context handle keeps the conversation ID with each operation.

## What it shows

- Appends two turns to a fresh conversation on the shared `agent.sessions` topic with `ctx.append(topic, payload)`.
- Reads the conversation back under a policy chain, the last 20 turns within a 4,000-token budget: `ctx.fetchWith(topics, new Chain([new LastN(20), new TokenBudget(4_000)]))`. The shape of a prompt's context is a declared policy, not slicing logic spread through the application.
- Prints the assembled turns in order.

It runs on Apache Iggy without LaserData Cloud.

## Run it

Run `npm run setup` once, then run from `examples/typescript`:

```sh
npm run example:context
```

## Learn more

- Docs: https://docs.laserdata.cloud/laser-sdk/context
- Full system built on this primitive: [`incident-desk`](../incident-desk): conversation context assembled alongside keyed state and forks.
