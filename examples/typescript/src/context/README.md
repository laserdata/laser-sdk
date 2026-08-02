# context - one conversation, fully assembled

> Everything one conversation touched - messages, memories, graph entries - scoped by id and assembled on demand under a token budget. Stop hand-rolling context windows.

## What it shows

- Appends a command and a response to a fresh conversation with `ctx.append(topic, payload)`.
- Reads the conversation back under a policy chain - the last 20 turns capped to a 4,000-token budget: `ctx.fetchWith(topics, new ContextChain([new LastN(20), new TokenBudget(4_000)]))`. The shape of a prompt's context is a declared policy, not slicing logic spread through the application.
- Prints the assembled turns in order.

Runs against Apache Iggy - no LaserData Cloud needed.

## Run it

Run `npm run setup` once, then run from `examples/typescript`:

```sh
npm run example:context
```

## Learn more

- Docs: https://docs.laserdata.cloud/laser-sdk/context
- Full system built on this primitive: [`concierge`](../concierge) - conversation context assembled alongside keyed state and forks.
