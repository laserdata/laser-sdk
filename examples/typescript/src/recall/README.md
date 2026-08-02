# recall - durable, auditable agent memory

> Four verbs: remember, recall, improve, forget. Every change is a message on your log, so durable memory is versioned and auditable by construction. The log-backed path recalls by recency. Vector and reranker paths add similarity ranking.

## What it shows

- `laser.memory("customer:42")` scopes a memory handle to a customer, connected the same as every other example.
- Remembers one fact under a conversation scope with `memory.remember(payload).conversation(id).send()`, which returns the item's id.
- Recalls the newest facts with `memory.recall().conversation(id).recent().limit(5).folded().fetch()`. The full memory scenario shows true similarity ranking with the vector backend.
- Reinforces then retires the same item with `improve` and `forget`. Both are records on the memory topic, so the store stays an auditable history rather than a mutable cell.

Runs with no Cloud deployment, against plain Apache Iggy: `.folded()` reads the memory topic in process instead of the managed key-value read view, the same opt-in Rust and Python carry.

## Run it

Run `npm run setup` once, then run from `examples/typescript`:

```sh
npm run example:recall
```

## Learn more

- Docs: https://docs.laserdata.cloud/laser-sdk/memory
- Full system built on this primitive: [`memory`](../memory) - durable, governed memory combined with the graph primitive in one woven scenario.
