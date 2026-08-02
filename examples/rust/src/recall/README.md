# recall - the Memory primitive

Four verbs: remember, recall, improve, forget. Every change is a message on your log, so durable memory is versioned and auditable by construction. The log-backed path recalls by recency. The vector and reranker paths add similarity ranking.

Named `recall` (one of the four verbs), not `memory`, since that name is already the full deep-dive scenario next door. The accessor is still `laser.memory(..)`, connected the same as every other example. Runs with no Cloud deployment: `.folded()` reads the memory topic in process instead of the managed read view, so it works against plain Apache Iggy.

## What it shows

- `laser.memory("customer:42")` scopes a memory handle to a customer.
- Remember a fact: `.remember(payload).scope(conversation).send()`, which returns the item's id.
- Recall the newest facts under a limit: `.recall(conversation).recent().limit(5).folded().fetch()`. The full memory scenario shows true similarity ranking with the vector backend.
- Reinforce then retire the same item: `improve(&scope, Feedback::new(id, 1.0))` and `forget(&scope, id)`. Both are records on the memory topic, so the store stays an auditable history rather than a mutable cell.

## Run it

Run from `examples/rust`:

```sh
just up && cargo run --example recall
```

## Learn more

- Docs: https://docs.laserdata.cloud/laser-sdk/memory
- Full system built on this primitive: [`memory`](../memory/README.md)
