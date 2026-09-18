# recall - the Memory primitive

This example records, retrieves, improves, and forgets memory items. Log-based memory reads recent records, while vector memory can rank them by similarity.

The `recall` example covers the focused memory API. The larger `memory` example covers additional behavior. `laser.memory(..)` provides the handle. `.folded()` reads the topic locally, so this example works on Apache Iggy without a managed view.

## What it shows

- `laser.memory("customer:42")` scopes a memory handle to a customer.
- Remember a fact: `.remember(payload).scope(conversation).send()`, which returns the item's id.
- Use `.recall(conversation).recent().limit(5).folded().fetch()` to read recent facts. The larger memory example demonstrates vector similarity.
- Use `improve(&scope, Feedback::new(id, 1.0))` to record feedback. Use `forget(&scope, id)` to record deletion. Both append records to the memory topic.

## Run it

Run from `examples/rust`:

```sh
just up && cargo run --example recall
```

## Learn more

- Docs: https://docs.laserdata.cloud/laser-sdk/memory
- Full system built on this primitive: [`memory`](../memory/README.md)
