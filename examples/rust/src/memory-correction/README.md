# memory-correction: replace a fact for one user

This example remembers three facts about a user, corrects one of them in a later conversation with `supersede`, and prints what recall returns as JSON. It copies alone: it uses only `laser-sdk`, `tokio`, `serde` and `serde_json`, and no helper from this crate.

## What it shows

- `Laser::connect_env()` reads `LASER_CONNECTION_STRING`.
- `root.scratch_stream("memory")` creates a uniquely named stream, and `scratch.delete()` removes it whatever the run returned.
- `laser.memory("assistant")` creates its memory topic on the first write.
- `.remember(fact).scope(conversation).user(USER).send()` returns the fact's id.
- `.supersede(old_id, fact)` remembers the new fact, then forgets the old one in every conversation, so recall for the user never returns it again.
- `.recall(None).user(USER).semantic(QUERY).limit(10).fetch()` reads every conversation of the user, returns `MemoryItem`s ranked by the words the query shares with each fact, newest first among equals, and drops nothing. It reads the writes this connection made, so it returns the correction at once.
- The program prints `user`, `query` and `facts`, the same fields in the same order as the Python and TypeScript programs.

## Run it

Run from `examples/rust`:

```sh
LASER_CONNECTION_STRING='<connection string>' cargo run --example memory-correction
```

It runs on Apache Iggy, Laser Stack, and LaserData Cloud. Recall reads the managed memory view where the deployment serves one and folds the memory topic in process on Apache Iggy.

## Learn more

- Docs: https://docs.laserdata.com/laser-sdk/memory
- The four memory verbs: [`recall`](../recall/README.md)
