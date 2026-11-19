# memory-correction: replace a fact for one user, then recall the current facts

The first conversation stores three facts about a user. A later conversation corrects one of them with `supersede`, which remembers the new fact and forgets the old one in every conversation. Recall for the user reads every conversation and returns the correction at once. The program prints the user, the question and the recalled facts as JSON. It imports only `@laserdata/laser-sdk`, so it runs when copied alone into any project that installed the package.

## What it shows

- `root.scratchStream("memory")` creates a stream with a unique name, and `await using` deletes it when the program ends. The first memory write creates the memory topic.
- `memory.remember(text).scope(conversation).user(user).send()` returns the fact's id.
- `memory.supersede(id, text).scope(conversation).user(user).send()` corrects it for the user in every conversation.
- `memory.recall().user(user).semantic(query).limit(10).fetch()` returns `MemoryItem`s ranked by the words each fact shares with the question, newest first among equals, and drops none. It waits until the memory view shows this connection's own writes, so the correction is there at once.
- The program prints `user`, `query` and `facts`, the same fields in the same order as the Python and Rust programs.

## Run it

```sh
LASER_CONNECTION_STRING='<connection string>' npm run example:memory-correction
```

It runs against plain Apache Iggy, Laser Stack, and LaserData Cloud. A deployment that serves no managed memory view folds the memory topic in process for the same recall.
