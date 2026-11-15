# memory-correction: replace a fact for one user, then recall the current facts

The first conversation stores two facts about a user. A later conversation corrects one of them with `supersede`, which remembers the new fact and forgets the old one in every conversation. Recall for the user reads every conversation and returns the correction at once. The program prints the recalled facts as JSON. It imports only `@laserdata/laser-sdk`, so it runs when copied alone into any project that installed the package.

## What it shows

- `laser.scratchStream("memory-demo")` creates a stream with a unique name, and `await using` deletes it when the program ends. The first memory write creates the memory topic.
- `memory.remember(text).scope(conversation).user("sam").send()` returns the fact's id.
- `memory.supersede(id, text).scope(conversation).user("sam").send()` corrects it for the user in every conversation.
- `memory.recall().user("sam").semantic("where does Sam live").fetch()` ranks every fact of the user by the words it shares with the question and drops none. It waits until the memory view shows this connection's own writes, so the correction is there at once.

## Run it

```sh
LASER_CONNECTION_STRING='<connection string>' npm run example:memory-correction
```

It runs against plain Apache Iggy, Laser Stack, and LaserData Cloud. A deployment that serves no managed memory view folds the memory topic in process for the same recall.
