# memory - recent, semantic, durable, and connected knowledge

Exercises the memory facade as one workflow rather than treating recall as a
standalone vector search.

## What it does

1. Stores facts in deterministic 64-dimensional vector memory.
2. Compares recent and semantic recall.
3. Applies operator feedback and forgets a superseded fact.
4. Stores conversation-scoped durable memory on LaserData Cloud.
5. Registers the `ops-knowledge` graph, polls the asynchronous apply, links
   services to components, and reads graph neighbors.

## Run it

```sh
npm run build
node dist/src/memory/main.js
```

The vector phase runs locally on raw Apache Iggy. Durable recall and graph
traversal are capability-gated because their read models are managed.

## Highlights

- Memory IDs, kinds, feedback, and scopes use SDK types.
- The deterministic embedder is a test seam, not a production embedding model.
- Conversation memory and graph knowledge retain their distinct scopes.
