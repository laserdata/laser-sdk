# recall

> An agent that learns from feedback: the four agentic-memory verbs as one loop.

## What it does

Agentic memory is one loop, and this example runs all of it: remember what you
know, recall what is relevant, improve the recall from feedback, and forget what
is stale.

1. **Remember** the assistant stores what it knows, each item a fact.
2. **Recall** a question recalls the semantically closest facts, ranked by
   similarity.
3. **Improve** the operator upvotes the fact that actually resolved the
   incident, and the next recall ranks it first. The agent learns.
4. **Forget** a superseded fact is forgotten and stops surfacing.

It runs the whole loop in process over `VectorMemory` with a deterministic
bag-of-words embedder (the model seam an app fills with a real embedding model),
so it needs no server.

## Run it

```sh
cargo run --release --example recall
```

## Where it goes next

The same four verbs run durably and at scale by swapping the backend with no
change to the loop: `Laser::memory()` (log- or KV-backed, picked from the
negotiated capabilities) or `QueryMemory` (vector recall over a managed
materialized index). The durable memory builds a knowledge graph as it learns,
its own managed surface (AGDX A13), browsable in the management console's graph
explorer.

## Highlights

- The agentic-memory verbs `remember` / `recall` / `improve` / `forget` on the
  `Memory` trait, one backend-agnostic surface.
- Feedback-weighted recall: `improve` reweights ranking, so the agent gets
  better with use rather than only with more data.
- The model seam: `Embedder` is the one place a real embedding model plugs in,
  the same boundary as the `LlmClient` seam in the other examples.
