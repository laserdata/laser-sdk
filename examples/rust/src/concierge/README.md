# concierge - an AI support desk on the log

The agentic example. One realistic system: an AI support desk operating a live incident end to end, with every agent coordinating only through the log, never a direct call. Each platform feature does the job it exists for inside one story instead of starring in its own demo. Layer: agentic, and the full-AGDX showcase: it exercises every surface in one run, streaming and the agent envelope, materialized views and query, key-value, and forks.

## What it does

1. **World model.** A ticket firehose (`LASER_MESSAGES`, batched by `LASER_BATCH`) bulk-ingests into a `support_tickets` topic. Every field rides as an indexed header and the JSON body is inlined, so LaserData Cloud materializes a fully queryable ticket table while the log keeps the raw bytes. Tickets carry the `message_type` and `ts` convention fields, so the reserved columns fill and the `message_type` / `time_range` query sugar works.
2. **Memory.** Past resolution notes are remembered in a shared in-process `VectorMemory` (a deterministic `Embedder` behind the same seam a real model plugs into) and recalled semantically when the incident arrives. The desk runs in one process, so the specialist reads the index the desk seeded at startup.
3. **The desk.** Four agents on the agent topics:
   - **triage** (Commands, responds on Responses) queries the index as a tool for the live blast radius, fans one diagnostic angle per specialist call under a deadline (each on its own correlation conversation), and synthesizes the findings into a diagnosis with the LLM.
   - **specialist** (ToolCalls to ToolResults) answers each angle from recalled memory plus the LLM.
   - **resolver** (Commands, KV-deduplicated) applies remediation credits. The effect is a read-modify-write on a KV balance, which is exactly why the `Deduplicator` gate in front of it matters: the credit list is sent twice and the totals come out exact. Credits at or above the threshold hold for a durable approval first.
   - **approver** (HumanInput to Responses) stands in for the human behind that gate.
4. **Speculation.** The diagnosis proposes bulk-resolving the open critical checkout backlog. The desk stages the plan in a copy-on-write fork, compares the forked backlog against the trunk, and leaves the fork open with the verdict logged so the LaserData Cloud can show it. `LASER_APPLY_PLAN=1` acts on the verdict instead: promote when the plan clears the criticals, squash when it does not (the trunk never changed).
5. **Memory loop.** The diagnosis is remembered as a new note, so the next similar incident recalls what this one learned.
6. **Audit.** The whole incident is one conversation on the log. The run ends by rebuilding it with `ConversationState::load`, the same fold a crashed coordinator runs on restart. No side database, the stream is the state.

The queryable world model, KV, approvals, and forks run on LaserData Cloud. On raw Apache Iggy the example prints how to point at a deployment and exits green before provisioning the desk. The in-process `VectorMemory` remains useful independently in the `memory` example.

## Run it

```sh
# local server: prints the managed requirement and exits green
just up && cargo run --release --example concierge

# against LaserData Cloud, full desk
LASER_CONNECTION_STRING=iggy://user:pwd@your-laserdata-cloud-host \
  cargo run --release --example concierge

# heavy world: a million tickets
LASER_MESSAGES=1000000 LASER_BATCH=1000 cargo run --release --example concierge

# real model instead of the deterministic mock
ANTHROPIC_API_KEY=... cargo run --release --example concierge --features llm-anthropic
OPENAI_API_KEY=...    cargo run --release --example concierge --features llm-openai

# apply the speculative plan's verdict instead of leaving the fork open
LASER_APPLY_PLAN=1 cargo run --release --example concierge

# a heavily rate-limited deployment (a free-tier bandwidth cap, say) needs more
# than the default 180s to settle the credit-apply phase
LASER_CONCIERGE_CREDIT_TIMEOUT_SECS=600 \
  LASER_CONNECTION_STRING=iggy://user:pwd@your-laserdata-cloud-host \
  cargo run --release --example concierge
```

## Where to look (LaserData Cloud)

- **Query**: index `support_tickets` (the world model) and `concierge_memory` (the embedded notes).
- **KV**: namespaces `concierge-credits-<run>` (the applied balances) and `concierge-dedup-<run>` (the idempotency keys that blocked the redelivery). The run logs the exact names.
- **Forks**: `bulk-resolve-plan` stays open after a default run (with `LASER_APPLY_PLAN=1` it was promoted or squashed by the end).
- **Messages**: the agent topics carry the whole conversation, provenance headers included.

## Highlights

- `Agent::builder()` with `.listen_on` / `.respond_on` / `.deduplicator`, `ctx.request(..)` fan-out under a `deadline`, `ctx.respond(..)` replies, `laser.request(..)` awaiting the desk end to end.
- A shared `VectorMemory` + `Embedder` for remember and recall, closing the loop by remembering the new resolution.
- A KV-backed `Deduplicator` turning at-least-once delivery into effectively-once effects, proven by a deliberate redelivery.
- A coordination demo: a credit-ledger compare-and-swap (`set(..).expect_absent()` / `.expect_version(v).commit()`) with a conflict-retry loop for lock-free optimistic concurrency, a `read_your_writes()` query for read-after-write, and `LaserError::code()` classifying every outcome into the unified `ResultCode` (so an unserved level reports cleanly rather than failing the run).
- A durable approval gate over `AgentTopic::HumanInput`.
- `laser.fork(id)` create / `put_row` / overlay query / `promote` / `squash` as a guarded what-if.
- `ConversationState::load` rebuilding the incident from the log alone.
