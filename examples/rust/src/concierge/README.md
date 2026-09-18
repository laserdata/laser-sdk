# concierge - an AI support desk on the log

This example runs a support desk whose agents coordinate through the log. It combines tickets, queries, memory, credits, approval, and a proposed change in a fork.

## What it does

1. Publish tickets to `support_tickets` using `LASER_MESSAGES` and `LASER_BATCH`. Indexed fields and inline JSON support managed queries. `message_type` and `ts` fill the reserved query fields.
2. Store resolution notes in a shared local `VectorMemory`. The example uses a deterministic `Embedder`. The specialist reads this same index.
3. The desk. Four agents on the agent topics:
   - Triage reads Commands, queries affected tickets, requests specialist input under a deadline, and returns its diagnosis on Responses.
   - The specialist reads ToolCalls and returns memory-assisted model answers on ToolResults.
   - The resolver reads Commands and updates credit balances. Its KV-backed `Deduplicator` suppresses the repeated credit request in this example. Credits at or above the threshold require recorded approval.
   - The approver reads HumanInput and answers on Responses.
4. Stage the proposed backlog change in a fork and compare it with the trunk. The default leaves the fork open. With `LASER_APPLY_PLAN=1`, promote a successful plan or squash it.
5. Record the diagnosis as a note for later incidents.
6. Rebuild the incident with `ConversationState::load` from its conversation records.

Queries, KV, approvals, and forks require Laser Stack or LaserData Cloud. Without `laser-plane`, the example explains the requirement and exits before creating the desk. The `memory` example also demonstrates local `VectorMemory` independently.

## Run it

Run from `examples/rust`:

```sh
# local server: prints the managed requirement and exits green
just up && cargo run --release --example concierge

# against LaserData Cloud, full desk
LASER_CONNECTION_STRING=user:pwd@your-laserdata-cloud-host \
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
  LASER_CONNECTION_STRING=user:pwd@your-laserdata-cloud-host \
  cargo run --release --example concierge
```

## Where to look (LaserData Cloud)

- Query: index `support_tickets` (the world model) and `concierge_memory` (the embedded notes).
- KV: namespaces `concierge-credits-<run>` (the applied balances) and `concierge-dedup-<run>` (the idempotency keys that blocked the redelivery). The run logs the exact names.
- Forks: `bulk-resolve-plan` stays open after a default run (with `LASER_APPLY_PLAN=1` it was promoted or squashed by the end).
- Messages: the agent topics carry the whole conversation, provenance headers included.

## Highlights

- `Agent::builder()` with `.listen_on` / `.respond_on` / `.deduplicator`, `ctx.request(..)` fan-out under a `deadline`, `ctx.respond(..)` replies, `laser.request(..)` awaiting the desk end to end.
- A shared `VectorMemory` + `Embedder` for remember and recall, closing the loop by remembering the new resolution.
- A KV-backed `Deduplicator` suppresses the deliberate repeated request in this scenario. It does not establish exactly-once external effects across arbitrary failures.
- The coordination demo uses `set(..).expect_absent()` and `.expect_version(v).commit()` with conflict retries. It also uses `read_your_writes()` and classifies outcomes through `LaserError::code()` and `ResultCode`.
- A durable approval gate over `AgentTopic::HumanInput`.
- `laser.fork(id)` create / `put_row` / overlay query / `promote` / `squash` as a guarded what-if.
- `ConversationState::load` rebuilding the incident from the log alone.
