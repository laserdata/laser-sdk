# concierge - an AI support desk on the log

> The full-stack agent example, combining a queryable ticket world, semantic memory, durable coordination, approvals, speculative planning, and replay.

## What it does

1. Generates deterministic support tickets and publishes them in bounded batches to `support_tickets`, explicitly inlining each body into its materialized row.
2. Registers a body-extracted projection, waits until every ticket is queryable, and decodes one selected payload to verify the world model retains its source body.
3. Seeds an in-process vector memory with prior resolutions.
4. Starts four long-running agents: triage, specialist, resolver, and approver.
5. Fans three deadline-bounded specialist questions from triage, then synthesizes the findings through the example-owned LLM seam.
6. Applies remediation credits through a KV-backed deduplicator even though every credit command is sent twice.
7. Routes large credits through a correlated human approval gate before the resolver changes state.
8. Stores the diagnosis as durable memory under the incident conversation.
9. Writes a speculative bulk-resolution row into the `bulk-resolve-plan` fork and optionally promotes it.
10. Rebuilds the incident from agent command, response, tool, and result topics through `ConversationState`.

The example requires query, KV compare-and-swap, and forks for the full desk. On stock Apache Iggy it reports the missing managed surfaces and exits before starting the agents.

## Run it

```sh
npm run example:concierge
```

Run the complete desk on LaserData Cloud.

```sh
LASER_CONNECTION_STRING=user:pwd@your-laserdata-cloud-host \
  npm run example:concierge
```

Scale ticket ingestion or apply the speculative plan.

```sh
LASER_MESSAGES=200000 LASER_BATCH=1000 \
  npm run example:concierge

LASER_APPLY_PLAN=1 \
  npm run example:concierge
```

Set `ANTHROPIC_API_KEY` or `OPENAI_API_KEY` to replace the deterministic `MockLlm` without changing any agent, routing, memory, or transport code.

## Where to look (LaserData Cloud)

- **Query**: the `support_tickets` world model, including payload selection for the original ticket JSON.
- **KV**: `concierge-credits-<run>` balances and `concierge-dedup-<run>` idempotency keys. The run ID is printed in the namespace.
- **Forks**: `bulk-resolve-plan`, left open unless `LASER_APPLY_PLAN=1`.
- **Conversations**: commands, specialist calls, approvals, responses, and the replayed incident audit trail.
- **Memory**: the diagnosis remembered under the incident conversation.

## Highlights

- `Agent.builder()` defines identity, inbox, reply topic, handler, deduplication, and poll behavior without hiding the underlying Iggy topics.
- `publishBatch().inlinePayload()` preserves each ticket body for query payload selection without duplicating indexed values in headers.
- `context.request()` carries causality into specialist sub-conversations and bounds every branch with a deadline.
- The KV deduplicator uses `expectAbsent().commit()` so at-least-once delivery does not duplicate the credit effect.
- `approvalGate()` composes human input from ordinary correlated AGDX commands and responses.
- `laser.fork(id)` isolates a what-if row until the caller promotes it.
- `ConversationState.load()` proves the incident can be rebuilt from the durable log alone.
