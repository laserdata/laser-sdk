# Building agentic apps

A recipe guide. It shows how the SDK's primitives compose into a real multi-agent system: one scenario worked end to end (a customer-support triage desk), then a set of common product journeys mapped onto the exact SDK calls. For the primitives on their own, start with the [tutorial](tutorial.md). For the protocol underneath, see the [AGDX notes](agdx.md).

One idea underpins everything. You **append a message to a topic on the log, and you read messages back from an offset**. Memory, working state, and materialized views are read models built over that one message log. Replay, audit, fan-out, and multi-agent collaboration follow from that shape rather than needing separate machinery.

With the SDK's `vsr` feature, the same append, consume, reliable-agent, AGDX, cursor, and folded log-memory code runs through Apache Iggy's VSR cluster client. Managed query, KV-backed memory, graph, forks, run registry, and live presence still require custom command codes that the current upstream VSR encoder does not accept, so capability checks keep those paths unavailable rather than silently changing their behavior.

## The primitives, and the call that reaches each

| You want to | Reach for | The call |
| --- | --- | --- |
| Append a message to a topic | a topic | `laser.topic("triage.classified").publish().json(&c)?.send().await?` |
| Read from an offset, resumably | a `Cursor` | `laser.topic("triage.actions").replay()?` then `poll` / `from_offsets` |
| Run an agent on a topic | `Agent::builder` | `.listen_on(AgentTopic::Commands).respond_on(..).handler(H).build().spawn(laser)` |
| Ask an agent and await the reply | the agent accessor | `laser.agent("caller").ask(req, reply, body, &prov, timeout).await?`, or `ctx.request(..)` inside a handler |
| Stream a large result in chunks | `AgdxStream` | `laser.agdx(..).stream(corr, "context").buffered(64, linger)` then `finish` |
| Send a large body by reference | a claim-check | `.claim_check(&store, threshold)` on the publish, `resolve_body(&store)` on read |
| Make an external effect happen once | a KV compare-and-swap | `laser.kv("effects").set(key).bytes(b).expect_absent().commit().await?` |
| Look up structured facts | the query surface | `laser.query("orders").where_eq(..).fetch_typed::<Order>().await?`, or `.raw_sql(..)` |
| Reason over how things connect | the graph | `laser.graph("services").neighbors(node, EdgeDir::Out, None, 2).await?` |
| Remember and recall | memory | `laser.memory("support").remember(fact).scope(c).dedup().send().await?` / `.recall(c).semantic(q).fetch()`, a vector handle built from a governed `Laser` applies the same pre-write policy locally |
| Keep named working state | memory named items | `laser.memory("session").set("plan", json).await?` / `.fetch("plan")` |
| Pause for a human, then resume | the human-input gate | `ctx.approval_gate(reply, prompt, timeout).await?`, an approver replies with `ctx.respond_input(..)` |
| Move a task through its lifecycle | the A2A task state | responses carry `TaskState` (`Working`, `InputRequired`, `Completed`, `Failed`) |
| Survive a crash mid-flow | replay + idempotency | `ConversationState::load(.., ReplayBound::Full, ..)`, and commit offsets only after the effect's key |
| Never double-apply a redelivery | the reliable consumer | `Agent::builder().deduplicator(..)`, undecodable and retry-exhausted messages dead-letter |

Every call rides one authenticated connection. None of them needs a second database, a queue, or a cache.

## Governing agents and managed data

There are two authorization layers, and they guard different things.

Native Iggy permissions decide whether a credential can see streams, create topics, send records, and poll records. Use them to isolate tenants and agent inboxes at the stream/topic boundary. A stream the principal cannot read must be treated like a missing stream.

LaserData governance roles decide whether the same server-stamped user can call managed surfaces: query, projections, KV, graph, forks, the run registry, workflow control, and the `authz` administration band. A role is a set of grants:

```text
allow kv:read on prefix:support/
allow projection:admin on literal:support_tickets
deny kv:write on prefix:support/secrets/
```

Roles bind to raw Iggy user ids, never to a client-supplied claim. A newly created user has no managed-surface capabilities unless an operator explicitly binds roles with `laser.bind_roles(user_id, roles)` or through the console. The only seeded managed role is the reserved root `admin` role for the default administrator, so bootstrap is possible without granting new users anything by accident.

For agents, use separate credentials when the agent needs its own operational identity. Give that account the narrow native Iggy permissions for its inbox/outbox topics and the narrow governance roles for the managed names it may touch. When the agent acts on behalf of a human or tenant user, authorize by intersection: the agent's grants and the invoking user's grants must both allow the operation. The agent never inherits more than the user, and the user never inherits the agent's service-account reach.

Run budgets are governance controls, not permissions. They cap events, model calls, tool calls, patches, depth, wall time, or cost for a submitted run, but they do not grant access to data. A run still needs the underlying stream/topic permission and the managed-surface grant for every read or write it performs.

The action governor is the third layer, above both: a pre-effect policy hook (`Laser::with_governor`, or `governor` on the agent builder) that decides per side effect before the SDK publishes it: allow, observe, block, step-up on an approval scope, modify the body, or defer. It sees the action's kind, target, conversation, tool, delegation subject, the advisory `purpose`/`data_classification` metadata, and session counters, and every non-allow decision lands as a digest-chained `PolicyEvidence` event on the audit topic. Start a rollout in observe mode (shadow: record everything, block nothing), inspect the evidence, then switch to enforce. It is defense in depth for what an agent does with access it legitimately holds, while the server-owned layers above still decide what it can access at all.

The SDK exposes the same model in both languages:

```rust
use laser_sdk::wire::authz::{Action, Effect, Feature, Grant, ResourcePattern, Role};

laser
    .define_role(Role {
        name: "support-reader".into(),
        grants: vec![Grant {
            effect: Effect::Allow,
            feature: Feature::Kv,
            action: Action::Read,
            resource: ResourcePattern::prefix("support/"),
        }],
    })
    .await?;

laser.bind_roles(user_id, vec!["support-reader".into()]).await?;
let who = laser.whoami().await?;
```

End-to-end enforcement lives in the Iggy fork and LaserData managed plane. In this repo the contract is pinned by `wire/tests/wire_fixtures.rs` and `wire/tests/constants.rs`, the pure deny-wins and delegation rules are unit-tested in `laser_wire::authz`, Rust and Python SDK parity is covered by the RBAC bindings/tests, and the mirrored `governance` examples exercise the live role and binding calls when a connected deployment advertises `authz`.

## The scenario: a support-triage desk

Four agents move a ticket from "a customer said something" to "resolved, or waiting on a human". No agent calls another directly. Each reads a topic and appends to the next. It is the shape of the [`concierge`](../examples/rust/src/concierge/README.md) example.

**Classifier** reads inbound tickets, classifies them, and appends the result.

```rust
let mut classifier = Agent::builder()
    .id("classifier".parse()?)
    .listen_on(AgentTopic::Commands)
    .respond_on(AgentTopic::Responses)
    .handler(Classifier { llm: llm.clone() })
    .build()
    .spawn(laser.clone());
```

**Retriever** reads the classification, looks up grounding facts with the query surface (the structured IR, or the read-only SQL escape hatch for a join), and streams the context back as chunks.

```rust
let orders = laser
    .query("orders")
    .where_eq("customer_id", customer)
    .order_desc("ts")
    .limit(20)
    .fetch_typed::<Order>()
    .await?;
// A read-only join the IR does not express:
let prior = laser.query("tickets").raw_sql("SELECT ... JOIN ...").fetch().await?;
```

**Resolver** drafts the fix, and makes the external effect happen exactly once over an at-least-once log with a compare-and-swap on the idempotency key. A redelivery finds the key already set and does not issue a second refund. If the resolver runs under an `ActionGovernor`, that final topic publish is governed at the same effect boundary as AGDX sends and memory writes.

```rust
match laser
    .kv("effects")
    .set(format!("refund:{order_id}"))
    .bytes(proposal)
    .expect_absent()
    .commit()
    .await
{
    Ok(_) => issue_refund().await?,          // first time: apply the effect
    Err(e) if e.is_version_conflict() => {}  // already proposed: skip, do not duplicate
    Err(e) => return Err(e),
}
```

**Escalator** applies policy. Within policy it auto-approves. Over the threshold it requests human input, which parks the flow at `input-required` and resumes from the committed offset when the decision arrives. An approver agent, listening on the human-input topic, resolves it with `ctx.respond_input(..)`.

```rust
let decision = ctx
    .approval_gate(AgentTopic::Responses, prompt, Duration::from_secs(60))
    .await?;
```

The whole ticket is rebuildable from its conversation with `ConversationState::load`, so an operator can reconstruct exactly what each agent saw.

## The product journeys, mapped

Each of these is something a real support product does, and each is a handful of SDK calls.

**Update a customer's memory, and have everything downstream see it.** `set` appends a record to the memory topic. A deployment materializes that record into the read view, and every consumer of the topic sees it. There is no separate publish step.

```rust
laser.memory("profiles").set("customer:123", subscription_json).await?;
```

**Keep a preference with a full audit trail.** The same `set`. Because the write is a record on the log, "who changed it, when, and whether another workflow overwrote it" is answered by reading the topic back. The history is the log itself, not a reconstruction from current state.

**Escalate to a human, and have subscribers react.** The escalator requests input, which moves the task to `input-required` and parks the flow. A human console, a dashboard, and any other agent all read the same status topic. No polling, no database triggers. The reply resumes the flow from the committed offset.

```rust
let decision = laser
    .agdx(AgentTopic::HumanInput, agent_id.wire_id(), prov.conversation_id.into())
    .request_input(AgentTopic::Responses, prompt, timeout)
    .await?;
```

**Collaborate across agents.** One agent remembers a fact. Another reads the memory topic and reacts. Loose coupling, no shared database.

```rust
let mut reader = laser.topic("agent.audit").replay()?;
loop {
    for message in reader.poll().await? { /* react to each new memory record */ }
}
```

**Recover from a crash without double-charging.** The resolver commits a stable idempotency key before it commits its offset. A crash and replay then re-reads the batch, finds the effect already applied, and moves on. This is an idempotent effect over an at-least-once log, not a blanket exactly-once transport guarantee. A reassigned lease holder additionally needs fenced CAS with the workflow and effect using the same KV namespace.

**Replay a production incident.** Reconstruct a conversation from the log alone, or re-run a fixed agent version over historical offsets.

```rust
let state = ConversationState::load(&laser, conversation, topics, ReplayBound::Full, init, fold).await?;
```

**Watch operations live.** A dashboard tails the topics. Where the change feed is served, it waits for a change record instead of re-querying on a timer. This is what the management console does.

**Analytics without ETL.** Declare a projection once per topic and query the materialized rows: volumes, deflection rate, mean time to resolution, sentiment trends. The log is the change stream, so no CDC tooling is needed.

```rust
let by_category = laser.query("tickets").group_by(["category"]).count().fetch().await?;
```

**See everything one conversation touched.** Because every managed read model records the conversation that wrote each row, a read narrows to one conversation. `laser.query(index).conversation(c)` returns only that conversation's rows, `laser.graph(g).conversation(c).neighbors(..)` returns only what it asserted in the graph, and `laser.kv(ns).scan().conversation(c)` returns only its memory. It is the console's conversation lens, a read-side filter over provenance the log already carries, not a new boundary.

```rust
let rows = laser.query("tickets").conversation(conversation).fetch().await?;
let facts = laser.kv("profiles").scan().conversation(conversation).entries().await?;
```

**Add an integration later with no agent code change.** A new sink is a new consumer on an existing topic, or a new projection. The agents that produce the messages never learn it exists.

## One log, many sinks

Because the log is the source of truth, observability, analytics, and lake sinks are ordinary consumers of the same topics. An agent appends a message once, and a trace store, an OLAP store, and a lakehouse each read it independently. The SDK stamps OpenTelemetry `gen_ai.*` headers on each record, so a span view populates with no instrumentation in the agent. Adding a sink means adding a reader, not changing a producer.

## What the SDK gives you, and where you write code

The SDK owns the hard mechanics: the durable log and its offsets, idempotency and fenced-effect primitives, chunk reassembly, the dead-letter path, replay, the typed envelope, and capability negotiation, so the same code runs against raw Apache Iggy or LaserData Cloud. You bring the model seam (`Embedder`, `LlmClient`, `Reranker`, and `Summarizer` are traits you implement or point at a provider) and the business logic in each handler. There is no separate orchestration server, state store, or message queue to run. The one connection is the whole substrate.
