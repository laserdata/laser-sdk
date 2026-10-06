# Building agentic apps

This guide builds a support-triage workflow, then maps related application tasks to SDK calls. Start with the [tutorial](tutorial.md) for individual operations. Read the [AGDX notes](agdx.md) for the data contract. The snippets use Rust, the reference SDK. The [three-language guides](https://docs.laserdata.cloud/laser-sdk) show each feature in TypeScript, Rust, and Python, and the [parity matrix](parity.md) maps every Rust call to its Python and TypeScript spelling.

Applications append records to topics and read them by offset. Memory, state, and queryable views derive from these records. Agents use the same log to coordinate and retain history.

All append, consume, reliable-agent, AGDX, cursor, and folded log-memory code runs through Apache Iggy's VSR cluster client. Managed query, KV-backed memory, graph, forks, run registry, and live presence use the custom command band. The streaming server promotes authorization mutations to dedicated replicated operations.

## The primitives, and the call that reaches each

| You want to | Reach for | The call |
| --- | --- | --- |
| Append a message to a topic | a topic | `laser.stream("support").topic("triage.classified").publish().json(&c)?.send().await?` |
| Read from an offset, resumably | a `Cursor` | `laser.stream("support").topic("triage.actions").replay()?` then `poll` / `from_offsets` |
| Run an agent on a topic | `Agent::builder` | `.listen_on(AgentTopic::Commands).respond_on(..).handler(H).build().spawn(laser)` |
| Ask an agent and await the reply | the agent accessor | `laser.agent("caller".parse()?).ask(req, reply, body, &prov, timeout).await?`, or `ctx.request(..)` inside a handler |
| Stream a large result in chunks | `AgdxStream` | `laser.agdx(..).stream(corr, "chat").buffered(64, linger)` then `finish` |
| Send a large body by reference | a claim-check | `.claim_check(&store, threshold)` on the publish, `resolve_body(&store)` on read |
| Make an external effect happen once | a KV compare-and-swap | `laser.kv("effects").set(key).bytes(b).expect_absent().commit().await?` |
| Look up structured facts | the query surface | `laser.query("readings").where_eq(..).fetch_typed::<Reading>().await?`, or `.raw_sql(SqlDialect::DataFusion, ..)` |
| Reason over how things connect | the graph | `laser.graph("services").neighbors(node, EdgeDir::Out, None, 2).await?` |
| Remember and recall | memory | `laser.memory("support").remember(fact).scope(c).dedup().send().await?` / `.recall(c).semantic(q).fetch()`, a vector handle built from a governed `Laser` applies the same pre-write policy locally |
| Keep named working state | memory named items | `laser.memory("session").set("plan", json).await?` / `.fetch("plan")` |
| Pause for a human, then resume | the human-input gate | `ctx.approval_gate(reply, prompt, timeout).await?`, an approver replies with `ctx.respond_input(..)` |
| Move a task through its lifecycle | the A2A task state | responses carry `TaskState` (`Working`, `InputRequired`, `Completed`, `Failed`) |
| Survive a crash mid-flow | replay + idempotency | `ConversationState::load(.., ReplayBound::Full, ..)`, and commit offsets only after the effect's key |
| Never double-apply a redelivery | the reliable consumer | `Agent::builder().deduplicator(..)`, undecodable and retry-exhausted messages dead-letter |

Streaming and managed calls share one authenticated connection. Managed deployments maintain the storage needed for their read models.

## Governing agents and managed data

There are two authorization layers, and they guard different things.

Native Iggy permissions decide whether a credential can see streams, create topics, send records, and poll records. Use them to isolate users, teams, and agent inboxes at the stream/topic boundary. A stream the principal cannot read must be treated like a missing stream.

LaserData governance roles decide whether the same server-stamped user can call managed surfaces: query, projections, KV, graph, forks, the run registry, workflow control, and the `authz` administration band. A role is a set of grants:

```text
allow kv:read on prefix:support/
allow projection:admin on literal:support_tickets
deny kv:write on prefix:support/secrets/
```

Roles bind to the authenticated Iggy user ID. New users receive no managed capabilities until an operator assigns roles through `laser.bind_roles(user_id, roles)` or the Console. The default administrator receives the reserved `admin` role for initial setup.

Give an agent separate credentials when it needs its own identity. Limit its native Iggy permissions to the required topics. Limit managed roles to the names it needs. For delegation, both the agent grants and the invoking user grants must allow the operation. Neither identity gains the other identity broader access.

Run budgets limit events, model calls, tool calls, patches, depth, elapsed time, and cost. They do not grant data access. Each operation still requires native permissions and the applicable managed grant.

`ActionGovernor` applies policy before an SDK effect. Configure it through `Laser::with_governor` or the agent builder `governor`. It can allow, observe, block, require approval, modify a body, or defer an action. It receives the target, conversation, tool, delegated identity, counters, and advisory `purpose` and `data_classification` metadata. Decisions that are not allow produce linked `PolicyEvidence` records.

To introduce a policy, start in observe mode and inspect the evidence. Then select enforce mode. This policy can restrict permitted actions but cannot expand server access.

The SDK exposes this model through the language bindings:

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

End-to-end enforcement lives in the Iggy fork and the LaserData managed plane. In this repo the contract is pinned by `wire/tests/wire_fixtures.rs` and `wire/tests/constants.rs`. Rust, Python, and TypeScript test the typed client surface and shared governance scenarios. The mirrored `governance` examples exercise live role and binding calls when a deployment advertises `authz`.

## The scenario: a support-triage desk

Four agents process a ticket until it is resolved or needs a human decision. They exchange records through topics. The [`incident-desk`](../examples/rust/src/incident-desk/README.md) example uses this pattern.

Classifier reads inbound tickets, classifies them, and appends the result.

```rust
let mut classifier = Agent::builder()
    .id("classifier".parse()?)
    .listen_on(AgentTopic::Commands)
    .respond_on(AgentTopic::Responses)
    .handler(Classifier { llm: llm.clone() })
    .build()
    .spawn(laser.clone());
```

The returned `AgentHandle` owns the running agent. Dropping it signals a graceful shutdown, so keep it until the desk is done. Call `shutdown().await` to wait for the in-flight message and read the consumer result.

The retriever reads the classification and queries relevant facts. It uses the structured query API or read-only SQL for a join. It returns context as chunk records.

```rust
let readings = laser
    .query("readings")
    .where_eq("host_id", host)
    .order_desc("ts")
    .limit(20)
    .fetch_typed::<Reading>()
    .await?;
// A read-only join the IR does not express:
let prior = laser.query("tickets").raw_sql(SqlDialect::DataFusion, "SELECT ... JOIN ...").fetch().await?;
```

The resolver records an idempotency key and publishes a capacity grant. A repeated request can detect the stored key. These are separate operations, so this example does not guarantee exactly-once external effects across crashes. An `ActionGovernor` can also govern the final publication.

```rust
match laser
    .kv("effects")
    .set(format!("grant:{ticket_id}"))
    .bytes(proposal)
    .expect_absent()
    .commit()
    .await
{
    Ok(_) => apply_grant().await?,           // first time: apply the effect
    Err(e) if e.is_version_conflict() => {}  // already proposed: skip, do not duplicate
    Err(e) => return Err(e),
}
```

The escalator applies the approval policy. Above the threshold, it requests human input and waits for the decision. An approver reads the human-input topic and answers through `ctx.respond_input(..)`.

```rust
let decision = ctx
    .approval_gate(AgentTopic::Responses, prompt, Duration::from_secs(60))
    .await?;
```

`ConversationState::load` rebuilds the ticket conversation from retained records. Operators can inspect the recorded inputs and replies.

## The operator journeys, mapped

The following examples map support tasks to SDK calls.

`set` records a memory change on the topic. The deployment applies it to the read view. Other topic consumers can read the same record.

```rust
laser.memory("profiles").set("host:node-7", config_json).await?;
```

A recorded `set` preserves the configuration change in log history. Read retained records to inspect the writer, time, and later changes.

A human-input request publishes a prompt and waits for a correlated response. Consoles and agents can read its topic. With a verifier, only a valid signed response can resume the request. An approver with a signing key uses `ctx.respond_input(..)` to provide that response.

```rust
let decision = laser
    .agdx(AgentTopic::HumanInput, agent_id.wire_id(), prov.conversation_id.into())
    .request_input(AgentTopic::Responses, prompt, timeout)
    .await?;
```

An agent can publish a memory fact, and another can read the memory topic and react.

```rust
let mut reader = laser.stream("support").topic("agent.audit").replay()?;
loop {
    for message in reader.poll().await? { /* react to each new memory record */ }
}
```

Use stable idempotency keys to identify repeated requests after recovery. Recording a key separately from an external effect does not make those steps atomic. The effect handler needs a transaction or an external operation that is safe to repeat. A reassigned lease holder also needs fenced CAS in the workflow coordination namespace.

Replay a production incident. Reconstruct a conversation from the log alone, or re-run a fixed agent version over historical offsets.

```rust
let state = ConversationState::load(&laser, conversation, topics, ReplayBound::Full, init, fold).await?;
```

Watch operations live. A dashboard tails the topics. Where the change feed is served, it waits for a change record instead of re-querying on a timer. This is what the management console does.

Declare a projection and query the resulting rows for volumes, resolution time, and other aggregates. The projector consumes the source topic directly.

```rust
let by_category = laser.query("tickets").group_by(["category"]).count().fetch().await?;
```

Managed read models retain the originating conversation when available. `laser.query(index).conversation(c)` filters rows, and `laser.graph(g).conversation(c).neighbors(..)` filters graph assertions. `laser.kv(ns).scan().conversation(c)` filters memory-view entries. These filters narrow results but do not define an access boundary.

```rust
let rows = laser.query("tickets").conversation(conversation).fetch().await?;
let facts = laser.kv("profiles").scan().conversation(conversation).entries().await?;
```

Add an integration later with no agent code change. A new sink is a new consumer on an existing topic, or a new projection. The agents that produce the messages never learn it exists.

## One log, many sinks

Observability, analytics, and lakehouse consumers can read the same topics independently. An agent publishes each source record once. OpenTelemetry `gen_ai.*` metadata can support trace projections. Adding a reader does not require a producer change.

## What the SDK gives you, and where you write code

Apache Iggy owns the log, transport, and offsets. Laser SDK adds typed records, replay helpers, agent processing, chunk assembly, and managed operation clients. Applications supply handler logic and model integrations through `Embedder`, `Reranker`, and `Summarizer`. The SDK ships no model client, and the examples define their own `LlmClient` seam. Managed features require a deployment that provides their backend services.

Periodic consolidation uses the agent identity as its owner scope when the agent has an ID. A summarizer writes one summary per conversation. Python hooks accept a callable or an object implementing the corresponding method, with either a direct result or an awaitable. Asynchronous hooks keep event-loop context and receive cancellation when their operation stops.
