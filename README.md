# LaserData - Laser SDK

Laser SDK provides streaming, queries, key-value state, forks, graphs, agent memory, durable workflows, and agent coordination through one client over [Apache Iggy](https://iggy.apache.org). [LaserData, Inc.](https://laserdata.com) maintains the SDK. Rust ([`laser-sdk` on crates.io](https://crates.io/crates/laser-sdk)) defines the reference behavior. Python ([`laser-sdk` on PyPI](https://pypi.org/project/laser-sdk/), [source](foreign/python/README.md)) calls the Rust implementation. TypeScript ([`@laserdata/laser-sdk` on npm](https://www.npmjs.com/package/@laserdata/laser-sdk), [source](foreign/typescript/README.md)) is a native Node client. The [Go client](foreign/go/README.md) uses the Apache Iggy Go SDK over TCP.

## One grammar, every primitive

An accessor selects a feature on the connected client. A method performs an action on that feature. The calls follow this form: `object.verb(input).await`. The Go programs in the table use the same primitives through the Go client.

| Accessor | Primitive | Reach for it to | Go example |
| --- | --- | --- | --- |
| `laser.stream(name).topic(name)` | Log | publish and consume records, replay by offset, batch | [log](examples/go/log/main.go) |
| `laser.query(index)` | Views | filter, aggregate and page operational queries | [query](examples/go/query/main.go) |
| `laser.graph(name)` | Graph | link entities, traverse, find neighbors and nearest vectors | [graph](examples/go/graph/main.go) |
| `laser.watch()` | Change feed | consume advancement records instead of re-querying blind | [watch](examples/go/watch/main.go) |
| `topic.consumer_group(...).filter()` | [Consumer filters](https://docs.laserdata.com/laser-sdk/consumer-filters) | read only the records a filter selects, on the server, with fenced acknowledgments, previews, saved filters, and bound consumer groups | [cdc](examples/go/cdc/main.go) |
| `laser.kv(namespace)` / `laser.fork(id)` | State | point reads and writes, CAS, leases, copy-on-write branches | [kv](examples/go/kv/main.go) |
| `laser.memory(scope)` | Memory | remember, supersede a fact, recall (semantic / keyword / hybrid), consolidate | [recall](examples/go/recall/main.go) |
| `laser.context(id)` | Context | append and assemble one conversation's record, and scope its memory to that conversation | [context](examples/go/context/main.go) |
| `laser.sessions()` | Session | start and end a unit of work, record model and tool calls, keep state, hand work to an agent, and replay from saved offsets | [sessions](examples/go/sessions/main.go) |
| `laser.agent(id)` / `laser.contract(..)` / `laser.workflow(..)` | Fabric | directed asks, deadline contracts, ordered workflows whose steps are child sessions | [orchestra](examples/go/orchestra/main.go) |

## More about the SDK

**The log is the source of truth.** A log stores records in append order, and managed services build query results and state from those records. A read model is data organized for a specific query, and services can rebuild it from retained log records. Shared test data and behavior scenarios compare the clients. The [`laser-wire` crate](https://crates.io/crates/laser-wire) ([source](wire/README.md)) defines the data exchanged between them.

> [Laser Stack](https://github.com/laserdata/laser-stack) runs Laser Streams and the managed backend for local development. It supports the managed SDK operations, including queries. In the Laser Stack checkout, run `./scripts/up` to start the services and obtain `LASER_CONNECTION_STRING`. Laser Stack excludes proprietary Cloud features such as the Stream and Data UI.

Read the SDK guides at [docs.laserdata.com/laser-sdk](https://docs.laserdata.com/laser-sdk). Start with the [quickstart](https://docs.laserdata.com/laser-sdk/quickstart) or the focused [`examples/`](examples/README.md).

After the focused examples, two public example repositories go further. [Photon Market](https://github.com/laserdata/laser-example-photon-market) shows a more realistic Rust system built with Laser SDK across multiple modules and microservices. [Frostline](https://github.com/laserdata/laser-example-frostline) is the consumer-filter showcase: one change feed, three teams reading their own slice, and a benchmark that measures how much payload never left the server.

**Filter before the network.** Consumer filters select records on the server so each reader receives only its matching subset of a topic and its partitions. The shared CDC example delivers **4 of 240 records** and saves **98.5% of payload transfer**. It preserves original payloads and offsets, supports exact-width typed headers, and acknowledges only completed work. See the [Consumer Filters guide](https://docs.laserdata.com/laser-sdk/consumer-filters) and the [SDK examples](https://github.com/laserdata/laser-sdk/tree/main/examples).

## Quick start

Choose where to run:

- In [LaserData Cloud](https://laserdata.cloud), create a Free deployment. Copy its connection string from the Console Credentials tab. Export the value as `LASER_CONNECTION_STRING`.
- In the [Laser Stack](https://github.com/laserdata/laser-stack) checkout, run `./scripts/up`. Copy the printed `LASER_CONNECTION_STRING` export.
- Start Apache Iggy for streaming. Set `LASER_CONNECTION_STRING` to its connection string.

The repository examples read `LASER_CONNECTION_STRING`. Keep the connection string there, read it inside the program, and never print or log it. Export it once or set it for one command:

```sh
export LASER_CONNECTION_STRING='<connection string>'
LASER_CONNECTION_STRING='<connection string>' cargo run --example log
```

The examples below publish one record and read it back. They read `LASER_CONNECTION_STRING`, work in a scratch stream, and delete it at the end. The default TCP port is `:8090`, so a connection string can omit it. Laser SDK uses the transport that Iggy provides.

TypeScript ([npm](https://www.npmjs.com/package/@laserdata/laser-sdk))

```sh
npm install @laserdata/laser-sdk tsx
```

```ts
import { Laser } from "@laserdata/laser-sdk"

await using laser = await Laser.connectEnv()
await using scratch = await laser.scratchStream("telemetry")
const topic = scratch.laser.topic("inferences")
await topic.ensure(4)
await topic.publish().json({ latency_ms: 42 }).send()

const messages = await (await topic.replay()).poll()
console.log(`read ${messages.length} message(s)`)
```

Rust ([crates.io](https://crates.io/crates/laser-sdk))

```sh
cargo add laser-sdk
cargo add tokio --features macros,rt-multi-thread
cargo add serde_json
```

```rust,no_run
use laser_sdk::prelude::*;

#[tokio::main]
async fn main() -> Result<(), LaserError> {
    let laser = Laser::connect_env().await?;
    let scratch = laser.scratch_stream("telemetry").await?;
    let topic = scratch.laser().topic("inferences");
    topic.ensure(4, None).await?;
    topic.publish().json(&serde_json::json!({ "latency_ms": 42 }))?.send().await?;

    let messages = topic.replay()?.poll().await?;
    println!("read {} message(s)", messages.len());
    scratch.delete().await
}
```

Python ([PyPI](https://pypi.org/project/laser-sdk/))

```sh
python -m pip install laser-sdk
```

```python
import asyncio

from laser_sdk import Laser


async def main() -> None:
    async with await Laser.connect_env() as laser:
        async with laser.scratch_stream("telemetry") as scratch:
            topic = scratch.laser.topic("inferences")
            await topic.ensure(partitions=4)
            await topic.publish().json({"latency_ms": 42}).send()

            messages = await topic.replay().poll()
            print(f"read {len(messages)} message(s)")


asyncio.run(main())
```

The Go client requires Go 1.25 or later and connects to Apache Iggy over TCP. Run the [Go log example](examples/go/log/main.go) from the checkout with `cd examples/go && go run ./log`. To try the quick start below, save it as `main.go` in a new subdirectory of `examples/go` and run `go run .` there. The examples module already points to the client in this checkout.

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := laser.ConnectEnv(ctx)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, client.Close()) }()

	scratch, err := client.ScratchStream(ctx, "telemetry")
	if err != nil {
		return err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		err = errors.Join(err, scratch.Delete(cleanup))
	}()

	topic := scratch.Laser().Topic("inferences")
	if err := topic.Ensure(ctx, 4, nil); err != nil {
		return err
	}
	if _, err := topic.Publish().JSON(map[string]int{"latency_ms": 42}).Send(ctx); err != nil {
		return err
	}
	cursor, err := topic.Replay()
	if err != nil {
		return err
	}
	messages, err := cursor.Poll(ctx)
	if err == nil {
		fmt.Printf("read %d message(s)\n", len(messages))
	}
	return err
}
```

The examples use open streaming. Add `managed` for projections, queries, KV, forks, and graphs on Laser Stack or LaserData Cloud. Handlers, memory, contracts, and workflows are in the default Rust build.

Streaming sends return Apache Iggy commit confirmations. A confirmation identifies the stream, topic, partition, and first offset in a batch. The list can be empty when the server does not report offsets. Completion follows the topic durability policy. Use the stream, topic, and partition together with the offset.

One connection can address every stream that the user can access. Use `laser.stream(name).topic(name)` to name the stream and topic. A default stream enables the shorter `laser.topic(name)` form. It does not restrict the connection to that stream. `laser.streams()`, `stream(name).get()`, `exists()`, and `topics()` inspect streams in Rust, Python, and TypeScript. `topic.get()`, `partitions()`, `consumer_groups()`, `delete()`, `add_partitions(n)`, and `delete_partitions(n)` inspect and change topics. `get()` and `exists()` read a missing stream or topic as absent, and `topics()`, `partitions()`, and `consumer_groups()` fail with the stream-or-topic-not-found error. `stream(name).rename(new_name)`, `topic.update(...)`, and `topic.delete_segments(partition_id, count)` rename streams and change topic settings, see [client behavior](docs/client-behavior.md#stream-and-topic-inspection). `laser.users()`, `laser.user(name)`, `laser.personal_access_tokens()`, `laser.ping()`, `laser.stats()`, `laser.me()`, `laser.clients()`, `laser.cluster()`, `laser.snapshot()`, `laser.describe_options(scope)`, and `laser.metadata_changes(...)` administer the server under its own permission checks, see [server administration](docs/client-behavior.md#server-administration).

## Primitive examples

The same calls in each client. The Go column of the table above links a runnable program per primitive.

The Rust form follows:

```rust,ignore
use std::time::Duration;

let laser = Laser::connect("iggy:iggy@127.0.0.1").await?;
let readings = laser.stream("fleet").topic("readings");
let audit = laser.stream("audit").topic("events");

// Log: streams group topics, topics carry your records.
readings.ensure(4, None).await?;
audit.ensure(4, None).await?;
readings.publish().json(&reading)?.send().await?;
audit.publish().json(&event)?.send().await?;
let mut replay = readings.replay()?;

// Views: declared projections answer queries, the graph answers traversals.
let rows = laser
    .query("readings_v1")
    .where_eq("status", "degraded")
    .limit(10)
    .fetch()
    .await?;
let nearby = laser.graph("kg").neighbors(node, EdgeDir::Out, None, 2).await?;
let mut feed = laser.watch().index("readings_v1").from_end().records().await?; // await-then-query

// State: point reads and writes, optimistic concurrency, branches.
laser
    .kv("sessions")
    .set("user:42")
    .json(&session)?
    .ttl(Duration::from_secs(300))
    .send()
    .await?;
let draft = laser.fork("what-if");

// Fabric: identities, context, memory, coordination.
let reply = laser.agent(id).ask(commands, replies, task, &prov, timeout).await?;

// Context: one task streams its messages, keeps its memory, resolves its deps.
let ctx = laser.context(conversation);
ctx.append(AgentTopic::Sessions, b"step done").await?;
let facts = ctx.memory("support").recall().semantic("rollout incidents").fetch().await?;
let deps = ctx.graph("services").neighbors(node, EdgeDir::Out, None, 2).await?;

// Session: one unit of work with a recorded lifecycle, model and tool calls, and state.
let (session, lease) = laser.sessions().create("ticket-42").agent("triage".parse::<AgentId>()?).begin().await?;
let call = session.model(ModelRequest::new("gpt-4o", prompt), None).await?;
call.complete(ModelResponse { body: answer, ..Default::default() }).await?; // the SDK never calls a model
session.state().set("status", serde_json::json!("triaged")).await?;
let turns = session.context().await?; // the session lane, last 50 records within 4000 tokens
let facts = session.memory().search("login bug").await?;
let checkpoint = session.checkpoint().await?; // serializable, persist it anywhere
session.end().await?;
drop(lease);

// Hand work to another agent as a submitted session.
let submitted = laser.sessions().submit("worker".parse::<AgentId>()?, task).from("triage".parse::<AgentId>()?).send().await?;

laser.memory("notes").set("current-plan", plan_json).await?; // named point state, an event on the memory topic
let outcome = laser.workflow("rollback").step(/* .. */).run().await?; // a root session, each step a child
```

The Python form follows:

```python
laser = await ls.Laser.connect("iggy:iggy@127.0.0.1")
readings = laser.stream("fleet").topic("readings")
audit = laser.stream("audit").topic("events")

# Log
await readings.publish().json(reading).send()
await audit.publish().json(event).send()

# Views + graph + change feed
rows = await laser.query("readings_v1").where_eq("status", "degraded").limit(10).fetch()
nearby = await laser.graph("kg").neighbors(node, dir="out", depth=2)
feed = await laser.watch(index="readings_v1", from_end=True)

# State
await laser.kv("sessions").set("user:42").json(session).ttl(300_000).send()

# Fabric: one task streams its messages, keeps its memory, resolves its deps
ctx = laser.context(conversation)
await ctx.append(ls.AgentTopic.Sessions, b"step done")
facts = await ctx.memory("support").recall(semantic="rollout incidents")
turns = await ctx.fetch(n=20, token_budget=4_000)
deps = await ctx.graph("services").neighbors(node, dir="out", depth=2)

# Session: one unit of work with a recorded lifecycle, model and tool calls, and state
async with laser.sessions().create("ticket-42").agent("triage") as session:
    call = await session.model(ls.ModelRequest("gpt-4o", prompt))
    await call.complete(ls.ModelResponse(answer))  # the SDK never calls a model
    await session.state().set("status", "triaged")
    turns = await session.context()
    checkpoint = await session.checkpoint()
# The block ended the session, or failed it with the traceback if it raised.
submitted = await laser.sessions().submit("worker", task).from_("triage").send()
```

The TypeScript form uses camelCase method names:

```ts
const laser = await Laser.connect("iggy:iggy@127.0.0.1")
const readings = laser.stream("fleet").topic("readings")
const audit = laser.stream("audit").topic("events")

// Log
await readings.publish().json(reading).send()
await audit.publish().json(event).send()

// Views + graph + change feed
const rows = await laser
  .query("readings_v1")
  .whereEq("status", "degraded")
  .limit(10)
  .fetch()
const nearby = await laser.graph("kg").neighbors(node, "out", undefined, 2)
const feed = await laser.watch().index("readings_v1").fromEnd().records()

// State
await laser.kv("sessions").set(key).json(session).ttl(300_000).send()
const draft = laser.fork("what-if")

// Fabric: one task streams its messages, keeps its memory, resolves its deps
const ctx = laser.context(conversation)
await ctx.append(AgentTopic.Sessions, new TextEncoder().encode("step done"))
const facts = await ctx
  .memory("support")
  .recall()
  .semantic("rollout incidents")
  .fetch()

// Session: one unit of work with a recorded lifecycle, model and tool calls, and state
const encode = (text: string) => new TextEncoder().encode(text)
const { session, lease } = await laser.sessions().create("ticket-42").agent(AgentId.new("triage")).begin()
const call = await session.model(new ModelRequest("gpt-4o", encode(prompt)))
await call.complete({ body: encode(answer) }) // the SDK never calls a model
await session.state().set("status", "triaged")
const turns = await session.context()
const checkpoint = await session.checkpoint()
await session.end()
lease.release()
const submitted = await laser.sessions().submit(AgentId.new("worker"), task).from(AgentId.new("triage")).send()
```

### Streaming contract

- Accessors are free to construct. IO starts at terminal verbs such as `.send()` and `.fetch()`.
- `topic.producer()` supports batching, linger, retries, and key or partition routing.
- `topic.consumer(..)` and `topic.consumer_group(..).consumer()` provide live reads, replay, polling control, and automatic or explicit offset commits.
- `ConsumerMessage` preserves the exact Apache Iggy headers and log position.
- `topic.iggy_producer()`, `topic.iggy_consumer_group()`, `laser.client()`, and `laser_sdk::iggy` expose Apache Iggy directly when the Laser surface is not enough.

All three SDKs use Apache Iggy VSR framing. Managed commands use the same connection and are dispatched according to the capabilities reported by the server.

Data platform (the core, stands on its own):

| Primitive | What you get |
| --- | --- |
| Publish / consume | Typed serde values or raw records onto topics, direct producer batching/linger/routing, and live async partition or consumer-group readers with server offsets and configurable commit policies. |
| Consumer filters | **Receive only matching records across the network.** Payload predicates, exact-width typed headers, text matching (equals, prefix, suffix, contains, glob, regex, optionally case-insensitive), saved group policies, previews, and safe acknowledgments over the original log. One log can carry many kinds of events in different codecs: records in another format are skipped, and records with unexpected field types can be delivered marked unevaluated so the consumer decides. The CDC example saves **98.5% of payload transfer**, and the [Frostline example](https://github.com/laserdata/laser-example-frostline) reports **96.9% of payload and 96.5% of total TCP application traffic avoided** over ten million records in its own measurements. |
| Projections + query DSL | Filters, aggregates, time ranges, pagination, and vector recall over indexes you declare once per topic, with opt-in read-your-writes consistency, and a `conversation(id)` filter that narrows any query to the records one conversation wrote. |
| Key-value + forks | Working state with compare-and-swap, conditional ops, expiry, JSON merge-patch, and revocable holder-scoped leases, plus **copy-on-write branches of the read model** for speculative work. |
| Knowledge graph | Content-addressed nodes and edges, traversal / neighbor / nearest-vector / path reads, bitemporal valid-time edges, source back-links, and a `conversation(id)` filter that narrows a traversal to one conversation. |
| Governance (RBAC) | Capability grants over the managed surfaces: `effect feature:action [on resource]` assembled through roles bound to the unforgeable server-stamped user, deny-wins, default-deny. New users receive no managed capabilities unless roles are explicitly bound. `laser.whoami()` + the role/binding/history verbs, including revision-guarded role binding. Orthogonal to Iggy's own permissions, enforced server-side at the edge. |

Agent fabric (opt in with the `agent` feature):

| Primitive | What you get |
| --- | --- |
| Reliable runtime | A consumer with **dedup, retry, and dead-letter**, request/reply correlation, conversation and causality tracking, routing, sessions, and context assembly. |
| Agentic memory | One durable model: `remember` / `recall` / `improve` / `forget` / `supersede` publish to the memory topic (`agent.memory` by default, created on the first write) that materializes to a versioned key-value read view. A default recall waits until the view reflects the writes the same connection made, for at most 10 seconds. On a server without the view, such as Apache Iggy alone, a default recall folds the topic in process. It recalls by recency, ranked first by the summed feedback for every strategy except recent. A semantic or hybrid query ranks by the share of query words each item holds plus its feedback and drops nothing, while keyword keeps only the items that share a word. `supersede(old_id, fact)` remembers the new fact and then forgets the old one in every conversation. A forget is a timed tombstone, so an older write stays forgotten whichever partition it rides. The topic is configurable (`memory_topic(name).stream(..).partitions(n).ttl(d)`). The in-process vector backend and rerank seam add embedding similarity ranking. Consolidation, token-budgeted `to_context_block`, and content-addressed dedup compose above both. Vector memory created from a `Laser` inherits its action governor even though the index itself stays local. A scan over the read view narrows to one conversation with `conversation(id)`, the same lens the query and graph reads carry. |
| Discovery | Agents advertise a capability card and a live inbox, fused into one cached registry with health-aware resolution and reversible operator `quarantine` / `unquarantine`. One connection may advertise one agent. Sensitive routes can require the presence's server-authenticated principal. |
| Coordination | `contract` (a directed task with a deadline and a real consumed / completed / timed-out answer), `fan_out` / `scatter` (ask every capable agent, gather under a policy), and `approval_gate` (pause for a named approver and get a grant a governed re-send carries). With signing enabled, terminals fail closed on unsigned or wrongly signed replies and expose the verified principal. |
| Workflow engine | Plain functions as steps with `step` and `workflow` (`run_with` in Rust), and the builder `laser.workflow(..).step(..)` and `.step_local(..)`: dependency-ordered steps routed to agents or run in process, a run that creates its own topics, an outcome with the status and the completed steps in order, budgets, verifier panels, saga compensation, **crash-recovery replay from a journal**, and per-step fenced leases. Use `.exclusive_in(namespace)` when the handler commits an external effect with `kv(target_namespace).cas_fenced(key, namespace, ..)`. The engine races bounded renewal against completion, keeps the lease through verification and completion journaling, then releases it before `OnTimeout::Reassign` gives a fresh holder a new fence. |
| Sessions | `laser.sessions()`: one conversation with a recorded lifecycle on `agent.sessions`. Start, end, fail, or cancel it, record model and tool calls with the context they received, keep a JSON state document, hand work to an agent with a budget that agents and workflows enforce, and stop it through the operator-only `agent.control` topic. Workflows run as root sessions with a child session per step. A registered stream gets a managed session index that `laser.sessions()` reads with `list`, `get`, `events`, `state`, `links`, `sources`, `changes`, and `watch`, and the log stays the truth. |
| AGDX envelope | A typed, versioned, fixture-pinned agent message format on the log, with producer verbs, resumable token streams, and deterministic reassembly. ([notes](docs/agdx.md)) |
| Action governance | A pre-effect policy hook (`ActionGovernor`) over everything an agent publishes: allow, observe, block, step-up, modify, or defer each send, every record of a typed or raw topic publish, batch, producer send, or session append, AGDX verb, and memory write **before it runs**. Enforce or shadow mode records every non-allow decision as digest-chained evidence, one chain per governed handle and conversation. `QuorumGovernor` runs named governors concurrently under `All` / `Any` / `AtLeast(n)`, each bounded by a voter timeout. Every mandatory voter must affirm, invalid configurations and mandatory errors block, and conflicting body replacements block. `SwappableGovernor` changes the active policy without reconnecting. Defense in depth above server-owned RBAC. |
| Durable intent | SDK-level typed records for asynchronous effect approval, not an AGDX wire extension. Fallible `Intent::builder().build()` validates the frozen voter set, threshold, deadline, and body digest. Fallible `Vote::cast` binds an eligible voter to that digest and policy version. `decide` takes each ballot with the time the log assigned it (`ObservedVote`), ignores invalid ballots and those observed early, late, or in the future, then returns a canonical commit or abort. Mandatory voters must allow, conflicting repeats abort, and `Decision::authorizes` verifies the exact intent and a quorum of considered votes before an effect runs. Voter identity is trusted only under a signed-principal or topology-isolated deployment profile. |
| Swarm activity | A coordinator's replay-safe read model over governance evidence: `SwarmActivity::observe` deduplicates by decision id, `.agent(name)` reads one agent's counts and deterministic latest decision, and `.agents()` lists every folded agent busiest first. |
| Crash context | A recovery tool's one-call bundle over an already-read journal tail, dead-letter capsule, and latest governance decision. `.summarize()` emits a bounded deterministic digest with control characters escaped, so untrusted payloads cannot forge diagnostic lines. It performs no I/O and never invokes a model. |
| Edge bridges | A2A, MCP, and AG-UI mapped onto AGDX over the durable log, no SSE. ([interop](docs/interop.md)) |

Agent routing, contracts, fan-out, and workflows run as client-side state machines. They track offsets, deadlines, leases, and replies in the log. They do not require a separate orchestration server. Their durability and ordering depend on the underlying log and the selected policies.

## Two complete programs

Each program below runs as written and reads the connection string from `LASER_CONNECTION_STRING`. The workflow runs on the stream named by `LASER_STREAM`, or on a stream named after the workflow. It creates that stream when it is missing and keeps it after the run, so the run can be resumed. The memory program works in a scratch stream with a unique name and deletes the stream when it ends, so a second run prints the same. With `LASER_CONNECTION_STRING` set, a program needs no Docker and no local server. Then there is no server to start. A LaserData API key is not a connection string. Only without the variable, copy the connection string from the deployment's Credentials tab in the [LaserData Cloud](https://laserdata.cloud) Console, or run `./scripts/up` in a [Laser Stack](https://github.com/laserdata/laser-stack) checkout and use the `LASER_CONNECTION_STRING` it prints. Never print or log it.

- TypeScript: `npm install @laserdata/laser-sdk tsx`, Node 22.14 or later. Save a program as `main.mts` and run it with `npx tsx main.mts`.
- Rust: `cargo add laser-sdk`, `cargo add tokio --features macros,rt-multi-thread,time`, `cargo add serde_json`, and `cargo add serde --features derive` for the memory program.
- Python: `python -m pip install laser-sdk`, Python 3.10 or later. `python -c "import laser_sdk; print(laser_sdk.__doc__)"` then prints the package's own summary of the calls below.

To keep what the memory program writes, use a named stream instead of the scratch stream: set `LASER_STREAM`, pass it when connecting (`Laser.connect_env(stream=name)` in Python, `Laser.connectEnvWithStream(name)` in TypeScript, `Laser::connect_env_with_stream(name)` in Rust), or call `with_default_stream(name)` (`withDefaultStream` in TypeScript). A workflow run and the first memory write create a missing stream. The guides are at [docs.laserdata.com/laser-sdk](https://docs.laserdata.com/laser-sdk) and on [Context7](https://context7.com/laserdata/laser-sdk).

### A three-step workflow

A workflow is a function, and each step is a function it calls in ordinary code: `@ls.step` and `@ls.workflow` in Python, `step` and `workflow` in TypeScript, `run_with` and `run.step` in Rust. The steps run in this process, and `analyze` waits one second before it returns. A step takes and returns what JSON holds. Each completed step is journaled, so a resumed run returns the recorded value without running that step again, and it must call the same steps in the same order. A step name holding `#` is refused, because the journal numbers repeated calls with it. The run creates its own stream and topics, so the program needs no `bootstrap`, agent, or handler. For steps routed to agents, verifiers, compensation and budgets, use the builder, `Laser.workflow(name).step(...)`, with `step_local` in Rust and `stepLocal` in TypeScript for its in-process steps.

The [Go workflow program](examples/go/workflow/main.go) runs the same steps and prints their outcome.

TypeScript

```ts
// npm install @laserdata/laser-sdk tsx
// Run with: npx tsx main.mts
// Needs only LASER_CONNECTION_STRING. No server to start when it is set.
import { setTimeout as sleep } from "node:timers/promises"
import { step, workflow } from "@laserdata/laser-sdk"

const ingest = step("ingest", () => "frames from sat-7")
const analyze = step("analyze", async (frames: string) => {
  await sleep(1000)
  return `analyzed ${frames}`
})
const report = step("report", (analysis: string) => `report on ${analysis}`)

const pipeline = workflow("pipeline", async () => report(await analyze(await ingest())))

const run = await pipeline.run()
console.log(JSON.stringify({ status: run.status, steps: run.completedSteps, report: run.value }))
```

Rust

```rust,no_run
// cargo add laser-sdk
// cargo add tokio --features macros,rt-multi-thread,time
// cargo add serde_json
// Needs only LASER_CONNECTION_STRING, which connect_env() reads. No server to start when it is set.
use laser_sdk::prelude::*;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), LaserError> {
    let laser = Laser::connect_env().await?;
    let run = laser
        .workflow("pipeline")
        .run_with(async |run| {
            let frames: String = run
                .step("ingest", async || Ok("frames from sat-7".to_owned()))
                .await?;
            let analysis: String = run
                .step("analyze", async || {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    Ok(format!("analyzed {frames}"))
                })
                .await?;
            run.step("report", async || Ok(format!("report on {analysis}")))
                .await
        })
        .await?;
    let report = serde_json::json!({
        "status": run.status,
        "steps": run.completed_steps,
        "report": run.json::<String>("report")?,
    });
    println!("{report}");
    laser.close().await
}
```

Python

```python
# python -m pip install laser-sdk
# Needs only LASER_CONNECTION_STRING. No server to start when it is set.
import json
import time

import laser_sdk as ls


@ls.step
def ingest():
    return "frames from sat-7"


@ls.step
def analyze(frames):
    time.sleep(1)
    return f"analyzed {frames}"


@ls.step
def report(analysis):
    return f"report on {analysis}"


@ls.workflow
def pipeline():
    return report(analyze(ingest()))


run = pipeline.run()
print(json.dumps({"status": run.status, "steps": run.completed_steps, "report": run.value}))
```

Each program prints the run as JSON with `status` `completed`, `steps` `["ingest", "analyze", "report"]`, and `report` `report on analyzed frames from sat-7`. Rust's `serde_json::json!` sorts the keys. The `WorkflowOutcome` also carries `run_id`, `value` (what the workflow returned, as JSON bytes in Rust), `outputs`, each step's output by label as bytes, `json(label)`, and `text(label)`. Resume a stopped run with `pipeline.resume(run_id)` in Python, `pipeline.resume(runId)` in TypeScript, and `laser.workflow(name).run_id(id).run_with(...)` in Rust. A step that fails ends the run as failed and its error reaches the caller unchanged, with no outcome to read the run ID from, so a caller that wants to resume a failed run names the run ID first with `resume(id)` (`run_id(id)` in Rust). The completed steps stay journaled, so that resume runs the failed step and what follows it.

### A memory correction

A first conversation stores three facts about the user `user-42`. A later conversation corrects the city with `supersede`, which remembers the new fact first and then forgets the old one in every conversation. A recall for the user reads every conversation and returns a list of `MemoryItem`, ranked by the words each fact shares with the query, newest first among equals, and drops none. It waits for the writes this connection made, so the correction shows at once. On LaserData Cloud and Laser Stack recall reads the managed memory view. Apache Iggy serves no view, so there recall folds the memory topic in process, and the same program runs on every server.

The [Go memory correction program](examples/go/memory-correction/main.go) remembers a fact, supersedes it and recalls the correction.

TypeScript

```ts
// npm install @laserdata/laser-sdk tsx
// Run with: npx tsx main.mts
// Needs only LASER_CONNECTION_STRING, which connectEnv() reads. No server to start when it is set.
import { ConversationId, Laser, memoryItemText } from "@laserdata/laser-sdk"

const user = "user-42"
const query = "Where does the user live, and what should I know before booking dinner?"

await using root = await Laser.connectEnv()
await using scratch = await root.scratchStream("memory")
const memory = scratch.laser.memory("assistant")

// Conversation 1: the user shares three facts.
const first = ConversationId.new()
const city = await memory.remember("Lives in Gdansk").scope(first).user(user).send()
await memory.remember("Allergic to peanuts").scope(first).user(user).send()
await memory.remember("Prefers a window seat").scope(first).user(user).send()

// Conversation 2: the user moved. supersede stores the new fact and forgets the old one.
const second = ConversationId.new()
await memory.supersede(city, "Lives in Lisbon").scope(second).user(user).send()

// Recall for the user across every conversation.
const facts = await memory.recall().user(user).semantic(query).limit(10).fetch()
console.log(JSON.stringify({ user, query, facts: facts.map(memoryItemText) }, null, 2))
```

Rust

```rust,no_run
// cargo add laser-sdk
// cargo add tokio --features macros,rt-multi-thread
// cargo add serde --features derive
// cargo add serde_json
// Needs only LASER_CONNECTION_STRING, which connect_env() reads. No server to start when it is set.
use laser_sdk::prelude::*;
use serde::Serialize;

const USER: &str = "user-42";
const QUERY: &str = "Where does the user live, and what should I know before booking dinner?";

#[derive(Serialize)]
struct Recall {
    user: &'static str,
    query: &'static str,
    facts: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = Laser::connect_env().await?;
    let scratch = root.scratch_stream("memory").await?;
    let result = correct_and_recall(scratch.laser()).await;
    scratch.delete().await?;
    println!("{}", serde_json::to_string_pretty(&result?)?);
    Ok(())
}

async fn correct_and_recall(laser: &Laser) -> Result<Recall, LaserError> {
    let memory = laser.memory("assistant");

    // Conversation 1: the user shares three facts.
    let first = ConversationId::new();
    let city = memory
        .remember("Lives in Gdansk")
        .scope(first)
        .user(USER)
        .send()
        .await?;
    memory
        .remember("Allergic to peanuts")
        .scope(first)
        .user(USER)
        .send()
        .await?;
    memory
        .remember("Prefers a window seat")
        .scope(first)
        .user(USER)
        .send()
        .await?;

    // Conversation 2: the user moved. supersede stores the new fact and forgets the old one.
    let second = ConversationId::new();
    memory
        .supersede(city, "Lives in Lisbon")
        .scope(second)
        .user(USER)
        .send()
        .await?;

    // Recall for the user across every conversation.
    let facts = memory
        .recall(None)
        .user(USER)
        .semantic(QUERY)
        .limit(10)
        .fetch()
        .await?;
    Ok(Recall {
        user: USER,
        query: QUERY,
        facts: facts.iter().map(MemoryItem::text).collect(),
    })
}
```

Python

```python
# python -m pip install laser-sdk
# Needs only LASER_CONNECTION_STRING, which connect_env() reads. No server to start when it is set.
import asyncio
import json

import laser_sdk as ls

USER = "user-42"
QUERY = "Where does the user live, and what should I know before booking dinner?"


async def main():
    async with await ls.Laser.connect_env() as root:
        async with root.scratch_stream("memory") as scratch:
            memory = scratch.laser.memory("assistant")

            # Conversation 1: the user shares three facts.
            first = ls.new_conversation_id()
            city = await memory.remember("Lives in Gdansk", conversation=first, user=USER)
            await memory.remember("Allergic to peanuts", conversation=first, user=USER)
            await memory.remember("Prefers a window seat", conversation=first, user=USER)

            # Conversation 2: the user moved. supersede stores the new fact and forgets the old one.
            second = ls.new_conversation_id()
            await memory.supersede(city, "Lives in Lisbon", conversation=second, user=USER)

            # Recall for the user across every conversation.
            facts = await memory.recall(user=USER, semantic=QUERY, limit=10)
            print(
                json.dumps(
                    {"user": USER, "query": QUERY, "facts": [fact.text() for fact in facts]},
                    indent=2,
                )
            )


asyncio.run(main())
```

It prints `{"user": "user-42", "query": "Where does the user live, and what should I know before booking dinner?", "facts": ["Lives in Lisbon", "Prefers a window seat", "Allergic to peanuts"]}`. Gdansk is gone. On a Python client `memory(...)`, `workflow(...)`, the builder's `step(...)` and `sessions(...)` return at once, and `scratch_stream(prefix)` is entered with `async with`. `Laser.connect_env()`, `memory_topic(...)` and every call that reads or writes (`remember`, `supersede`, `recall`, `forget`, the builder's `run()`, `bootstrap`) return an awaitable. The same programs live in [`examples/`](examples/README.md) as `workflow` and `memory-correction` (`workflow.py` and `memory_correction.py` in Python), where each runs when copied alone.

## Why it is good to build on

- **Streaming and managed operations share one client, Iggy transport, and record metadata.**
- **Read models can rebuild from retained records.** A new projection, index, or agent can read earlier records by offset.
- `laser.stream("fleet").topic("readings").json::<Reading>()` publishes and replays `Reading` values. A schema-bound handle checks each value against its registered schema before sending. Decode failures include the record position. Producers and consumers support batches.
- Streaming, the agent runtime, log-based memory, and open coordination run on Apache Iggy. Managed operations use capabilities reported by LaserData Cloud or Laser Stack.
- A fence is an increasing token that identifies a lease holder. An `.exclusive_in(namespace)` step and `kv(namespace).cas_fenced(..)` use the same fence sequence. **The protected state rejects writes from a replaced holder.**
- Rust defines the reference behavior. Python calls that implementation. TypeScript uses the same encoded test data and **shared behavior scenarios**. The [cross-SDK parity matrix](docs/parity.md) maps every public Rust symbol to its Python and TypeScript spelling.

## Open core, managed surface

| Deployment | Available surfaces |
| --- | --- |
| Apache Iggy | Streaming, provenance, AGDX, the agent runtime, log-backed memory, contracts, and workflows |
| Laser Stack | Everything above, plus consumer filters, query, projections, KV, forks, graph, durable memory, the managed session index, and fenced leases |
| LaserData Cloud | The complete SDK surface with managed deployment and UI services |

Capability negotiation runs during connection setup. A managed call against Apache Iggy without a managed backend returns `LaserError::Unsupported`. The underlying client remains available through `topic.iggy_producer()`, `topic.iggy_consumer(..)`, and `laser.client()`.

Laser Stack runs Laser Streams and the managed backend. Laser Streams owns the durable log and VSR connection. The managed backend maintains the managed read models and handles query, projection, schema, KV, fork, graph, and run operations.

### Authorization

- Apache Iggy RBAC controls server, stream, and topic access.
- LaserData governance RBAC controls managed capabilities.
- The layers are independent. Creating a user does not grant a managed role.
- `LaserError::is_permission_denied()` and `is_stream_or_topic_not_found()` classify native access failures. Managed authorization uses the unified unauthorized result.

### TLS

Connections to `*.laserdata.cloud` and `*.laserdata.com` automatically use the public LaserData CA bundled with the SDK. `LASER_TLS_CERT=<path>` enables TLS with an explicit CA for any host or overrides the bundled CA. `LASER_NO_TLS=1` disables automatic TLS setup. Other hosts keep the TLS settings from their connection string when neither variable is set. Rust and Python connect over TCP (`iggy+tcp://`), QUIC (`iggy+quic://`) or WebSocket (`iggy+ws://`), and TypeScript over TCP. QUIC verifies certificates only against the operating system trust store: a LaserData Cloud host needs the LaserData root CA installed there, and other hosts keep Apache Iggy's default `validate_certificate=false`, so set `validate_certificate=true` outside local development. [Client behavior](docs/client-behavior.md#connections) has the transport table and the TLS rules per transport.

## Consumer groups and offsets

Provision a filtered group once, then consume it by name or ID. Every consumer in the group shares its policy and divides the partitions, so use separate groups for A/B revisions to keep their offsets independent. A fresh consumer that starts at `Next` begins at the first retained record. The default `all` commit policy stores a poll once its last record is handled. Under the polling commit policy a native consumer can store a polled batch's offset before the application has handled it, and a group-aware automatic commit advances only past records already delivered, on the next read or at shutdown. When that matters, disable automatic commits and commit after processing. Filtered readers acknowledge explicitly and keep at most 1024 unacknowledged record-bearing pages per partition by default (`max_unacked_pages` in Rust and Python, `maxUnackedPages` in TypeScript), so a reader that never acknowledges stops at the bound instead of growing without limit. A record the default `stop` fault policy stops on blocks its partition until the reader or the group consumer calls `skip(partition, offset)` with the offset the fault names, which acknowledges past it once the records before it are handled. An unnamed TypeScript consumer gets an isolated identity and does not commit automatically. Give it a stable name to resume durable progress. Codec profiles and examples are in the [Consumer Filters guide](https://docs.laserdata.com/laser-sdk/consumer-filters).

## Connect and publish limits

Connecting gives up after 30 seconds. Each publish attempt times out after 60 seconds and is retried three times with exponential backoff. Every client can change these limits on its builder or connect call, or through `LASER_CONNECT_TIMEOUT_MS` and the `LASER_PUBLISH_*` environment variables. A publish that gives up returns an error listing the committed and unconfirmed records, and it never exits the process. Each client also reports when its connection is connected, disconnected, signed in, signed out, and shut down, through `connection_events()` (`connectionEvents()` in TypeScript). See [connect timeout and cleanup](docs/connect-timeout.md), [publish recovery](docs/publish-recovery.md), and [connection events](docs/client-behavior.md#connection-events).

## Documentation

- [Tutorial](docs/tutorial.md): a progressive guide from one message to projections, queries, vector recall, codecs, multi-stream topologies, and the agent fabric.
- [Building agents](docs/building-agents.md): a recipe guide that works one multi-agent scenario end to end, including governed agents, managed-surface RBAC, and concrete SDK calls.
- [AGDX notes](docs/agdx.md): an in-repo development reference for the Agent Data Exchange Protocol the SDK implements (the envelope, Apache Iggy binding, the surfaces). The protocol home is [agdxprotocol.ai](https://agdxprotocol.ai).
- [Interop](docs/interop.md): the A2A / MCP / AG-UI edge bridges over AGDX.
- [Producer statistics](docs/producer-statistics.md): optional per-handle send counters and latency percentiles, what they do and do not prove.
- [Examples](examples/README.md): aligned Rust, Python, TypeScript, and Go systems runnable against Apache Iggy, Laser Stack, or LaserData Cloud.
- [`wire/README.md`](wire/README.md): the contract crate and its compatibility rules.

## Workspace

| Crate | What it is |
| --- | --- |
| [`laser-wire`](wire/README.md) (`wire/`) | the wire contract: codes, envelopes, query IR, dictionaries, caps, the AGDX envelope, and the golden fixture corpus. Runtime-free and wasm-portable. |
| [`laser-sdk`](sdk/README.md) (`sdk/`) | the client and agent runtime, re-exporting the wire crate as `laser_sdk::wire`. |
| [`foreign/python`](foreign/python/README.md) | the Python SDK, PyO3 bindings over the Rust crate. |
| [`foreign/typescript`](foreign/typescript/README.md) | the native TypeScript SDK over Apache Iggy. |
| [`foreign/go`](foreign/go/README.md) | Native Go client over the Apache Iggy Go SDK. See [supported capabilities and transport constraints](foreign/go/IMPLEMENTATION_STATUS.md). |
| [`examples`](examples/README.md) | Two standalone programs, `workflow` and `memory-correction`, run when copied alone. Nine focused examples cover `log`, `query`, `watch`, `kv`, `cdc`, `graph`, `recall`, `context`, and `agent` in Rust, Python, TypeScript, and Go. Nine larger examples cover streaming, event analytics, a fleet telemetry tape (`fleet-tape`), load generation, an incident desk (`incident-desk`), memory, interoperability, `orchestra`, and governance. |

## Benchmarks

Run the maintained native campaign from the repository root:

```sh
just bench
just bench 15 3 8 # seconds per arm, repetitions, parallel lanes
```

- Matched raw Iggy and Laser streaming workloads use TCP VSR and one connection per producer or consumer lane.
- The maintained campaign covers streaming, AGDX, managed surfaces, MCP, startup, and recovery.
- The test runner runs in release mode and keeps progress output outside timed regions.
- Results include immutable JSON evidence, HDR histograms, CSV exports, and a standalone HTML report.
- `bench/.env` can select caller-provided native binaries. Otherwise the test runner resolves the maintained signed artifacts.

Use `just bench-smoke` to validate the test runner and `just bench-full` for the exhaustive matrix. Read [`bench/README.md`](bench/README.md) for workload definitions, result interpretation, and authoritative campaign requirements.

## Development

Run the repository gates from the root:

```sh
just lint    # fmt + sort + machete + clippy -D warnings
just test    # workspace unit tests
just test-it # integration tests against Apache Iggy
just bdd     # cross-SDK BDD conformance (needs Docker)
just parity-check   # Rust, Python, and TypeScript surfaces match docs/parity.md
just ci      # the full gate (lint, tests, integration, python-docs, vocabulary, parity, wasm, deny, advisories, fuzz, bdd)
```

Feature profiles select Laser capabilities:

- Default: the agent runtime with workflows, memory and sessions, key-value state, typed streaming, and provenance.
- `--features managed`: every managed surface.

A program that only streams trims the build with `default-features = false, features = ["streaming"]`.

Every profile uses VSR.

## Delivery model

At-least-once with idempotent operations, per-conversation (per-partition) ordering, and replay-friendly throughout. Materialized indexes can rebuild from explicit source offsets and snapshots instead of making full replay a hot-path default.

## License

Apache-2.0. Copyright LaserData, Inc. Apache and Apache Iggy are trademarks of the Apache Software Foundation, and use does not imply endorsement.

## Materialization and operational SQL

Materialization setup is available in Rust, Python, and TypeScript through `topic.materialize().apply()`. It registers a whole-record projection by default. A typed projection selects extracted fields. Reuse the setup operation identity after an uncertain response. The resulting handle reports binding status and waits against a captured source target. Readiness and complete historical coverage are separate results. A wait also ends early as failed, blocked or not found, and `topic.materialization(projection_id)` returns the handle again in a later process, see [client behavior](docs/client-behavior.md#materialization-and-operational-sql).

Projection history defaults to append. Keyed history selects a typed header key or a payload JSON pointer and can name a delete header. Without one, a keyed view never deletes a key. A key stays in one partition per topic incarnation. Older updates cannot replace a newer value or restore a deleted value. Keyed history does not claim complete append history. Projection and binding history cannot change in place.

Bindings use `invalid_records` and `oversize` policies. Both default to block. Dead-letter handling permits automatic skips of rejected records. Reference handling omits an oversized body while retaining its source location. A record that fails its declared writer schema follows the invalid-record policy.

Raw SQL requires an explicit dialect in every client. Rust uses `raw_sql(dialect, sql)` or `raw_sql_with(dialect, sql, params)`. Python uses `raw_sql(dialect, sql, params=None)`. TypeScript uses `rawSql(dialect, sql, params?)`. Every referenced index must be authorized for the caller and belong to the same stream and backend. Unsupported managed capabilities fail explicitly on original Apache Iggy. Standard streaming remains available.
