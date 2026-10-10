# SDK client behavior

This guide describes how the Rust, Python, and TypeScript clients behave. Operation and envelope versions are 1.

Rust, Python, and TypeScript share the features and defaults listed in [the parity matrix](parity.md). The matrix covers inherent and trait methods, generated builder controls, and standalone SDK functions. Names and construction follow each language.

## Agents and sessions

All three clients implement agent sessions. The [AGDX specification](agdx.md#a15-agent-sessions) defines the session records, topics, and reads.

### Sessions

Session writes retain lane identity and reject changed stream or topic generations and partition counts before publication. On managed deployments, a metadata-only Sources check compares the registration using native lane send permission. Aggregate reads keep their existing grants. Explicit registration recovery creates a fresh reference, while old handles remain stale. A refused state snapshot stays pending for retry, and old-generation leases do not heartbeat into a recreated stream.

A session is one conversation with a recorded lifecycle. Its id is the conversation id. `laser.sessions()` returns the session factory. Python takes the configuration as keywords on `laser.sessions(...)`, and TypeScript takes an optional `SessionConfig`.

- `create(label)` derives the session id from the stream, the namespace, and the label, so the same label reaches the same session. `start()` makes a fresh id. Both return a builder that needs `.agent(id)` and ends with `begin()`. `begin()` writes the start record on `agent.sessions` and returns the session and a `SessionLease`. Python also supports `async with builder as session:`.
- `open(id)` is a lens. It does no I/O, takes no lease, and has no author until `as_agent(id)` names one.
- `end()`, `fail(error)`, and `cancel()` write the terminal record. Every clone of a handle shares one terminal latch. The first verb fixes the terminal state and record id, a repeated call resends that same record, and a different verb fails with an invalid error. `end()` first writes a state snapshot when the session state changed since the last one.
- Rust `Session::run(lease, work)` ends the session by the outcome. A panic is written as a failure with `panic: true` in its detail and then re-raised. Python `async with` fails the session with the exception type and traceback when the block raises and ends it otherwise. The original exception wins when the terminal write also fails. TypeScript `session.run(lease, work)` catches synchronous throws and awaited rejections. Detached promises stay application-owned, and the SDK installs no process-wide rejection listener. No guard captures a dropped future, process death, or a Rust `panic = "abort"` build. Such a session shows idle once its heartbeat stops.
- A held lease lists the session in the process heartbeat on `agent.heartbeats`. The defaults are a 5-minute idle timeout and a 60-second heartbeat. Drop or release the lease when the process stops working on the session.
- `sessions().submit(agent, input).from(submitter)` (Python `from_`) writes a submitted session and its first command, addressed to the agent. Chain `label`, `namespace`, `budget`, `tag`, and `operation` as needed. `send()` returns `Submitted { session, correlation }`. The agent's reliable consumer marks the session working when it picks the command up and holds a lease while the handler runs. Inside the handler, `ctx.session()` returns the handled record's session, writing as the handling agent, with no lease of its own.
- `sessions().control(stream, id).as_operator(op)` writes `pause`, `resume`, `cancel`, and `force_cancel` records on `agent.control`. The account needs send permission on that topic. `signed_by(key)` (TypeScript `signedBy`) signs each control record, and `Session::signed_by(key)` signs the session's terminal record. The managed index reports a verified signer as `verified_actor` on the event. Untargeted control records carry `agdx.to = *`. `Session::cancel_requested()` reads the session's control records, so it also answers on open Apache Iggy.
- The reliable consumer follows `agent.control` and records pause and cancel requests for each session it handles, rebuilt from the retained control records on first sight. The follower reads every control partition directly, outside any consumer group, so every instance of a role sees every request. `Session::pending_control()` (TypeScript `pendingControl`) returns them. The runtime never interrupts a handler, so the handler decides when to stop.
- `assemble(policy)`, `model(request, assembled)`, `tool(name, args)`, `record_model_call`, `record_retrieval`, and `record_compaction` record context, model, and tool facts. The SDK never calls a model. The application calls its provider and completes or fails the returned call. Tool arguments and JSON model request bodies pass the session's redactor first. The default drops the values of `authorization`, `api_key`, `token`, `password`, `secret`, and `cookie`. `redact(fn)` replaces it. Redaction is a convenience, not a guarantee.
- `state()` returns the session state document. `set`, `patch`, and `replace` append JSON Patch deltas, `snapshot` writes the whole document, and `get` folds the retained lane. A handle starts from the folded lane before its first write, so a lens such as `ctx.session()` writes against the current revision. The SDK writes a snapshot after every 64 deltas and before `end`. A successful append does not prove a patch applied.
- `context()`, `checkpoint()`, `turns_at`, `turns_since`, `state_at`, and `replay` read the session lane. Each turn carries its display type, such as `session.started` or `tool.call`.
- `Sessions::bootstrap(partitions, retention)` bootstraps the agent topics. When the server announces `sessions` and source registration is on, it also registers the stream as a session source. It waits until the session reads see that registration, retrying temporary read failures until the publish timeout. A refused registration is logged and reported as `registered: false`, never an error, because provisioning can register a stream for an account without session administration rights.

The `sessions` capability is set by the server only when it serves the managed session reads. A client starts with it off and must not infer support from the SDK version. Without it, every write and lane read above works on open Apache Iggy.

### Session reads

- `Sessions::get(id)`, `list()`, `events(id)`, `state(id, history_limit)`, `links(id, surface)`, `sources(id)`, `changes(after, limit)`, and `watch(poll_every)` read the managed session index of the factory's stream, and `Session::status()` reads one summary. Python returns the replies as dicts and takes the list and event filters as keywords. TypeScript uses the same names. Without the `sessions` capability they fail with an unsupported error before sending.
- A failed session read is `LaserError::Session` in Rust and `SessionError` in Python and TypeScript, classified by its result code.
- `list()` also filters by `root(id)`, the tree rooted at one session, and `label_prefix(prefix)` (TypeScript `labelPrefix`, Python `root=` and `label_prefix=`).
- `SessionInfo.held` counts the records held while the session was paused and not yet handled. `SessionFlags.liveness_unknown` is set while the server's heartbeat tail has not caught up, so `idle` is not meaningful yet. `SessionEvent.verified_actor` names the principal whose key verified a signed record.
- `watch` starts from now and reports the changed session ids of each poll, or a resync when it fell below the retained change floor.
- `Laser::read_at(source)` (TypeScript `readAt`) reads the one record a message source reference names with a standard poll. It returns nothing when the record is gone or the topic was recreated, and works on open Apache Iggy.
- [Session sizing](session-sizing.md) reports what a local managed stack measured for role scans, filtered page admission, fold rate, state history growth, and the change feed.

### Pause and resume

- `sessions().control(stream, id).as_operator(op).pause()` writes a pause request that names its participants: the agents given to `participants(..)`, or else the agents the session lane shows working on the session. `resume()` lifts it.
- Each named participant acknowledges the request with a `Paused` status that names the request's exact position. An agent outside the set acknowledges when it first receives work for the paused session.
- An agent holds work that arrives while the session is paused. It writes a `session_parked` record on the lane before it commits the source, and never treats parked work as handled. After the resume it acknowledges with a `Working` status, handles each held record at least once before new work, and writes `session_unparked` after each one. A crash between the effect and that record can repeat the effect, so effects still need an idempotency key or a fenced write.
- A cancel while paused ends the session as canceled. Held records are not handled. `Session::parked()` lists them with a `complete` flag that is false when the bounded read could not prove the list complete, and the managed index counts them as `SessionInfo.held`.
- The handler is never interrupted. Recovery after a restart, rebalance, or reconnect rebuilds the held set from the lane and reports incomplete recovery instead of dropping work.
- Timelines show the holds as `session.parked` and `session.unparked`. No capability advertises the pause runtime yet.

### Session budgets

`Session::over_budget()` reports whether a session passed the budget in its start record: the summed input and output tokens of its records over `Budget.tokens`, or their summed `cost_micros` over `Budget.cost_micros`. With the `sessions` capability it reads `SessionInfo.over_budget` from the index. On open Apache Iggy, and for a session the index does not know yet, it folds the retained lane by the same rule. A workflow checks its run session after the cancel check at every step boundary, compensates the completed steps, and returns a budget exceeded error, and every budget breach of a workflow ends the run session failed with reason `budget`. The reliable consumer checks a session before handing its work to the handler, once per session per poll batch, and only on a deployment that serves sessions, because folding the lane for every record would cost too much on open Apache Iggy. Work for a session over its budget ends the session failed with reason `budget` and an error naming the ceiling, and is committed without reaching the handler. The terminal latch keeps that failure to one record, and later work for the session is committed the same way. Budget reads are eventually consistent, so a budget is a cooperative limit, not a hard spending cap.

### Stream-scoped resource names

- A `Laser` with a default stream scopes every managed resource name it sends to that stream as `stream:<stream>/<name>`: key-value and memory namespaces, lease and fence namespaces, the key registry, graph names, projection IDs and index names, query indexes, fork IDs, and the change-feed index filter. A name that already starts with `stream:` is sent as is. A `Laser` without a default stream sends bare names.
- `Laser::resource_name(name)` (TypeScript `resourceName`) returns the name a handle sends. `Kv::resource_namespace()` and `ForkHandle::resource_id()` (TypeScript `resourceNamespace` and `resourceId`) return a handle's scoped name.
- Listings of namespaces, projections, and forks return only the handle's own scoped names, with the prefix stripped.
- The projection selector header `agdx.ref` on a published record keeps the local projection ID. The managed backend resolves it against the bindings of the record's own stream and topic, first as written and then as `stream:<stream>/<ref>` with the stream the record was read from, so a scoped projection matches a bare header and a record can never select another stream's projection.
- Consumer filter catalog names are never chosen by the client. A group's own filter is named from the group's verified identity, which includes the stream ID and the stream creation time, so two streams cannot share one. A raw catalog `Register` request sent through the low-level `mutate` call carries its name as written: the streaming server authorizes a scoped name against the named stream, and a deployment in stream tenancy mode refuses a bare one.
- Schema requests carry the default stream, so a writer schema ID resolves in that stream's registry. `ConsumerFilter::with_schema_stream(stream)` names the registry a filter's `schema_refs` resolve in.
- Python `Laser.query(index)`, `Laser.query_target` with an operational index, and `QueryRequest.fork(id)` scope their names like Rust and TypeScript. Python `Kv.namespace` and `ForkHandle.id` return the caller's local name, also when the caller passed a scoped one.
- `ResourceNaming::Bare` opts out and sends every name exactly as written: Rust `LaserBuilder::resource_naming(ResourceNaming::Bare)` or `laser.with_resource_naming(ResourceNaming::Bare)`, Python `Laser.connect(..., resource_naming="bare")` or `with_resource_naming("bare")`, and TypeScript `resourceNaming("bare")` on the builder or `withResourceNaming("bare")`.
- `Capabilities.stream_tenancy` (TypeScript `streamTenancy`) is set when the deployment scopes every managed name to one stream. On such a deployment a scoped `watch` reads its stream's own change topic, and a role grant that spans streams fails with `AuthzError::TenancyViolation`.

### Wire

- Codes 1_000_700 to 1_000_703 and feature bit `1 << 4` are reserved and never reused.
- The display types include `invalid` for a record whose header and body name different identities, and `session.parked` and `session.unparked`.
- `ChangeRecord.stream` names the stream a change record belongs to when the deployment publishes per stream.
- The HTTP path helpers percent-encode every name they place in a path: fork IDs, KV namespaces and keys, graph names, graph nodes, graph IDs, and projection IDs. A scoped name is one path segment. Pass names unencoded, or they are encoded twice.
- `Capabilities::from_versions` sets `sessions` from the `SESSIONS` feature bit, so `GET /agdx/capabilities` reports session reads when the deployment serves them.

### Topics and bootstrap

- `AgentTopic` has `Sessions`, `Streams`, `Heartbeats`, `Control`, `Memory`, `Audit`, `Registry`, `WorkflowJournal`, `Dlq`, and `Custom`. Every agent record rides `agent.sessions` unless the caller names another topic.
- `Laser::bootstrap(partitions, retention)` requires a `TopicRetention` for `agent.sessions`. `TopicRetention::expire_after(age)` is the usual choice. A policy that never expires and has no size bound is refused. Bootstrap also creates `agent.heartbeats` with a one-hour expiry and `agent.streams`, `agent.memory`, `agent.dlq`, `agent.audit`, and `agent.workflow_journal`. The first agent card creates `agent.registry`. Bootstrap never creates `agent.control`, which provisioning creates with an operator-only send grant. A stream that already exists is used as it is.
- The default memory topic is `agent.memory`.
- `ContextAssembler` and `ContextScope` read `agent.sessions` by default.

### Dispatch and replies

- The reliable consumer classifies every record before dispatch as work, observational, reply, lifecycle, control, or foreign. Only work for the handler's operations reaches the handler. Everything else is skipped and committed, and `Laser::consumed` reports it as `Skipped`. The author never decides the class, so an agent can send work to itself. Rust `Agent::builder().operations(..)`, Python `spawn_agent(operations=)`, and TypeScript `AgentBuilder.operations(..)` limit the command operations a handler serves.
- A panic in a Rust handler becomes a non-retryable error. The record dead-letters as rejected and the consumer keeps running.
- Delivery runs on the consumer-group consumer with explicit commits. Each agent id has its own group, named after the agent id unless the builder names another. On `agent.sessions` and `agent.control`, when the server resolves group policies and serves filtered reads and the filter catalog, and the `Laser` was built from a connection string, the runtime binds the agent's group to the addressee filter `agdx.to In [<agent>, "*"]` before it reads, and refuses a group bound to another filter. Otherwise, on open Apache Iggy or with a client the application brought, the group stays unbound and the client classifies every record.
- A record that `send_agent`, `request`, or their Python and TypeScript forms (`send_agent`/`sendAgent`, `request`) publish on `agent.sessions` or `agent.control` without a target carries `agdx.to = *`, the same rule the envelope path applies, so an agent whose group is bound to the addressee filter still receives it. A named target rides unchanged, and other topics keep the caller's headers verbatim.
- A record on a shared session topic that carries no `agdx.to` at all, such as a raw `topic("agent.sessions").send(...)`, never passes the addressee filter of an agent whose group is bound to it. A server with group-aware reads keeps the record on the log, and that agent neither handles nor dead-letters it. Open Apache Iggy classifies on the client and dead-letters the malformed record.
- A dead letter keeps the record's conversation, also for a record that did not decode. A dead letter never fails its session unless the agent runs with `fail_on_dead_letter`: Rust `Agent::builder().sessions(SessionConfig::new().fail_on_dead_letter(true))`, Python `spawn_agent(..., sessions=laser.sessions(fail_on_dead_letter=True))`, and TypeScript `AgentBuilder.sessions(new SessionConfig().failOnDeadLetter(true))`. That configuration also shapes the `ctx.session()` handles.
- A request waiter accepts a reply only when it carries the request's correlation, belongs to the request's session, is a response or error when it has an envelope, and is addressed to the requester when the request named one. The request's own record never answers it, even on the same topic.
- `respond`, `reply_on`, and `respond_input` address the requester and carry the request's record id and full log position as the cause. A generic reply always carries a correlation, the request's own or its message id.

### Partition routing

- A stream's layout is `SessionLayout::Shared` (the default), `PerAgentTopic(map)`, `PerAgentPartition(map)`, or `SinglePartition`. Python spells them `SessionLayout.Shared()` and the others, and TypeScript uses `{ kind: "shared" }` and the other kinds. Declare a layout through the session configuration. It then holds for every later send on that stream through the same connection.
- Lifecycle and state always ride the session's partition on `agent.sessions`. In the per-agent partition layout, a command, response, error, or chunk addressed to a declared agent lands on that agent's partition. A command is keyed by its addressee and a reply by its requester. Every other record rides the session partition. Partition ids are zero-based, and the mapping must be declared because hashing agent names can collide.
- In the per-agent topic layout, the map names each declared agent's own topic. On `agent.sessions` sends, a command addressed to a declared agent goes to that agent's topic, and a response, error, or chunk goes to its addressee's declared topic, keyed by session. Plain records sent with a target follow the same rule. Lifecycle, state, broadcast records, and records for an undeclared agent stay on the session lane. `Sessions::bootstrap` creates the declared topics with the lane's partition count and retention and refuses a declared `agent.sessions` or `agent.control`. A reliable consumer for a declared agent that listens on `agent.sessions` reads its own topic plus `agent.control`. A request, contract, input request, or MCP tool call by a declared requester awaits its reply on the requester's topic. Session reads cover the declared topics because bootstrap registers every topic of the stream. Python spells it `SessionLayout.PerAgentTopic(topics={"planner": "planner.inbox"})`, TypeScript `{ kind: "perAgentTopic", topics: new Map([["planner", "planner.inbox"]]) }`.
- `SinglePartition` bootstraps every agent topic with one partition.
- Every record on `agent.sessions` carries `agdx.to`, with `*` when it is addressed to every agent.

### Context, memory, and lineage

- `ContextMessage` carries `timestamp_micros`, the broker append time, and the numeric `stream_id` and `topic_id` beside the topic name, so a reader can build a source reference without a lookup.
- `ContextPolicy` has `name()`, `version()`, and `selection(history)`, which returns the kept and dropped records and the reason. A context manifest records the policy name and version.
- Every SDK uses one token estimate: the byte count divided by four, rounded up.
- `ScopedMemory::with_lineage(origin, producer)` stamps remembered items with the record that motivated them and the agent or policy that wrote them. `Session::linked_memory()` does this for the session's current record.
- `Kv::in_session(reference)` and `GraphHandle::in_session`, `produced_by`, and `sourced_from` link key-value and graph writes to a session. `Session::kv(namespace)` and `Session::linked_graph(name)` return handles that are already linked.

### Workflows and child sessions

- `Workflow::run` is a root session whose id is the run id. Each step and compensation is a child session with an id derived from the run and the step label. The engine checks `agent.control` for a cancel request at every step boundary, compensates, and returns a cancelled error. It then checks whether the run session is over its budget, as [Session budgets](#session-budgets) describes.
- `ContractBuilder::parent(parent, root)` runs a contract as a child session. It writes the child's submitted start before the command and ends the child by the outcome.
- `A2aBridge::submit_in(parent, root, params)` and `McpBridge::call_tool_in(parent, root, name, params)` run a bridge call as a child session. Python takes `root=` as a keyword, and TypeScript names them `submitIn` and `callToolIn`. MCP ends the child by the tool result. A2A leaves that to the handling agent.
- A bridge call and an input request are addressed to every agent (`agdx.to = *`) unless the caller names one, so on a shared `agent.sessions` topic every listening agent receives them. `A2aBridge::submit_to` and `submit_in_to`, `McpBridge::call_tool_to` and `call_tool_in_to`, and `Agdx::request_input_from` take a target agent and address the command to it alone. Python takes `target=` on `submit`, `submit_in`, `call_tool`, `call_tool_in`, and `request_input`. TypeScript takes a `{ target }` option on `submit`, `submitIn`, `callTool`, `callToolIn`, and `requestInput`. The JSON-RPC bridge routes stay unaddressed.

### Other behavior

`Laser::spawn_subconversation(parent, author)`, Python `Laser.spawn_subconversation(parent, author)`, and TypeScript `Laser.spawnSubconversation(parent, author)` require the spawning agent's identity. A handler context supplies its own agent identity when it creates a child conversation, so the child's author is the spawning agent and never the incoming sender.

Log memory searches use the supplied text to filter items before applying the result limit. Keyword, semantic, and hybrid searches use lexical matching on log memory. Recent recall ignores query text and orders by recency only. This applies to the managed memory view and the folded log view in all three clients.

A forget or feedback call with a conversation in its scope acts only on an item remembered in that conversation. `ScopedMemory.forget` and `improve` always pass their conversation, so they never remove or reweight another conversation's item. A call without a conversation acts on the item in any conversation. The log fold, the in-process vector index, and the managed memory view apply the same rule in all three clients.

Managed memory recall orders items by the broker append time of their source record, then by source position. Consolidation keeps the newest items by the same arrival order in all three clients. The memory view row carries the broker time as `timestamp_micros`. Periodic agent consolidation passes the agent's own scope, with the agent id set and every other field empty. A consolidator with a summarizer writes one durable `Summary` per conversation, scoped and attributed to that conversation.

`Laser::consumed` (Python `consumed`, TypeScript `consumed`) returns `Skipped` when an agent's consumer group has committed past a record that is not work for that agent, such as a record addressed to another agent, a reply, or a status record. The status names the classification, for example `foreign`. Records that the group handled report `Consumed`. A probe of a named consumer, or of a record that is gone or does not decode, keeps the offset-only answer.

All three clients stamp record headers through one shared encoder. Generic records carry the fence, deadline, and token counts as typed `u64` headers and the cost as a typed `f64`. Envelope records also carry their author as `gen_ai.agent.id`. An `agdx.to` value of `*` addresses every agent and decodes as no single target.

A group reader that reads from partition primaries asks the coordinator for the cluster topology and the partition route before its first read. A transient refusal of either request is retried with the connection's publish retry count and backoff, as [publish recovery](publish-recovery.md) describes, before it reaches the caller. This applies to Rust and TypeScript. Python reads through the Rust reader.

Rust keeps an agent running only while its `AgentHandle` lives. Dropping the handle signals a graceful worker shutdown and stops periodic consolidation. Call `shutdown().await` to see drain failures.

Python `Laser.close()` does not print the Iggy producer's `Client has been shutdown` warning. That one record, which every cached producer writes when its own client closes, reaches Python logging at debug. Every other Rust log record keeps its level.

AG-UI rendering emits `RUN_ERROR` for failed or rejected task status. It uses the status detail when available. Completed and canceled task status emits `RUN_FINISHED`.

Fold snapshots name the stream, its creation time, the fold, and each topic generation. Built-in snapshot stores require a fold name. Managed snapshots use the stream, conversation, and fold in their key. State replay checks source generations and resumes each topic at its own saved offsets. `ConversationState` replay folds the whole requested range for a full, offset, checkpoint, or position bound. A `Last(n)` bound stays windowed.

A query above `MAX_PAGE_SIZE` fails with an invalid-argument error before any round trip.

## Publication and reads

A publish that gives up reports the confirmed ranges and the unconfirmed records. Retries keep the message IDs of the first attempt. See [publish recovery](publish-recovery.md).

A batching producer keeps its timer running after a failed timer flush. The next `send`, `flush`, or `close` reports the kept failure. A `send` that finds it refuses its own record and adds it to the unconfirmed records. `flush` and `close` drain the queue first. Inspect the confirmed ranges before you retry.

Topic creation settings apply only when the topic is absent. Two clients that create the same topic at once both succeed. A background send returns once the queue accepts the records. `shutdown` drains the queue and reports a kept background failure. A TypeScript background error callback can return a promise, and a failing callback does not replace the original publish failure.

Live waits end with a typed timeout. TypeScript keeps an unfinished poll so the next call can still receive its late record. Cancelling a wait does not acknowledge a record that was never delivered.

A query with a zero row ceiling sends nothing. Results, pages, status, and cancellation stay bound to the execution ID that was requested.

TypeScript MessagePack writes named maps and the shortest integer encoding. It rejects integers outside the signed and unsigned 64-bit ranges. Bodies the caller has already encoded stay opaque bytes.

## Coordination and ownership

All three clients expose the two-phase fenced-lease client. Prepare an acquisition, renewal, release, or fenced compare-and-swap once. The prepared object keeps its operation ID and exact frame bytes. Repeat it only when its recovery action allows that.

Each attempt has a 10-second timeout by default. Rust takes a `Duration`, Python seconds, and TypeScript milliseconds. A mutation with an uncertain outcome retires the connection and reports a recovery action for that operation. After an uncertain acquisition, wait out the requested lease lifetime. Renew and release can repeat the prepared object. A fenced write needs reconciliation against its target.

The low-level client reports the recovery action and leaves the acquisition wait to the caller. The ordinary `Kv` lease calls perform that wait themselves. Reads return plain timeouts. Readiness and request validation fail before anything is sent.

`close` is terminal. `reset` retires the transport connection and allows reuse. A dedicated transport's readiness check waits until the old connection is retired. A closed `Laser` cannot reopen its coordination connection.

An agent handle owns its worker. Python follows the same rule as Rust, and TypeScript handles support asynchronous disposal. The shutdown grace period bounds pending SDK work, and application callbacks must honor cancellation.

Periodic consolidation needs both an interval and a consolidator. The consolidator owns its memory and receives the agent's own scope. A failed pass does not stop later ticks. `join` keeps consolidation running until the worker exits.

## Memory and context

`ConsolidationReport` counts summarized, reweighted, pruned, and derived items. The default pass looks at no more than 10,000 items, keeps the newest IDs, and counts only removals that succeeded. The built-in summarize pass writes one durable `Summary` item per conversation, and optional pruning removes the messages it covered. Log storage does not persist a separate lifetime field.

`MemoryHandler` remembers a successfully handled message under its conversation and agent. It stores nothing until `auto_remember` selects a kind. A failed memory write does not run the handler again.

Lifetime is metadata for writes and custom backends. Built-in recall filters on identity fields, not on lifetime. A reranked handle keeps explicit kinds and content IDs. A custom backend keeps the custom callback contract and does not gain named-item operations from the code behind it.

Context assembly supports custom policies, ancestry, start offsets, and checkpoints. Each partition read examines at most 10,000 raw records before it filters by conversation. A checkpoint takes precedence over raw start offsets. A custom selection keeps the source topic of each message.

Python takes full memory scopes and queries as keywords. `to_context_block` renders recall results in the order given and keeps the first item even when it alone exceeds the token estimate.

## API spellings

| Feature | Rust | Python | TypeScript |
| --- | --- | --- | --- |
| Prepared coordination | `FencedLeaseClient::connect_dedicated`, `prepare_acquire`, `acquire` | `FencedLeaseClient.connect_dedicated`, `prepare_acquire`, `acquire` | `FencedLeaseClient.connectDedicated`, `prepareAcquire`, `acquire` |
| Attempt timeout | `with_attempt_timeout(Duration)` | `with_attempt_timeout(seconds)` | `withAttemptTimeout(milliseconds)` |
| Recovery action | `PreparedMutation::ambiguous_recovery` | `PreparedMutation.ambiguous_recovery` | `PreparedMutation.ambiguousRecovery` |
| Summaries | `DefaultConsolidator::with_summarizer`, `prune_summarized` | `consolidate(..., summarizer=, prune_summarized=)` | `consolidate(scope, maxItems, { summarizer, pruneSummarized })` |
| Content helpers | `MemoryId::content`, `MemoryKind::class` | `memory_id_content`, `memory_kind_class` | `MemoryId.content`, `memoryClass` |
| Card predicates | `RegisteredCard::is_fresh`, `serves`, `available_for` | `RegisteredCard.is_fresh`, `serves`, `available_for` | `cardIsFresh`, `cardServes`, `cardAvailableFor` |
| Cause position | `with_cause(cause, Some(position))` | `cause_at=LogPosition(stream_id, topic_id, partition_id, offset)` | `withCause(cause, causeAt)` |
| Agent route | `Agent::builder().inbox_route(..)` | `spawn_agent(..., fixed_inbox=)` | `AgentBuilder.inboxRoute` |
| Governor retention | `governor_retention(GovernorRetention)` | `spawn_agent(..., governor_retention=(capacity, idle_ttl_ms))` | `AgentBuilder.governorRetention(retention)` |
| Periodic memory | `consolidate_every(Duration)`, `consolidator(..)` | `consolidate_every_ms=`, `consolidator=` | `consolidateEvery(intervalMs)`, `consolidator` |
| Arrow fingerprint | `Record::logical_schema_fingerprint` | `BatchPublishRequest.add_record(logical_schema_fingerprint=)` | `Record.logicalSchemaFingerprint` |

Python send verbs take envelope refinements as keywords. A cause position requires a cause ID, and per-kind validation still applies. A claim check moves a body to the supplied store when it reaches the threshold. Request-input helpers and chunk writers do not sign records.

Python `Topic.cbor` and `Topic.schema` return typed handles. Their `publish(body)` requires a body, and one cached handle encodes and reads with a single registry lookup and compile. Plain `topic.publish()` stays the raw builder. Protobuf publication takes encoded bytes with a schema ID, while TypeScript can encode schema-backed bodies itself. Batch defaults fill a record's unset content type, projection, schema ID, and inline payload, and batch index entries and headers merge with the record's own, which win. An Arrow fingerprint must be 32 bytes and requires Arrow content.

`spawn_agent(None, ...)` in Python opens an unscoped reliable consumer. It needs a consumer group and cannot advertise capabilities. Capabilities accept skill names or descriptor dicts.

## Portable helpers and callbacks

The pure A2A and MCP converters keep the original request bytes and open no connection. Card signatures, delegation checks, claim checks, snapshot bytes, resume offsets, and reciprocal-rank fusion follow the same native contract in every client. Python exports them in snake_case, for example `encode_snapshot` and `decode_snapshot`, and TypeScript in camelCase, for example `encodeSnapshot` and `decodeSnapshot`. `SystemClock` and `TestClock` count epoch microseconds as unsigned 64-bit values.

Python workflow builders, verifiers, and compensation callbacks can return a value directly or through an awaitable. Objects with `build` or `verify` methods work too. A custom snapshot store implements `latest(conversation)` and `save(snapshot)`, and `state_with` accepts it directly or wrapped in `SnapshotStore`. Callback errors keep their SDK class, and cancellation retires the asynchronous work in flight.

A custom Python route policy is `None`, a built-in policy name, or a synchronous callable or object with a `select` method. The scorer receives `skill_id` and `RouteCandidate` objects with `agent`, `card`, and `capability`. It returns a candidate index, or `None` to refuse them all. An async scorer is rejected.
