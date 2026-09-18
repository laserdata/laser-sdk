---
name: agent-runtime
description: The Laser SDK runtime - `sdk/src/agent/`. Use when changing the `Laser` facade (bootstrap, send_agent, request/reply, producer cache, spawn_subconversation), the typed AGDX producer verbs (`Laser::agdx` -> `Agdx`/`AgdxStream`), the `ChunkAssembler` stream reassembly, the reliable consumer (dedup, retry, DLQ, deadline, undecodable handling), `Agent::builder`/`AgentHandle` shutdown, `Router`, or `SessionPolicy`.
---

# Agent runtime

The native TypeScript peer is `foreign/typescript/src/agent`, with contracts, workflows, and real-Iggy runtime tests under `foreign/typescript/test`.

`agent/` is the customer-facing runtime: how you send, consume reliably, route, and spawn agents. Load [laser-sdk-overview](../laser-sdk-overview/SKILL.md) first. Repo rules in [AGENTS.md](../../../AGENTS.md).

Iggy provides the VSR transport and AGDX command classifier. Agent publish, poll, consumer-group, replay, retry, and dead-letter paths use standard Iggy commands. Managed presence, registry enrichment, fenced workflows, and run-registry commands use the non-replicated extension path and remain capability-gated by the connected Iggy server.

## STOP and ask the user before

- Changing public signatures: `Laser::{send_agent, request, bootstrap, spawn_subconversation}`, `AgentHandler::handle` (takes `&AgentMessage` + an `&AgentCtx<'_>`), `AgentCtx`, `Agent`/`AgentHandle`, `ReliableConsumer::run`. These are the API customers code against.
- Both `Serial` and `SerialPerPartition` use `AutoCommit::Disabled` and store offsets after successful handling. Changing this policy requires explicit authorization. Do not weaken at-least-once handling.
- Changing dedup-before-handle ordering or the DLQ behavior.

## Key files and symbols

- `laser.rs` - `Laser` facade.
  - `send_agent`: encode provenance -> headers, partition by `provenance.partition_key()`, send via a per-topic cached producer.
  - The producer cache stores `Arc<OnceCell<Arc<IggyProducer>>>` values in a `DashMap`, keyed by stream and topic. Hold the map lock only to obtain the cell. Run `producer.init().await` through the cell after releasing the map lock.
  - `request` uses `ReplyHub` in `agent/replies.rs`. It creates a fresh `correlation_id` and records it in `agdx.corr`. `AgentCtx::respond` and `reply_provenance` echo that correlation, not `idempotency_key`. A conversation ID alone cannot match a request. Call `subscribe(correlation)` before publication and `ticket.wait(timeout)` afterward. One reply reader serves all waiters.

AGDX contracts and workflows use a tail-seeded `AgentReplyReader`. `poll_contract` and `next_agdx_match` apply their pickup, terminal, and signature rules.
  - `spawn_subconversation(&Provenance) -> Provenance`: fresh `conversation_id`, `parent_conversation_id` = parent's, `root_conversation_id` carried through.
  - `bootstrap`/`ensure_topic`/`ensure_stream`: idempotent create-if-missing. `bootstrap` also warms a producer per well-known topic concurrently.
- `consumer.rs` provides public `ReliableConsumer::run` and private `ReliableWorker`, which implements Iggy `MessageConsumer`. Decode each record first. `dead_letter_undecodable` preserves invalid payloads with `DeadLetterReason::DecodeFailed`. Apply `provenance.target_agent_id` filtering, stale-fence rejection, duplicate suppression, and deadline handling before the handler. Expired fence-map entries are removed after its soft limit.

Retry `is_retryable()` handler failures through `max_attempts`. `LaserError::Rejected` and `LaserError::HandlerConfig` do not retry. Exhaustion publishes an `AgentDeadLetter`. Its reason is `DecodeFailed`, `DeadlineExceeded`, `Rejected`, or `RetryExhausted`. It includes attempts, full `LogPosition`, and the original payload. Preserve provenance, clear the deadline, and set `agdx.ct = cbor`.

`dead_letter` and `dead_letter_undecodable` call `publish_dead_letter`. A failed required publication reports through the optional `DeadLetterSink`, fails the worker, and leaves the source offset uncommitted. Ordered `AgentMiddleware` runs `before_handle` once before retries and `after_handle` after each attempt. A middleware rejection bypasses the handler and uses the dead-letter path.

`run` reports readiness after joining. `POLL_INTERVAL` defaults to 10ms and can be configured. `Serial` uses `IggyConsumerMessageExt::consume_messages` with `AutoCommit::Disabled`. It processes records serially and stores the group offset after the wrapper succeeds.

`SerialPerPartition { max_partitions }` reads the Iggy stream with automatic commits disabled. It sends work to bounded per-partition lanes and calls `store_offset(offset, Some(partition))` after successful handling. Global record and byte semaphores bound queued work. Lanes run concurrently across partitions and serially within each partition. A partition beyond the lane cap is handled inline. A failed lane stops its successors and ends consumption before a later offset can pass it.

Shutdown stops new polls and drains accepted work. The serial path stops the Iggy loop, and the lane path closes its senders. `shutdown_grace` defaults to 30s. Exceeding it returns `LaserError::Timeout`. `AgentHandle::abort()` stops immediately.
- `Laser::redrive_dead_letter(&AgentDeadLetter)` in `agent/laser.rs` reads the original source record and republishes its body and headers. It uses numeric stream and topic IDs and conversation routing. Missing or expired source records return `LaserError::Invalid`.
- Use `message.header.offset` in `AgentMessage::from_received`. `received.current_offset` is the partition head and can be identical for several messages in a batch.
- `from_received` uses `is_agdx` and `agdx.av` to detect an `AgentEnvelope`. It exposes the result through `AgentMessage.envelope` and dead-letters decode failures as `DecodeFailed`. `provenance_from_envelope` supplies conversation, source, target, idempotency, and deadline values for runtime decisions. Non-AGDX records use `Provenance::try_from` with `envelope = None`.

Use `envelope.unmet_requirements(understood)` with the supported `agent::features` bits. Reject or dead-letter records that require unknown features. No bits are currently defined, so valid records use `must_understand == 0`.
- `Provenance::try_from(&IggyMessage)` matches keys first and fails-not-skips: a known provenance key with a non-string value is a decode error, while foreign/unknown keys are ignored. The AGDX typed headers (`agdx.ct` u8, `agdx.av` u32, the `Uint128` routing duplicates) coexist on one record because they are foreign to the provenance dictionary, not because non-string values are skipped.
- `Deduplicator` provides asynchronous `observe(key) -> bool`. `SlidingWindow` uses the bounded local `DedupWindow`. Applications can supply another implementation through `Agent::builder` or `ReliableConsumer`. `StateStore` and managed `Kv` can supply persistent storage. Warmup preloads the configured duplicate-suppression implementation.
- `Laser::capabilities()` returns grouped `Capabilities` from `capabilities.rs`. Original Apache Iggy defaults to `OPEN`. `AGDX_HELLO` and `refresh_capabilities()` discover current support. A `BackendAnnounce` with `ready=false` retains descriptive versions and topology but must not enable managed operations.

Builder capabilities remain the additive seed during refresh. `Laser::with_capabilities(..)` is an authoritative handle-local override. Inspect capabilities before using managed operations. Groups include `managed`, `query`, `kv`, `graph`, `forks`, and `a2a_gateway`. Nested values include `capabilities.kv.available`, `capabilities.kv.cas`, and `capabilities.query.consistency`. `examples::cloud_feature_ready` demonstrates this pattern.
- `Agent::builder` requires identity, input topic, and handler. It also accepts `respond_on`, `poll_interval`, `inbox_route`, and `capabilities`. Runtime controls include `dedup_window`, `retry(RetryPolicy)`, `verifier`, `signing_key`, `shutdown_grace`, `concurrency(ConcurrencyPolicy)`, `middleware`, `on_dead_letter`, and `governor`. `spawn(Laser)` returns `AgentHandle`.

`InboxRoute::Advertised` is the default, and the chosen route passes to the consumer and context. Non-empty capabilities trigger `advertise` before consumption. It publishes a card through `Laser::publish_card` and can call `advertise_presence` when supported. Presence is query-gated and best-effort except `PresenceConflict`. One connection can advertise one identity.

`ack_on_pickup` defaults off. When enabled, a consumer publishes `Working` on `respond_on` before handling a command. `ready()` waits for group membership and polling readiness. `shutdown()` drains within `shutdown_grace`, `join()` returns the task result, and `abort()` stops immediately. Dropping the handle does not stop the agent.
- `../govern.rs` - effect-boundary policy. `QuorumGovernor` runs voters concurrently. Every mandatory voter must affirm. Mandatory errors, empty sets, invalid thresholds, duplicate names, and conflicting body replacements block. Non-mandatory errors abstain. `SwappableGovernor::swap` returns the replaced policy and `current` reads the active one.
- `../swarm.rs` - replay-safe supervisor fold. `observe` drops unattributed evidence and deduplicates by `decision_id`. Latest evidence is selected by `(at_micros, decision_id)`.
- `../crash_context.rs` - combines already-read journal, dead letter, and policy evidence. `summarize` bounds and escapes untrusted text so payload control characters cannot forge lines.
- `../intent.rs` - SDK typed records, not AGDX wire. `Intent::builder().build`, `Vote::cast`, and `decide` are fallible. Construction and replay validate voter sets, thresholds, deadlines, and body digest. Ballots are time-bounded and canonicalized, every mandatory voter must allow, and `Decision::authorizes` verifies id, digest, and policy version before an effect. Names remain claims unless signing or topic ACLs authenticate authorship.
- `AgentCtx` supplies `respond`, `reply_on`, `send`, `request`, `respond_input`, `spawn_subconversation`, and `laser` and `message` accessors. It carries the selected agent identity, `respond_on`, and `inbox_route`. Replies preserve correlation and use the agent signing key when configured.

`fan_out(selector, payload, GatherPolicy, deadline) -> Gather` resolves capable agents and sends each branch to its inbox. Replies return through the orchestrator reply topic. An unresolved inbox produces a branch failure, not a shared-topic fallback. `approval_gate(reply_topic, prompt, timeout) -> Vec<u8>` delegates to `Agdx::request_input` and reports rejection as `Rejected`.

Python `PyAgentCtx` in `foreign/python/src/agent_runtime.rs` exposes these operations. It constructs a temporary `laser_sdk::testing::agent_ctx` because Python classes cannot retain the borrowed Rust `AgentCtx`.
- `Laser::agdx(topic, source, conversation)` creates `Agdx`. Its `command`, `respond`, `emit`, `status`, and `fail` methods return `AgdxSend` builders. They check the envelope at `send()`. `request_input(reply_topic, prompt, timeout)` creates a fresh interrupt correlation and waits for its response. An error reply returns `LaserError::Rejected`. Configured verifiers reject unsigned or invalid replies.

`stream(correlation, purpose)` creates `AgdxStream`. The opening chunk carries purpose and deadline. Each write assigns a sequence, and `finish` or `fail` supplies the terminal. `.buffered(max_chunks, linger)` groups records, including the terminal, without a background task. `Agdx::assemble` is shared by individual and buffered sends.

`sdk/src/batching.rs` supplies `topic.batching()` for general records. It bounds records, bytes, and delay, and requires one partition key per handle. Size-triggered flush runs inline to apply backpressure. `flush()` and `close()` are explicit, while drop flushing is best-effort and logs failures.

Sends attach `agdx.av`, `agdx.ct`, a conversation `Uint128`, and the target name when present. Partition routing uses the canonical conversation base32 string. `AgdxStream::write` rejects bodies above `MAX_CHUNK_BODY_BYTES` before publication and advances the sequence only after success. The guidance constants are `DEFAULT_CHUNK_FLUSH_BYTES`, `DEFAULT_CHUNK_LINGER_MS`, and `MAX_CHUNK_BODY_BYTES`.
- `ChunkAssembler` in `assembler.rs` applies chunks in order and counts discarded duplicates. A gap creates a local `gap` terminal. Records after a terminal are dropped. `abandon()` creates a local `abandoned` terminal. `StreamEvent::{Body, Finished, Failed}` reports results without I/O or a clock.
- `router.rs` - `Router::{To, ToPrincipal, Broadcast, ToCapable, AllCapable}`. `ToPrincipal` and `CapabilitySelector::principal(PrincipalId)` require the selected live presence to carry that server-authenticated principal, a missing or foreign claim is `RoutePrincipalMismatch` (Forbidden, non-retryable). Capability policies rank only the principal-filtered candidates. `InboxRoute::{Advertised, Fixed(topic)}` turns a resolved target into the topic to address: `Advertised` resolves live presence, `Fixed` uses a caller-owned topic. A route resolves within the caller's stream, cross-stream addressing is deferred federation.
- `Laser::agent_registry` creates `AgentRegistry` from cards and optional live presence. `refresh` selects current `RegisteredCard` values and expires old cards by `ttl_micros`. `resolve(skill, now)` selects available agents. `RegistryCache` stores cards, quarantines, presence, and cursor offsets per data stream so reads resume incrementally.

Presence uses `refresh_presence`, with a cache lifetime of about 2s. `Laser::advertise_presence` and `clear_presence` use `AGDX_SET_CLIENT_METADATA`. `Laser::client_metadata()` reads paged discovery data. These methods require `query`. `PresenceEntry` retains the authenticated `user_id`. `inbox_for_principal(agent, user_id)` requires that identity, while `inbox_for(agent)` resolves the claim alone.

`Laser::quarantine(operator, agent)` appends a status fact, and `apply_quarantine` excludes it from `resolve`. `is_quarantined` reports the state. `Laser::unquarantine(operator, agent)` reverses it. Registry-topic permissions control these writes. With `sign` and `LaserBuilder::verifier(Arc<KeyRegistry>)`, `quarantine_signed` and `unquarantine_signed` also require valid operator signatures.
- `contract.rs` - `Laser::contract(Router) -> ContractBuilder` resolves one target and watches pickup plus terminal. With a verifier enrolled, plain, invalid, and wrong-identity replies are ignored. A principal-bound route binds verification to the same authenticated principal, a claim route binds to the target's enrolled identity. Accepted replies carry `AgentMessage::verified_principal`, and scatter preserves it per branch. Python keeps `contract`/`scatter` as body-only conveniences and exposes identity through `contract_report`/`scatter_report`.
- `Laser::workflow(name)` creates `Workflow`, with `budget`, `inbox_route`, `step(label, Router, StepFn)`, and `run`. `.registered()` requires `runs`. `StepHandle` supplies `after`, `verify_with`, `exclusive`, `exclusive_in(namespace)`, `compensate_with`, and `on_timeout`. `run()` orders dependencies, passes outputs through `StepContext`, enforces `Budget`, verifies results, and records completion. Failure runs compensation in reverse order. An `all_capable` step succeeds when at least one dispatched agent completes.

Exclusive steps require `kv_fenced_leases` and a unique holder per assignment. The default coordination namespace is `WORKFLOW_FENCE_NAMESPACE`, or `agdx.workflow.fence`. `exclusive_in` selects the namespace shared with the handler `Kv::cas_fenced(target_key, fence_namespace, run_id, token)` call. Renew halfway through the granted lifetime and bound renewal by expiry and workflow deadline. Retain the lease through verification and the completion journal write.

`OnTimeout::Reassign` reacquires only after release, so the next holder receives a greater fence. Failed renewal or release must stop the workflow. Keep the command conversation derived from `run_id` and step label. Reject reassignment on non-exclusive steps. The handler must use fenced CAS to protect external state.

Registered runs inspect cancellation between steps and record terminal status. `AgentMessage::body()` supports AGDX and ordinary agent messages. Wire `RunBudget` is separate from coordination `Budget`.
- `session.rs` defines `SessionPolicy::{PerCall, PerUser}`. `PerUser` uses `derive`. `Laser::sessions()` and `sessions_with(SessionConfig)` return `Sessions`, with `create(id)`, `start()`, and `open(conversation)` methods that return a `Session`.

Each `SessionTurnKind` uses one conversation-level `AgentTopic`. The kinds are `instruction`, `response`, `model.response`, `tool.call`, `tool.result`, and `human.input` across Rust, Python, and TypeScript. A turn is an ordinary agent message. `ContextMessage.topic` identifies its kind. `SessionConfig` selects the stream, one distinct topic per kind, the memory namespace, and context limits.

`checkpoint()` records the next offset per topic partition. `turns_at` and `state_at` read before those offsets. `turns_since` and `replay` read from those offsets. Sessions use `ContextScope`, `ConversationState`, and `ScopedMemory` without another store or a new wire format.
- `laser_sdk::testing` in `../testing.rs` provides `agent_message(payload, provenance)` and `agent_ctx(&laser, &message, agent, respond_on, inbox_route)`. These construct handler inputs without a live consumer. A handler needs a server only for I/O methods it calls. Python exposes equivalent owned objects through `foreign/python/src/agent_runtime.rs`.
- `state.rs` - `ConversationState::load(laser, conversation, topics, bound, init, fold)` under an explicit `ReplayBound`, plus `load_with(store, ..)` seeding from a `SnapshotStore`. See [context-and-memory](../context-and-memory/SKILL.md).

## Rules specific to this area

- `Serial` delegates polling to `IggyConsumerMessageExt::consume_messages`. `SerialPerPartition` reads the existing `IggyConsumer` stream with `AutoCommit::Disabled` and explicit `store_offset`. Keep successful handling before commit in both paths. Do not add a second Iggy transport or standard consumer implementation.
- Returning `Ok(())` from the wrapper permits the serial committer to store the source offset. Skip, dedup, deadline, and successfully published dead-letter paths return `Ok(())` on purpose. A required dead-letter publication failure returns `Err` and must leave the offset uncommitted.
- Anything that takes a topic and needs an `Identifier` on a poll/get path uses `AgentTopic::as_identifier()`. Produce/consumer-group calls take the `&str` name (`topic_string()`).

## Review smells

- A lock held across `.await` (especially the producer map or dedup).
- `request` rescanning from offset 0 each poll instead of advancing a cursor.
- A spawned task whose error is dropped (use `AgentHandle::join`/`shutdown`).
- Dropping `AgentHandle` and expecting the agent to stop (it does not, that is by design - call `shutdown()`).
- Undecodable messages silently skipped instead of dead-lettered.
- A permanent / bad-input failure retried `max_attempts` times instead of returning `LaserError::rejected(..)` (immediate DLQ).
- Assuming a premium capability is present without checking `Laser::capabilities()`.

## Publish recovery

Publish attempts default to 60 seconds with three retries. Retry delays start at 250 ms, double after each failure, and stop increasing at 30 seconds.

Rust builder methods are `publish_timeout`, `publish_max_retries`, and `publish_retry_backoff`. Python `Laser.connect` keywords are `publish_timeout_ms`, `publish_max_retries`, and `publish_retry_backoff_ms`. TypeScript builder methods are `publishTimeout`, `publishMaxRetries`, and `publishRetryBackoff`. Explicit configuration overrides `LASER_PUBLISH_TIMEOUT_MS`, `LASER_PUBLISH_MAX_RETRIES`, and `LASER_PUBLISH_RETRY_BACKOFF_MS`.

Exhausted retries return an error for the application to handle. They do not exit the process. Preserve message identity and confirmed chunks across retries. See [publish recovery](../../../docs/publish-recovery.md).
