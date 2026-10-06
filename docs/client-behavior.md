# SDK 0.6.0 client behavior

0.6.0 is a minor release with breaking changes in all three clients. Read [Upgrading from 0.5](#upgrading-from-05) before you bump the version. The wire contract does not change. Operation versions and conversation derivation stay at version 1.

Rust, Python, and TypeScript share the features and defaults listed in [the parity matrix](parity.md). The matrix covers inherent and trait methods, generated builder controls, and standalone SDK functions. Names and construction follow each language.

## Upgrading from 0.5

Each item is one breaking change. Names that only moved keep working under their new path.

### All clients

- `Capabilities` no longer has `sessions` or `durable_dedup` (TypeScript `durableDedup`). No server ever set them, so they were always false. Rust also drops `Capabilities::with_sessions` and `with_durable_dedup`, and Python drops the `sessions=` and `durable_dedup=` keywords of `with_capabilities`.
- Two scenario examples were replaced: `incident-desk` and `fleet-tape` take their place in all three languages. Fixture payloads use neutral systems vocabulary. A project that embeds the fixture corpus must take the regenerated files.
- Periodic agent consolidation passes the agent's own scope, with the agent id set and every other field empty. It used to pass an empty scope and summarize the whole namespace. A consolidator with a summarizer now writes one durable `Summary` per conversation, scoped and attributed to that conversation, instead of one summary per pass. Pruning still covers the whole consolidation scope.
- Deduplicated memory content ids now include the user and the application in the owner. An id minted by 0.5 `dedup` no longer matches, so re-remembering a 0.5 item with dedup stores a second item.
- Folded memory recall skips 0.5 memory records, because they lack the `agdx.mem.ns` header. There is no fallback. Re-remember anything you still need from 0.5.
- A batching producer's `send` reports a failed timer flush. It adds its own record to that failure's unconfirmed records and returns the failure without queueing the record.
- `Recent` recall orders by recency only. Feedback no longer reorders it, and its items carry no feedback score or signal.
- Feedback-derived recall signals report the `Auto` strategy in every backend.
- `ConversationState` replay folds the whole requested range for a full, offset, checkpoint, or position bound. It used to stop at the newest 10,000 records per partition. A `Last(n)` bound stays windowed.
- A query above `MAX_PAGE_SIZE` fails with an invalid-argument error before any round trip, not a too-large query error.
- Both bridges expose transport-independent JSON-RPC dispatch: `handle_rpc(request)` in Rust and Python and `handleRpc(input)` in TypeScript. Malformed input returns a public error response.
- Native TypeScript group polling preserves exact microsecond timestamps and raw header blocks. Avro bytes and fixed fields encode and decode as arrays of byte values in Rust and TypeScript.

### Rust

- Keep the `AgentHandle` until `shutdown` or `join`. Dropping it now signals a graceful worker shutdown and stops periodic consolidation. Call `shutdown().await` to see drain failures.
- A fork row's `embedding` takes `impl IntoIterator<Item = f32>` instead of a string.
- `A2aBridge` and `McpBridge` append their id to `bridge_hops` on submit, cancel, and tool calls, like TypeScript. `with_bridge_hops(previous)` continues an upstream hop list and fails on a loop.
- `laser_sdk::stream::producer_statistics` is no longer public. `ProducerRecorder` and `ProducerObservation` are gone from the API. See [producer statistics](producer-statistics.md).
- `Consumer::stored_offset(partition)` returns the stored `StoredOffset`, or `None`.
- `LaserError::StepUpRequired(scope)` is the struct variant `LaserError::StepUpRequired { scope }`.
- `laser_sdk::query` no longer re-exports the browse, control, and query protocol frames (`BrowseReply`, `ControlEnvelope`, `QueryEnvelope`, `QueryReply`, and the rest). Import them from `laser_sdk::wire::{browse, control, query}`.
- `laser_sdk::fork` no longer re-exports `ForkCreate`, `ForkDelete`, `ForkList`, `ForkOutcome`, `ForkPromote`, `ForkPut`, or `ForkReply`. They live in `laser_sdk::wire::fork`.
- `laser_sdk::kv` no longer re-exports the request and reply frames (`KvSet`, `KvCas`, `KvOutcome`, `KvReply`, `KvScan`, and the rest). They live in `laser_sdk::wire::kv`.
- `laser_sdk::rbac` no longer re-exports the authorization request and reply frames (`WhoamiReq`, `BindRolesReq`, `AuthzReply`, `RoleBinding`, and the rest). They live in `laser_sdk::wire::authz`.
- `laser_sdk::filters` no longer re-exports the saved-filter catalog frames (`FilterRef`, `FilterMutation`, `FilterPage`, `FilterSummary`, `FilteredPage`, `RecordFault`, and the rest) or `ExactDecimal`. They live in `laser_sdk::wire::filter`. `FilterAnnounce` moved to `laser_sdk::capabilities`.
- `laser_sdk::stream::GroupTarget` is private.
- `laser_sdk::prelude::full` no longer exports `SourceCut` or `SourcePartitionCut`. They live in `laser_sdk::wire::source`. The prelude now exports `RunBudget`.

### Python

#### Errors

- A publish that gives up raises `PublishFailedError` with `stream`, `topic`, `committed`, and `unconfirmed`, a list of `IggyMessage`. The original failure is its `__cause__`. Every other exception lost `committed` and `unconfirmed_count`.
- Each Rust error variant has its own exception class with its fields, and every class carries class-level `code` and `retryable` defaults. A handler rejection raises `RejectedError`, no longer an `InvalidError`. `FenceViolationError` calls the held token `held`.
- A callback that raises an SDK exception keeps its class and fields across the SDK. `asyncio.CancelledError` becomes a non-retryable cancellation and the builtin `TimeoutError` a retryable timeout.
- `IdError` and `ProvenanceError` carry `kind` with class constants. Intent failures raise `IntentError` and envelope validity failures raise `ValidateError`, both with `kind`.
- `SendMessagesConfirmation` is `SendMessagesConfirmationResponse`, and `UnconfirmedMessage` is `IggyMessage`.

#### Connection and capabilities

- `Laser.with_stream(name)` is `Laser.with_default_stream(name)`.
- `Capabilities.query`, `kv`, `filters`, and `destinations` are `QueryCaps`, `KvCaps`, `FilterCaps`, and `DestinationCaps` objects. Read `caps.query.available`, `caps.query.consistency`, `caps.kv.cas`, `caps.kv.fenced_leases`, `caps.filters.catalog`, `caps.filters.evaluates(version, codec)`, and `caps.destinations.consistency`. The flat `query_consistency`, `kv_cas`, `filters_catalog`, and similar attributes and `Capabilities.evaluation` are gone.
- `BackendDescriptor.kind`, `version`, `ready`, and `observed_runtime_configuration_revision` are gone. Read `implementation`, `readiness`, and `runtime_configuration_revision`.

#### Messages and publishing

- `Message.message_id` is `Message.id`, a `MessageId` with `partition_id`, `offset`, and the `p:o` string form.
- `ConsumerMessage.offset` is gone. `ConsumerMessage.position` is a `MessageId`, so read `message.position.offset`.
- `new_correlation_id()` is `mint_ulid()`.
- `laser.topic(name, cls=Reading)` is `laser.topic(name).json(Reading)`. `TypedTopic.name` is gone.
- Publish builders name their argument like Rust: `json(body)`, `msgpack(body)`, `avro(body)`, `encode_with(body, ...)`, `payload(payload)`, `raw_bytes(payload)`, and `arrow_ipc(payload)`. Batch `add_*` methods follow, as do `Producer.send_batch(messages)` and `Topic.batch(messages)`. A `value=` keyword no longer works.
- `Topic.producer(background=True, background_shards=n)` is gone. `background=True` takes Apache Iggy's background defaults, and `background=BackgroundConfig(...)` sets shards, sharding, flush limits, the byte budget, in-flight writes, the failure mode, and an `error_callback`. Without a callback a failed background write is logged on the `laser_sdk` logger and dropped.
- `Producer.shutdown` waits for sends in flight instead of raising `InvalidError`. A second call returns at once.
- `Consumer.name` is gone. `Consumer.stored_offset(partition)` returns the stored offset, or `None`.
- Consumers default to `commit_interval_ms=0`. The default `auto_commit="polling"` now stores offsets on each poll only, without the extra one-second timer. `auto_commit="interval"` needs an explicit `commit_interval_ms` greater than zero.
- `Laser.consumed(position, *, group=, consumer=)` is `Laser.consumed(target, at)`. `target` is `ConsumerRef.Group(name)` or `ConsumerRef.Consumer(id)`, `at` is a `LogPosition`, and the result is `ConsumptionStatus.Consumed(committed, head)` or `ConsumptionStatus.NotYetConsumed(behind_by)` instead of a dict.
- A watch reader raises `UnsupportedError` when it is opened on a deployment without the change feed, not on each `poll()`.

#### Queries, projections, and schemas

- `QueryRequest.rows()` and `rows_typed()` return `QueryRows` and `TypedQueryRows` async iterators instead of awaitables that return a list. Write `async for row in request.max_rows(n).rows()`. Without `max_rows` they raise `InvalidError` at once, and `max_rows(0)` yields nothing without a round trip.
- `rows_typed`, `fetch_typed`, and `fetch_one` raise `ConfigError` for a row without its original payload and `ProtocolError` for a non-binary payload.
- `QueryFilter` is `Filter`.
- `QueryResult.offset`, `limit`, `total`, `has_more`, and `next_cursor` moved to `QueryResult.page`, a `Page`. Read `result.page.total`.
- `QueryRequest.to_dict()` is `QueryRequest.into_query()`.
- `Laser.register_schema(..)` is `laser.schemas().register(..)`.
- `Laser.list_projections(topics=, search=)` is `laser.projections().list(topics=, search=)`. `register_projection` and `apply_binding` are `laser.projections().register(..)` and `laser.bindings().apply(..)`.

#### Agents and runs

- `ls.Topics.COMMANDS` and the other `Topics` constants are `ls.AgentTopic.Commands`, `Responses`, `ToolCalls`, `ToolResults`, `LlmIo`, `HumanInput`, `Audit`, and `Dlq`.
- `AgentMessage.message_id` is `AgentMessage.id`, a `MessageId`. `AgentMessage.topic` is gone.
- `AgentMessage.conversation_id`, `agent`, `idempotency_key`, and `correlation_id` are gone. Read them on `message.provenance`.
- `AgentMessage.agdx_body` is gone. Call `message.body()` or read `message.envelope["body"]`. `AgentMessage.json()` is gone. Use `json.loads(message.payload)`.
- `Provenance.deadline_micros` is `Provenance.deadline`. `Provenance.input_tokens`, `output_tokens`, and `cost_usd` moved to `Provenance.usage`, an `LlmUsage` or `None`.
- `Laser.contract` and `AgentHandle.contract` return a `Contract`: `Contract.Completed(reply)`, `Contract.Failed(reply)`, `Contract.NotConsumed()`, or `Contract.TimedOut()`. They used to return the body or a dict. `Laser.contract_report` is gone.
- `contract` waits 30 seconds by default instead of 10, the same as Rust and TypeScript.
- `Laser.scatter_report` returns a `ScatterReport` with `outcomes`, `completed()`, and `failures()`.
- `AgentCtx.fan_out` returns a `Gather` with `ok`, `failures`, and `replies()`.
- `Workflow.run` returns a `WorkflowOutcome` with `outputs` and `run_id`.
- `AgentRegistry.agents`, `lookup`, and `resolve` return `RegisteredCard` objects with `agent`, `card`, and `observed_at_micros`. `AgentRegistry.card_is_fresh`, `card_serves`, and `card_available_for` are gone. Call `is_fresh`, `serves`, and `available_for` on the card.
- Route scorer callbacks receive `RouteCandidate` objects with `agent`, `card`, and `capability` instead of dicts.
- `Laser.client_metadata` returns a `ClientMetadataPage` with `clients` and `next_cursor`.
- `cause_at=` takes `ls.LogPosition(stream_id, topic_id, partition_id, offset)` instead of a tuple.
- `RunInfo` is `AgentRunInfo`. `Runs.list(agent_id=)` is `Runs.list(agent=)`.
- `ChunkAssembler.feed(envelope)` takes the envelope, either `message.envelope` or its encoded bytes, not the message. A finished event carries `usage` instead of `input_tokens` and `output_tokens`, and a failed event carries `body` instead of `bytes`.
- `Agdx.status(task_state=)` takes `ls.TaskState.Working` and the other `TaskState` values, not a string.
- `agent_event_is_understood()` is gone. Build the event with `event_envelope(..., requiring=)` and check `unmet_requirements`.
- Hooks take a plain callable or an object with the hook method (`handle`, `observe`, `on_dead_letter`, `consolidate`, `embed`, `rerank`, `summarize`, `build`, `verify`), each sync or async. Anything else raises `InvalidError` when the hook is registered. An async hook runs on the event loop of the call that reached it, or the registering loop for agent lanes, with the caller's context variables. A sync hook runs on an SDK worker thread in a copy of the caller's context, so it must not block or call event-loop APIs.
- The `spawn_agent` consolidator receives a scope dict naming the agent id, with the other keys `None`, instead of an empty dict.
- Middleware `after_handle(message, result, attempt)` receives a dict with `ok` and `error` in place of the old boolean `ok`.
- The dead-letter callback is `dead_letter(message, capsule, publish_error)`. It receives the complete capsule as a dict, and `publish_error` is a typed SDK exception when publishing the dead letter failed, otherwise `None`. The old form was `(message, reason, attempts, published)`.
- Asynchronous callbacks must let `asyncio.CancelledError` propagate. Shutdown cancels them.
- `Laser.a2a_bridge(...)` is `A2aBridge(laser, ...)` and `Laser.mcp_bridge(...)` is `McpBridge(laser, ...)`. `A2aBridge.submit(params_json)` and `McpBridge.call_tool(name, params_json)` take raw JSON.
- `A2aBridge` and `McpBridge` stamp `bridge_hops` and gain `with_bridge_hops(previous)`.

#### Governance and signing

- `ActionDecision.verdict` returns a `Verdict` instead of a string. Compare with `Verdict.block()` or read `verdict.as_str()`. `ActionDecision.scope` and `body` moved to `decision.verdict.scope` and `decision.verdict.body`.
- `ActionDecision.with_policy(pack_id, pack_version, rule_ids)` is `ActionDecision.with_policy(PolicyRef(pack_id, pack_version, rule_ids))`. `ActionDecision.policy` and `PolicyEvidence.policy` return a `PolicyRef` instead of a tuple.
- `GovernedAction.sends`, `requests`, and `bytes_sent` moved to `GovernedAction.counters`, an `ActionCounters`.
- `Laser.with_governor_retention(governor, mode, capacity=, idle_ttl_secs=)` is `Laser.with_governor_retention(governor, mode, GovernorRetention(capacity=, idle_ttl_secs=))`.
- `KeyRecord.verifying_key` is `KeyRecord.verifying`. `KeyRegistry.enroll`, `enroll_operator`, `KvKeyRegistry.enroll`, and `verify_card` name the key argument `verifying`.
- `KeyRegistry.verify_at` and `verify_observed_at` return a `VerifiedPrincipal` with `principal` and `kind` instead of a dict.
- `Grant(feature, action, *, effect, resource=ResourcePattern.prefix(...))` replaces the `resource_kind=` and `resource_value=` keywords. `AuthzEvent.op` is a dict.
- `authz_history_all`, `authz_history_role`, and `authz_history_binding` are one `laser.authz_history(target, after_revision=, limit=)`, where `target` is `"all"`, `{"role": name}`, or `{"binding": {"user_id": id}}`.
- `authorize_edge` returns an `EdgeDenial` or `None` instead of a tuple.

#### Memory and context

- The `Memory` class is `MemoryHandle`. `LogMemory`, `VectorMemory`, and `RerankedMemory` subclass it.
- `Laser.vector_memory(...)` is gone. Use `VectorMemory.governed(laser, embedder)`, or `VectorMemory(embedder)` without a connection.
- `Memory.to_context_block`, `Memory.content_id`, and `Memory.kind_class` are the module functions `to_context_block`, `memory_id_content`, and `memory_kind_class`. `memory_id_content` takes `user=` and `application=`.
- `MemoryHandle.backend_name` is `MemoryHandle.backend`. `forget` and `append` name their first argument `id`, and `improve` names it `target`, instead of `memory_id`.
- `MemoryItem.signals` returns `RecallSignal` objects with `strategy`, `rank`, and `score` instead of tuples. `MemoryItem.text()` is a method and decodes lossily like Rust. `MemoryItem.conversation_id` is gone. Read `item.provenance.conversation_id`.
- `MemoryHandler(inner, memory)` names its first argument `inner` instead of `handler`.
- `ContextScope.fetch` and `block` take `n=` instead of `last_n=`. `ContextScope.state` and `state_with` take `init` instead of `initial`, as do `Session.replay` and `state_at`.
- Context reads, folds, custom policies, and token estimators receive `ContextMessage` objects (`id`, `provenance`, `payload`, `envelope`, `topic`) instead of `AgentMessage`. `Laser.assemble_context` and `SessionTurn.message` return them too. `SessionTurn.payload` is gone. Read `turn.message.payload`.
- `laser.topic_snapshot_store(topic=)` and `laser.kv_snapshot_store(namespace=)` are `TopicSnapshotStore.on_topic(laser, topic)` and `KvSnapshotStore.in_namespace(laser, namespace)`.
- A blob store exception keeps its SDK class instead of becoming `CodecError`, and a wrong return type raises `ConfigError`.

#### Key-value, graph, forks, and filters

- `KvSetRequest.payload()` is gone. Use `bytes()`.
- `Kv.exists` returns a `KvMetadata` with `version`, `expires_at_micros`, and `size_bytes` instead of a tuple.
- `PreparedMutation.ambiguous_recovery` returns an `AmbiguousMutationRecovery` instead of a dict.
- `Kv.cas_fenced`, `copy_to`, and `move_to` return request builders. Awaiting the returned object still runs the call.
- `ForkHandle.fork_id` is `ForkHandle.id`.
- `graph_node` and `graph_edge` are `graph_node_entity` and `graph_edge_relate`. Graph nodes, edges, results, and `SourceRef` are dicts. `laser.graph(name).query(match_label=, hops=)` is the fluent `laser.graph(name).start_match(..).out(..).fetch()`.
- `ConsumerFilter.evaluate` and `explain` are gone. Use `CompiledFilter.compile(filter).evaluate(..)` and `explain(..)`.
- `FilterExpr.text` and `header_text` lost `case_insensitive=`. Chain `.case_insensitive()`.
- `ConsumerGroup.reader(start=)` takes `"next"`, `"first"`, `"last"`, `{"offset": n}`, or `{"timestamp": micros}`. The `start_offset=` and `start_timestamp_micros=` keywords are gone.
- `FilteredReader.owns(page)` takes a `MatchedPage`.
- `ConsumerGroup.create()` and `info()` return a `ConsumerGroupInfo` with `id`, `name`, `identity`, and `filter` instead of a dict.

### TypeScript

#### Errors

- A publish that gives up throws `PublishFailedError`, so an `instanceof TransportError` check around a publish no longer matches. `publishCause()`, or the free `publishCause(error)`, returns the original error, and classifiers such as `isRetryable` and `isPermissionDenied` answer for that cause.
- Every publish path throws `PublishFailedError`, including `sendAgent`, `request`, the AGDX verbs, memory writes, and `redriveDeadLetter`. Agent-level sends used to throw a bare `TransportError`.
- `LaserErrorKind` is no longer exported. Read `error.kind`, which adds `publish-failed`, `fence-violation`, `quarantined`, `no-respond-topic`, and `checkpoint`. An exhaustive `switch` needs the new cases.
- Id parse failures throw `IdError` and provenance header failures throw `ProvenanceError`, not `InvalidError` or `CodecError`.
- `RoutingError` and `RoutingErrorReason` are gone. Catch `NoCapableAgentError`, `NoInboxError`, or `RoutePrincipalMismatchError`, or use `isNoCapableAgent` and `isPermissionDenied`.
- `ProtocolError.resultCode`, `ProtocolError.commandCode`, and `TransportError.retryable` are internal. Use `iggyErrorCode()` and `isRetryable()`.
- `FilterStopError` is gone. A stop throws `FilterFaultError` or `FilterOversizedRecordError`. `FilterFaultError.faultReason` is `reason`.
- `FilterExecutionError.reason` and `ConsumerGroupSetupError.reason` are gone. Use `filterReason(error)`, or `filterReason(error.cause)` for a setup error.

#### Connection and capabilities

- `LaserBuilder.connect()` is async. A configuration error rejects the promise instead of throwing synchronously.
- `Laser.iggyClient` is `Laser.client` and `Laser.fromIggyClient` is `Laser.fromClient`. `fromClient` options keep only `ownership`. Set the default stream, capabilities, and observer with `withDefaultStream`, `withCapabilities`, and `withObserver`.
- `Laser.deadLetterTopic` and `withDeadLetterTopic` are `dlqTopic` and `withDlqTopic`. `LaserBuilder.iggyClient`, `defaultStream`, and `deadLetterTopic` are `client`, `stream`, and `dlqTopic`.
- `LaserBuilder.token`, `Laser.withVerifier`, and `Laser.policyEvidence` are gone. Pass credentials in the connection string, set the verifier on `LaserBuilder.verifier`, and read policy evidence from the audit topic.
- `QueryCapabilities`, `KvCapabilities`, `DestinationCapabilities`, and `FilterCapabilities` are `QueryCaps`, `KvCaps`, `DestinationCaps`, and `FilterCaps`. The hello `Feature` bit set is `feature`.
- `Capabilities.topology` is gone. `Capabilities.destinations.checkpointVersion` is gone, use `versions.checkpoint`. `destinations.consistency` is new.

#### Ids and codecs

- `AgentId.asString` and `ConsumerGroupName.asString` are `asStr`. `parseWireAgentId` is gone from the root. Use `AgentId.new(name).wireId()`.
- `jsonCodec(f)`, `cborCodec(f)`, and `messagePackCodec(f)` are `new Json(f)`, `new Cbor(f)`, and `new Msgpack(f)`. `ValueDecoder` is no longer exported, and a custom `Codec<T>` declares `contentType`.
- `CompiledSchema.encode` and `CompiledSchema.codec` are not public. Encode Avro with `encodeAvro(body)` and publish other schemas through `topic.schema(id, decode)`.
- `utf8` and `decodeUtf8` are no longer exported. Use `TextEncoder` and `TextDecoder`.
- `IggyHeaderValue` is `HeaderValue`. `BackgroundFailureMode` is no longer a named export.
- Wire `Value` kinds spell strings `"str"` instead of `"string"`, and integers past the signed 64-bit range decode as `"uint"`.
- `validateTypedValue`, `validateTypedValueAgainst`, and `canonicalSchemaBytes` are `typedValueValidateCanonical`, `typedValueValidateAgainst`, and `logicalSchemaCanonicalFingerprintBytes`.

#### Messages and publishing

- `ConsumerMessage.offset` is gone. Read `message.position.offset`. `ConsumedMessage` is gone, and `Cursor.poll()` and `stream()` yield `Message`, so read `message.id.offset`.
- `TypedRecord` has `position` only, without `partitionId` or `offset`.
- `ConsumerOptions.autoCommit` is gone. `commitPolicy: { kind: "disabled" }` replaces `autoCommit: false`.
- `Consumer.nextWithin(ms)` throws a typed `TimeoutError` instead of returning `null`.
- The native consumer polls with no wait by default (`pollIntervalMs` 0, was 250 ms), like Rust.
- `Topic.replay()` and `TypedTopic.records(readerName)` take no options. Size polls with `cursor.batch(n)` and `records.batch(n)`. `CursorOptions` is gone.
- `Topic.ensure` takes only the partition count. `TopicEnsureOptions` is gone.
- Topics created by `ensure`, bootstrap, and the agent registry never expire, like Rust. `ensure` no longer changes an existing topic's expiry.
- `PublishRequest.encode`, `messagePack`, and `metadata` are `encodeWith`, `msgpack`, and `header`. `BatchPublishRequest.metadata` and `addMessagePack` are `header` and `addMsgpack`.
- `Producer.flush` is no longer public. A background producer drains and reports a kept failure on `shutdown()`.
- `Producer.isBackground`, `PublishRequest.partition`, `Topic.sendRecords`, `partitionCount`, `tailOffsets`, and `streamName` are internal. To resend unconfirmed records with their ids, pass `PublishFailedError.unconfirmed` to `Topic.batch`.
- `UNLIMITED_TOPIC_SIZE`, `ProducerSendOptions`, `RawSendOptions`, `RecordSnapshot`, `TypedTopicKind`, and `TypedPollResult` are no longer exported.
- `Topic`, `Stream`, `Producer`, `Consumer`, `Cursor`, `QueryRequest`, `Runs`, `Projections`, `Schemas`, `Watch`, `Sessions`, `Session`, `ScopedMemory`, `MemoryHandle`, `TypedTopic`, and their builders have no public constructor. Obtain them from `Laser`, as in Rust.

#### Queries, projections, and key-value

- `QueryRequest.executionId(value)` is gone. `executionId()` returns the request's id.
- `QueryRequest.aggregateAs` is `aggAs`, `stdDev` is `stddev`, and the `byKey` alias is gone. Use `whereEq`.
- `kvEntryKeyString` is `kvEntryKeyStr`.
- `Kv.expire(key, ttlMicros, nowMicros?)` takes a relative lifetime, like Rust and Python. Use `expireAt(key, expiresAtMicros)` for an absolute time.
- `KvKeyRegistry.enroll(principal, verifyingKey)` enrolls an agent key, like Rust. Pass a `KeyRecord` to `enrollRecord`.
- `KeyRecord.verifyingKey` is `KeyRecord.verifying`.
- `Fork` is `ForkHandle`, and its `forkId` property is `id`.
- `Checkpoint.toJSON()` writes `{"per_topic": {...}}` with integer offsets, the shape Rust and Python write. A checkpoint saved by TypeScript 0.5 no longer loads, and invalid input throws `InvalidError`.
- `Checkpoint.capture`, `Checkpoint.empty()`, and `Checkpoint.topics` are gone. Use `contextCheckpoint(laser, topics)`.
- The filter catalog frames (`FilterRef`, `FilterMutation`, `FilterPage`, `FilterSummary`, `FilteredPage`, and the rest), `ExactDecimal`, `KvOutcome`, `ForkOutcome`, `GraphStart`, `Hop`, `GraphError`, `JsonValue`, and `DEFAULT_DECODE_LIMITS` left the root export. Import `wire` from `@laserdata/laser-sdk/full`.
- `ExactDecimal.fromDouble` is `fromF64`. `timestampFromText` and `timestampFromInteger` are `timestampFormatMicrosFromText` and `timestampFormatMicrosFromInteger`. `nextFilteredStart` is `wire.filteredPageNextStart`.
- `CompiledFilter.outcomeOf` is `evaluateWithFault`, and `needsHeaders` is `headerNeed`, which returns `"none"`, `"content_type"`, or `"all"`. `CompiledFilter.faultReason` is gone. Read `evaluateWithFault(...).fault`.
- `GroupFilter.revisions(page?, pageSize?)` takes positional arguments instead of an options object.
- `MatchedRecord` keeps the consumed record in `record.message`. Read `record.message.payload` and `record.message.headers` instead of `record.payload` and `record.headers`.

#### Agents and workflows

- `Agent.builder()...spawn(laser)` is `Agent.builder()...build().spawn(laser)`.
- `AgentContext` is `AgentCtx`, and the testing helper `agentContext` is `agentCtx`.
- `AgentBuilder.deadLetterSink` and `ReliableConsumerOptions.deadLetterSink` are `onDeadLetter`.
- `AgentBuilder.governor(policy, mode)` takes one `[policy, mode]` tuple. Set retention with `governorRetention(retention)`.
- `FULL_REPLAY`, `ANY_ROUTE_POLICY`, `ADVERTISED_INBOX_ROUTE`, `BEST_EFFORT`, `REQUIRE_ALL`, and `SERIAL_CONCURRENCY` are gone. Write `{ kind: "full" }`, `{ kind: "any" }`, `{ kind: "advertised" }`, `{ kind: "bestEffort" }`, `{ kind: "requireAll" }`, and `{ kind: "serial" }`. `DEFAULT_RETRY_POLICY` is gone, and the default stays `{ maxAttempts: 5, baseDelayMs: 200 }`.
- `new QuorumGovernor("all" | "any" | n)` takes `{ kind: "all" }`, `{ kind: "any" }`, or `{ kind: "at-least", required: n }`.
- `new Workflow(...)` is gone. Use `laser.workflow(name)`. `StepBuilder` is `StepHandle`, and `StepHandle.done()` is gone. `WorkflowVerifier` is `Verifier`.
- `AgentHandle`, `AgentRegistry`, `ClientMetadataRequest`, `AgentScope`, `ContextScope`, `ContractBuilder`, `ContextAssembler`, and `AgentCtx` have no public constructor. Get them from `Agent.spawn`, `Laser.registry`, `Laser.clientMetadata`, `Laser.agent`, `Laser.context`, `Laser.contract`, `ContextAssembler.builder()`, and the `agentCtx` testing helper.
- `laser.advertisePresence({ agent, inbox })` takes the wire `AgentPresence`. Write `laser.advertisePresence(newAgentPresence(agent.wireId(), inbox))`. `AgentPresenceInput` is gone.
- `AgentDefinition` is no longer exported. `AgentErrorCodeName`, `FilterLiteral`, and `GraphAttr` left the root export and live under `wire` in `@laserdata/laser-sdk/full`.
- Root `AgentCard` is the A2A Agent Card, which was `A2aAgentCard`. The AGDX registry card is `wire.AgentCard` in `@laserdata/laser-sdk/full`. `A2aTask` is `Task`.
- `A2aBridge.submitJson` is folded into `submit`, which takes bytes or a string as raw JSON. `McpBridge.callToolJson` is gone, and `callTool(name, bytes)` sends raw JSON. `McpContent.type` is `kind` on returned objects.
- `OPERATION_*`, `METADATA_BRIDGE_HOPS`, `WireConversationId`, `NodeId`, `Path`, `TokenUsage`, `RunBudget`, and `TaskStateName` left the root export. Import `wire` from `@laserdata/laser-sdk/full`.
- `signingInput`, `topologicalOrder`, `selectRoute`, `applyJsonPatch`, `envelopesToAgUi`, and the free AG-UI functions are no longer exported. `Laser.aguiEvents`, `publishStateSnapshot`, `publishStateDelta`, and `reconstructState` remain.

#### Memory, sessions, and context

- `laser.sessions({ stream, topics, memoryNamespace, contextTurns, contextTokens })` is `laser.sessions(new SessionConfig().stream(..).topic(kind, topic).memoryNamespace(..).contextTurns(..).contextTokens(..))`. `SessionOptions` is gone. The `SessionConfig` getters are `streamName`, `memoryNamespaceName`, `contextTurnBound`, `contextTokenBound`, and `topics()`.
- `ContextAssembler.builder(conversation)` is `ContextAssembler.builder().conversationId(conversation)`. `ContextChain` is `Chain`. `ContextScope.stateWith` takes `decodeState` last and defaults it to JSON.
- `LogMemory.set`, `fetch`, `fetchFolded`, `update`, and `remove` are `setNamed`, `fetchNamed`, `fetchNamedFolded`, `updateNamed`, and `forgetNamed`. `MemoryHandle` keeps the short names.
- `MemoryScope.application` is `MemoryScope.app`. Builders keep `.application()`.
- `memory.remember(..).conversation(c)` is `.scope(c)`. `memory.recall().conversation(c)` is `memory.recall(c)`. `RecallBuilder.tokenBudget(n)` is `block(n)`.
- `ScopedMemory.context(tokenBudget)` is `block(tokenBudget)`.
- `MemoryHandle.custom`, `log`, `logTopic`, and `logBackend` are internal. Use `laser.memoryCustom`, `laser.memory`, and `laser.memoryOnTopic`.
- `ConsolidationReport` has `summarized`, `reweighted`, `pruned`, and `derived`. Use `pruned` where you read `forgotten`. `scanned` and `kept` are gone.
- `Consolidator.consolidate(scope, signal?)` no longer receives a memory argument. The consolidator holds its own memory handle. The `AgentConsolidator` alias is gone.
- `ZeroEmbedder` is gone. `MemoryHandle.vector(embedder)` and `new VectorMemory(embedder)` require an embedder.
- A fork row's `embedding` takes `Iterable<number>` instead of a string.
- `TestClock` rejects values outside the unsigned 64-bit range, and `advance` wraps like Rust.
- Context assembly reads the newest 10,000 raw records per partition before it filters by conversation, the same window as Rust and Python. Earlier releases scanned whole topics and kept the newest 10,000 matching records. To reach older turns, end the read at an earlier checkpoint with `toCheckpoint`, which anchors the window there.

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
| Governor retention | `governor_retention(GovernorRetention)` | `spawn_agent(..., governor_retention=(capacity, idle_ttl_secs))` | `AgentBuilder.governorRetention(retention)` |
| Periodic memory | `consolidate_every(Duration)`, `consolidator(..)` | `consolidate_every_ms=`, `consolidator=` | `consolidateEvery(milliseconds)`, `consolidator` |
| Arrow fingerprint | `Record::logical_schema_fingerprint` | `BatchPublishRequest.add_record(logical_schema_fingerprint=)` | `Record.logicalSchemaFingerprint` |

Python send verbs take envelope refinements as keywords. A cause position requires a cause ID, and per-kind validation still applies. A claim check moves a body to the supplied store when it reaches the threshold. Request-input helpers and chunk writers do not sign records.

Python `Topic.cbor` and `Topic.schema` return typed handles. Their `publish(body)` requires a body, and one cached handle encodes and reads with a single registry lookup and compile. Plain `topic.publish()` stays the raw builder. Protobuf publication takes encoded bytes with a schema ID, while TypeScript can encode schema-backed bodies itself. Batch defaults fill a record's unset content type, projection, schema ID, and inline payload, and batch index entries and headers merge with the record's own, which win. An Arrow fingerprint must be 32 bytes and requires Arrow content.

`spawn_agent(None, ...)` in Python opens an unscoped reliable consumer. It needs a consumer group and cannot advertise capabilities. Capabilities accept skill names or descriptor dicts.

## Portable helpers and callbacks

The pure A2A and MCP converters keep the original request bytes and open no connection. Card signatures, delegation checks, claim checks, snapshot bytes, resume offsets, and reciprocal-rank fusion follow the same native contract in every client. Python exports them in snake_case, for example `encode_snapshot` and `decode_snapshot`, and TypeScript in camelCase, for example `encodeSnapshot` and `decodeSnapshot`. `SystemClock` and `TestClock` count epoch microseconds as unsigned 64-bit values.

Python workflow builders, verifiers, and compensation callbacks can return a value directly or through an awaitable. Objects with `build` or `verify` methods work too. A custom snapshot store implements `latest(conversation)` and `save(snapshot)`, and `state_with` accepts it directly or wrapped in `SnapshotStore`. Callback errors keep their SDK class, and cancellation retires the asynchronous work in flight.

A custom Python route policy is `None`, a built-in policy name, or a synchronous callable or object with a `select` method. The scorer receives `skill_id` and `RouteCandidate` objects with `agent`, `card`, and `capability`. It returns a candidate index, or `None` to refuse them all. An async scorer is rejected.
