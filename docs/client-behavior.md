# SDK 0.5.4 client behavior

Rust, Python, and TypeScript share the features and defaults in [the parity matrix](parity.md). The matrix covers inherent and trait methods, generated builder controls, and standalone SDK functions. Operation versions and conversation derivation remain at version 1. Naming and construction follow each language.

## Publication and reads

A failed publish reports confirmed ranges and unconfirmed records. Python exposes confirmed ranges and an unconfirmed count on the cause exception. Topic sends preserve transport failure details. Internal retries retain the attempted message IDs.

Batching producers keep running after a failed timer flush. Explicit flush and close drain before reporting collected failures. A send reports only its own inline failure. Inspect confirmed ranges before retrying.

Creation settings apply only to absent topics. Concurrent creation accepts an existing topic. Background sends acknowledge queue acceptance. Flush and shutdown drain the queue. TypeScript error callbacks can return promises. Failed notifications retain the original publish failure.

Live waits return typed timeouts. TypeScript retains an unfinished poll so the next call can receive its late record. Cancellation does not acknowledge an undelivered record. A zero query row ceiling performs no query. Query results, pages, status, and cancellation remain bound to the requested execution ID.

TypeScript MessagePack uses named maps and the shortest integer encoding. It rejects integers outside the supported signed and unsigned 64-bit ranges. Caller-supplied encoded bodies remain opaque bytes.

## Coordination and ownership

All clients expose a two-phase fenced-lease client. Prepare an acquisition, renewal, release, or fenced compare-and-swap once. Repeat its prepared object only when the recovery action permits it. The object retains its operation ID and exact framed bytes.

The default attempt timeout is 10 seconds. Rust uses Duration, Python uses seconds, and TypeScript uses milliseconds. An uncertain mutation retires the connection and reports operation-specific recovery. An uncertain acquisition requires waiting through its requested lifetime. Renew and release can repeat the prepared object. A fenced write requires target reconciliation.

The low-level client reports recovery instead of performing the acquisition wait. Ordinary Kv lease calls perform that wait. Reads return plain timeouts. Readiness and request validation failures occur before mutation.

Close is terminal. Reset retires a transport connection and permits reuse. Dedicated readiness waits for retirement. A closed Laser cannot reopen its coordination connection.

Keep the Rust agent handle until shutdown or join. Dropping it signals graceful worker shutdown and stops consolidation. Python uses the same ownership rule. TypeScript supports asynchronous disposal. Shutdown grace limits pending SDK work. Application callbacks must honor cancellation.

Periodic consolidation needs an interval and a consolidator. The callback owns its memory and receives a scope. Pass failures do not stop later ticks. Join keeps consolidation active until the worker exits.

## Memory and context

ConsolidationReport contains summarized, reweighted, pruned, and derived counts. Default pruning keeps newest IDs within a pass of at most 10,000 items and counts successful removals. Built-in summarizers write Summary items with a durable scope. Log storage does not persist a separate lifetime field. Optional pruning removes the covered messages.

MemoryHandler remembers successful turns under their incoming conversation and agent. Remembering is disabled until auto_remember selects a kind. A failed memory copy does not repeat the handler.

Lifetime remains write/custom-backend metadata. Built-in recall filters identity fields rather than lifetime. Reranked handles preserve explicit kinds and content IDs. Explicit custom backends retain the custom callback contract. They do not gain named-item operations from their underlying implementation.

Context assembly supports custom policies, ancestry, start offsets, and checkpoints. Each partition read examines at most 10,000 raw records before conversation filtering. A checkpoint overrides raw start offsets. Custom selection retains source topics.

Python exposes full memory scopes and queries through keywords. Memory.to_context_block formats exact recall results in their supplied order. It retains the first item even when that item exceeds the estimate.

## API spellings

| Feature | Python | TypeScript |
| --- | --- | --- |
| Prepared coordination | FencedLeaseClient.connect_dedicated, prepare_acquire, acquire | FencedLeaseClient.connectDedicated, prepareAcquire, acquire |
| Attempt timeout | with_attempt_timeout(seconds) | withAttemptTimeout(milliseconds) |
| Recovery | PreparedMutation.ambiguous_recovery | PreparedMutation.ambiguousRecovery |
| Summaries | consolidate(..., summarizer=, prune_summarized=) | consolidate(scope, maxItems, { summarizer, pruneSummarized }) |
| Content helpers | Memory.content_id, Memory.kind_class | MemoryId.content, memoryClass |
| Card predicates | AgentRegistry.card_is_fresh, card_serves, card_available_for | cardIsFresh, cardServes, cardAvailableFor |
| Cause position | cause_at=(stream_id, topic_id, partition_id, offset) | withCause(record, position) |
| Agent route | spawn_agent(..., fixed_inbox=) | AgentBuilder.inboxRoute |
| Governor retention | governor_retention=(capacity, idle_ttl_secs) | AgentBuilder.governor(governor, mode, retention) |
| Periodic memory | consolidate_every_ms=, consolidator= | consolidateEvery(milliseconds), consolidator |
| Raw fingerprint | BatchPublishRequest.add_record(logical_schema_fingerprint=) | Record.logicalSchemaFingerprint |

Python send verbs accept envelope refinements as keywords. A cause position requires a cause ID. Per-kind validation still applies. Claim checks use the supplied store at or above the threshold. Request-input helpers and chunk writers do not sign records.

Python CBOR and schema publication requires a body. One cached typed handle performs encoding and reading with one registry lookup and compile. Plain topic publication remains the raw path. Per-record metadata does not inherit batch defaults. An Arrow fingerprint requires 32 bytes and Arrow content.

## Portable helpers and callbacks

Pure A2A and MCP converters preserve original request bytes without opening a connection. Card signatures, delegation verification, claim checks, snapshot bytes, resume offsets, and reciprocal-rank fusion use the same native contract. Python exports snake_case helpers. TypeScript exports camelCase helpers. Snapshot byte helpers use encode_snapshot/decode_snapshot in Python and encodeSnapshot/decodeSnapshot in TypeScript. SystemClock and TestClock use epoch microseconds and unsigned 64-bit values.

Python workflow builders, verifiers, and compensation callbacks can return directly or through an awaitable. Objects with build or verify methods work too. Custom snapshot stores implement latest(conversation) and save(snapshot). state_with accepts the object directly or a SnapshotStore wrapper. Callback errors retain their SDK class and cancellation retires active asynchronous work.

Custom Python route policies accept None, a built-in word, or a synchronous callable/select object. A scorer receives skill_id and candidates with agent, card, and capability fields. Return an index or None to refuse all candidates. An async scorer is rejected.

## Migration

TypeScript nextWithin returns a message or throws a typed timeout. Replace null timeout handling with exception handling. Kv.expire takes a lifetime. expireAt takes an absolute timestamp. Fork embeddings take numeric vectors.

ConsolidationReport uses the four shared count names. Replace forgotten with pruned. The old scanned and kept aliases are removed. Consolidators own their backend and receive a scope.

Retain Rust agent handles. A discarded handle stops its worker. Use shutdown to observe drain failures. TypeScript low-level consumers expose shutdownGraceMs. Python callbacks must propagate asyncio.CancelledError.

Update Python middleware after_handle(message, result, attempt) to inspect result["ok"] and result["error"]. Update dead-letter callbacks to dead_letter(message, capsule, publish_error). They receive the complete capsule and typed error. TestClock inputs must fit u64, and advance wraps like Rust.

Python spawn_agent accepts a named identity or None for an unscoped reliable consumer. An unscoped consumer requires a group and cannot advertise capabilities. Capability input accepts names or descriptor dictionaries.

Use plain topic.publish() for raw builders. Use typed_topic.publish(body) for CBOR or schema publication. Protobuf publication uses encoded bytes with a schema ID. TypeScript can encode schema-backed bodies.

The old examples are replaced by fleet-tape and incident-desk. Fixture payloads use neutral systems examples. Downstream fixture consumers must embed the matching corpus.
