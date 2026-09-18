# Agent Data Exchange Protocol (AGDX)

Home: [agdxprotocol.ai](https://agdxprotocol.ai)

AGDX defines how agents and services exchange data over a durable log. A substrate is the system that stores and transports records. The protocol separates the data contract from the substrate binding. A binding maps the contract to a specific system. Streaming records, queryable views, and working state share the contract and connection. Agents and conventional services use the same data operations.

AGDX defines the records and data operations used by agents and services. A2A, MCP, and AG-UI provide external interfaces through bridges. Internal participants continue to use the log, while external clients use their own protocols. The [edge interoperability guide](interop.md) describes these mappings.

Part A defines the core model. Part B defines bindings, including the normative Apache Iggy binding, a Kafka portability example, and HTTP management. Part C records design decisions and planned work. The appendix records the SDK API policy.

Core requirements apply across substrates. Transport-specific header keys, command codes, byte order, and frame layouts belong to a binding.

## In brief

AGDX gives agents and services one connection for data exchange. The log stores the source records. Read models organize retained records for queries and state operations. Three groups of operations share the connection:

- Streaming appends typed records to topics and reads them by offset.
- Materialized views store projections of topic records for queries.
- Working state provides key-value records and copy-on-write forks for coordination.

Agent commands, responses, token streams, status, and errors use a typed CBOR envelope on the same log (A9). A binding maps the model to Apache Iggy (B1), Kafka, or another log (B2). The logical model does not depend on one substrate.

```
   edges      agents   .   services   .   edge bridges (A2A / MCP / AG-UI)
                                          |
                                   one connection
                                          |
   +=========================================================================+
   | fabric     agent envelope, runtime, coordination, memory (A9, A13)      |
   +=========================================================================+
   | platform   streaming        materialized views      working state       |
   |            (the log)        (projections, query)    (key-value, forks)  |
   +=========================================================================+
   | wire       the typed contract: envelopes, dictionaries, caps, fixtures  |
   +=========================================================================+
                                          |
                                   binding (Part B)
                                          |
   substrate            durable, partitioned, replayable log
                          Apache Iggy today, others possible
```

The substrate stores the log. The wire layer defines portable data types. The platform supplies streaming, views, and state. The fabric adds agent messages, coordination, and memory. Edges expose A2A, MCP, and AG-UI interfaces. An application can use each layer without adopting the layers above it.

Read Part A for the common model, Part B for bindings, and Part C for design decisions and planned work.

## 0. Status and conventions

This specification describes the current design. The repository is pre-1.0 and permits breaking changes. A contract change must update its implementations, specifications, and reference test data together.

- The normative sections define the current protocol. Roadmap sections describe proposals that are not part of the contract. Add reference test data when a proposal becomes part of the contract.
- Field tables give the logical type. The byte form is named-field CBOR (A2) unless a binding says otherwise.
- Requirement words retain their defined force. Required behavior uses must or must not. Recommendations and permitted behavior remain distinct.
- Component names appear in prose. File paths do not.
- The contract uses the `agdx` namespace for interoperable data. Bindings identify fixed names and names that deployments can configure. Apache Iggy and Kafka appear in binding chapters. OpenTelemetry keys retain the `gen_ai.` namespace.

---

# Part A. The core (substrate-neutral)

Part A defines the shared data model. It assumes an append-only log with partitions, offsets, replay, and key-based ordering. Records carry attributes alongside their bodies. The substrate also provides request and reply operations or topic pairs that can implement them.

## A1. Overview

### A1.1 What the protocol provides

An implementation provides three things over one authenticated connection.

1. Typed, batched message streaming over a durable, partitioned, replayable log.
2. A general data surface on that log: declared projections with a query DSL, a key-value store, and copy-on-write forks of the materialized read model.
3. An optional agentic layer: a reliable runtime and a typed agent envelope on the streaming layer.

The three groups of operations share one specification and implementation contract. The agent envelope is a streaming payload within that model. Reference test data covers the data types and their versions.

The log stores the source records. Projections, key-value state, and coordination derive from those records. Deployments manage the resulting read models.

The contract uses five layer names. Substrate means the durable log. Wire means the portable types, codes, envelopes, dictionaries, limits, and reference test data. Platform means streaming, projections, queries, key-value state, and forks. Fabric means agent envelopes, runtime behavior, coordination, and memory. Edges means the A2A, MCP, and AG-UI bridges.

### A1.2 The thesis: specify the data, bind the transport

Transport details can change without changing the logical data model. The specification separates framing, attributes, and payloads. The substrate owns framing. The core defines the logical attributes and payloads.

| Layer | Owner | Content |
| --- | --- | --- |
| 1. Transport framing | the substrate | byte delimitation, segmentation, the substrate's own message or command frame |
| 2. Out-of-band metadata | the core names which attributes, the binding says how they ride | the attributes a reader or router acts on without decoding the body |
| 3. Payload | the core | the typed, versioned, named-field CBOR object or envelope, byte-identical everywhere |

> An attribute belongs outside the body only when a reader or router must act on it before decoding the body. Each binding defines where these attributes are stored.

### A1.3 The surfaces: a data platform on a log

The log stores source records. Views and working state derive from these records. Three groups of operations share one connection.

| Surface | What it is | Nature |
| --- | --- | --- |
| Streaming | the log itself and the agent envelope | the foundation: append a record, read it back by offset, pull not push (A1.5) |
| Materialized views | projections and the query DSL | read models declared per topic, queried like a database index |
| Working state | key-value and copy-on-write forks | mutable state and speculative branches, addressed by key |

Streaming records support the other operations. AGDX also lets a client query a view, coordinate through shared state, or create a branch of that state.

### A1.4 What the core does not specify

The substrate owns connection negotiation, flow control, keepalive, and connection sharing. It also supplies ordering, retention, offsets, consumer groups, and pull-based backpressure. AGDX must not duplicate these mechanisms.

### A1.5 The streaming layer is a log, not a queue

The streaming layer stores records for replay. Views, working state, and agent coordination derive from the log. Their behavior follows log offsets and retention.

The substrate provides two stream operations: append a record and read records from an offset. Consumers request records at their own pace. This pull model controls the incoming workload. The server does not push records to a consumer.

The protocol uses the following delivery rules:

- Delivery is at least once, with replay by offset. Ordering is total within a partition. Agent records use the conversation ID as their partition key. Other records use the selected partitioning.
- An acknowledgment stores the consumer offset. Commit after processing to retain at-least-once behavior. A restarted reader resumes from its stored offset.
- Consumer deduplication tracks business keys to suppress repeats. The protocol does not guarantee exactly-once external effects.
- After retry exhaustion, the runtime publishes a record to a dead-letter topic. The record describes the failure and source message.

The following queue concepts either do not apply or use existing offset and replay operations:

| Broker or queue primitive | Why it does not apply on a log |
| --- | --- |
| server-push delivery (a DELIVER verb) | the log is pull. Consumers poll and replay by offset |
| ack and nack as settle verbs | acknowledgement is an offset commit, consumer-side. There is no negative-ack or requeue. A consumer does not advance its offset, or re-reads |
| delivery-mode negotiation (at-most / at-least / exactly-once) | the log is at-least-once with replay by construction. Exactly-once is consumer dedup, not a selectable mode |
| redelivery count, visibility timeout, ack deadline | queue bookkeeping. On a log, retry is the reliable consumer's local policy over re-read offsets |
| broker-managed dead-letter queue | dead-lettering is a runtime convention (a capsule on a DLQ topic), not a managed queue |
| subscribe versus consume as two modes | one primitive: read a topic from an offset under a consumer group |
| message priority | a log is ordered by offset within a partition, not reorderable by priority |

Logs, metrics, traces, and events can use ordinary topic records with OpenTelemetry-aligned metadata. Projections can organize those records into trace views. This convention adds no operation codes.

The SDK creates spans for its methods and runtime loops under the `laser` target. Frequent operations use `debug`, while lifecycle events use `info`. Span fields follow the same names as record metadata. This mapping lets an OpenTelemetry subscriber connect client spans with traces derived from records:

| Span field | Header key | Envelope field |
| --- | --- | --- |
| `conversation` | `gen_ai.conversation.id` | `AgentEnvelope.conversation` |
| `correlation` | `agdx.corr` | `AgentEnvelope.correlation` |
| `agent` | `gen_ai.agent.id` | `AgentEnvelope.source` |
| `topic` / `index` | (the address, not a header) | the topic the record rides / the materialized index queried |
| `operation` | (envelope-only, no header) | `AgentEnvelope.operation` (client verbs outside the envelope use the verb name: `publish`, `poll`, `send`, `ask`, `handle`, `contract`, `workflow`, `managed`) |
| `code` | (managed calls only) | the command code of the managed operation |

The SDK exposes spans through `tracing`. The deployment supplies a subscriber that exports them to OpenTelemetry.

A2A, MCP, and AG-UI bridges use publication, offset replay, and reply correlation. Candidate ATP and LangChain-streaming bridges use the same model. A stream resumes by offset, and a task reply matches its correlation ID. The [edge interoperability guide](interop.md) describes these mappings.

The MCP and A2A edges apply the MCP-2025-11 authorization model. They accept only tokens issued for their audience. They do not forward incoming tokens to the log or upstream services. When a caller lacks a scope, the edge returns `403` and identifies the required scope. Internal log access uses the authenticated identity supplied by the server (B1.4).

## A2. Encoding rules

- Every payload uses named-field CBOR (RFC 8949). One encoding entry point keeps the format consistent.
- Encode fields in declaration order. Omit unused optional fields. Decoders can ignore unknown fields when the contract permits it.
- A payload must contain exactly one CBOR item. Reject trailing bytes and malformed known fields. Ignore only fields that the contract permits readers to ignore.
- Encode machine IDs as fixed-width 16-byte CBOR byte strings. Do not encode them as tagged large integers.
- Signing input starts with `agdx.signature.v1`, then an encoded `SignatureContext` when present, then the envelope with its signature cleared (A9.5). Declaration order and omitted optional fields make this encoding deterministic. Reference test data fixes the expected signing bytes.

Each binding defines message boundaries and request-reply transport (B1.4).

## A3. The data object and identity

Every operation acts on a data object.

| Component | Meaning |
| --- | --- |
| Identity | the (namespace, collection, key) triple for stored objects, or the (namespace, topic, offset) tuple for stream records |
| Metadata | content-type, schema reference, version, expiry, timestamp, causality, an attribute map |
| Value | the opaque body, codec per content-type |

Logical identity is core. Its mapping to a physical address is binding-owned (B1.1, B2).

Id types:

| Type | Form | Notes |
| --- | --- | --- |
| record / conversation / correlation / channel id | u128 | rides the payload as 16 big-endian bytes (17 on the wire with the CBOR byte-string head). Display form is 26-character Crockford base32. The routing-header duplicate uses the substrate's typed 128-bit value, byte order per binding. Generation is SDK-side, never in the wire crate |
| log position | an opaque, binding-defined byte string | the locator half of a causal pointer, deployment-local. The one substrate-shaped slot in the envelope, opaque so it stays binding-neutral. The Iggy binding packs its four-level address (stream, topic, partition, offset). A Kafka binding packs its own (topic name or UUID, partition, offset). A consumer that cannot interpret it falls back to `cause` (C1.2) |
| agent id | bounded UTF-8 name, non-empty, at most 256 bytes, no ASCII control characters | a named principal (A2A name or URL, MCP server name, OTel agent id) |
| idempotency key | non-empty UTF-8, at most 64 bytes | a readable business key |

A record ID remains stable when a record moves to another partition or recovery cluster. Its log position changes.

## A4. The out-of-band attribute set

The core defines which attributes must be carriable out of band and what they mean. It does not define how they are encoded.

| Attribute | Type | Why out of band |
| --- | --- | --- |
| wire version | u32 | a long-lived reader selects its decoder before decoding |
| content-type | u8 code | a consumer chooses the codec before parsing |
| agent routing / ordering key | 128-bit | the substrate partitions an agent conversation without reading the body. Generic streaming may use another binding-supported key or partitioning mode |
| operation identity | name or code | a request is dispatched without a parse |
| correlation | 128-bit | a reply is matched and a stream is filtered cheaply |
| fence token | u64, optional | a consumer rejects a stale-holder replay before the effect runs, without reading the body |

### A4.1 Trusted versus advisory fields

An out-of-band field is an attribute stored outside the body. A trusted field has evidence that establishes its claim. An advisory field is a claim supplied by a writer. It can help display or correlate records, but cannot establish access rights or billing facts alone.

A deployment selects a security profile from B1.1. A valid principal signature can establish authorship on a shared topic. Exclusive write access can establish authorship through topic permissions. Unsigned records on a shared topic remain advisory. The table identifies the evidence for each field:

| Field | Carrier | Who can write it | What verifies it | Bill or audit on it |
| --- | --- | --- | --- | --- |
| conversation, agent (`source`) | envelope, `gen_ai.*` headers | any topic writer | nothing when unsigned, a verified envelope proves the enrolled principal (A9.5), while an ACL-bound write-exclusive topic proves the allowed producer identity | only under the signed-principal or topology-isolated profile in B1.1 |
| usage, cost (`gen_ai.usage.*`) | envelope `usage`, headers | any topic writer | nothing | no, advisory accounting only |
| idempotency key, correlation | envelope, headers | producer | nothing, used for matching and dedup, not as a claim | no |
| content-type (`agdx.ct`), wire version (`agdx.av`) | headers, out of band | producer, and any intermediary on relay | covered by the signature when the signer binds a `SignatureContext` (A9.5), so a hop that flips codec or decoder selection invalidates it, advisory otherwise | only under a signature whose context binds them |
| routing / ordering key | header | producer | nothing | no |
| fence token | envelope, header | the store-issued fence sequence | the store rejects a stale holder before the effect runs (A10.3) | yes, the store enforces it |
| signature | envelope capsule | the holder of the enrolled principal's key | SDK-side verification against the key registry, bound to the server-stamped principal (A9.5) | yes, this is the trust anchor |
| roles, grants | governance band | `authz:admin` or near-root server management (B1.4) | journalled and enforced by the streaming server | yes |
| policy context (`purpose`, `data_classification`, `task_context`, `session_intent`) | envelope metadata (A9.6) | any topic writer | nothing when unsigned, inside the signed envelope span when the producer signs (A9.5) | only under a signed envelope. A governance hook (C3) may still key an advisory decision on them, as defense in depth, never as the access boundary |

## A5. The operation registry

The core registry names operations and defines their meaning. Each binding maps those names to its dispatch mechanism.

| Op id | Surface | Semantics | Status |
| --- | --- | --- | --- |
| `hello` | control | capability and version probe |  |
| `authz.whoami` | governance | the caller's own effective capabilities (roles + flattened grants), answered to any authenticated caller |  |
| `authz.list_roles` / `get_role` / `get_bindings` | governance | browse roles and a user's bound role set |  |
| `authz.define_role` / `delete_role` / `bind_roles` | governance | define/replace, delete, and bind roles, journalled. Role names must pass `validate_role_name` (64-byte charset safelist, B1.4). `bind_roles` takes an optional `expect_revision` compare-and-swap precondition (a mismatch is a `conflict`). Requires `authz:admin` or the near-root server-management permission (B1.4) |  |
| `authz.history` | governance | read the authorization change log for a role, a user's bindings, or all, paged by revision (who granted what, when). Requires `authz:read` |  |
| `query` | views | run a query IR, return rows or aggregates |  |
| `query.page` / `cancel` / `status` | views | continue one execution from an opaque cursor, request cancellation, or inspect execution state |  |
| `checkpoint.mutate` | views | apply one revision-guarded public destination or query-route mutation. Replicated worker transitions are a separate internal envelope |  |
| `destination.get` / `list` | views | read destination declarations and checkpoint state at an explicit read consistency |  |
| `query_route.list` | views | read explicit operational and lakehouse query routes |  |
| `registry.get_projection` / `list_projections` | views | browse projections |  |
| `registry.get_schema` / `list_schemas` | views | browse writer schemas |  |
| `registry.register_schema` | views | allocate a writer-schema id |  |
| `registry.decode_record` | views | decode a body under a registered schema |  |
| `kv.get` / `set` / `delete` / `delete_many` / `scan` / `namespaces` | state | key-value operations |  |
| `kv.copy` / `move` | state | copy the value at one key to another key (optionally across namespaces) in one backend transaction. Move is copy plus delete of the source. `Committed` on success, `NotFound` when the source is absent, destination overwritten (a guarded copy composes `exists` + `cas`) |  |
| `kv.cas` | state | conditional set on a version token |  |
| `kv.cas_fenced` | state | conditional set applied in one transaction with a live-lease check at the request's coordination namespace/key and a fence-sequence match (A10.3) |  |
| `kv.exists` / `expire` / `patch` | state | metadata probe, in-place expiry, merge patch (formal object primitives, C6) |  |
| `kv.lease` / `lease_renew` / `release` | state | revocable holder-scoped lease on a key: acquire bumps the fence, renew extends without bumping, release validates holder and fence (A10.4, unsupported error when the backend cannot serve it) |  |
| `fork.create` / `delete` / `promote` / `list` / `put` | state | copy-on-write branch operations |  |
| `graph.query` / `neighbors` / `upsert` | views | knowledge-graph traversal, one-hop neighbors, node/edge upsert (A13). The graph name is at most 128 B, non-empty, control-character-free (`validate_graph_name`), enforced by the SDK edge and the serving plane |  |
| `batch` | control | the mixed-operation batch: up to `MAX_BATCH_OPS` (64) managed requests in one round trip, each item carrying its own command code and encoded request, each result that op's own reply bytes in order. Amortizes the round trip and nothing else: items execute independently, a failed item fails alone (explicitly NOT atomic), a nested batch is rejected. An old backend answers the unknown code with the surface-agnostic `CommandError`, decoded client-side as the typed unsupported, so no capability bit is needed |  |
| `agent.submit` / `cancel` / `status` / `list` | coordination | the run registry: submit records intent and mints the run identity (content-addressed, so a retried submit converges), delivery stays the envelope the SDK publishes, transitions are folded from the status records a registered run stamps with the `run` metadata key (A9.6), cancel records an intent flag the engine observes at a step boundary. `submit` MAY carry a multi-dimensional `RunBudget` (events, model calls, tool calls, patches, recursion depth, wall-clock, cost) the run fold accumulates, failing the run when a cap is crossed. It is a governance governor, not a grant. A managed read model over the log, never a second source of truth |  |
| change feed (no request op) | views | change notification over the read model: a projection binding opts in with `notify`, the projector publishes one change record per committed batch on the changes channel (A11.8, B1.1), and a consumer reads it by offset like any topic. Gated by the `watch` feature bit (A12), it adds no request op, so there is no `watch`/`unwatch` verb to register |  |

The memory API uses `remember`, `recall`, `improve`, and `forget`. These SDK methods combine `publish`, `query`, and `graph` operations (A13). They do not add wire operation codes.

Streaming uses substrate append and offset-read operations (A1.5). Agent messages also use six envelope kinds (A9). The envelope identifies the message kind. The registry adds no subscribe, consume-mode, ack, nack, or deliver operation.

## A6. Dictionaries (pinned codes)

Code dictionaries use fixed small integers. Existing codes must not be renumbered. An unknown code decodes to a value that preserves the original number for forwarding.

Content-type:

```
raw=0  json=1  msgpack=2  cbor=3  bson=4  avro=5  protobuf=6  arrow=7  ref=8  any=255
```

`ref` marks the body as a claim-check capsule (A9.5). `any` is a best-effort sentinel.

Task state, the agentic lifecycle, A2A-aligned:

```
submitted=1  working=2  input-required=3  completed=4  canceled=5
failed=6  rejected=7  auth-required=8  unknown=9
```

Terminal set: completed, canceled, failed, rejected.

Agentic error code, the `error` body discriminator:

```
invalid_request=1  unauthorized=2  unsupported=3  deadline_exceeded=4
cancelled=5  tool_failure=6  internal=7
```

Dead-letter reason:

```
retry_exhausted=1  rejected=2  decode_failed=3  deadline_exceeded=4
```

A query selects a `Consistency` level. The enum uses these snake-case strings:

```
eventual  read_your_writes  strong
```

`eventual` is the default and is omitted from the encoded request. `bounded_staleness` and `linearizable` are reserved names without defined behavior. If a substrate cannot satisfy the requested level, it must return `stale` or unsupported. It must not return a weaker guarantee as success.

## A7. The unified result-code space

`ResultCode` classifies outcomes across managed operations. Each operation group also defines errors with more specific details. A client can use the shared code without parsing error text. The numeric codes and their HTTP mappings form a shared contract.

| Code | Numeric | HTTP | Meaning |
| --- | --- | --- | --- |
| ok | 0 | 200 | success (no error to classify) |
| unsupported | 1 | 501 | the op, or the managed surface, is not available here |
| not_found | 2 | 404 | a named index, fork, key, destination, or route does not exist |
| invalid_argument | 3 | 400 | malformed request or an out-of-range field |
| too_large | 4 | 413 | a result or value exceeded a cap |
| conflict | 5 | 409 | a precondition lost a race (compare-and-swap mismatch, fork conflict) |
| stale | 6 | 503 | a consistency level could not be met in time (the read model is catching up) |
| version_skew | 7 | 400 | the wire op version is not accepted |
| unauthenticated | 8 | 401 | no credential, or an invalid one: the caller is not authenticated |
| backend | 9 | 502 | the managed backend failed or was unreachable |
| forbidden | 10 | 403 | authenticated, but the grant needed for the operation is missing |
| step_up_required | 11 | 403 | authenticated, but a stronger authentication step is needed |
| unavailable | 12 | 503 | the operation may succeed later without changing the request |
| resource_limit | 13 | 429 | the query exceeded a server-enforced resource budget |
| cancelled | 14 | 409 | the query was cancelled through its execution identity |
| deadline_exceeded | 15 | 408 | the query did not finish before its absolute deadline |
| expired_snapshot | 16 | 410 | the requested historical snapshot is no longer retained |
| stale_generation | 17 | 409 | the requested destination or backend generation is no longer current |
| target_unavailable | 18 | 503 | the resolved target is not ready to serve the request |

An unknown result code decodes as `unrecognized(code)` and retains its original bytes when re-encoded. Each binding defines how to carry the logical result. HTTP uses the status mapping in B4.

`CommandError` contains `{ code, message }`. A server uses it when it does not handle a command and cannot select a reply type for that operation. The client first attempts to decode the expected reply. If that fails, it attempts `CommandError` and returns the typed result. These formats are distinct, so the fallback does not reinterpret a valid reply.

The key-value, fork, and agent-workflow error enums define `NotLeader`. The SDK marks this error as retryable and `not_leader`. A caller can find the current owner and retry. Servers do not yet emit this variant. Its externally tagged encoding needs a coordinated rollout and capability selection before servers start emitting it.

## A8. Versioning, causality, idempotency, expiry, consistency

- A durable record carries its wire version outside the body so a reader can choose a decoder first (A4). Managed operations also negotiate versions during connection setup (A12). The agent envelope can require individual features through its must-understand marker (A9.1).
- A causal link identifies a parent record and can include its log position (A9.1). A token for ordering events across regions remains a proposal (C3).
- Idempotency makes a repeated operation produce no extra effect. Its business key (A3) is scoped to the authenticated identity.
- Expiry is an absolute epoch-microsecond time. An expired object reads as absent.
- A query selects `eventual`, `read_your_writes`, or `strong` through `Consistency`. If the requested level cannot be met, return `stale` rather than a weaker result (A11.3, A11.4).
- A fence is an increasing token that identifies the current holder. The lease grant supplies a per-task fence (A10.3), carried outside the body (A4). Fenced compare-and-swap protects key-value effects. Consumers reject log records with a fence below the highest accepted token for the task. Apply this rule before duplicate suppression.

Wire compatibility rules for the named-field CBOR encoding:

- Decoders can ignore unknown fields and use defaults for absent optional fields. Optional additions are safe only when ignoring them preserves the requested behavior. If a field changes service behavior, require a reported capability before using it (A12). This rule applies to requested `consistency`.
- An externally tagged enum rejects unknown variants. If a server can send a new variant without an explicit request, change that operation group version. A reply variant requested only by clients that support it does not reach older clients. `query.stale`, `kv.committed`, and `kv.version-conflict` use this request-specific form.
- The u8 dictionaries preserve unknown values. Existing content-type, task-state, error, and dead-letter codes must not change. Added codes retain their numeric value when forwarded.

## A9. The streaming layer: the agent envelope

Each agent message contains one named-field CBOR envelope. It is a streaming payload in the shared data model. Its version is 1 and is carried outside the body (A4).

This example pairs a `command` with its `response`. The encoded form is CBOR. The example displays 16-byte IDs in base32:

```
command {
  kind:         "command",
  record:       "01J9Z3K8Q8M4...",   // producer-assigned id
  conversation: "01J9Z3J0ABCD...",   // ordering unit, trace id, partition key
  source:       "planner",           // producing agent (a claim, not enforced identity)
  correlation:  "01J9Z3KPZ9...",     // pairs this request with its reply
  operation:    "execute_tool",
  tool:         "search",
  body:         <bytes>              // params, codec per the content-type attribute
}

response {
  kind:         "response",
  record:       "01J9Z3M2H1...",
  conversation: "01J9Z3J0ABCD...",   // same conversation
  source:       "search-worker",
  correlation:  "01J9Z3KPZ9...",     // same correlation, so it matches the command
  finish_reason:"stop",
  body:         <bytes>              // the result
}
```

A streamed answer contains `chunk` records with the same `channel`. Their `sequence` values define order. `last = true` ends the stream (A9.4). Unused optional fields are omitted (A2).

### A9.1 Envelope fields

| Field | Type | Meaning |
| --- | --- | --- |
| `kind` | enum | `command \| response \| event \| chunk \| status \| error`. Closed vocabulary, a new kind needs a version bump |
| `record` | u128, optional | producer-assigned id, required on every kind except `chunk` |
| `conversation` | u128 | ordering unit, partition key, and trace id |
| `source` | agent id | producing agent, a claim |
| `target` | agent id, optional | routing refinement within a shared topic, never an access control |
| `cause` | u128, optional | causal parent's record id (portable identity half) |
| `cause_at` | opaque locator bytes, optional | causal parent's locator, an opaque binding-defined byte string. The Iggy binding packs its four-level position. A consumer that cannot interpret it falls back to `cause`. The one substrate-shaped slot (C1.2) |
| `correlation` | u128, optional per kind | request and reply pairing |
| `channel` | u128, optional | chunk-stream grouping |
| `sequence` | u64, optional | chunk ordering within a channel |
| `last` | bool | terminal flag, skipped when false |
| `idempotency_key` | string, optional | business idempotency |
| `deadline_micros` | u64, optional | drop-dead time, and on an opening chunk the abandonment bound |
| `finish_reason` | string, optional | why a stream or response ended (open OTel vocabulary) |
| `task_state` | u8 code, optional | the task-state dictionary |
| `operation` | string, optional | OTel operation name, with two closed sub-vocabularies (A9.3) |
| `tool` | string, optional | OTel tool name |
| `usage` | token usage struct, optional | advisory accounting (input, output, optional reasoning and cache counts) |
| `metadata` | map<string, scalar>, optional | envelope-native extension slot with pinned keys (A9.6) |
| `must_understand` | u64 bitset, optional | must-understand marker: feature bits a receiver MUST implement to process this message, else reject. `0` (the default, skipped on the wire) is the open-world "ignore anything unknown". No bits are defined yet, so the marker is the mechanism awaiting its first strict-handling feature, letting a message demand strict handling without a whole-envelope version bump. The bound is inherent: a receiver predating the field ignores it like any unknown field, so the marker only binds receivers from the release that introduced it forward, which is why it ships now with zero bits ahead of any feature that needs it |
| `body` | bytes | the content, codec per the content-type attribute |
| `signature` | Signature capsule, optional | a detached signature over the canonical encoding with the signature absent, domain-separated by `agdx.signature.v1` (A9.5). Absent means an unsigned record (the open-world default). Verified SDK-side, the wire crate stays crypto-free. May ride any kind, like `metadata` |

### A9.2 The per-kind validity matrix

R means required, O means optional, and X means invalid. Wire validation, SDK constructors, and receivers enforce the matrix. Positive and negative reference cases cover it.

| Field | command | response | event | chunk | status | error |
| --- | --- | --- | --- | --- | --- | --- |
| `record` | R | R | R | O | R | R |
| `conversation`, `source` | R | R | R | R | R | R |
| `target`, `cause`/`cause_at`, `metadata` | O | O | O | O | O | O |
| `correlation` | R | R | O | R | O (R for `task`) | R |
| `channel` | X | X | X | R | X | O (stream terminal) |
| `sequence` | X | X | X | R | X | O (with `channel`) |
| `last` | X | X | X | O | O | X |
| `finish_reason` | X | O | X | O (with `last`) | X | X |
| `idempotency_key` | O | O | O | X | X | X |
| `deadline_micros` | O | X | X | O (opening chunk) | X | X |
| `task_state` | X | O | X | X | R (`task`) | O |
| `operation` | O | O | O | R on opening chunk, X after | R (`task`\|`card`\|`progress`\|`quarantine`\|`unquarantine`) | O |
| `tool` | O | O | O | O | X | O |
| `usage` | X | O | O | O (terminal chunk) | O | O |
| `body` | R | R | R | R (empty only with `last`) | O | R |
| `signature` | O | O | O | O | O | O |

A `command` expects a reply or effect and requires `correlation`. An `event` does not expect a reply. Commands cannot omit correlation to request fire-and-forget behavior.

### A9.3 Closed sub-vocabularies

- A `status` uses `operation` to select `task`, `card`, `progress`, `quarantine`, or `unquarantine`. `task` requires `correlation` and `task_state`. `card` reports liveness and capabilities, and `progress` reports advisory progress. `quarantine` excludes the agent named in its body from routing. `unquarantine` restores that agent. Registry-topic write permissions control both operations.
- Chunk-stream purpose (`operation` on `sequence = 0`, required there, invalid after): `chat`, `reasoning`, `tool_args`.
- State sync convention (an `event`, never a new kind): `operation = state_snapshot` (body is the full state) or `state_delta` (body is an RFC 6902 JSON Patch).

### A9.4 Streaming and reassembly

A stream contains `chunk` records with the same `channel`, ordered by `sequence` in one conversation partition. It ends with `last = true` and `finish_reason`, or an `error` that names the channel. Readers resume by offset. Every implementation follows these assembly rules:

- Apply chunks in `sequence` order from 0, once per sequence.
- Drop duplicate sequences and count them.
- On a sequence gap, end the local stream with `finish_reason = "gap"`.
- Drop and count records after the first terminal record.
- Require the opening chunk to carry the purpose and abandonment deadline.
- Carry whole-stream `usage` once, on the terminal chunk.

The reader creates `abandoned` and `gap` locally. They do not appear on the log. Replay returns the original records.

### A9.5 Capsules (all CBOR, all fixtured)

- `BodyRef` with content-type `ref` points to external content. `reference` is a non-empty URI, object key, or KV key of at most 1024 bytes. The fields also include `size_bytes`, a 32-byte `sha256`, and optional `encryption`. Absent `encryption` means plaintext. After fetching, the consumer must compare the content with its digest.
- A dead-letter capsule includes the source log position, `reason`, `attempts`, optional `detail`, and `payload`. The payload preserves the original encoded envelope for redrive.
- `AgentCard` is the body of a `card` status. It has optional `name`, `version`, and `ttl_micros`, plus at most 64 capability descriptors. A card past its lifetime no longer proves liveness. Each descriptor names a bounded `skill_id` and can include input or output `ContentRef` values. It can also include advisory cost, latency, concurrency, health, and load. Load uses per-mille capacity, and health preserves unknown codes alongside `healthy`, `degraded`, and `unavailable`.
- `AgentPresence` uses the binding connection-metadata channel. Its fields are `v`, `agent`, and optional `inbox`. The body carries its own version because this channel has no separate version header. The inbox names the current work topic in the relevant stream. Presence disappears on disconnect.

A client must not advertise a second agent on the same connection. The SDK rejects the attempt without replacing the first identity. The registry keeps the authenticated principal with each presence record. Principal-bound routing must match that principal and reject missing or foreign identities. Without an inbox, presence proves only liveness. A target without an inbox produces a routing error.
- `FoldSnapshot` saves the result of reading records into client-side state. It contains `conversation`, inclusive per-partition `as_of` offsets, and encoded `state`. Resume each partition at its saved offset plus 1. The snapshot uses an existing content type, so A6 is unchanged. It bounds conversation and workflow replay. Registry state instead uses incremental `AgentCard` and `AgentPresence` updates with expiry.
- `Signature` is the optional envelope signature (A9.1). Its fields are `scheme` (Ed25519 = 1), an 8-byte `key_id`, 64-byte `bytes`, and optional `context`. `SignatureContext` contains `content_type` and `agent_version`. Signing input is `agdx.signature.v1`, the encoded context when present, and the canonical envelope with its signature absent.

A signed context binds `agdx.ct` and `agdx.av` to the envelope. Changing either observed header invalidates that signature. A signature without context uses the context-free input. The wire crate defines the data, while SDK code performs cryptographic checks. Keys bind to an authenticated principal rather than the claimed `source`.

When a verifier is configured, an unsigned reply, unknown key, invalid signature, or wrong signer must not resolve a correlated wait. Contracts, the reply dispatcher, `request_input`, and bridge task reads use this rule. They compare observed headers with the signed context and evaluate key validity at the server-recorded timestamp. Principal-bound routes use the authenticated principal for both discovery and reply checks. Accepted contract and fan-out results report that principal. An absent principal means no verifier was configured, not a failed check.

Keys have an agent or operator kind and a validity window. Quarantine and unquarantine facts require an operator key valid when the registry applies them. The registry ignores repeated record IDs so replay cannot apply the same control fact twice.

### A9.6 Pinned metadata keys

| Key | Type | Meaning |
| --- | --- | --- |
| `role` | string | chat role, recommended `user` / `assistant` / `system` / `tool` |
| `bridge_hops` | list of strings | the loop guard. A bridge appends its id and drops a message whose hop list already contains it |
| `run` | string | the run-registry id a status record belongs to, stamped by a registered workflow or contract and read by the run fold (A5 `agent.*`). A record without it never enters the fold, so the key costs and means nothing for everything that is not a registered run |
| `on_behalf_of` | string | the delegation subject, the user an agent acts on behalf of (`METADATA_DELEGATED_BY`). It rides `metadata`, so it falls inside the signed envelope span and the effective grant intersects the agent's with this user's (B1.4) |
| `purpose` | string | the declared purpose of the operation, a stable policy-engine input at the effect boundary (C3). Advisory unless the envelope is signed (A4.1) |
| `data_classification` | string | the declared classification of the data the operation touches. Advisory unless signed |
| `task_context` | string | the task this operation serves. Advisory unless signed |
| `session_intent` | string | the session's declared intent. Advisory unless signed |

An envelope can carry the fence token through `agdx.fence` metadata instead of a binding header. Each message must use exactly one carrier (B1.2).

### A9.7 Envelope caps

| Cap | Value |
| --- | --- |
| vocabulary string (`operation`, `tool`, `finish_reason`), each | 256 B |
| idempotency key | 64 B |
| metadata entries / key / value / total | 32 / 256 B / 1024 B / 8192 B |
| body reference | 1024 B |
| card capabilities | 64 |

## A10. Working state: key-value and forks

### A10.1 Key-value entry

| Field | Type | Notes |
| --- | --- | --- |
| `key` | bytes, at most 512 B | arbitrary bytes, no string form required |
| `namespace` | string, at most 128 B | non-empty, no ASCII control characters, charset otherwise open (`/`-style hierarchy is legal). The rule is wire-owned (`validate_namespace`) and enforced on every namespaced op by the SDK edge and the serving plane |
| `value` | bytes, at most 8 MiB | opaque |
| `expires_at_micros` | u64, optional | absolute expiry, expired entries hidden on read |
| `version` | u64, optional | optimistic-concurrency token, store-assigned and bumped on every mutation. `0` (omitted on the wire) means an unversioned store |
| `source` | `SourceRef`, optional | the origin log record the entry was folded from (stream, topic, partition, offset). Every managed write is log-first through a mutation topic, so a stored entry points back to the record that wrote it, the same provenance a memory row carries (A13). A reader navigates a value back to its source message while it is still on the log. Absent on an entry written before provenance stamping. Omitted on the wire when absent |

`MutationPosition` is the named-field barrier `{ topic_generation: u64, partition: u32, offset: u64 }`. A lease grant or renewal returns the position at which its mutation was applied. A takeover reader passes that exact value as `kv.get.min_position` so it cannot observe state older than its own lease epoch.

### A10.2 Key-value operations

| Op | Request fields | Reply outcome |
| --- | --- | --- |
| `kv.get` | namespace, key, optional `if_none_match(version)`, optional `min_position` (barriered read: the answering fold must have applied at least that mutation position, honored only under the `kv_fenced_leases` capability) | `Value(Option<entry>)`, `NotModified` when the conditional version matches, or `Stale { required }` when the barrier is not reached within the server wait (never an absent value) |
| `kv.set` | namespace, key, value, optional expiry | `Written` |
| `kv.cas` | namespace, key, value, optional expiry, precondition (`match(version)` \| `absent`) | `Committed { version }`, or `VersionConflict { current }` on a precondition miss |
| `kv.cas_fenced` | namespace, key, value, optional expiry, precondition, fence namespace, fence key, fence token | `Committed { version }`, `LeaseLost` on a stale fence or a lease no longer live, or `VersionConflict { current }` on a precondition miss |
| `kv.delete` | namespace, key, optional `if_match(version)` | `Deleted(bool)`, or `VersionConflict` when the conditional version misses |
| `kv.delete_many` | namespace, composed bounds (prefix, range, substring) | `DeletedMany(count)` |
| `kv.scan` | namespace, composed bounds, limit, cursor | `Page { entries, cursor }` |
| `kv.namespaces` | none | `Namespaces([{ namespace, entries }])` |
| `kv.exists` | namespace, key | `Metadata(Option<{ version, expires_at_micros, size_bytes }>)`, the cheap presence/precondition probe without the value |
| `kv.expire` | namespace, key, optional expiry (none clears) | `Versioned { version }` (the value is untouched) |
| `kv.patch` | namespace, key, patch bytes, optional `if_match` | `Versioned { version }` (a merge patch on a structured value, no full-object transfer) |
| `kv.lease` | namespace, key, lease ttl (`MIN_LEASE_TTL_MICROS`..=`MAX_LEASE_TTL_MICROS`), holder id, optional subject user id (delegated acquisition, `kv_lease:admin`) | `Leased { lease_token, granted_ttl_micros, position }`, or a clean unsupported error when the backend cannot serve leases |
| `kv.lease_renew` | namespace, key, holder id, optional subject user id, lease token, lease ttl (the same range) | `Renewed { lease_token, granted_ttl_micros, position }` with the unchanged token, or `LeaseLost` when the lease expired, was released, or was re-acquired |
| `kv.release` | namespace, key, lease token, holder id | `Released(bool)`, or `LeaseLost` on a stale fence token |

Key-value errors include unsupported, invalid key, invalid namespace, too large, backend, version, version-conflict, lease-lost, not-found, and stale. `expire` and `patch` return not-found for absent or expired keys. A `kv.get` read that cannot reach its required mutation position returns stale, never an absent value.

Namespaces group keys within one shared store. The authenticated user ID records identity for audit and does not itself create a separate dataset. Access depends on the binding permissions (B1.4). Scan pages contain at most 1000 entries and default to 100. `exists`, `expire`, `patch`, `lease`, `lease_renew`, `release`, `if_match`, and `if_none_match` implement the C6 object operations.

For example, writing a session flag with an expiry and reading it back:

```
kv.set { namespace: "sessions", key: "user:42", value: <bytes>, expires_at_micros: 1700000000000000 }
   -> Ok(Written)

kv.get { namespace: "sessions", key: "user:42" }
   -> Ok(Value(Some({ key: "user:42", value: <bytes>, expires_at_micros: 1700000000000000 })))
```

### A10.3 Compare-and-swap (optimistic concurrency)

The store assigns each entry a `version: u64` and increments it after each successful mutation. A conditional write compares this version before changing the entry.

The operation:

```
kv.cas { namespace, key, value, expires_at_micros?, expect }
expect = match(version) | absent
```

Success returns `Committed { version }` with the new version. A later conditional write can use it without reading again. A failed precondition returns `version-conflict` and the current version. `some(v)` means the entry exists, and `none` means it is absent. The caller can read again or handle the conflict.

The `kv_cas` capability indicates support for transactional conditional writes. A backend without this support leaves the flag clear and returns unsupported. An unversioned store reports entry version `0`. A lease stores holder identity, token, and expiry in one transaction (A10.4). Its fencing token comes from a separate counter that never expires. The lease row version cannot serve as that counter because it can reset after expiry.

`kv.cas_fenced` requires `kv_fenced_leases`, which includes the older `kv_cas_fenced` capability. It applies the target write and precondition in one transaction. The transaction also requires a live lease at (`fence_namespace`, `fence_key`) and a matching `fence_token`. The separate counter increases on every acquisition, including acquisition after expiry.

A stale token, expired lease, or released lease returns `lease-lost`. A failed target precondition returns `version-conflict`. This protects key-value effects from replaced holders. `kv.lease`, `kv.lease_renew`, `kv.release`, and `kv.cas_fenced` use `KV_LEASE_OP_VERSION = 1`. Their holder fields are required without decode defaults. Other `v` values return typed errors.

### A10.4 Revocable lease

```
kv.lease { namespace, key, lease_ttl_micros, holder_id, subject_user_id? }
   -> Leased { lease_token, granted_ttl_micros, position }
kv.lease_renew { namespace, key, holder_id, subject_user_id?, lease_token, lease_ttl_micros }
   -> Renewed { lease_token, granted_ttl_micros, position }
kv.release { namespace, key, lease_token, holder_id } -> Released(bool)
```

A lease grants temporary ownership and supports revocation. Acquisition creates the live lease row and increments the persistent fence counter in one transaction. The reply contains the token, granted lifetime, and mutation position. A takeover read sends this position as `kv.get` `min_position`.

A grant or renewal has a positive lifetime no greater than requested. Requests must fall between `MIN_LEASE_TTL_MICROS = 1_000_000` and `MAX_LEASE_TTL_MICROS = 300_000_000`. Each client and server boundary rejects values outside the range before executing them. A holder can renew before expiry. `holder_id` is a non-empty UTF-8 identity of at most `MAX_HOLDER_ID_BYTES = 128` bytes. Renewal and release must name the same holder.

`subject_user_id` allows acquisition on behalf of another user and requires `kv_lease:admin`. Without it, the lease protects the authenticated caller mutations. Acquisition conflicts with a live lease. `kv.lease_renew` requires the same holder and subject, a live row, and the current fence token. It extends expiry and returns the same token. It does not increment the fence.

Release compares the presented token with the persistent fence counter and removes the live row in the same transaction. A missing or expired row with the current token returns `Released(false)`. A stale token returns `lease-lost`. A backend without lease support returns `unsupported`. These operations implement `LEASE`, `RENEW`, and `RELEASE` from C6.

Fenced coordination commands use the non-replicated managed extension. Clients must not assume that the streaming transport deduplicates `operation_id`. If an acquisition can reach the server but its reply is lost, the client lacks the token needed to renew or release. It must wait through the requested lifetime from that attempt before acquiring again. A convenience API must enforce this wait before returning an ambiguous-acquisition error. A transport must not automatically reconnect and replay an acquisition.

Renew and release are safe to repeat with the same holder and token. Reconcile an uncertain fenced CAS through its required target precondition. Generic handler retries must treat an ambiguous mutation as terminal. They must not create a new operation identity and retry it.

Acquire, renew, and release require `kv_lease:admin` on the coordination namespace. `kv.cas_fenced` requires `kv_fence:read` there and `kv:write` on the target namespace. Permission to write data does not grant control over its lease. The lease holder does not need access to the protected data.

### A10.5 Forks

Copy-on-write branches of the materialized read model.

| Type | Fields |
| --- | --- |
| `ForkKind` | `severed` (frozen snapshot) \| `continuous` (live branch, default) |
| `ForkStatus` | `open` \| `promoted` \| `squashed` |
| `ForkInfo` | fork_id, optional parent, kind, user_id, status, created_at_micros, row_count |

| Op | Request | Outcome |
| --- | --- | --- |
| `fork.create` | fork_id, optional parent, kind, tables | `Created(ForkInfo)` |
| `fork.delete` | fork_id | `Deleted(bool)` |
| `fork.promote` | fork_id | `Promoted { rows }` |
| `fork.list` | none | `List([ForkInfo])` |
| `fork.put` | fork_id, table, partition_id, offset, projection id and version, fields, metadata, optional payload, optional embedding, tombstone flag | `Written` |

A fork ID contains at most 128 bytes. Allowed characters are ASCII letters, digits, `-`, `_`, and `.`. `validate_fork_id` enforces the rule before SDK I/O and in the managed plane. A caller cannot use an arbitrary SQL identifier as a fork name.

A query may resolve against a fork's overlay (trunk plus the fork's speculative rows) by naming the fork.

## A11. Materialized views: projections and query

### A11.1 Projection model

A projection turns a payload into a queryable row. It is global and reusable. Bindings declare where it applies.

| Type | Fields |
| --- | --- |
| `Projection` | id (recommended `name.vN`), name, version, `kind` (`row` default \| `graph`), content-type, extraction schema, optional entity schema (graph only), inline-payload default |
| `ProjectionKind` | `row` materializes queryable rows (the default), `graph` materializes a knowledge graph (nodes, edges, triplets). A growable u8 dictionary, so an old reader relays an unknown kind rather than failing the listing |
| `IndexField` | name (the index key), pointer (RFC 6901 into the payload), optional field-type hint (`text` / `int` / `float` / `bool`) |
| `IndexSchema` | fields, optional vector-field pointer (default `/embedding`), inline-payload flag |
| `EntitySchema` | node/edge extraction for a `graph` projection: node rules (label + RFC 6901 value pointer + optional embedding pointer) and edge rules (edge type + source/target pointers + optional valid-from/valid-to pointers for a bitemporal extracted edge). Pointer-based and deterministic, so building the graph needs no model call (A13) |
| `ProjectionBinding` | source (stream, topic), allowed projection refs, default projection, one operational index, optional exact backend resource generation, `notify` flag, and optional retention policy. No target list, delivery mode, role, or required mirror metadata remains |
| `SchemaDef` | id (u32, permanent), source, optional name, optional version |
| `SchemaSource` | internally tagged on `kind`: `{kind: avro, schema}` \| `{kind: json_schema, schema}` \| `{kind: protobuf, descriptor_set, message_type}`. `descriptor_set` is a byte string. An unknown `kind` from a newer peer decodes to a forward-compatible `unknown` rather than failing the whole reply (the same shape `RetentionPolicy` uses), so an old client still reads a registry holding a source kind it cannot decode against. It must not re-register an `unknown`. |

The log retains original message bytes under its retention policy. A projector extracts indexed columns for filters, sorting, and aggregation. It can also copy the body into the row to avoid another log read. Each row records its numeric stream ID, topic ID, partition, and offset. These IDs remain valid after a rename.

A record with no indexed fields produces no row. A record can have at most 32 indexed fields. The inline body limit is 8 MiB. Larger bodies still produce indexed columns, but the row does not copy their payload. Readers can fetch the original record or follow a `ref` body. An explicit field directive takes precedence over schema extraction for that field.

A materialized view contains records already processed by a projector. It is eventually consistent by default, so a new record becomes queryable after projection. The delay depends on the projector and backend. A `read_your_writes` query waits for the projector to reach the source head. If it cannot reach that position in time, the query returns `stale`. The source log remains readable by offset after the append completes.

`ConsistencyGate { applied, required }` compares the applied offset with the required source position. `eventual` always passes this gate. Other levels require the projector to reach the source head captured for the query. A failed gate returns `stale`. `strong` also requires the backend to establish agreement across replicas.

### A11.2 Control commands (durable on the control topic)

The control envelope contains `{ v, timestamp_micros, command }`. Commands are `RegisterProjection`, `DropProjection`, `ApplyBinding`, `RemoveBinding`, `RegisterSchema`, `DropSchema`, `RegisterGraph`, `DropGraph`, `RegisterRunSource`, and `RemoveRunSource`. A graph projection registers through `RegisterGraph` so deployments can control graph registration separately. Schema IDs are permanent and cannot collide. Dropping a schema does not prevent decoding records that already reference it.

`RegisterRunSource` and `RemoveRunSource` name a `{ stream, topic }` source of run-tagged records. They change the run registry source set without restarting the deployment. Repeating either operation for the same source is safe. Older decoders reject an unknown command variant.

### A11.3 The query IR

The query IR is a logical request compiled by the selected backend.

| Field | Meaning |
| --- | --- |
| `execution_id` | a nonzero 128-bit identity shared by execution, paging, status, cancellation, and errors |
| `target` | `operational { index }` or `lakehouse { destination_id, destination_generation, snapshot? }` |
| `deadline_micros` | one absolute deadline for the complete execution, never extended by page retrieval |
| `by_key` | exact-match key constraints, AND-composed |
| `message_type`, `time_range` | sugar for equality on the type field and a nonempty half-open range on the timestamp |
| `filter` | a predicate tree (`all` / `any` / `not` / `pred`) |
| `vector` | nearest-neighbour search (field, embedding, top_k), distance in the row score |
| `text` | lexical relevance search (the query text, optionally one indexed field), relevance in the row score, text capped at 1024 bytes. Capability-gated (`keyword_search`): an unaware backend would silently drop the additive field, so a client refuses an unadvertised `text` before sending, and a backend without a lexical index answers unsupported rather than a contains approximation |
| `order` | sort clauses (field, direction) |
| `page` | bounded limit, optional offset, optional opaque cursor, and optional exact-total request |
| `aggregate` | group-by keys, aggregate calls, optional tumbling window |
| `having` | a filter on aggregate output |
| `distinct` | distinct over the selected fields |
| `select` | projected fields and an inline-payload flag |
| `fork` | resolve against a fork's overlay |
| `raw_sql` | explicit dialect, read-only SQL text, and ordered typed parameters. The server never guesses a dialect |
| `consistency` | read-consistency level, absent on the wire for the default `eventual` |

| Vocabulary | Values |
| --- | --- |
| comparison op | `eq`, `ne`, `lt`, `lte`, `gt`, `gte`, `in`, `contains`, `prefix` |
| aggregate func | `count`, `count_distinct`, `sum`, `avg`, `min`, `max`, `percentile`, `std_dev` |
| typed value | tagged null, boolean, int, long, float, double, decimal, date, time, timestamp, timestamp with timezone, string, UUID, fixed, binary, struct, list, or map |
| consistency | `eventual` (serve as-is, the default), `read_your_writes` (wait for the projector to reach the source log head, else `stale`), `strong` (linearizable cross-replica). `read_your_writes` and `strong` are backend-gated |

### A11.4 Query reply

`Ok(QueryResult)` or `Err(QueryError)`. Errors cover unsupported operations, authorization, missing indexes and forks, backend faults, version skew, stale reads, size limits, cancellation, deadline expiry, expired snapshots, stale generations, unavailable targets, and resource budgets. Every typed error projects into the unified result code in A7.

`QueryResult.fields` is the ordered logical schema for the page. Every `Row.values` entry is a canonical tagged `TypedValue` at the same position. A vector or lexical query additionally returns its backend-native finite distance or rank in `Row.score`. The maximum page is 1,000 rows. Bulk columnar interchange uses Arrow IPC rather than the query page contract.

`Page` carries an optional offset, limit, optional exact total, `has_more`, and an optional opaque next cursor. `has_more` and `next_cursor` must agree. A caller retrieves another page through the same execution identity. It does not reconstruct the query or extend the deadline.

Every result includes `QueryContext`. It records the engine, version, target, backend identity, generations, runtime configuration revision, and requested and delivered consistency. It also records truncation and resource metrics. Lakehouse results add destination generation, table UUID, snapshot, schema, partition specification, materialization digest, checkpoint revision, and global state revision.

For example, the ten most recent high-latency calls for one model, newest first:

```
query {
  execution_id: "01...",
  target: { kind: "operational", index: "inferences" },
  deadline_micros: 1717171747000000,
  filter: { all: [ { pred: { field: "model", op: eq, value: "gpt-4o" } },
                   { pred: { field: "latency_ms", op: gt, value: 500 } } ] },
  order:  [ { field: "ts", dir: desc } ],
  page: { limit: 10 },
  select: { fields: ["model", "latency_ms", "user_id"], payload: false }
}
   -> Ok(QueryResult { fields, rows: [ { values: [...] }, ... ], page, context })
```

### A11.5 Logical schema and canonical values

A logical schema has a nonzero 128-bit ID, nonzero version, 32-byte fingerprint, and ordered fields. Field IDs must be positive and unique throughout the nested schema. Names must not contain control characters. Sibling names must be unique. Users cannot declare reserved provenance names or IDs.

Logical types include booleans, integers, floating-point numbers, decimals, dates, times, timestamps, strings, UUIDs, fixed bytes, binary data, structs, lists, and maps. Times use microseconds since midnight. Timestamps use microseconds, with a separate variant for UTC instants with timezone meaning. Decimal precision ranges from 1 to 38, and scale cannot exceed precision. UUIDs use RFC 4122 network byte order. Floating-point values reject NaN, infinity, and negative zero.

Map keys support boolean, integer, decimal, date, time, timestamp, string, UUID, fixed, and binary types. This includes both timestamp variants. Canonical map order follows the encoded key bytes.

The reserved provenance fields use the `__laser_` namespace and fixed IDs from `PROVENANCE_FIELD_ID_START`. They include source and destination identity, original payload and content type, and source position. Users cannot declare those names or IDs.

### A11.6 Destinations, query routes, and checkpoints

A `MaterializationDestination` defines an immutable generation. It connects a source scope, partition-recreation policy, projection version, schema fingerprint, backend generation, and physical table identity. It also specifies Parquet, Iceberg v2, start behavior, new-partition behavior, blocking errors, and desired state. Runtime ownership and observed progress belong to status records.

A `QueryRoute` independently names an operational index or one lakehouse destination generation. Query routing never follows a projection target role.

`CheckpointRequestEnvelope` is the bounded public request. It contains the operation version, request ID, expected global revision, mutation, and optional signed supervisor assertion. The AGDX bridge authenticates the caller and forwards this request. Plane creates a `ReplicatedCheckpointMutation` with the commit timestamp, authenticated Iggy actor, and verified supervisor evidence. Plane then appends it to the managed checkpoint mutation topic. Separate reference files and decoders prevent the public and committed forms from being confused.

Destination status includes declaration and checkpoint revisions, desired and effective state, backend and schema bindings, table UUID, lease, and ordered partition boundaries. It also records prepared attempts, completion, retention gaps, blocking errors, repairs, and read consistency. Owner, epoch, sequence, and deadline identify the valid lease. Prepared attempts fix source ranges, the resulting boundary, and Iceberg commit requirements. They also fix manifest and object digests, schema and projection identity, table base state, and credential generations.

### A11.7 Arrow IPC input

Each Iggy message contains one self-contained Arrow IPC stream. It includes its schema, dictionaries, and record batches. Messages cannot share continuation state. `agdx.sfp` carries the logical schema fingerprint. `ArrowIpcPolicy` limits encoded bytes, fields, batches, rows, and dictionaries.

Time and timestamp units must be microseconds. Reject unions, extension types, unsupported dictionaries, and decimals wider than 128 bits. Also reject missing schema messages, trailing bytes, shared stream state, and policy violations. Return the corresponding `ArrowIpcRejectionCode`. `laser-wire` does not depend on Arrow implementation crates.

### A11.8 The change feed

A projection binding enables change notifications through `notify` (A11.1). After a committed batch advances a notifying table, the projector publishes one change record for that table. It writes the record to the ops stream changes channel (B1.1). Consumers read and resume by offset, as with other topics.

| Type | Fields |
| --- | --- |
| `ChangeRecord` | `v` (op version, 1), `index` (the materialized index that advanced), `partition_id`, `from_offset` / `to_offset` (the inclusive source-offset window the batch committed), `rows` (rows written) |

A change record reports that a view advanced through an offset range. Read the rows through `query` (A11.3) or the log. Notifications are best-effort after commit, so losing one does not lose the projected data. If a consumer misses the feed retention window, it reads the view directly.

The feed reports progress. Read-your-writes establishes whether the view includes a required write. The `watch` capability advertises feed support. Without it, the client rejects the request to open a feed before waiting on a channel.

## A12. Capability negotiation

A single connection negotiates what is available. A managed feature works against a managed implementation or returns `unsupported`.

- Run `hello` at connection time and again when refreshing capabilities. The reply reports versions for query, control, checkpoint, key-value, fork, agent, and graph, plus feature bits. The current versions are 1. A zero version means the operation group is unavailable. Fenced leases also require their feature bit.
- `BackendDescriptor` reports versioned backend identity, mode, label, implementation, generations, configuration revisions, state, and readiness. It also reports materialization, query, type, time-travel, consistency, paging, cancellation, schema, maintenance, and limit support. It must not expose URLs, credentials, secrets, or mutable configuration requests.
- Readiness reports the current backend condition through stable reason codes. Unavailable or degraded backends retain their identity and capability descriptions. Refresh capabilities after startup races, failover, or backend restarts.
- SDK capabilities group features by their dependencies. `managed` indicates that a managed plane is connected. Managed groups include `query`, `destinations`, `kv`, `graph`, `forks`, and the A2A gateway. Platform-native groups include `sessions` and `durable_dedup`. Memory combines query and graph operations and has no separate capability.

`query.consistency` reports the strongest supported level: `eventual < read_your_writes < strong`. A stronger level includes the weaker levels. `kv.cas` reports conditional writes. `kv.cas_fenced` reports fence-protected writes. `kv.fenced_leases` reports holder-scoped acquisition, renewal, release, fenced CAS, and reads with a required mutation position.

The wire reply retains the flat `features` bitset. Its bits include `kv_cas`, `read_your_writes`, `strong_consistency`, `kv_cas_fenced`, `agent_workflow`, `keyword_search`, `watch`, `authz`, `destinations`, and `kv_fenced_leases`. SDKs convert these bits into grouped capabilities. HTTP reports the grouped form (B4). Without managed support, the corresponding capabilities remain off and calls return unsupported.
- If the reported operation version differs from the SDK version, reject the call before sending. Return the typed version error for that operation group.
- If an optional request field changes service behavior, require its capability before sending it. This includes the `consistency` field. A distinct command code can receive an explicit unsupported reply, but an unknown optional field can be ignored.

Do not send `kv.lease`, `kv.lease_renew`, `kv.release`, `kv.cas_fenced`, or `kv.get` with `min_position` unless the peer advertises `kv_fenced_leases`. Apply the same rule inside raw `batch` requests. A batch must not bypass capability requirements.
- The binary `hello` feature bit and corresponding HTTP capability must agree. A feature cannot be available through only one declaration of the same server capability.
- Features default to unavailable until the server explicitly reports support. HTTP defaults leave `kv.cas` and `graph` off and `query.consistency` at `eventual`. The binary reply uses zero feature bits and a zero `graph` version. Report a feature only when the backend can provide it.
- If the server and managed backend run separately, the backend supplies its own capability and readiness report. The server requests live `BackendAnnounce` data through their private socket for client hello and HTTP capability requests. After a failed probe, cached information can be returned only with unavailable status.
- `BackendAnnounce.ready` distinguishes readiness from configuration. A configured backend that cannot answer reports `ready = false`. If a later probe fails, retain known features and topology only as descriptive information. Mark the backend unavailable. Clients must keep its managed operations unavailable and support refresh without reconnecting. The encoded form omits `ready` when it is true.
- Optional `WireTopology` reports the ops stream, control, dead-letter, change-feed, and managed mutation topic names. The mutation topics are `kv`, `fork`, `run`, `graph`, and `checkpoint`. Explicit client configuration takes precedence over reported names. Each field has a default, so a partial report does not produce empty names. Omit absent topology from the encoded form.
- Create one stable identity for each logical Plane-served mutation, outside transport retry loops. Wrap it in `ManagedRequestEnvelope { v, operation_id, payload }`. `operation_id` is a required nonzero u128 with a ULID value. The server rejects bare or zero-identity mutations and preserves the identity when forwarding.

The deployment appends `MutationCommandEnvelope { v, operation_id, timestamp_micros, command_code, payload }` to the managed mutation topic. Each mutation topic has one partition until the contract defines cross-partition transactions. Only the deployment plane can publish there. The backend stores each outcome atomically with its effect, keyed by operation identity. Repeated identities return the saved outcome.

Reads can reconnect and retry. Mutations can retry only with their original identity. Do not retry deterministic rejection, such as invalid input or an oversized reply. Reject peers that cannot carry mutation identity. The three Iggy managed authorization writes use their existing `mutation_id` and dedicated replicated operations instead of the Plane envelope.
- The managed key registry uses a KV namespace, `agent.keys` by default. Its key is the lowercase hexadecimal form of the first 8 SHA-256 bytes of the verifying key. The value is `KeyRecord { v, principal, key_id, verifying_key, kind, valid_from_micros, valid_to_micros?, revoked }` with `v = 1`.

Every client reads and writes the same named-field CBOR form. Enrollment and revocation use compare-and-swap. A snapshot skips invalid records. Reject a record whose key ID does not match its verifying key.

## A13. Agentic memory and the knowledge graph

Agent memory combines publication, key-value state, queries, and graph operations. It adds no separate command range. Every memory write appends a `MemoryRecord` to a configurable topic. Its variants describe an item, forgetting an item, or feedback. Each scope maps to one partition.

The deployment builds a versioned key-value read view from the topic. Topic retention and read-view retention are independent. Default recall reads the managed view. Local topic folding is an explicit alternative for small deployments without that view. A local vector index supports similarity reads, and the graph supports relationship reads.

The read view is shared across principals. Managed capability grants control access at the command boundary (B1.4). Reading requires `kv:read` on the materialized namespace, while writing requires publication access to the memory topic. The fold does not infer per-record ownership.

Headers carry the logical namespace `agdx.mem.ns` and scope fields such as `agdx.mem.user`, `agdx.mem.app`, agent, and conversation. The view uses those scopes rather than the physical topic as its logical key. The fold also records the originating conversation from `gen_ai.conversation.id` and a `SourceRef` with numeric stream, topic, partition, and offset. These references survive renames and can locate the source while the log retains it. Conversation filters narrow reads and do not establish ownership.

The memory verbs (SDK facade, no wire op).

- `remember` appends an item to the memory topic. With duplicate suppression, the ID derives from the durable owner, kind, and body. Repeated content for that owner resolves to the same item. Graph entities come from `graph.upsert` or a bound graph projection that extracts nodes and edges.
- `recall` retrieves candidates, combines scores, optionally reranks, and returns the highest-ranked items. `auto` selects available signals. `recent` and `temporal` read by time, while `semantic` uses vectors and `keyword` uses lexical matching. `graph` traverses relationships. `hybrid` combines semantic and keyword ranks with reciprocal-rank fusion. Each result retains the contributing strategies, ranks, and original scores.

The embedded keyword engine ranks token coverage before term frequency. Reranking is supplied by the application, like embedding and consolidation. A managed plane can route recall using its known graph. Otherwise, the client selects from reported capabilities and its configured embedder, with recency as the fallback.
- `improve` records feedback that a ranking backend can use. Consolidation supplies further work such as summaries, relationship weighting, pruning, and fact extraction. Applications or managed backends implement this extension.
- `forget` appends a deletion record that removes the item from the read view. An optional cascade also removes derived graph nodes, edges, and vectors.

Memory kinds are SDK labels: `fact`, `message`, `summary`, `entity`, `feedback`, and `procedure`. `message` is episodic memory, `procedure` is procedural memory, and the other kinds are semantic memory. Lifetimes are `session` or `durable`. Scopes include `user`, `agent`, `session`, `app`, and the physical stream. An unset scope field broadens recall across that field.

A context handle selects one conversation. Its session-memory view uses that conversation for reads and writes without repeating it in each call. This uses the existing scope fields and adds no wire operation. Durable memory and graphs can span conversations. The context graph accessor returns the graph without applying a conversation filter.

Content-addressed IDs derive from content. `content_id` applies the shared FNV function to byte segments and returns a 16-byte ID (A3). A memory ID uses durable owner, kind, and body. A graph node ID uses entity label and value. Matching inputs produce the same ID across clients. Reference vectors fix this behavior.

Content IDs support convergence and duplicate suppression. They do not establish trust. FNV does not protect against deliberate collisions. Writers with permission to submit an ID can also submit it directly. Signed principals or exclusive write permissions provide the integrity boundary (A4.1, B1.1). A cryptographic replacement remains an option if the ID itself needs collision resistance.

A `graph` projection names a managed graph view (A11.1). Its entity schema selects labels and source pointers for node and edge extraction. `graph.upsert` writes nodes and edges by their content IDs. Repeating the same upsert does not create duplicate identities. The `graph` operation version controls support (A12):

| Op | Request | Reply |
| --- | --- | --- |
| `graph.query` | graph name, start (`ids` \| predicate `match` \| vector `nearest`), hop spec (edge type + direction + max), optional node/edge filters (the same `Filter` predicate language as query, A11.3), return (`nodes` \| `edges` \| `paths` \| `triplets`), limit, optional fork, consistency, optional valid-time `as_of`, optional `conversation` lens | `nodes`, `edges`, `paths` |
| `graph.neighbors` | graph name, node id, direction (`out` \| `in` \| `both`), optional edge type, depth, limit, optional valid-time `as_of`, optional `conversation` lens | the reachable nodes and traversed edges |
| `graph.upsert` | graph name, nodes, edges (the projector path, idempotent on content-addressed ids) | written |

An edge can include `valid_from` and `valid_to` in epoch microseconds. Missing bounds are open-ended. This valid-time window describes when the relationship was true. The log position supplies the separate observation time. To replace a relationship, close the old window and record the new relationship while retaining history. The window does not affect the edge ID.

A traversal with `as_of` follows only edges valid at that instant. Omitted time fields do not change the earlier encoded form. Reads by log offset provide the separate system-time axis.

A node or edge can carry `source` to identify its origin. `SourceRef` can name a message position, a key-value entry, or a memory item. A message position includes numeric stream, topic, partition, offset, and optional conversation. The field is omitted when unknown and does not affect the content ID.

An edge retains the source that most recently asserted it. A node retains the first source that introduced the entity. Later observations can change embeddings or attributes without replacing that first source. If nothing changes, the node is not rewritten. The reference is bounded by `MAX_SOURCE_REF_BYTES`. The log retains the full history for replay.

`wire/src/limits.rs` limits traversal depth to 8, total returned nodes and edges to 10000, and labels per node to 16. The server rejects excessive depth and oversized upserts with too-large errors. A query can name a fork to read its overlay. Graph reads reuse query `Filter`, `Value`, and `Consistency` types.

`node_filter` controls entry into each traversal step. A rejected node is neither returned nor expanded. `edge_filter` controls which edges are followed and returned. These filters prune the walk before later steps. Client-side filtering of completed results does not provide that behavior.

The managed engine supports `nodes`, `edges`, `paths`, and `triplets` results. Starting modes are `ids`, `match`, and vector `nearest`. Relational tables store nodes and edges by `(graph, id)`. Adjacency indexes support reads over the current frontier. Nearest starts use the backend vector distance. Unsupported modes must return unsupported instead of partial results.

A traversal or neighbor read can use `as_of` in epoch microseconds. It follows only edges whose valid-time window contains that instant. Log offsets provide the separate observation-time axis.

Use `graph.upsert` to write graph elements directly. Alternatively, use `ApplyBinding` to attach a graph projection to a source topic. The projector extracts and upserts each record under its entity schema. Reprocessing converges on the same content IDs. `link(from, relation, to)` upserts two entity nodes and their edge. `unlink(..)` closes the edge with `valid_to` at call time and retains the historical fact.

This realizes the data-object collection primitives that suit a graph (C6) without a separate query language.

Conversation filters use the originating `gen_ai.conversation.id` value. Graph queries and neighbors match it against each element source. Projections expose it through the reserved `conversation_id` field for ordinary query predicates. Memory-view scans can filter by conversation, while generic key-value entries without that metadata are excluded.

These fields are optional and omitted when unset. They narrow results but do not prove authorship or grant access. The security profile in B1.1 defines those guarantees.

---

# Part B. Bindings

A binding maps logical identities to addresses and defines attribute encoding, command dispatch, request-reply transport, and the `cause_at` locator. The remaining rules come from Part A. Reference tests define the expected binding-specific representations.

## B1. The Iggy binding (normative)

### B1.1 Identity to physical address

| Logical | Iggy address |
| --- | --- |
| streaming record | stream, topic, partition, offset |
| agent ordering key | the conversation id as the partition key. Generic streaming preserves order within the caller-selected Iggy partition |
| collection / topic | a topic on a data stream |
| managed ops | a reserved command range against the connection (B1.4), not a topic |
| `cause_at` locator packing | the four-level (stream, topic, partition, offset) address as 20 big-endian bytes in the opaque locator slot |

The Iggy binding uses `_agdx` for its ops stream. `control.commands`, `dlq`, and `changes` carry projection control, dead letters, and change notifications. Use the shared constants for these names. `Laser` also provides `ops_stream`, `control_topic`, `dlq_topic`, and `changes_topic` overrides for deployment or test configuration. Managed queries use the reserved command range rather than a request topic (B1.4).

Connection bootstrap and environment variables are SDK concerns documented in the tutorial, not part of this binding.

The SDK uses standard Apache Iggy transport framing. Append, poll, consumer-group, offset, and managed commands share the connection. The fork handles role-definition, role-deletion, and role-binding commands through the established custom replicated operations. Clients do not implement a second transport or command registry.

The Iggy fork runs `iggy-server`. It authenticates the caller and attaches the trusted user and client identities. It enforces command access, then handles an extension command or forwards it to `laser-plane`. Capability discovery includes the connected plane report. Laser Stack packages the fork with `laser-plane`. LaserData Cloud adds Warden, deployment services, and proprietary interfaces.

Agent records use the conversation ID as their partition key. This preserves order within a conversation while different conversations can use separate partitions. One conversation is limited by one partition and its owning shard. Generic streaming supports balanced, keyed, or explicit partition selection. Partitions define ordering and workload placement. Apache Iggy enforces access at stream and topic level.

Authorship uses an explicit deployment security profile:

| Profile | Authorship guarantee | Typical topology |
| --- | --- | --- |
| Advisory | `source` and provenance headers are self-asserted | shared topics for local development or mutually trusted writers |
| Signed principal | a verified envelope binds the enrolled signing key and authenticated principal to the record | shared agent topics with receiver-side verification |
| Topology isolated | Iggy ACLs bind one authenticated principal to a write-exclusive stream or topic, optionally with signatures for defense in depth | control and effect channels requiring an exclusive writer |

A receiver must know the deployment security profile. It must not use advisory fields for billing or access decisions. Shared topics can use the advisory profile for trusted writers or the signed-principal profile with verification. Topology isolation requires exclusive write routes and access rules. Deployments can combine signatures with those rules. Neither mechanism changes ordinary Iggy message bodies or adds an authorship header.

Apache Iggy accepts stream and topic names or numeric IDs. The binding can use resolved numeric IDs to reduce addressing bytes. This optimization does not change the logical model. Other bindings can retain their native addressing.

### B1.2 Out-of-band carriage: the header dictionary

Apache Iggy carries attributes in typed headers. Routing IDs use its typed 128-bit value with little-endian byte order.

The custom keys are standardized under the `agdx.` namespace, fixed so independent implementations interoperate. The full key is the contract. Keys drawn from OpenTelemetry use the `gen_ai.` namespace verbatim, an external standard.

| Header | Type | Core attribute or role |
| --- | --- | --- |
| `agdx.ct` | u8 | content-type code |
| `agdx.sid` | u32 | writer-schema id, resolved managed-side for schema-first codecs |
| `agdx.ref` | string | projection selector, routes to a materialization rule |
| `agdx.inline` | bool | per-record inline-payload override |
| `agdx.idx.<name>` | string | an indexed scalar, becomes a queryable field |
| `agdx.av` | u32 | the agent envelope wire version |
| `agdx.corr` | u128 | generic request and reply correlation, independent of the agentic layer |
| `agdx.on_behalf_of` | string | reserved binding alias for a delegation subject. Signed on-behalf-of delegation uses the envelope metadata key `on_behalf_of` (A9.6, B1.4), not this header |
| `gen_ai.conversation.id` | u128 | conversation id (OpenTelemetry) |
| `gen_ai.agent.id` | string | producing agent (OpenTelemetry) |
| `gen_ai.usage.input_tokens` / `output_tokens` | u64 | token usage (OpenTelemetry) |
| `agdx.cause`, `agdx.parent_conv`, `agdx.root_conv`, `agdx.to`, `agdx.idem`, `agdx.deadline`, `agdx.cost` | mixed | provenance: causal parent, parent and root conversation, addressee, dedup key, deadline, cost |
| `agdx.fence` | u64 | the strictly-monotonic per-task fence the producer held, so a consumer drops a stale-holder replay of a log-resident effect |
| `agdx.mem.ns` | string | the logical memory namespace a record materializes under, so the read view is keyed by scope rather than by the physical topic |
| `agdx.mem.user` / `agdx.mem.app` | string | the user and app scope layers a memory record belongs to, materialized onto the read-view row so recall narrows by them |

Headers have a 1024-byte soft limit per record. Each value is limited to 255 bytes. Each header also uses 9 framing bytes, counted in the total.

A typed `AgentEnvelope` carries its own message fields. Headers contain only content type, wire version, conversation routing ID, and a targeted addressee. `source`, `cause`, `correlation`, `deadline_micros`, and `idempotency_key` remain in the envelope. Generic messages without an envelope use the provenance header dictionary instead. Each field therefore has one authoritative carrier for that message form.

### B1.3 Versioning carriage

Agent records carry their version in `agdx.av`. Managed envelopes carry `v`, and `hello` also reports the supported versions (A12). The client uses discovery to reject unsupported operations before sending. The request version provides another check at the receiver.

The table defines the version carrier for each operation group. Hello slots and fenced-lease requests currently use version 1. Fenced leases also require their feature bit. If a later contract supports multiple simultaneous versions, it can use a minimum and maximum on the same slot.

| Surface | Mechanism | Carrier |
| --- | --- | --- |
| `query`, `control`, `checkpoint`, `kv`, `fork`, `agent`, `graph` | hello-negotiated | the `OpVersions` slot in the `hello` reply (A12), `0` or absent means not advertised |
| compare-and-swap, read-your-writes, strong consistency, fenced CAS, agent-workflow, keyword search, watch, authz | feature-gated | a bit in the `hello` `features` bitset (A12), not a version |
| `kv.lease`, `kv.lease_renew`, `kv.release`, `kv.cas_fenced` | payload-versioned and feature-gated | `v = KV_LEASE_OP_VERSION = 1` in the named-field request plus the `kv_fenced_leases` hello bit, both gates must pass |
| `batch`, `authz`, `client-metadata`, `presence`, `change` | body-versioned | the request/reply's own `v` first field, checked on decode (no hello slot) |
| the agent envelope | header-versioned | the `agdx.av` header, read before decode to select the decoder |
| the signature scheme, content-type, dead-letter reason, task state, agent error | not negotiated | a growable dictionary whose unknown values pass through (A7-style), so a new value never needs a version |

### B1.4 Operation dispatch and the command range

The Iggy binding maps each registered operation to a `u32` command code. Original Apache Iggy rejects unsupported managed commands. Standard streaming operations remain available.

| Op id | Code |
| --- | --- |
| `hello` | 1_000_000 |
| `backend_hello` (internal: the managed backend announces its `OpVersions` to the streaming server, not client-facing) | 1_000_001 |
| `set_client_metadata` / `get_clients_metadata` (the connection-scoped discovery channel: a connection advertises an opaque metadata blob, an `AgentPresence` for an agent, and a filtered, paginated read lists live connections with their metadata) | 1_000_002 / 1_000_003 |
| `batch` (mixed-operation, A5) | 1_000_020 |
| checkpoint mutation / destination get / destination list / query-route list | 1_000_021 .. 1_000_024 |
| `authz.whoami` / `list_roles` / `get_role` / `get_bindings` | 1_000_100 .. 1_000_103 |
| `authz.define_role` / `delete_role` / `bind_roles` | 1_000_104 .. 1_000_106 |
| `authz.history` | 1_000_107 |
| `query` | 1_000_200 |
| query page / cancel / status | 1_000_201 / 1_000_202 / 1_000_203 |
| `registry.get_projection` / `list_projections` | 1_000_210 / 1_000_211 |
| `registry.get_schema` / `list_schemas` | 1_000_220 / 1_000_221 |
| `registry.register_schema` / `decode_record` | 1_000_222 / 1_000_223 |
| `kv.get` / `set` / `scan` / `delete` / `delete_many` / `namespaces` / `cas` | 1_000_300 .. 1_000_306 |
| `kv.exists` / `expire` / `patch` / `lease` / `release` | 1_000_307 .. 1_000_311 |
| `kv.cas_fenced` | 1_000_312 |
| `kv.copy` / `move` | 1_000_313 / 1_000_314 |
| `kv.lease_renew` | 1_000_315 |
| `fork.create` / `delete` / `promote` / `list` / `put` | 1_000_400 .. 1_000_404 |
| `graph.query` / `upsert` / `neighbors` | 1_000_600 .. 1_000_602 |
| `agent.submit` / `cancel` / `status` / `list` | 1_000_700 .. 1_000_703 |

Authorization and system management use the first management block, `+100`, after internal and discovery commands. Feature blocks follow it. The base value of one million avoids collisions with ordinary Iggy codes. Blocks are 100 codes wide. These fixed numbers belong to this binding and its reference tests.

The server forwards CBOR requests to `laser-plane` through a local Unix socket. It attaches authenticated identity that the SDK cannot choose. `ForwardedQuery` carries the trusted user ID, client ID, audit correlation, and query envelope. Other operations use `ForwardedCommand`, with a command code and a retained field that no longer selects data.

Socket frames use `[len: u32 little-endian][named-field CBOR payload]` and a 64 MiB limit. `laser-plane` dispatches queries, registry reads, KV, forks, graphs, runs, and batches. Its projectors and state readers maintain models from the durable Iggy logs. Forwarded commands operate on those models.

Managed access uses grants independently of ordinary Apache Iggy permissions. A grant has the form `effect feature:action [on resource-pattern]`. A matching deny takes precedence over allow. Features include `kv`, `memory`, `projection`, `graph`, `query`, `fork`, `agent`, `workflow`, `authz`, `kv_lease`, and `kv_fence`. Actions are the closed set `read`, `write`, `delete`, and `admin`. New capability meanings belong to features rather than new actions.

Resource patterns are `all`, `literal`, or `prefix`. Only `all` matches a request without a keyed resource. Literal and prefix grants require a concrete resource and cannot authorize an entire list implicitly. `kv.lease`, `lease_renew`, and `release` require `kv_lease:admin` on the coordination namespace. Fenced CAS requires `kv:write` on the target and `kv_fence:read` on the coordination namespace.

Roles bind grants to the authenticated user ID supplied by the server. A role name contains at most 64 bytes. Allowed characters are ASCII letters, digits, `-`, `_`, and `.`. `validate_role_name` enforces this rule during define and bind operations in the SDK, server edge, and console. Replay does not reapply the name rule to stored state. Effective access combines role grants, then removes matching denies.

With managed authorization enabled, a user without roles has no managed access. The command code determines its feature and action. The request supplies the keyed resource, such as a namespace, fork ID, or projection ID. The server derives these values and rejects unauthorized requests before forwarding. It checks each operation inside a batch. The managed backend checks query and graph access to individual sources.

The established authorization operations store roles and grants in durable metadata state and restore them on startup. Enforcement reads the resident capability set. Namespace and prefix grants provide name-based separation within shared storage. Per-record ownership is not inferred. `authz:admin` or near-root server management permits role definition and binding. The server-management permission also provides initial administrative access before grants exist.

An authenticated caller can read its own `whoami` result. Role catalogs, binding lists, and history require `authz:read` or near-root server management. Signed `on_behalf_of` metadata names a delegated user (A9.6). Effective permission is the intersection of the agent grants and that user grants. Delegation cannot increase either set.

### B1.5 Low-latency features exploited

The binding uses standard Iggy framing, typed headers, numeric IDs, partition routing, and shared connections. Batch producers and chunk writers group records without changing their contents. Rust keeps reference-counted payload bytes on direct streaming paths. TypeScript creates Node `Buffer` views over `Uint8Array` at its Iggy boundary. Python retains the Iggy payload until it creates Python `bytes`.

Managed reads use the non-replicated extension path. The three managed authorization writes use dedicated replicated operations. These choices preserve the core payload encoding.

## B2. The Kafka binding (illustrative, roadmap)

Kafka provides topics, partitions, offsets, consumer groups, retention, log compaction, an idempotent producer, and transactions. Only the binding concerns change.

| Concern | Kafka realization |
| --- | --- |
| identity to address | a collection or topic maps to a Kafka topic, the offset is the address, a namespace maps to a topic-name prefix |
| agent ordering key | the conversation id becomes the record key, hashed to a partition, ordered per key. Generic streaming uses the caller-selected Kafka key or partition |
| `cause_at` locator packing | the slot is an opaque byte string, so a Kafka binding packs its own locator (topic name or UUID, partition, offset) into it with no envelope change. A consumer that cannot interpret it falls back to `cause` |
| out-of-band carriage | Kafka headers are name and byte-array pairs, untyped. The binding encodes the attributes into bytes (content-type one byte, version four big-endian bytes, conversation id sixteen bytes in a binding-declared order, names UTF-8) |
| operation dispatch | no command-code namespace. The operation name rides a header or the front of the payload, and the consumer dispatches on it |
| managed request and reply | no command channel. Managed ops ride a request topic and a reply topic correlated by the correlation id |
| state surface | a key-value store maps to a log-compacted topic keyed by the key, materialized into a state store. A tombstone is a null-value record. Compare-and-swap needs a single-writer-per-key processor or an external conditional store |
| dedup and exactly-once | the idempotent producer and transactions map onto the producer-dedup roadmap and the business idempotency key. The agent-id-to-group-id mapping is stated because Kafka group-id constraints differ from the 256-byte agent-name limit |
| query surface | not a Kafka primitive. Query, search, and aggregate are a managed layer above the log |

The payload, the envelope, the dictionaries, the validity matrix, and the result-code space are identical on Kafka and on Iggy. That identity is the proof the model is substrate-neutral.

## B3. Other substrates and the substrate requirement

- NATS JetStream can map subjects to topics and stream sequences to offsets. Durable consumers can represent consumer groups. JetStream KV can supply working state, while its headers remain untyped.
- Apache Pulsar can map topics, partitions, message IDs, and subscriptions to the model. Compaction can support state materialization.
- A single-stream broker can supply IDs, consumer groups, and replay. Its durability and partitioning depend on the chosen system. One stream remains one ordering unit.

A substrate needs an append-only log with partitions, offsets, replay, and key-based ordering. It must carry attributes with message bodies. It also needs request-reply operations or topic pairs that can provide them. Typed headers, local delivery, compaction, and transactions are optional. Queries remain a managed layer above the log.

## B4. The HTTP binding (management and UI)

The HTTP binding maps managed operations to REST routes for browser and WebAssembly clients. `laser-wire` provides a typed HTTP client over a transport supplied by the caller. It uses the same operation contract as the binary binding.

| Operation | Route |
| --- | --- |
| capabilities probe (`hello`) | `GET /capabilities`, never gated, answers the managed flags and per-surface versions for UI feature-detection |
| `query` | `POST /query` (a `Query` JSON body) |
| query execution status, page, cancel | `GET /query/{execution_id}` / `POST /query/{execution_id}/pages` / `POST /query/{execution_id}/cancel` |
| destination list, detail, create | `GET /destinations` / `GET /destinations/{id}` / `POST /destinations` |
| destination enable and disable | `POST /destinations/{id}/enable` / `POST /destinations/{id}/disable` |
| accepted destination operation | `GET /destinations/operations/{operation_id}` |
| destination checkpoint and issues | `GET /destinations/{id}/status` / `GET /destinations/{id}/checkpoint` / `GET /destinations/{id}/retention-gap` / `GET /destinations/{id}/prepared-attempt` |
| query routes | `GET /query-routes` |
| physical table and schema | `GET /destinations/{id}/table` / `GET /destinations/{id}/table/schema` |
| snapshots | `GET /destinations/{id}/table/current-snapshot` / `GET /destinations/{id}/table/snapshots` / `GET /destinations/{id}/table/snapshots/{snapshot_id}` |
| files and metrics | `GET /destinations/{id}/table/files` / `GET /destinations/{id}/table/metrics` |
| `registry.list_projections` / `get_projection` | `GET /projections?topic=&name_contains=&id_prefix=&search=` / `GET /projections/{id}` (the projection listing narrowed to row-kind, non-graph projections, the mirror of `/graphs`) |
| register / drop projection | `POST /projections` / `DELETE /projections/{id}` (control envelope) |
| `registry.list_schemas` / `get_schema` / `register_schema` / `decode_record` | `GET /schemas?name_contains=` / `GET /schemas/{id}` / `POST /schemas` / `POST /schemas/{id}/decode` |
| apply / remove binding | `POST /bindings` / `DELETE /bindings` (control envelope) |
| `kv.get` / `set` / `delete` | `GET` / `PUT` / `DELETE /kv/{namespace}/{key}` (`GET` replies the value as the raw response body with the optional expiry in the `agdx-expires-at-micros` response header, or `404` when absent. `PUT` takes the value as the raw body with `?expires_at_micros`. A scan page instead carries the base64url `KvEntryView` JSON, since a JSON array cannot hold raw bytes.) |
| `kv.cas` | `PUT /kv/{namespace}/{key}/cas?expect_version=&expect_absent=` (the value rides the raw body, a `409` with the current version on a precondition miss) |
| `kv.scan` / `delete_many` / `namespaces` | `GET /kv/{namespace}?prefix=&start=&end=&key_contains=&conversation=&limit=&cursor=` / `DELETE /kv/{namespace}?...` / `GET /kv` |
| `fork.list` / `create` / `delete` / `promote` / `put` | `GET` / `POST /forks`, `DELETE /forks/{id}`, `POST /forks/{id}/promote`, `PUT /forks/{id}/rows` |
| `graph.query` / `neighbors` | `POST /graph/{name}/query` (a `GraphQuery` JSON body) / `GET /graph/{name}/neighbors/{node}?dir=&edge_type=&depth=&limit=&as_of=&conversation=` |
| `agent.list` / `submit` / `status` / `cancel` | `GET /runs?agent_id=&state=&limit=&cursor=` (a `RunPageView` page, cursor base64url) / `POST /runs` (a JSON `AgentSubmit` body) / `GET /runs/{id}` / `POST /runs/{id}/cancel` (cancel records the intent and returns the run) |
| `registry.list_graphs` / `get` / register / drop graph projection | `GET /graphs?topic=&name_contains=&id_prefix=&search=` / `GET /graphs/{id}` / `POST /graphs` / `DELETE /graphs/{id}` (the projection listing narrowed to graph-kind projections, register and drop riding the control envelope) |
| `authz.whoami` / `list_roles` / `get_role` / define / delete role / `get_bindings` / bind roles | `GET /authz/whoami` / `GET /authz/roles` / `GET /authz/roles/{name}` / `PUT /authz/roles/{name}` / `DELETE /authz/roles/{name}` / `GET /authz/users/{id}/roles` / `PUT /authz/users/{id}/roles` (gated by the `authz` capability, B1.4. `whoami` reads the caller's own bound roles and effective grants, `list_roles` a JSON array of `Role` and `get_role` one `Role` or `404`. A role `PUT`/`DELETE` and a user bind (`PUT` a bare JSON array of role names) journal to the server-side authorization band, the reads forward like any managed read.) |

`RegisterGraph` and `DropGraph` update graph projections through the control topic (A11.2). Row and graph projections share one registry. `/graphs` lists graph projections, and `/projections` lists row projections. An ID route returns `404` for the other kind. Both list routes use `list_projections` and filter by kind.

For `/graphs`, `POST` registers, `DELETE` removes, and `GET` reads. `graph.upsert` writes node and edge data. `/graph/{name}/query` and `/neighbors` read that data. Registration routes do not write graph elements.

JSON requests and replies use representations of the CBOR types. Key-value keys use base64url in paths and query parameters because keys can contain arbitrary bytes. Values use the raw request body, with optional `expires_at_micros` in the query. Authentication uses the same identity boundary as the binary binding. Deployments can configure the route prefix. The default `/agdx` places `GET /capabilities` at `/agdx/capabilities`.

Each wire error maps to an HTTP status. Clients can use that mapping without parsing error text:

| Condition | Status |
| --- | --- |
| missing index, fork, or key | 404 Not Found |
| unsupported op, or the managed surface disabled | 501 Not Implemented |
| result or value too large | 413 Payload Too Large |
| malformed input or version skew | 400 Bad Request |
| a compare-and-swap or fork promote/squash conflict | 409 Conflict |
| a consistency level could not be met within the deadline | 503 Service Unavailable |
| managed backend failure or unreachable | 502 Bad Gateway |
| missing or invalid credential (unauthenticated) | 401 Unauthorized |
| authenticated but missing the grant (forbidden), or a step-up is required | 403 Forbidden |

A successful `2xx` response carries the inner `Ok` value. Other responses carry `{ code, message, detail? }`. `code` is the shared `ResultCode` (A7), `message` is display text, and `detail` is optional structured information. The status derives from the code.

Browse lists return JSON arrays through `GET /projections` and `GET /schemas`. ID routes return one object through `GET /projections/{id}` and `GET /schemas/{id}`, or `404`. They do not use the binary `BrowseReply` or `BrowseOutcome` wrapper. Registration returns the allocated ID.

The `http` and `http_client` modules own route constants, path builders, query parameters, error bodies, and the typed client. Callers supply a transport, such as `gloo-net` or `reqwest`. Shared types avoid separate route and encoding implementations.

HTTP capabilities use the grouped representation from A12. Queries can report paging, cancellation, and execution-status support. Destinations can report lifecycle, checkpoints, routes, schema, snapshots, files, metrics, and checkpoint-read consistency. Each feature defaults off. The server reports it only when supported.

Checkpoint mutations carry a bounded public request with an ID and expected revisions. Asynchronous work returns `AcceptedOperationView`. This contains operation and request IDs, state, timestamps, optional errors, and the destination or route result after success. A `2xx` response proves completion only when the operation state is `succeeded`.

---

# Part C. Rationale and roadmap

## C1. Design rationale

### C1.1 Invariants

Reference tests cover these implementation requirements:

- One CBOR encoding with named fields, and decoding fails on trailing bytes.
- The durable layer versions out of band, never with a body field.
- Dictionaries are pinned small integers, and an unknown code passes through rather than failing the record.
- Record identity is decoupled from log position, so a record keeps its id across replay, mirroring, and republish.
- The per-kind validity matrix is enforced at three layers (the wire validate function, the SDK constructors, and receivers) and pinned by positive and negative fixtures.
- A large or external body rides as a claim-check with a digest, verified against the fetched bytes.
- Capability negotiation returns `unsupported` for an unavailable surface.
- Framing is sans-io, pure functions over byte slices, with no transport of its own.

### C1.2 Why it is built this way

These decisions are settled. They are recorded here because they are not obvious from the field tables alone.

An `AgentEnvelope` keeps message data in the payload. Headers carry only content type, version, conversation routing, and an optional addressee. `source`, `cause`, `correlation`, `deadline_micros`, and `idempotency_key` remain in the envelope. Structured fields can exceed header limits, and header formats differ across substrates. Generic messages without an envelope use the provenance headers as their sole carrier (B1.2).

Each error maps to one `ResultCode` and HTTP status while retaining its specific details (A7). Generic clients use the code. Operation-specific clients can inspect the typed error.

`cause_at` is an opaque, binding-defined byte string, and `cause` is the portable identity. A foreign consumer that cannot interpret the locator falls back to `cause` (a portable id). This keeps the envelope substrate-neutral while letting each binding pack its native address. The Iggy binding packs its four-level position as 20 big-endian bytes.

Optimistic concurrency is an entry version plus a conditional write. The key-value store carries a version token, and `kv.cas` commits only against the expected version or absence (A10.3). It is gated by the `kv_cas` capability, so a backend that cannot do a conditional write returns unsupported rather than a wrong answer.

Read consistency is a per-query level. A query names `eventual`, `read_your_writes`, or `strong` (A11.3). A level the backend cannot serve returns `stale` or `unsupported` and is gated by the `read_your_writes` and `strong_consistency` capabilities.

`must_understand` lets one message demand strict handling without a version bump. The marker is a u64 bitset on the envelope (A9.1). A clear bit means ignore-if-unknown, and a set bit a receiver does not implement means reject. No bits are defined yet, so the mechanism is in place for the first feature that needs it.

## C2. What not to adopt

- Queue settlement operations, delivery-mode negotiation, visibility timeouts, and message priority do not define log behavior. Offsets, replay, and the reliable consumer provide the relevant delivery mechanisms (A1.5).
- Telemetry uses ordinary records with OpenTelemetry-aligned metadata and projections (A1.5). It does not add operation codes.
- The substrate owns framing, flow control, connection sharing, keepalive, and negotiation. AGDX must not add a parallel transport implementation.
- Cross-substrate distributed transactions. A transaction, if offered, is scoped to one connection and one responder.
- Mutable objects as the primary store. The state surface is a read model on the log. The log stays the source of truth.
- Agent-supplied fields remain claims. The capability owner enforces admission. A credential defines the identity available for access decisions.
- Model prompts do not enforce the security boundary. The command edge applies access rules (B1.4), and the effect boundary applies governance (C3). Decisions record evidence in the log.

## C3. Roadmap

The following proposals are not part of the settled contract and have no fixed reference encoding. Each needs data types, capability discovery, and unsupported behavior before adoption. Reserved fields can support later features, as the signature field did before SDK signing support.

| Proposal | Shape (draft) |
| --- | --- |
| strong-consistency semantics | the `strong` level is wired (A11.3) but its linearizable cross-replica semantics past read-your-writes are still being pinned |
| lease renewal | SHIPPED (A10.4): `kv.lease_renew` extends a held lease under the same holder, subject, and fence token without bumping the fence, and the whole fenced-lease family (holder identity, delegated subject, live-lease fenced CAS, barriered read) rides `KV_LEASE_OP_VERSION = 1` under the `kv_fenced_leases` feature bit |
| generalized causality token | an opaque happens-before token (a generalization of `cause_at`), recommended a hybrid logical clock, fail rather than reorder. Encoding still open until the cross-region backend proves it |
| signature activation and key registry (A9.5) | SHIPPED SDK-side (`sign` feature): the `Signature` envelope field activates with Ed25519 `SigningKey`/`KeyRegistry`, `Agent::builder().signing_key(..)` signs pickup and terminal replies, `LaserBuilder::verifier(..)` rejects an unsigned, unknown, or wrong-identity reply, and the managed `KvKeyRegistry` enrolls and snapshots verifying keys through the platform. Both SDKs share the same signing input and domain separator |
| content-block lifecycle and applied-through-offset ack | typed text, reasoning, data, tool-call blocks with start, delta, finish, and a reply naming the log offset a command took effect at (an offset, not a queue ack) |
| action governance hook | SHIPPED SDK-side (`ActionGovernor`): a pre-effect policy hook on agent sends, typed or raw topic publishes, requests and fan-out branches, the AGDX producer verbs (and through them the MCP/A2A bridges, `approval_gate`, and workflow dispatch), and memory writes. A vector-memory handle built from a governed `Laser` applies the hook before mutating its local index, and both log and vector item writes expose the proposed item body rather than their backend encoding. The hook sees the action kind, stream, topic, source and target, conversation, correlation, operation, tool, `on_behalf_of`, `purpose`, `data_classification`, the body, whether the record will be signed, and session counters. Its decision vocabulary is `allow`, `observe`, `block`, `step_up`, `modify` (applied before claim-check and signing), and `defer`. Chunk streams are exempt (per-chunk decisions are the wrong altitude). RBAC remains server-owned, the hook is defense in depth for regulated deployments |
| policy evidence capsule | the SDK emits each non-allow decision as a CBOR `event` (operation `policy_decision`) on the audit topic today: decision id, decision, mode, action attribution, reason, approved scope, policy pack/version/rules, risk score, a BLAKE3 receipt digest, the previous decision's digest (a per-conversation chain), outcome, and time. Both SDKs share the one encoder, so the shape is consistent without a wire pin. What remains roadmap is pinning it into the fixture corpus as a capsule once a non-Rust-core port needs byte-identical evidence |
| enforcement modes | SHIPPED SDK-side (`GovernorMode`): `observe` records what enforcement would have done and never impacts the effect, `enforce` applies the verdict, and an evidence-write failure on a proceeding enforced decision fails the call (a governed effect is never unrecorded). The mode is configuration, not an envelope claim, because an agent must not self-authorize weaker enforcement. A `step_up` that expires unanswered fails closed unless the deployment explicitly configures that governor to fail open. `progressive` (observe first, promote selected rules) remains the policy engine's concern above the hook |
| policy context metadata | SHIPPED (A9.6): pinned advisory metadata keys `purpose`, `data_classification`, `task_context`, and `session_intent`, signed when they must be trusted. They give a policy engine stable inputs without inventing a new prompt or telemetry surface |

## C4. Conformance and fixtures

A conforming client decodes and checks the complete envelope. It can produce only the kinds that its application needs. Positive and negative reference cases cover decoding and encoding. The base implementation needs CBOR, the 16-byte ID codec, constants, validation, and operation behavior. Optional signing adds cryptographic requirements.

A binding adds fixtures for three things only: the identity-to-address mapping, the operation dispatch, and the out-of-band header encoding. The payload fixtures are shared across every binding.

Application IDs such as `conversation`, `record`, `correlation`, and `channel` differ from Iggy message IDs and offsets. CBOR stores them as 16-byte big-endian byte strings (A3). An Iggy routing header stores the conversation through typed `Uint128`, which is little-endian (B1.2). Decode each carrier with its specified byte order. Reference tests cover both forms.

## C5. Stability and evolution

The contract is pre-1.0 and permits breaking changes without backward compatibility. Update every affected client, service, specification, and reference file together. Part A and the Iggy binding define the current contract. Roadmap entries remain proposals until adopted. Reserved fields do not promise implementation. The appendix describes the SDK API separately.

### C5.1 Right to forget (erasure posture)

Erasure depends on the store that holds the bytes. The substrate log is append-only, so retention controls removal of log records. A referenced body can follow its object store deletion policy while the log retains the `BodyRef`. Deleting an object is not itself proof that every backup copy is erased.

The managed plane can hide a deleted or superseded row after applying its record. A deployment can rebuild a projection while excluding selected records. AGDX does not promise in-place edits to retained log history. Deployments with erasure requirements must define retention and external-object deletion policies. This section describes deployment policy and adds no erasure operation.

### C5.2 Schema evolution

`RegisterSchema` assigns a permanent ID and rejects collisions (A11.2). `SchemaDef { id, source, name, version }` retains optional name and version metadata. Only the ID selects the decoder. A record carries its writer ID in `agdx.sid`. Readers resolve that ID even when a topic contains records from several schemas.

Register a new ID for a changed schema, then move producers to it. Existing IDs are not overwritten. Dropping a schema removes it from active registration and browsing but retains decoding for existing records. The registry does not enforce a compatibility mode. Applications choose their schema migration policy.

### C5.3 Cross-surface timeline

A conversation timeline reads relevant topics, filters by `ConversationId`, and orders the records by timestamp. It can include commands, responses, tool calls, memory writes, and run status. `ContextAssembler` provides this read pattern over selected topics. KV, memory, graph, and query remain separate views. The SDK does not add a separate `timeline()` operation.

## C6. Data-object operation map

AGDX expresses its data model as named operations rather than defining a second transport. The Iggy binding maps those operations onto Apache Iggy's streaming APIs and the managed command band per A1.2. This table records implementation coverage without claiming publication or adoption by an external standards body.

| Data-object op | Realized as | State |
| --- | --- | --- |
| `PUT` / `GET` / `DELETE` / `CAS` | `kv.set` / `kv.get` / `kv.delete` / `kv.cas` | shipped |
| `EXISTS` / `EXPIRE` / `PATCH` / `LEASE` / `RENEW` / `RELEASE` | `kv.exists` / `kv.expire` / `kv.patch` / `kv.lease` / `kv.lease_renew` / `kv.release` (A10.2, A10.4) | shipped |
| conditional `GET` / `DELETE` (`IF_MATCH` / `IF_NONE_MATCH`) | the `if_none_match` / `if_match` carriage on `kv.get` / `kv.delete` | shipped |
| `COPY` / `MOVE` | `kv.copy` / `kv.move` (A5): one backend transaction, move is copy plus source delete | shipped |
| `QUERY` / `AGGREGATE` / `COUNT` | the query IR (A11.3), `AggFunc` covering count/count_distinct/sum/avg/min/max/percentile/std_dev | shipped |
| `SEARCH` | the query IR's vector + filter (hybrid) plus the lexical `text` rider (A11.3), both landing relevance in the row score | shipped |
| `SCAN` / `LIST` | `kv.scan` + the registry browse + `kv.namespaces` | shipped |
| `PUBLISH` / `CONSUME` | `publish` + the log reader/cursor | shipped |
| `REGISTER` / `RESOLVE` | `RegisterSchema` (A11.2) + schema browse | shipped |
| `BATCH` | the mixed-operation batch (A5): per-op results, not atomic, nested rejected | shipped |
| authorization / access control | the `authz.*` band (A5, B1.4): `whoami`, the role define/delete/browse ops, and role binding, with the `effect feature:action on resource-pattern` grant grammar checked per command code | shipped |
| `BEGIN` / `COMMIT` / `ABORT` (txn) | optional, managed-plane only | roadmap |
| `EVENT` / `LOG` / `METRIC` / `TRACE` | telemetry is a published record plus the provenance OTel header dictionary (A6), not dedicated ops | convention |

`laser-wire` defines authorization types and codes, reference encodings, and grant-decision helpers. A compile-time assertion keeps the `Feature` count multiplied by `ACTION_COUNT` within 64 bits. Another assertion matches `ACTION_COUNT` to the `Action` variants. These prevent capability-bit overlap and omitted action rows.

Rust, Python, and TypeScript test their typed APIs and shared scenarios. The Iggy fork tests authorization replay, initial `admin` access, resource selection, batch decomposition, and rejection before forwarding. The managed plane checks access to each query or graph source. Role catalogs and binding lists require `authz:read`.

`Feature` and `Action` use `#[non_exhaustive]`. An unknown feature or action that enforcement cannot classify matches no grant. It must not create an allow decision. The `governance` examples run managed checks only when the deployment reports `authz`. Original Apache Iggy skips those managed phases.

`ActionGovernor` applies policy before an SDK effect. Decisions that are not allow produce linked evidence records. Shared client scenarios test this behavior. The hook can restrict agent actions but cannot grant access denied by server RBAC.

The draft status names map to A7 results. `CREATED`, `OK`, and `NO_CONTENT` map to `ok`. `VERSION_CONFLICT`, `ALREADY_EXISTS`, `LEASE_LOST`, and `TXN_CONFLICT` map to `conflict`. `NOT_FOUND` maps to `not-found`, and `NOT_IMPLEMENTED` maps to `unsupported`. `RESOURCE_EXHAUSTED` and backend faults map to `backend`, while `INVALID_ARGUMENT` maps to `invalid-argument`. `PARTIAL` uses the paging cursor rather than another status.

The draft push operations `SUBSCRIBE`, `DELIVER`, `ACK`, `NACK`, and `ACK_MODE` are not implemented. Delivery uses offsets, replay, duplicate suppression, and dead-letter records (A1.5, A9.5). `WATCH` and `NOTIFY` use the change feed from A11.8. Consumers read its topic by offset.

---

# Appendix. SDK API stability

The Rust SDK API and wire contract are separate. Both can change before 1.0 without preserving backward compatibility. The following conventions describe the current API:

- Use builders and fluent methods to construct SDK data. Public fields support result inspection and wire implementations. Before 1.0, fields and builders can change together.
- Wire-mirroring types keep public fields defined by the contract. Change those types together with the encoding and affected clients.
- End write builders with `.send().await` and read builders with `.fetch().await`. Use direct asynchronous methods when no builder is needed.
- Managed errors retain their typed wire details. Public error and capability types use `#[non_exhaustive]`, so matches need a wildcard arm. Growable u8 dictionaries use `Unrecognized(u8)` to preserve unknown numbers. `SchemaSource` and `RetentionPolicy` use lossy `Unknown` variants with `#[serde(other)]`. Do not resubmit these unknown variants as configuration. `ContentType` uses `from_code(u8) -> Option` for its encoded `agdx.ct` value.
- Add related operations to their existing feature handles rather than expanding the root client with unrelated methods.
- Accessors select scopes through `stream(name)`, `topic(name)`, `query(index)`, `kv(namespace)`, `fork(id)`, `graph(name)`, `memory(name)`, `context(conversation)`, `agent(id)`, and `runs()`. Methods act on those objects. Accessors perform no I/O. Required arguments are positional, and optional configuration uses fluent methods. Boolean opt-ins use `.thing()` rather than `.thing(true)`.
