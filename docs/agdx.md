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
   | fabric     agent envelope, sessions, runtime, memory (A9, A13, A15)     |
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

- Delivery is at least once, with replay by offset. Ordering is total within a partition. Agent records use the session, which is the conversation ID, as their partition key unless the stream declares a per-agent partition layout (A15.3). Other records use the selected partitioning.
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
| record / conversation / correlation / channel id | u128 | The payload uses 16 big-endian bytes. The display form is 26-character Crockford base32. The conversation routing header uses that string form. |
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
| `session.get` / `list` / `events` / `state` / `links` / `sources` / `changes` | coordination | managed reads of the session index that a deployment folds from the registered session topics of one stream (A15.10). Every request names its stream first. None of them mutates. Session lifecycle, state, and control are ordinary records on the log (A15), so there is no session write operation. A managed read model over the log, never a second source of truth |  |
| `filter.poll` / `ack` | streaming | read one bounded page of the records a consumer filter selects from one partition, and store its safe offset under a generation and ownership fence. Served by the streaming server itself (A14) |  |
| `filter.preview` / `test` / `validate` | streaming | judge stored records without progress, evaluate one supplied record, compile a filter (A14) |  |
| `filter.mutate` / `get` / `list` / `list_revisions` / `get_binding` / `list_bindings` / `operation` | streaming | the saved-filter catalog and consumer-group bindings, with operation-id idempotent mutations (A14.3) |  |
| change feed (no request op) | views | change notification over the read model: a projection binding opts in with `notify`, the projector publishes one change record per committed batch on the changes channel (A11.8, B1.1), and a consumer reads it by offset like any topic. Gated by the `watch` feature bit (A12), it adds no request op, so there is no `watch`/`unwatch` verb to register |  |

The run registry operations `agent.submit`, `cancel`, `status`, and `list` are retired. A submitted run is now a submitted session (A15.2), and a cancel request is a control record on `agent.control` (A15.5).

The memory API uses `remember`, `recall`, `improve`, and `forget`. These SDK methods combine `publish`, `query`, and `graph` operations (A13). They do not add wire operation codes.

Streaming uses substrate append and offset-read operations (A1.5). Agent messages also use six envelope kinds (A9). The envelope identifies the message kind. The registry adds no subscribe, consume-mode, ack, nack, or deliver operation.

## A6. Dictionaries (pinned codes)

Code dictionaries use fixed small integers. Existing codes must not be renumbered. An unknown code decodes to a value that preserves the original number for forwarding.

Content-type:

```
raw=0  json=1  msgpack=2  cbor=3  bson=4  avro=5  protobuf=6  arrow=7  ref=8  any=255
```

`ref` marks the body as a claim-check capsule (A9.5). `any` is a best-effort sentinel.

Task state, the agentic lifecycle, A2A-aligned with a LaserData paused extension:

```
submitted=1  working=2  input-required=3  completed=4  canceled=5
failed=6  rejected=7  auth-required=8  unknown=9  paused=10
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

The key-value and fork error enums define `NotLeader`. The SDK marks this error as retryable and `not_leader`. A caller can find the current owner and retry. Servers do not yet emit this variant. Its externally tagged encoding needs a coordinated rollout and capability selection before servers start emitting it.

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
| `parent` | u128, optional | parent conversation of a child session |
| `root` | u128, optional | root conversation of the session tree, requires `parent` |
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
| `usage` | token usage struct, optional | advisory accounting with input, output, optional reasoning and cache counts, and optional integer `cost_micros` |
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
| `target`, `cause`/`cause_at`, `parent`/`root`, `metadata` | O | O | O | O | O | O |
| `correlation` | R | R | O | R | O (R for `task`) | R |
| `channel` | X | X | X | R | X | O (stream terminal) |
| `sequence` | X | X | X | R | X | O (with `channel`) |
| `last` | X | X | X | O | O | X |
| `finish_reason` | X | O | X | O (with `last`) | X | X |
| `idempotency_key` | O | O | O | X | X | X |
| `deadline_micros` | O | X | X | O (opening chunk) | X | X |
| `task_state` | X | O | X | X | R (`task` or `session`) | O |
| `operation` | O | O | O | R on opening chunk, X after | R (`task`\|`session`\|`card`\|`progress`\|`quarantine`\|`unquarantine`) | O |
| `tool` | O | O | O | O | X | O |
| `usage` | X | O | O | O (terminal chunk) | O | O |
| `body` | R | R | R | R (empty only with `last`) | R for `session`, otherwise O | R |
| `signature` | O | O | O | O | O | O |

A `command` expects a reply or effect and requires `correlation`. An `event` does not expect a reply. Commands cannot omit correlation to request fire-and-forget behavior.

`usage.cost_micros` is the cost in micro-units of the deployment's configured currency. Producers convert a decimal amount by multiplying by one million and rounding half up, then reject values outside the unsigned 64-bit range. The integer amount stays advisory because agents write it themselves.

### A9.3 Closed sub-vocabularies

- A `status` uses `operation` to select `task`, `session`, `card`, `progress`, `quarantine`, or `unquarantine`. `task` requires `correlation` and `task_state`. `session` requires `task_state` and a CBOR body. `card` reports liveness and capabilities, and `progress` reports advisory progress. `quarantine` excludes the agent named in its body from routing. `unquarantine` restores that agent. Registry-topic write permissions control both operations.
- A `session` status carries `SessionStart` for `Submitted` and the first `Working`, `SessionEnd` for completed, canceled, failed, or rejected, and `SessionTransition` otherwise. `SessionStart` names the agent, SDK language and version, optional label, namespace, parent, root, idle timeout, token and cost budget, and tags. The label is at most 256 UTF-8 bytes with no control characters. Namespace and tags use their existing caps. `SessionTransition` can name an actor and the control position it acknowledges. `SessionEnd` can carry a reason and structured error. A terminal session status sets `last = true`, and other session status records leave it false. Parent and root cannot equal the record's own conversation. The root requires a parent. These IDs also ride `agdx.parent_conv` and `agdx.root_conv` as canonical Crockford strings in the header block.
- Chunk-stream purpose (`operation` on `sequence = 0`, required there, invalid after): `chat`, `reasoning`, `tool_args`.
- State sync convention (an `event`, never a new kind): `operation = state_snapshot` (body is the full state) or `state_delta` (body is an RFC 6902 JSON Patch). A15.8 defines how a reader applies them.
- Session operations use the open `operation` field with pinned names. Commands use `session_pause`, `session_resume`, `session_cancel`, and `force_cancel` for control (A15.5), `chat`, `text_completion`, and `generate_content` for model calls, `execute_tool` for tool calls, and `invoke_agent` for work handed to another agent. Events use `context_assembled`, `context_compacted`, `context_retrieved`, and `policy_decision`. These names follow the existing underscore style, and OpenTelemetry names stay as OpenTelemetry spells them.

`StateDelta` carries `base_revision`, an ordered patch, and a stable `op_id`. `StateSnapshot` carries `base_revision` and the complete JSON document. A patch has at most 256 operations. A patch body or state document has at most 8 MiB of JSON. JSON integers must fit the exact range from `-9007199254740991` to `9007199254740991` so Rust, Python, and TypeScript read the same value. Context manifests carry at most 1,024 fragments.

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
- `FoldSnapshot` saves the result of reading records into client-side state. It stores the stream name, stream ID, stream creation time, conversation, fold name, source offsets, and encoded state. Each source offset is a four-integer array with topic ID, topic creation time, partition ID, and last folded offset. The entries are sorted with no duplicates. A reader checks stream and topic creation times before it resumes at the last offset plus one. A new topic partition starts at zero. A changed source generation makes the snapshot stale. The fold name keeps two agents' snapshots separate. Registry state instead uses incremental `AgentCard` and `AgentPresence` updates with expiry.
- `Signature` is the optional envelope signature (A9.1). Its fields are `scheme` (Ed25519 = 1), an 8-byte `key_id`, 64-byte `bytes`, and optional `context`. `SignatureContext` contains `content_type` and `agent_version`. Signing input is `agdx.signature.v1`, the encoded context when present, and the canonical envelope with its signature absent.

Streaming producers use `ProducerPresence` on a separate shared observer connection through `AGDX_SET_CLIENT_METADATA` and `AGDX_GET_CLIENTS_METADATA`. The CBOR map contains `producer_presence_version: 1`, `observed_at_millis`, `expires_after_millis`, and bounded `producers`. Each `ProducerStatistics` reports a stable handle `instance_id`, `stream`, `topic`, first and last activity timestamps, submitted and confirmed record and payload-byte counters, nullable retries, failed calls, last successful call, and `latency` with samples and p50/p99/p99.9 microsecond bucket upper bounds. All values are SDK claims. The authenticated owner comes from the server's enclosing client record, not the opaque payload. The observer identity is not a publishing connection or node identity. Existing agent metadata remains unchanged.

A report includes at most 32 handles and fits within the existing 64 KiB metadata limit. SDK configuration can lower the handle limit or disable reporting. Expiration is three reporting intervals. Consumers of the payload must treat expired observations as stale and unknown or uninstrumented counters as unavailable. Synchronous confirmation counters include only fully successful calls and can omit a committed prefix from a failed batch. Background enqueue results do not establish confirmation counters. This node-local discovery does not enumerate producers across the cluster, and reported counters are neither authorization nor billing evidence.


A signed context binds `agdx.ct` and `agdx.av` to the envelope. Changing either observed header invalidates that signature. A signature without context uses the context-free input. The wire crate defines the data, while SDK code performs cryptographic checks. Keys bind to an authenticated principal rather than the claimed `source`.

When a verifier is configured, an unsigned reply, unknown key, invalid signature, or wrong signer must not resolve a correlated wait. Contracts, the reply dispatcher, `request_input`, and bridge task reads use this rule. They compare observed headers with the signed context and evaluate key validity at the server-recorded timestamp. Principal-bound routes use the authenticated principal for both discovery and reply checks. Accepted contract and fan-out results report that principal. An absent principal means no verifier was configured, not a failed check.

Keys have an agent or operator kind and a validity window. Quarantine and unquarantine facts require an operator key valid when the registry applies them. The registry ignores repeated record IDs so replay cannot apply the same control fact twice.

### A9.6 Pinned metadata keys

| Key | Type | Meaning |
| --- | --- | --- |
| `role` | string | chat role, recommended `user` / `assistant` / `system` / `tool` |
| `bridge_hops` | list of strings | the loop guard. A bridge appends its id and drops a message whose hop list already contains it |
| `run` | string | inert. The retired run registry read this key. The session index finds a record by its conversation header and has no gate key, so a record that still carries `run` is folded like any other record |
| `submitted` | bool | marks the first command of a submitted session. The agent that picks the command up writes the `Working` transition (A15.2) |
| `gen_ai.request.model` / `gen_ai.response.model` / `gen_ai.provider.name` | string | the requested model, the model that answered, and the serving provider of a model call (OpenTelemetry names) |
| `duration_micros` | u64 | the duration of a model or tool call, measured by the application around the call |
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

`kv.set`, `kv.cas`, `kv.delete`, and `kv.patch` may carry `session: SessionRef { stream, session }`. The stream is named rather than addressed by a reusable numeric ID. The serving edge validates that name against the trusted forwarded scope before storing the link. Older requests omit `session` and keep their existing bytes and behavior.

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

A fork ID contains at most 128 bytes. Allowed characters are ASCII letters, digits, `-`, `_`, and `.`. A stream-scoped ID `stream:<stream>/<local>` (A16) is also valid when both parts pass the same rule. `validate_fork_id` enforces the rule before SDK I/O and in the managed plane. A caller cannot use an arbitrary SQL identifier as a fork name.

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

The control envelope contains `{ v, timestamp_micros, command }`. Commands include projection, binding, schema, graph, and session-source registration. A graph projection registers through `RegisterGraph` so deployments can control graph registration separately. Schema IDs are permanent and cannot collide. Dropping a schema does not prevent decoding records that already reference it.

`RegisterSessionSource { stream, topics }` registers the session topics of one stream. `topics` is `All` or `Named([topic names])`. `All` covers the lane, `agent.control`, and the session satellites, and leaves out `agent.heartbeats` and implementation bookkeeping. `RemoveSessionSource { stream }` removes that stream's session registration. Session source registration never creates the stream. Registering a source replays its topics from the first retained record. Repeating either operation for the same stream is safe.

`RegisterRunSource` and `RemoveRunSource` are removed with the run registry. A deployment that replays a control topic holding one of them dead-letters that envelope once and continues.

### A11.3 The query IR

The query IR is a logical request compiled by the selected backend.

| Field | Meaning |
| --- | --- |
| `execution_id` | a nonzero 128-bit identity shared by execution, paging, status, cancellation, and errors |
| `target` | `operational { index }` or `lakehouse { destination_id, destination_generation, snapshot? }` |
| `deadline_micros` | one absolute deadline for the complete execution, never extended by page retrieval |
| `by_key` | exact-match key constraints, AND-composed |
| `message_type`, `time_range` | sugar for equality on the type field and a range on the timestamp with both bounds inclusive and the start before the end |
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
| `ChangeRecord` | `v` (op version, 1), `index` (the materialized index that advanced), `partition_id`, `from_offset` / `to_offset` (the inclusive source-offset window the batch committed), `rows` (rows written), optional `stream` (the stream whose source the batch read, set when the deployment publishes change records per stream) |

A change record reports that a view advanced through an offset range. Read the rows through `query` (A11.3) or the log. Notifications are best-effort after commit, so losing one does not lose the projected data. If a consumer misses the feed retention window, it reads the view directly.

Under stream tenancy (A16) each stream has its own changes topic, `stream:<stream>/_agdx/changes` on the ops stream, and a client scoped to a stream reads that topic.

The feed reports progress. Read-your-writes establishes whether the view includes a required write. The `watch` capability advertises feed support. Without it, the client rejects the request to open a feed before waiting on a channel.

## A12. Capability negotiation

A single connection negotiates what is available. A managed feature works against a managed implementation or returns `unsupported`.

- Run `hello` at connection time and again when refreshing capabilities. The reply reports versions for query, control, checkpoint, key-value, fork, agent, graph, and the filter catalog, plus feature bits. The current versions are 1. A zero version means the operation group is unavailable. Fenced leases also require their feature bit.
- `BackendDescriptor` reports versioned backend identity, mode, label, implementation, generations, configuration revisions, state, and readiness. It also reports materialization, query, type, time-travel, consistency, paging, cancellation, schema, maintenance, and limit support. It must not expose URLs, credentials, secrets, or mutable configuration requests.
- Readiness reports the current backend condition through stable reason codes. Unavailable or degraded backends retain their identity and capability descriptions. Refresh capabilities after startup races, failover, or backend restarts.
- SDK capabilities group features by their dependencies. `managed` indicates that a managed plane is connected. Managed groups include `query`, `destinations`, `kv`, `graph`, `forks`, `sessions`, the A2A gateway, and the saved-filter catalog (`filters.catalog`). The platform-native group is native consumer filters (`filters.native`), which the streaming server serves itself. Memory combines query and graph operations and has no separate capability.

`query.consistency` reports the strongest supported level: `eventual < read_your_writes < strong`. A stronger level includes the weaker levels. `kv.cas` reports conditional writes. `kv.cas_fenced` reports fence-protected writes. `kv.fenced_leases` reports holder-scoped acquisition, renewal, release, fenced CAS, and reads with a required mutation position.

The wire reply retains the flat `features` bitset. Its bits include `kv_cas`, `read_your_writes`, `strong_consistency`, `kv_cas_fenced`, `keyword_search`, `watch`, `authz`, `destinations`, `kv_fenced_leases`, `consumer_filters`, `group_policy_reads` (`1 << 11`), `sessions` (`1 << 12`), and `stream_tenancy` (`1 << 13`). `stream_tenancy` means the managed backend scopes every managed name to one stream (A16). `sessions` is set only by a server that serves the session reads (A15.10). Bit `1 << 4`, the former `agent_workflow`, is retired and never set, so an older client never reads it as run support from a server that no longer serves runs. `consumer_filters` is set by the streaming server itself when it serves filtered reads, with or without a managed plane, and the reply then names the served evaluator version and codecs (`filters`), so an SDK refuses to run a filter the server would evaluate differently. The saved-filter catalog additionally needs a ready backend that reports a nonzero `filter` version. SDKs convert these bits into grouped capabilities. HTTP reports the grouped form (B4). Without managed support, the corresponding capabilities remain off and calls return unsupported.
- If the reported operation version differs from the SDK version, reject the call before sending. Return the typed version error for that operation group.
- If an optional request field changes service behavior, require its capability before sending it. This includes the `consistency` field. A distinct command code can receive an explicit unsupported reply, but an unknown optional field can be ignored.

Do not send `kv.lease`, `kv.lease_renew`, `kv.release`, `kv.cas_fenced`, or `kv.get` with `min_position` unless the peer advertises `kv_fenced_leases`. Apply the same rule inside raw `batch` requests. A batch must not bypass capability requirements.
- The binary `hello` feature bit and corresponding HTTP capability must agree. A feature cannot be available through only one declaration of the same server capability.
- Features default to unavailable until the server explicitly reports support. HTTP defaults leave `kv.cas` and `graph` off and `query.consistency` at `eventual`. The binary reply uses zero feature bits and a zero `graph` version. Report a feature only when the backend can provide it.
- If the server and managed backend run separately, the backend supplies its own capability and readiness report. The server requests live `BackendAnnounce` data through their private socket for client hello and HTTP capability requests. After a failed probe, cached information can be returned only with unavailable status.
- `BackendAnnounce.ready` distinguishes readiness from configuration. A configured backend that cannot answer reports `ready = false`. If a later probe fails, retain known features and topology only as descriptive information. Mark the backend unavailable. Clients must keep its managed operations unavailable and support refresh without reconnecting. The encoded form omits `ready` when it is true.
- Optional `WireTopology` reports the ops stream, control, dead-letter, change-feed, and managed mutation topic names. The mutation topics are `kv`, `fork`, `graph`, and `checkpoint`. The `run` mutation topic is retired with the run registry. Explicit client configuration takes precedence over reported names. Each field has a default, so a partial report does not produce empty names. Omit absent topology from the encoded form.
- Create one stable identity for each logical Plane-served mutation, outside transport retry loops. Wrap it in `ManagedRequestEnvelope { v, operation_id, payload }`. `operation_id` is a required nonzero u128 with a ULID value. The server rejects bare or zero-identity mutations and preserves the identity when forwarding.

The deployment appends `MutationCommandEnvelope { v, operation_id, timestamp_micros, command_code, scope, payload }` to the managed mutation topic. `scope` is the optional server-stamped stream identity carried through replay. Each mutation topic has one partition until the contract defines cross-partition transactions. Only the deployment plane can publish there. The backend stores each outcome atomically with its effect, keyed by operation identity. Repeated identities return the saved outcome.

Reads can reconnect and retry. Mutations can retry only with their original identity. Do not retry deterministic rejection, such as invalid input or an oversized reply. Reject peers that cannot carry mutation identity. The three Iggy managed authorization writes use their existing `mutation_id` and dedicated replicated operations instead of the Plane envelope.
- The managed key registry uses a KV namespace, `agent.keys` by default. Its key is the lowercase hexadecimal form of the first 8 SHA-256 bytes of the verifying key. The value is `KeyRecord { v, principal, key_id, verifying_key, kind, valid_from_micros, valid_to_micros?, revoked }` with `v = 1`.

Every client reads and writes the same named-field CBOR form. Enrollment and revocation use compare-and-swap. A snapshot skips invalid records. Reject a record whose key ID does not match its verifying key.

## A13. Agentic memory and the knowledge graph

Agent memory combines publication, key-value state, queries, and graph operations. It adds no separate command range. Every memory write appends a `MemoryRecord` to a configurable topic, `agent.memory` by default. Its variants describe an item, forgetting an item, or feedback. Each scope maps to one partition.

The deployment builds a versioned key-value read view from the topic. Topic retention and read-view retention are independent. Default recall reads the managed view. Local topic folding is an explicit alternative for small deployments without that view. A local vector index supports similarity reads, and the graph supports relationship reads.

The read view is shared across principals. Managed capability grants control access at the command boundary (B1.4). Reading requires `kv:read` on the materialized namespace, while writing requires publication access to the memory topic. The fold does not infer per-record ownership.

Headers carry the logical namespace `agdx.mem.ns` and scope fields such as `agdx.mem.user`, `agdx.mem.app`, agent, and conversation. The view uses those scopes rather than the physical topic as its logical key. The fold also records the originating conversation from `gen_ai.conversation.id` and a `SourceRef` with numeric stream, topic, partition, and offset. These references survive renames and can locate the source while the log retains it. Conversation filters narrow reads and do not establish ownership.

The memory verbs (SDK facade, no wire op).

- `remember` appends an item to the memory topic. With duplicate suppression, the ID derives from the durable owner, kind, and body. Repeated content for that owner resolves to the same item. Graph entities come from `graph.upsert` or a bound graph projection that extracts nodes and edges.
- `recall` retrieves candidates, combines scores, optionally reranks, and returns the highest-ranked items. `auto` selects available signals. `recent` and `temporal` read by time, while `semantic` uses vectors and `keyword` uses lexical matching. `graph` traverses relationships. `hybrid` combines semantic and keyword ranks with reciprocal-rank fusion. Each result retains the contributing strategies, ranks, and original scores.

The embedded keyword engine ranks token coverage before term frequency. Reranking is supplied by the application, like embedding and consolidation. A managed plane can route recall using its known graph. Otherwise, the client selects from reported capabilities and its configured embedder, with recency as the fallback.
- `improve` records feedback that a ranking backend can use. Consolidation supplies further work such as summaries, relationship weighting, pruning, and fact extraction. Applications or managed backends implement this extension.
- `forget` appends a deletion record that removes the item from the read view. An optional cascade also removes derived graph nodes, edges, and vectors.

Memory kinds are SDK labels: `fact`, `message`, `summary`, `entity`, `feedback`, and `procedure`. `message` is episodic memory, `procedure` is procedural memory, and the other kinds are semantic memory. Lifetimes are `session` or `durable`. A `session` lifetime is scoped to one conversation, which is one session (A15). Scopes include `user`, `agent`, `session`, `app`, and the physical stream. An unset scope field broadens recall across that field.

A context handle selects one conversation. Its session-memory view uses that conversation for reads and writes without repeating it in each call. This uses the existing scope fields and adds no wire operation. Durable memory and graphs can span conversations. The context graph accessor returns the graph without applying a conversation filter.

`graph.upsert` may carry the same `SessionRef { stream, session }` link as a key-value mutation. The serving edge validates the stream name against its trusted forwarded scope before the plane stores the link.

`SourceRef::Message` may carry `generation`, the topic creation timestamp. A read by source position checks this value to detect a recreated numeric topic. A missing generation means the source identity cannot be proved. Memory item records may carry `origin: SourceRef` and `producer: ProducerInfo { name, version }`. A memory view row scope carries `timestamp_micros`, the broker append time of its source record, so a reader orders items from different partitions by that time and then by source position. `Forget` and `Feedback` records may carry `conversation`, the canonical conversation id the record is limited to. A limited record applies only to an item remembered in that conversation. A record without it applies to the item in any conversation. Graph nodes and edges may carry the same producer. These fields describe lineage and do not change content-addressed graph IDs.

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

## A14. Consumer filters

A consumer filter selects records on the streaming server, so a reader receives only the records it asked for, with their original offsets, ids, timestamps, headers, and payload bytes. Filtering never changes stored data or the standard poll. It is a separate read command that returns a standard polled-messages body with the non-matching records left out.

### A14.1 The filter

A `ConsumerFilter` holds an expression, a payload codec, a fault policy, the wire version `v`, and the `evaluator_version`. The expression composes `all`, `any`, and `not` over leaves:

| Leaf | Meaning |
| --- | --- |
| `pred` | Compare a payload field with `eq`, `ne`, `lt`, `lte`, `gt`, `gte`, `in`, `contains`, or `prefix` against a typed literal |
| `pred_as` | Compare after an explicit coercion: `number` (exact decimal) or `timestamp` in `rfc3339`, `epoch_seconds`, `epoch_millis`, or `epoch_micros` |
| `present` / `absent` | The payload path exists or does not. An explicit `null` is present |
| `header` | Compare one typed user header, keyed exactly |
| `text` / `header_text` | Match text by equality, prefix, suffix, contains, glob, or regex, with explicit case handling |

A field path separates keys with `.` and selects array elements with `[n]`, so `after.ground_stations[0]` reads the first station. A literal `.`, `[`, `]`, or `\` inside a key is escaped with `\`. There is one text form per path.

Truth is three-valued. A missing field or a type mismatch is unknown, `not` keeps unknown, and an unknown root selects nothing. `eq null` matches a missing field and `ne null` does not. Integers that fit 64 bits compare exactly, and an integer meets a double only when the double is an exact integer. Strings compare by code point. A numeric string is not a number and an RFC 3339 string is not an instant unless the filter says so with `pred_as`. An RFC 3339 value needs an explicit zone.

The codec is `json`, `cbor`, `avro`, `protobuf`, or `headers_only`. Avro and Protobuf require nonempty, sorted, unique `schema_refs` containing registered writer-schema IDs. Other codecs require an empty list. Each schema-first record selects its writer through the unsigned `agdx.sid` header. Schema resolution and compilation occur before scanning. Missing or unlisted writer IDs follow `foreign_policy`. An incompatible registered schema is a `schema_mismatch` decode fault when decoding is needed. Schema IDs are included in the policy digest.

CBOR accepts definite-length values with unique text map keys, no tags, and finite numbers. Bytes become integer arrays. Avro logical annotations use physical values. Protobuf uses schema field names, numeric enums, integer arrays for bytes, and exact signed/unsigned 64-bit integers. Proto3 scalar fields without presence tracking decode at their schema default, including zero and false. Unset fields with explicit presence and empty repeated/map fields remain absent. Legacy Protobuf groups are rejected. Schema definitions are bounded to 1 MiB and schema JSON nesting to 64.

A `headers_only` filter uses only header predicates, so the payload can be any format. Header predicates run before the payload is touched, and the payload is decoded at most once, only when the headers do not decide the record.

A payload that does not decode (malformed, over the size limit, or nested too deep) is a fault, separate from truth. The fault policy decides: `stop` (the default) ends the page in front of the record and leaves it unacknowledged, `pass` delivers it marked unevaluated, and `drop` skips it.

`foreign_policy` covers records with another content type or an absent or unlisted writer schema. `mismatch_policy` covers incompatible field types when the root remains unknown. Both default to `reject`, which skips the record, and accept `pass`, which delivers it unevaluated. A missing field alone is not a type mismatch. These policies remain separate from malformed-payload handling.

Text patterns support `equals`, `prefix`, `suffix`, `contains`, `glob`, and `regex`. Globs match the whole string with `*`, `?`, and escaped characters. Regex uses the server's bounded Rust engine syntax, and its verdict is the contract. Rust, Python, and TypeScript evaluate it locally with the same syntax, limits, and linear-time matching. TypeScript refuses the few Unicode properties V8 cannot express, such as `Age` and the break properties. Case-insensitive regex uses Unicode folding. Other text matches lowercase both sides.

The digest is SHA-256 over the domain tag `agdx.consumer-filter.v1\0` and the canonical JSON of the filter. Every semantic field is covered, so two filters share a digest exactly when their encodings are equal. The shared evaluator corpus covers verdicts and fault reasons, including exactly representable integers above 2^53. Caps: 8 KiB encoded, 128 nodes, depth 8, 16 path segments, 256-byte paths, 64 `in` items, 1 KiB string literals, and four compiled glob or regex predicates with 256 KiB per program.

### A14.2 Filtered reads

A `FilteredPollRequest` names the source, partition, consumer identity, policy selector, start, matching-record count, optional examined-record ceiling, reply byte cap and read mode. Its optional minimum catalog position carries a durable configuration operation receipt. The filter selector is `inline`, a saved `revision`, strict `bound`, or automatic `group`. Automatic group execution uses the active binding or an explicit unfiltered policy. Public SDK readers derive that selector from a consumer-group handle. The start is `next`, `first`, `last`, an `offset`, a `timestamp`, or `continue` with the previous page's continuation.

The server scans in bounded rounds and returns a `FilteredPage`: the matching records, the executed policy (digest, and filter id and revision for a saved one), the source generation, `next_scan_offset`, `safe_ack_offset`, the partition frontier, examined and matched counts, and the stop reason. A page stops when it is `filled`, when a `budget` is spent, at the `end_of_visible` records, at a `fault`, or at an `oversized_record` that alone exceeds the reply cap. A page can hold no records and still advance the scan. Pages with unevaluated records include optional `evaluation_limits` for the effective payload-byte and depth bounds. A local guard re-evaluates those records and requires a fault under the applicable `pass` policy. A legacy server without bounds cannot support local verification of pass-through decode faults.

The `primary` read mode reads from the partition primary through a data connection attached to the coordinator's consumer session. The data connection signs in on the node it dialed and never moves to the metadata leader. The owner proves the page came from the generation it names. `local` reads whatever replica the connection reaches and serves diagnostics only: a local page carries no `safe_ack_offset`, because a lagging replica can still hold records from before a committed purge, and its continuation carries the read mode, so it never resumes a primary read.

A `FilteredAck` submits the page's `safe_ack_offset` through a Primary data connection. The adapter checks the delivered source generation and the native offset attachment at owner admission, and a store that waits in the owner queue keeps the history it was admitted under and is refused at promotion after a purge. Stores are monotone. A revoked partition can finish accepted work while the native offset fence permits it.

A reader keeps at most 1024 unacknowledged record-bearing pages per partition by default, configurable in every SDK, and does not read that partition past the bound until it acknowledges, so read-ahead memory is bounded. A group reader joins over its own coordinator connection, so two readers are two members and a dropped reader leaves with its connection. A partition the group hands a reader after its build resumes after the group's stored offset, whatever start the reader was built with. A partition that fails, faults, or blocks waits one idle interval before it is read again, so the other partitions keep reading and a block clears once its record ages out. A `source_changed` or `conflict` restarts the partition from its stored offset once and reaches the caller.

Group pages, continuations, and acknowledgments also carry the actual `group_id`. An old page cannot acknowledge a deleted and recreated same-name group. The SDK keeps reader-instance ownership locally and rejects a page from another reader. These fields are part of the version-1 contract.

A preview judges stored records of one partition and explains each verdict. It joins no group and stores no offset. A sample test evaluates one supplied record. Validation compiles a filter and reports its digest.

### A14.3 The catalog and group bindings

The managed backend keeps saved filters. Each filter has a name, a description, a state (`active`, `archived`, or `dropped`), and immutable revisions. A mutation (`register`, `revise`, `describe`, `set_revision_enabled`, `archive`, `drop`, `bind`, `unbind`, `configure_group`) carries a caller-chosen `operation_id`. The catalog applies mutations in control-log order, authorizes each again against the catalog at that point of the log, and records each outcome, so a retry under the same id returns the first outcome. In JSON the `operation_id` is decimal text. Lists page newest first, and `before_id` pages stably while the catalog changes. The reply is `applied`, `rejected` with a typed error, or `pending` when the outcome is not recorded yet, and the operation id reads it later.

A binding pins a consumer group to one revision. The streaming server stamps the group's identity from its own metadata (stream and topic ids and creation times, and the group id), so a recreated group with the same name never inherits a binding. Every filtered read of a bound group runs the bound revision, and a reader that brings a filter with another digest is refused with `conflict`. Dropping a filter releases every group bound to it, each release advancing that group's policy generation like an unbind, and leaves a tombstone. Revisions, ids, and names are never reused.

The public SDK exposes group-scoped `configure`, revision, preview/test and `release` operations. The low-level wire retains exact-identity unbind and drop commands for administrative cleanup. A released binding does not erase its historical policy restriction. `ConfigureGroup` saves a definition owned by the verified group and binds it atomically in the catalog. Native group creation is a separate operation.

Catalog access uses the `filter` feature: `read` browses, `write` registers, revises, and describes, `delete` archives and drops, `admin` binds, unbinds, and enables or disables a revision. Filtered reads need only the ordinary Iggy permission to poll the source.

Automatic group pages explicitly identify filtered or unfiltered execution. Their policy generation fences transitions, including unbound → bound → unbound. Filtered mode carries a digest. Unfiltered mode carries none and does not decode payloads. Continuations and acknowledgments preserve those fields, and contradictory combinations are invalid.

Normal SDK group consumers set the matching-record ceiling and examined-record ceiling to their batch length. A batch of 100 examines at most 100 source records per partition request. Advanced readers may use independent match and scan ceilings. Empty selections retain safe progress and do not imply end of stream.

A configuration position contains partition, offset and an optional operation ID. JSON operation IDs use decimal text. The plane validates the operation receipt and its materialized revision, including across control-log offset resets. A head-confirmed lookup proves the examined record against current primary history. Acknowledgments reconfirm group policy and reject stale generations.

Group-owned grants scope to `stream/topic/group`. The automatic group capability is distinct from payload evaluation support. A server with disabled evaluation can serve an unbound group, but cannot broaden a bound group to unfiltered delivery. When the managed plane is disabled, a group read returns `catalog_unavailable` if the streaming server retains catalog history or the read requires a minimum catalog position. Otherwise the group reads unfiltered. Default SDK streaming includes the group transport. The optional `filters` feature adds local evaluation.

The HTTP management view includes `GET /agdx/group-policies/{stream}/{topic}/{group}` for a head-confirmed current policy, including explicit unbound status. It requires native source-read permission and managed filter-read permission for that group. A paused revision can be inspected without enabling new data reads. The existing group/member routes remain native.

### A14.4 Errors

Every failure is a typed `FilterError` with a shared `ResultCode` (A7) and a reason: `invalid_request`, `unsupported`, `version_skew`, `not_found`, `conflict`, `source_changed`, `membership_stale`, `not_primary`, `catalog_unavailable`, `too_large`, `unauthenticated`, `forbidden`, `unavailable`, `revision_disabled`, `capacity_exhausted`, or `backend`. A reason a client does not know decodes as `unknown`, and its code still classifies it. A request of another filter op version answers `version_skew`. `not_primary` and `membership_stale` mean the reader must route again, and `source_changed` means the source history is gone.

### A14.5 Catalog durability, identities, and reader checks

Catalog outcome lookup is scoped to the originating user. Reusing an operation ID with different mutation content returns Conflict. The plane stores results durably, independently of the control topic's message retention and the bounded memory cache. There is no lifetime operation-count cap. The default cache limit is 256 results, and zero disables caching. The fold carries recent outcomes across batches up to the configured cache limit. A cache miss reads the durable outcome store. Concurrent mutation admission defaults to 32 per plane and 8 per caller, with retryable refusals until slots are released. Deployment configuration controls these settings and the catalog resource limits. The internal `FilterCatalogCommand.limits` records the definition, tombstone, revision, and group-identity limits used for each command. Older records without that field use the original defaults of 1024, 4096, 256, and 4096 respectively, so changing configuration does not reinterpret history. Zero disables a resource limit. Definitions, bindings, and operation results survive control-topic message expiry through the plane's database snapshots.

Group consumers can be selected by name or numeric ID. `FilterConsumer::GroupId` serializes as `{ "kind": "group_id", "id": N }` and validates the native u32 offset-key range. Numeric membership, routing, acknowledgments, and page identity all use that ID. A same-name replacement group cannot inherit it.

A revision has an `enabled` flag, defaulting to true when absent in older state. `SetRevisionEnabled` requires scoped filter administration and leaves executable content and digest unchanged. A disabled revision refuses new production policy resolution with `revision_disabled`, mapped to retryable Unavailable. Internal `ResolveFilterPolicy.allow_disabled` permits already-delivered acknowledgments and diagnostic previews. Ordinary clients cannot invoke that internal resolver. A/B variants use independent native groups bound to their chosen revisions, because one native group's shared offsets cannot safely carry different policies.


Response validation always checks request identity, policy, source generation, mode, counts, and scan boundaries. The optional local guard adds payload re-evaluation. After membership loss, readers rejoin from committed progress and invalidate old page handles. Acknowledgment routing uses the native offset-routing operation so revoked partitions can drain without being pollable.

## A15. Agent sessions

A session is one conversation with a recorded lifecycle. Its id is the conversation id, so `gen_ai.conversation.id` names the session on every record and every existing conversation read finds it. The SDK noun is session and the wire noun stays conversation. Sessions add lifecycle, state, context, and control records to the existing envelope. They add no record kind and no write operation.

The stream is the isolation boundary. A session is addressed as `(stream, session)`. A session and its whole child tree live in one stream, and no link, derived id, or read crosses streams. The same conversation id written into two streams is two unrelated sessions. `SessionRef { stream, session }` names a session from a record that does not ride the session's own stream, such as a key-value or graph mutation. The stream is a name, never a reusable numeric id.

### A15.1 Identity and ancestry

- A labeled session derives its id from the stream name, the namespace, and the label, joined by the unit separator `\x1f` and hashed with the shared derivation. Two applications on one stream that pick the same namespace and label share a session on purpose. Separate namespaces or streams keep them apart. An unlabeled session gets a fresh id.
- Reopening a derived id does not reset a terminal session. A new lifecycle needs a new id.
- Work with its own lifecycle is a child session: a new conversation whose envelope sets `parent` and `root`, which also ride the `agdx.parent_conv` and `agdx.root_conv` headers. A root session sets neither. Contract children, A2A and MCP child calls, workflow steps, and compensations are child sessions. A workflow run is the root session of its steps, and a step's child id derives from the run id and the step label.
- Many agents can write in one session, each under its own agent id. The start record names the owning agent.
- A derived id carries hash bits where a fresh id keeps time. Readers order sessions by start time, never by id.

### A15.2 Lifecycle records

A `status` record with operation `session` carries the lifecycle on the session lane (A15.3). Its body is CBOR.

| `task_state` | Body | Written by |
| --- | --- | --- |
| `Submitted` | `SessionStart` | the submitter of a session handed to an agent |
| `Working`, first | `SessionStart` | the agent that starts the session itself |
| `Working`, later | `SessionTransition` | the agent that picks up submitted work, or that resumes after a pause |
| `Paused` | `SessionTransition` | each participating agent that acknowledges a pause request |
| `Completed`, `Failed`, `Canceled`, `Rejected` | `SessionEnd`, with `last = true` | the owning agent, or an operator through a forced cancel (A15.5) |

- `SessionStart { label, namespace, agent, sdk { language, version }, parent, root, idle_timeout_micros, budget { tokens, cost_micros }, tags }`. `SessionTransition { actor, acknowledges }` names the acting agent and the control record it acknowledges. `SessionEnd { reason, error }` carries a reason and a structured error whose `detail` can hold an exception type and traceback.
- A submission writes `status(session, Submitted)` and then a command addressed to the agent with metadata `submitted = true`, operation `invoke_agent` unless the submitter names another. The agent that handles that command writes `Working` with itself as `actor`.
- The first terminal record on the lane, by lane offset, wins among lane terminals. A repeated identical terminal record changes nothing. `status` records carry no idempotency key, so a retried end writes the record again.
- Records after the terminal record still count. A reader flags them `after_end` by broker time against the terminal record's address.
- A session without a start record is implicit: active, shown idle after its timeout, never completed. Its label is its id.
- Readers map task state to `SessionStatus`: `Submitted` to `submitted`, `Working`, `InputRequired`, and `AuthRequired` to `active`, `Paused` to `paused`, `Completed` to `completed`, `Canceled` to `canceled`, and `Failed` and `Rejected` to `failed`. `SessionStatus` is a snake-case string enum, and an unknown value decodes as `unrecognized`. The one spelling is `canceled`.
- Idle is derived at read time from the later of the last event and the last heartbeat, plus the session's idle timeout. It is never written, never terminal, and never replaces `submitted`, `paused`, or a terminal status. The next event makes the session active again.
- A budget breach is derived at read time from the start record's budget and the summed usage. `over_budget` is a flag, not a terminal state, because a deployment never writes customer topics. An agent that enforces its budget fails the session itself.
- The sum covers the `usage` of every enveloped record of the session: input plus output tokens against `budget.tokens`, and `cost_micros` against `budget.cost_micros`. The first start record's budget applies. A session is over budget only when a sum exceeds its set ceiling, so a session without a budget never is. A reader without the session index folds the retained lane by the same rule.
- SDKs enforce a budget at step and handler boundaries and end a session over its budget with `SessionEnd.reason = "budget"` and an error naming the ceiling. The budget check is eventually consistent, so a budget is a cooperative limit, not a hard spending cap.
- A dead-lettered record never fails a session by itself. It counts as an error and shows as `dead_letter` on the timeline. The dead-letter capsule keeps the record's conversation, including for a record whose body did not decode. A runtime configured with `fail_on_dead_letter` fails the session instead.

### A15.3 Topics, layouts, and routing

Each stream holds one session topic and a fixed set of satellites. The satellites stay separate because their readers, grants, and retention differ.

| Topic | Holds | Notes |
| --- | --- | --- |
| `agent.sessions` | the session lane: commands, responses, errors, user turns, model and tool records, lifecycle, state, and context records | keyed by session. Bootstrap requires an explicit retention and refuses a policy that never expires and has no size bound |
| `agent.streams` | chunk streams | collapsed into their response on a timeline |
| `agent.heartbeats` | process heartbeats (A15.4) | one-hour expiry. Never on a timeline and never folded into the session index |
| `agent.control` | control requests (A15.5) | keyed by session. Provisioning creates it with send permission for operators only. Agent bootstrap never creates it |
| `agent.memory` | memory records (A13) | the default memory topic |
| `agent.dlq` | dead-letter capsules | |
| `agent.audit` | policy evidence | |
| `agent.workflow_journal` | workflow step outcomes | |
| `agent.registry` | agent cards and registry facts | created by the first card |

Records describe themselves. Kind and operation live in the envelope, never in the topic name.

A stream picks one layout for its agents' work:

| Layout | Where work rides | Isolation between agents |
| --- | --- | --- |
| Shared (default) | `agent.sessions`, keyed by session | none. A server with filtered reads delivers each agent only its own and broadcast records |
| Per-agent topic | each declared agent reads its own declared topic, keyed by session | enforced by per-topic grants |
| Per-agent partition | `agent.sessions`, with a declared partition per agent | none |
| Single partition | every agent topic has one partition | none |

Routing rules:

- Lifecycle and state always ride the session's partition on `agent.sessions`, in every layout. That partition is the session lane.
- The session partition comes from the message key, which is the canonical conversation string.
- In the per-agent partition layout, a command, response, error, or chunk addressed to a declared agent lands on that agent's partition. A command is keyed by its addressee and a reply by its requester, who is the reply's addressee. Every other record rides the session partition. Partition ids are zero-based. The layout declares the mapping because hashing agent names can collide.
- In the per-agent topic layout, the layout declares a topic per agent. On a send to `agent.sessions`, a command addressed to a declared agent goes to that agent's topic, and a response, error, or chunk goes to its addressee's declared topic, keyed by session. A plain record with a target follows the same rule. Lifecycle, state, broadcast records, and records for an undeclared agent stay on the session lane. Bootstrap creates the declared topics with the lane's partition count and retention and never declares `agent.sessions` or `agent.control`. A declared agent reads its own topic plus `agent.control`, and a declared requester awaits replies on its own topic. Registering the stream as a session source covers every topic of the stream, the declared ones included.
- Control is keyed by session on `agent.control`. Heartbeats are keyed by process on `agent.heartbeats`.
- Membership is always an exact session id match. Partition placement is never a membership test.

Every record on a shared session topic carries `agdx.to`: the agent id it is addressed to, or `*` for every agent. A reader never parses `*` as an agent id. The headers-only consumer filter `agdx.to In [<self>, "*"]` lets a server deliver an agent only its own and broadcast records (A14). Its digest differs per agent identity, and the fixtures pin it for two identities and the broadcast-only case. A reliable consumer reads through one group per agent id and binds that group to this filter on `agent.sessions` and `agent.control` when the server resolves group policies, serves filtered reads and the filter catalog, and the client dialed the server itself from a connection string. It refuses a group already bound to another filter. Otherwise the group stays unbound and the client classifies every record. A filter is not an access boundary. In every layout the SDK keeps agents from misreading each other's records, and only separate topics with separate grants keep them from reading each other's records.

Within one partition, log order is exact. Across partitions, which happens in the per-agent layouts and on satellites, readers order records by broker append time and then by `(stream, topic, partition, offset)`. The producer's clock is never used for order. Offsets on different topics are never compared.

A session factory and its handles retain the stream creation generation, lane topic creation generation, and partition count. Before a lane write and each SDK retry, the SDK compares the current source with that retained identity. Lifecycle and state keep the session-key routing rule. A changed source returns `SessionError::Stale` before publication. A fresh managed handle also checks the registered lane. Recovery uses an explicit removal and registration, or an application migration. Existing handles retain their old identity after recovery. Native metadata checks do not serialize independent administrative changes with an append.

### A15.4 Heartbeats and leases

- A process that holds a lease on a session lists it in a heartbeat. The heartbeat is a `status(progress)` record on `agent.heartbeats`, keyed by the process id, with the CBOR body `SessionHeartbeat { process, stream, sessions }`. One record lists at most 2,048 sessions, and a larger set splits across records. A process with sessions in several streams writes one heartbeat per stream and never lists one stream's sessions in another stream's record.
- Starting a session or picking up submitted work takes a lease. Opening a session as a lens or handling a record does not. The lease scope is the stream name, the stream creation time, and the session, so a recreated stream is a new scope.
- The process beats at its configured heartbeat interval, 60 seconds by default, or at one fifth of the shortest idle timeout among its leases when that is shorter. The default idle timeout is 5 minutes. Heartbeats stop when the last lease is released.
- Heartbeats never enter the durable session index and never appear on a timeline. A process killed without a terminal record shows idle, never failed.

### A15.5 Control

- Control requests ride `agent.control`, keyed by session and addressed with `agdx.to`. An untargeted control record carries `agdx.to = *`, like a record on `agent.sessions`, so a filter-bound reader still receives it. Native send permission on that topic is the authority boundary. An operator may sign control records, which lets a verifying reader prove which operator sent one.
- `command` records with operation `session_pause`, `session_resume`, or `session_cancel` ask the session's agents to act. `force_cancel` is in the same reserved control set. An operator ends a session whose agent is gone by writing `status(session, Canceled)` with reason `forced` on `agent.control`. Readers resolve that forced terminal against lane terminals by broker time and address, never by arrival order.
- The same control operations found on any other topic are never applied. A timeline shows them as `unauthorized_control`, and a reliable consumer treats them as observational.
- Control is cooperative. A reliable consumer follows `agent.control` and records each pause and cancel request addressed to its agent or to every agent, rebuilding them from the retained control records when it first sees a session. The follower reads every partition of `agent.control` directly, outside any consumer group, so every instance of a role sees every request. A handler reads the recorded requests and decides when to stop, and the runtime never interrupts a handler. A workflow checks for a cancel request at every step boundary and then ends its root session as canceled. After the cancel check it reads whether the root session is over its budget and ends it failed with reason `budget`. A requester's identity in a control record is a claim unless the record is signed.
- An agent may also sign its terminal record. The managed session index verifies a signed record against the stream's key registry when it folds it and reports the verifying principal as `verified_actor` on the event. An unsigned record has none.

#### A15.5.1 Pause and resume

Pause means no new actions in the session until it resumes. An action already in flight finishes and is recorded. Pause is cooperative: the server cannot stop a client from appending.

1. Request. `command(session_pause)` on `agent.control` carries a JSON `SessionPauseRequest { participants }`, the agents whose acknowledgments complete the pause. The set is frozen when the request is written. The SDK fills it from an explicit list, or else from the session lane: the addressees of the session's work commands and the agents that picked the session up. An empty set names no agent. A reader shows `pause_requested`.
2. Acknowledge. A named participant writes `status(session, Paused)` with a `SessionTransition` whose `acknowledges` is the exact position of the request on `agent.control`. An agent outside the set acknowledges the same way when it first receives work for the paused session.
3. Hold. An agent that receives work for a paused session parks it before it commits the source: it appends `event(session_parked)` with a CBOR `SessionParking { source, role, request }` on the lane and confirms it. The source address with its topic generation, the agent, and the request position identify one parking, so a duplicated parking after a crash is folded once. A failed parking publish leaves the source uncommitted. Parking never marks the work handled.
4. Resume. `command(session_resume)` lifts the pause. Each agent acknowledges with `status(session, Working)` naming the resume request, rebuilds its held set from parking and completion facts on the lane, handles each held record at least once before new work, and appends `event(session_unparked)` with the same body after each one. A crash after the effect but before the completion record can repeat the effect, so handlers still need idempotent effects or a fenced write.
5. Recovery. At startup, after a rebalance, and after a reconnect, an agent rebuilds control and held work from a bounded read of the lane. A truncated read or a held record whose source expired or was recreated is reported as incomplete, never dropped silently.
6. Cancel while paused. The session ends canceled. Held records are not handled and stay listed as held.

The managed index counts the held records that no agent has reported handled as `SessionInfo.held`. No capability advertises the pause runtime yet.

### A15.6 Display mapping

A timeline type is derived from the record and never stored as a separate field. The wire owns one mapping, mirrored in every SDK and used by the session index:

| Display type | Record | Topic |
| --- | --- | --- |
| `session.submitted` | `status(session, Submitted)` | lane |
| `session.started` | `status(session, Working)` with a start body | lane |
| `session.resumed` | `status(session, Working)` with a transition body | lane |
| `session.paused` | `status(session, Paused)` | lane |
| `session.parked`, `session.unparked` | an `event` with operation `session_parked` or `session_unparked` | lane |
| `session.completed`, `session.failed`, `session.canceled` | a terminal `status(session)`. `Rejected` shows as failed | lane or `agent.control` |
| `session.heartbeat` | `status(progress)` | `agent.heartbeats` only, never a timeline row |
| `session.control` | a control `command` | `agent.control` |
| `unauthorized_control` | a control `command` | any topic other than `agent.control` |
| `user.message` | a `command` or `event` with metadata `role = user` | any |
| `model.request` | a `command` with operation `chat`, `text_completion`, or `generate_content` | any |
| `model.response` | a `response` with one of those operations | any |
| `model.stream` | a `chunk` with operation `chat` or `reasoning` | any |
| `tool.call` | a `command` with operation `execute_tool` | any |
| `tool.result` | a `response` or `error` with operation `execute_tool` | any |
| `agent.handoff` | a `command` with operation `invoke_agent` | any |
| `state.updated` | an `event` with operation `state_delta` or `state_snapshot` | lane |
| `context.assembled`, `context.compacted`, `context.retrieved` | an `event` with the matching operation | any |
| `policy.decision` | an `event` with operation `policy_decision` | any |
| `memory.created`, `memory.forgotten`, `memory.feedback` | a `MemoryRecord` item, forget, or feedback | memory topic |
| `task.status` | `status(task)`, or a session status in any other task state | any |
| `workflow.step` | a workflow journal record | journal topic |
| `error` | any other `error` | any |
| `dead_letter` | a dead-letter capsule | DLQ topic |
| `undecodable` | a record whose body does not decode | any |
| `invalid` | a record whose header and body name different identities, such as another conversation or author | any |
| `kv.set`, `graph.upsert` | a managed mutation that carries a `SessionRef` | no timeline position |
| `agent.message` | any agent record that matches no row above | any |

### A15.7 Dispatch classification and reply matching

A reliable consumer classifies every record before its handler sees it. The wire owns one classification, pinned by the shared table `dispatch_cases.json`, so every SDK and the session index agree:

| Dispatch | Records | What a reliable consumer does |
| --- | --- | --- |
| `work` | a `command` addressed to this agent, to every agent, or to nobody, outside `agent.control`, for an operation the handler serves | runs the handler |
| `observational` | an `event`, a `command` for an operation the handler does not serve, or a control operation outside `agent.control` | skips and commits |
| `reply` | a `response`, `error`, or `chunk` | skips. Reply waiters read it |
| `lifecycle` | any `status` record | skips |
| `control` | a control operation on `agent.control` addressed to this agent or to every agent | hands it to the control follower |
| `foreign` | a `command` addressed to another agent, or a non-control `command` on `agent.control` | skips and commits |

The author is never a discriminator, so an agent may send work to itself. A record without an envelope has no kind: it is `foreign` when addressed to another agent or found on `agent.control`, a `reply` when it carries both a causal parent and a correlation, and `work` otherwise. A response, status, chunk, state, or accounting record never becomes work because its addressee matches.

A waiter for a correlated reply accepts a record only when all of these hold:

- It carries the request's correlation and belongs to the request's session.
- An envelope record is a `response` or an `error`, never a `command` or a `chunk`.
- When the request named its sender, the reply is addressed to that sender. A reply without an addressee does not answer such a request.
- It is not the request itself. A request and its replies may share a topic.

A reply carries the request's record id as `cause` and the request's full log position as `cause_at`, and it is addressed to the requester.

### A15.8 Session state

Session state is one JSON document per session, folded from `state_delta` and `state_snapshot` events on the lane in lane order. The lane gives one total order, so every reader that applies the same records gets the same document.

- A delta carries an RFC 6902 patch, `base_revision`, the revision its writer last saw, and a stable `op_id`. A reader applies the whole patch to the current document at once. If any operation fails, the patch is `rejected` and the document stays unchanged. A delta whose `op_id` was already applied is `duplicate`, so a retried non-idempotent patch, such as an array insert, applies once.
- A snapshot carries `base_revision` and the complete document. It replaces the document, removing absent keys, only when its base revision equals the applied revision. Otherwise it is `stale` and the current document stays, so a delayed snapshot from one agent cannot overwrite another agent's later delta. A snapshot can also establish the baseline when the retained lane no longer holds the session's earlier state records.
- Each applied record advances the revision by one. Each outcome is kept in the state history with the revision, `op_id`, outcome, source position, broker time, the document digests before and after, and the failure reason.
- The SDK writes a snapshot after every 64 deltas and before `end` when the state changed. These snapshots are checkpoints, not overwrites.
- Append success does not prove a patch applied. A writer that needs proof reads the state view at or after its record's position.
- Without a managed deployment, the SDK folds the retained lane from the newest snapshot and reports `complete: false` when the records the document starts from are no longer retained.

`SessionStateView { revision, document, history, frontier, complete }` is the read reply. `StateChange { revision, op_id, outcome, at, broker_ts, old_digest, new_digest, reason }` is one history row, and `outcome` is `applied`, `rejected`, `stale`, or `duplicate`.

### A15.9 Context, model, and tool records

- A model call is a `command` with a model operation, addressed to the writing agent, with metadata `gen_ai.request.model` and optional `gen_ai.provider.name`. Its result is a `response` with usage, `gen_ai.response.model`, the finish reason, and `duration_micros`, or an `error`. One logical call has one correlation across its request, manifest, and result, so a retried helper never charges twice.
- A tool call is a `command` with operation `execute_tool`, the tool name, and the arguments as JSON. Its result is a `response` or `error` with the same correlation and `duration_micros`.
- `event(context_assembled)` carries `ContextManifest { policy, policy_version, fragments, tokens, bytes, frontier, correlation }` with the model call's correlation. Fragments are ordered. A fragment is a log message, a memory item, a key-value entry, a state key, or a summary, each with its address, version or digest where one applies, and its token and byte size. The frontier lists `(topic, partition, offset)` per source. A manifest holds at most 1,024 fragments.
- `event(context_compacted)` carries `ContextCompaction { summary_at, covered, summarizer }`, where `covered` lists `(topic, partition, first, last)` ranges.
- `event(context_retrieved)` carries `ContextRetrieval { query, items }`, with each memory item's id and score.
- Every SDK and the session index use one byte-based token estimate: the byte count divided by four, rounded up. The fixture `token_estimates.json` pins it.
- Usage rides once per logical call, on its result, never on a command. Model calls are counted from model commands and tool calls from `execute_tool` commands.
- Index summaries never hold values. A tool call keeps its tool name, sorted argument key names, size, and content hash. A tool result keeps status, latency, and size. Model records keep model, provider, usage, and finish reason, never prompt or completion text. Context records keep references, ids, and scores. Raw values stay on the log under its native read permission.
- The SDK redacts tool arguments and JSON model request bodies before publishing. The default redaction drops the values of `authorization`, `api_key`, `token`, `password`, `secret`, and `cookie` at any depth. It is a convenience, not a guarantee.

A memory item written through a session may carry `origin`, the record that motivated it, and `producer`, the agent or policy that wrote it (A13). A key-value or graph write stamped with a `SessionRef` links its key or element to the session.

### A15.10 Read surface

A deployment that registers a stream as a session source folds its session topics into a session index and serves seven reads. All of them are managed reads with no mutation.

| Op id | Code | Request | Reply |
| --- | --- | --- | --- |
| `session.get` | 1_000_710 | `SessionGet { stream, id }` | `SessionInfo` |
| `session.list` | 1_000_711 | `SessionList { stream, status?, agent?, text?, root?, label_prefix?, cursor?, limit, want_total }` | `SessionPage { items, cursor, total, searched, truncated }` |
| `session.events` | 1_000_712 | `SessionEvents { stream, id, cursor?, limit, fixed_frontier }` | `SessionEventsPage { items, cursor, ranges, frontier, fixed_frontier, gaps }` |
| `session.state` | 1_000_713 | `SessionState { stream, id, history_limit }` | `SessionStateView` (A15.8) |
| `session.links` | 1_000_714 | `SessionLinks { stream, id, surface? }` | `SessionLinksView { links, frontier, truncated }` |
| `session.sources` | 1_000_715 | `SessionSources { stream, id, lane_only? }` | `SessionSources { sources, lane? }` |
| `session.changes` | 1_000_716 | `SessionChanges { stream, after, limit }` | `SessionChanges { rows, floor, resync }` |

- Every request names its stream first, and the stream is never optional. A list across streams does not exist. A zero `limit` or `history_limit` leaves the size to the server.
- `SessionInfo` holds the stream, id, label, namespace, owning agent, parent, root, status, the derived `idle`, `over_budget`, `pause_requested`, and `cancel_requested` flags, start, end, first and last event and heartbeat times, counts of events, model calls, tool calls, errors, input and output tokens, cost in micro-units, the budget, the SDK, partial-view flags (`label_truncated`, `overflow`, `events_truncated`, `lane_conflict`, `rebuilding`), and the fold frontier.
- `SessionInfo.held` counts the work records held while the session was paused and not yet handled. `SessionFlags.liveness_unknown` is set while the server's heartbeat tail has not caught up, so `idle` is not meaningful yet.
- `SessionList.root` narrows a list to the tree rooted at one session, and `label_prefix` to labels with that prefix.
- `SessionEvent { at, session, broker_ts, kind, operation, display, agent, addressee, correlation, cause, tool, usage, after_end, verified_actor, summary }` is one timeline row. `verified_actor` names the principal whose enrolled key verified the record's signature. Its summary follows the value rule in A15.9.
- Every reply derived from the index carries the fold frontier per source. `SourceFrontier { topic_id, topic_generation, partition_id, folded, head, retained_from }` tells a reader whether a view is settled or still catching up. `SourceGap { topic_id, topic_generation, partition_id, from, to, reason }` reports a missing inclusive range: `expired_before_fold`, `pruned`, `truncated`, or `rebuilding`. `fixed_frontier` pins the frontier of the first page so a bounded walk stays consistent. Cursors are opaque and bound to the stream generation, the filters, and the frontier.
- `PayloadRange { topic_id, topic_generation, partition_id, first, last }` is a hint for fetching payloads with bounded polls, not a promise that every offset in it belongs to the session.
- A link names a surface (`memory`, `kv`, `graph_node`, `graph_edge`, `projection`, or `child`), a resource, an item, a relation (`wrote`, `recalled`, or `touched`), and the first and last positions.
- The change feed is a per-stream table, not a topic. `SessionChangeRow { seq, sessions, positions, truncated }` lists the sessions one committed fold batch touched. A reader polls with the last sequence it saw. `resync` is set when that sequence is below the retained `floor`, and the reader then lists sessions again.
- A read failure is `SessionError`: `unsupported`, `not_found`, `not_registered`, `invalid`, `unauthorized`, `stale` for a stream or topic generation the index does not hold, `backend`, or `unavailable`. Each maps to its `ResultCode` (A7).
- The `sessions` capability bit (`1 << 12`) is set by a server only when it serves these reads. A client starts with it off.
- The SDKs expose these reads on the session factory as `get`, `list`, `events`, `state`, `links`, `sources`, `changes`, and a polling `watch` over the change feed, plus `status()` on one session. A failed read surfaces the typed `SessionError`.
- A read by source position (`read_at`) uses a standard poll, not a session code. It checks the topic's creation time against the reference's `generation` and the returned offset against the requested one, and reports a missing record rather than a later one.
- Without a managed deployment, an SDK reads one session through bounded standard polls of the lane. Listing sessions needs the index and returns unsupported.

`SessionSources.lane_only = true` checks only the registered lane identity. Its reply contains no source rows or history, and `lane` is the three-integer tuple `(topic_id, topic_generation, partitions_count)`. The AGDX edge requires native send permission on `agent.sessions` and stamps the verified stream scope. SDK write guards use this mode so a writer does not need aggregate session read grants. Normal Sources reads and the HTTP route retain aggregate read authorization. An uninitialized registration returns unavailable, and a changed pin returns stale.

Authorization (B1.4): every session read needs `session:read` on `stream:<name>` and native read authority over the whole stream, so a reader limited to some topics of the stream gets `unauthorized` for the aggregate view. Source registration needs `session:admin` on `stream:<name>` and the same stream-wide read authority. The server stamps the trusted `ForwardedScope` on each request (B1.4).

### A15.11 Limits

| Item | Limit |
| --- | --- |
| session label | 256 B, valid UTF-8, no control characters |
| session namespace | 128 B, the key-value namespace rule |
| session tags | 16 tags of at most 64 B, the memory tag caps |
| sessions per heartbeat record | 2,048 |
| state patch operations | 256 per delta |
| state patch body or state document | 8 MiB of JSON |
| state JSON integers | from `-9007199254740991` to `9007199254740991` |
| context manifest fragments | 1,024 |

## A16. Stream-scoped resource names

An Iggy stream is an isolation boundary. Managed resources that belong to a stream carry the stream in their name: `stream:<stream>/<local>`. The bare stream resource is `stream:<stream>`. Stream names never contain `/`, and a local part may. The wire owns the helpers `scoped_resource(stream, local)`, which returns a name that already starts with `stream:` unchanged, and `split_scoped_resource(name)`.

- A client with a default stream scopes every managed name it sends by default: key-value and memory namespaces, lease and fence namespaces (including `agdx.workflow.fence`), the key registry namespace (`stream:<stream>/agent.keys`), graph names, projection IDs and index names, query indexes, fork IDs, and change-feed index filters. A name the caller already scoped is sent as is. A client without a default stream sends bare names.
- Memory scopes its namespace with the stream its memory records ride, both in the `agdx.mem.ns` header and in the read view, so writes and reads meet.
- Listings return only the caller's own scoped names, with the prefix stripped, so a caller keeps seeing its local names. Names under another stream's prefix are never returned.
- The projection selector header `agdx.ref` stays the local projection ID. The managed backend resolves it against the bindings of the record's own stream and topic, first as written and then as `stream:<stream>/<ref>` with the stream the record was read from, so a scoped projection matches a bare header and a record never selects another stream's projection.
- Consumer filter catalog names are not scoped by the client, because the client never chooses one. A group's own filter is named from the group's verified identity, which includes the stream ID and the stream creation time. A `Register` mutation carries its name as written: the streaming server authorizes a scoped name against the named stream, and in stream tenancy the managed backend refuses a bare one.
- Writer schema IDs stay `u32`. Schema control and browse requests carry `stream`, and the registry keys a schema by `(stream, id)`. `ControlEnvelope.stream` carries it for `RegisterSchema` and `DropSchema`. A consumer filter names the registry of its `schema_refs` with `schema_stream`.
- `KvScan`, `KvDeleteMany`, `GraphQuery`, and `GraphNeighbors` carry `stream` next to the conversation lens.
- A client can opt out per connection with bare naming, which sends every name exactly as written, the 0.6 behavior.
- The server stamps `ForwardedScope` (B1.4) on every request whose resource is `stream:<name>` or `stream:<name>/...`, refuses an unresolved stream, and refuses a request or batch that names resources in two streams.
- A stream deleted and created again under the same name is a new stream. Its numeric ID may be reused, so the stamped creation time tells the two apart. The managed backend binds every row under `stream:<name>/` (key-value entries and their versions, memory views, leases, graphs, and forks) and every writer schema keyed by the stream to the stream generation that wrote it. The first request or fold stamped with a newer generation removes the older generation's rows before anything is served. A request stamped with a deleted generation is refused as `unavailable`, and a retry is stamped with the live stream. A deleted stream's rows are removed even when nothing names the stream again. Writer schema drops wait until the control registry has replayed, and schema requests return `unavailable` until that replay finishes. Fence counters are kept, so a fencing token granted in a deleted stream never matches a later lease. Memory and projection folds follow the generation too, so a recreated stream or topic is folded from its first record. A bare name belongs to the deployment and is never removed this way.
- A deployment in stream tenancy mode announces `stream_tenancy` (A12). It rejects an unscoped name on every managed surface with a typed invalid error and checks that the stamped scope matches the name's stream. It refuses a role grant whose pattern spans streams, such as a whole-feature grant or a prefix outside one `stream:<name>/`, with `AuthzError::TenancyViolation`, which maps to `invalid_argument`. Each stream's change records and projector dead letters ride their own ops topics, `stream:<stream>/_agdx/changes` and `stream:<stream>/_agdx/dlq`.
- A deployment in the default deployment mode accepts both bare and scoped names.

# Part B. Bindings

A binding maps logical identities to addresses and defines attribute encoding, command dispatch, request-reply transport, and the `cause_at` locator. The remaining rules come from Part A. Reference tests define the expected binding-specific representations.

## B1. The Iggy binding (normative)

### B1.1 Identity to physical address

| Logical | Iggy address |
| --- | --- |
| streaming record | stream, topic, partition, offset |
| agent ordering key | the canonical conversation string as the message key, so a session's records share a partition. A declared per-agent partition layout places addressed work on the addressee's partition (A15.3). Generic streaming preserves order within the caller-selected Iggy partition |
| collection / topic | a topic on a data stream |
| managed ops | a reserved command range against the connection (B1.4), not a topic |
| `cause_at` locator packing | the four-level (stream, topic, partition, offset) address as 20 big-endian bytes in the opaque locator slot |

The Iggy binding uses `_agdx` for its ops stream. `control.commands`, `dlq`, and `changes` carry projection control, dead letters, and change notifications. Use the shared constants for these names. `Laser` also provides `ops_stream`, `control_topic`, `dlq_topic`, and `changes_topic` overrides for deployment or test configuration. Managed queries use the reserved command range rather than a request topic (B1.4).

Connection bootstrap and environment variables are SDK concerns documented in the tutorial, not part of this binding.

The SDK uses standard Apache Iggy transport framing. Append, poll, consumer-group, offset, and managed commands share the connection. The fork handles role-definition, role-deletion, and role-binding commands through the established custom replicated operations. Clients do not implement a second transport or command registry.

The Iggy fork runs `iggy-server`. It authenticates the caller and attaches the trusted user and client identities. It enforces command access, then handles an extension command or forwards it to `laser-plane`. Capability discovery includes the connected plane report. Laser Stack packages the fork with `laser-plane`. LaserData Cloud adds Warden, deployment services, and proprietary interfaces.

Agent records use the session, which is the conversation ID, as their message key. This preserves order within a session while different sessions can use separate partitions. One session lane is limited by one partition and its owning shard. Lifecycle and state stay on that lane in every layout (A15.3). Generic streaming supports balanced, keyed, or explicit partition selection. Partitions define ordering and workload placement. Apache Iggy enforces access at stream and topic level.

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
| `gen_ai.conversation.id` | string | conversation id (OpenTelemetry), canonical Crockford form |
| `gen_ai.agent.id` | string | producing agent (OpenTelemetry), on generic and envelope records |
| `gen_ai.usage.input_tokens` / `output_tokens` | u64 | token usage (OpenTelemetry) |
| `agdx.cause`, `agdx.parent_conv`, `agdx.root_conv`, `agdx.idem` | string | provenance: causal parent, parent and root conversation in canonical form, dedup key |
| `agdx.to` | string | addressee: an agent id, or `*` for every agent. A reader never parses `*` as an agent id. Every record on a shared session topic carries it |
| `agdx.deadline` | u64 | drop-dead time in epoch microseconds |
| `agdx.cost` | f64 | advisory call cost in USD |
| `agdx.fence` | u64 | the strictly-monotonic per-task fence the producer held, so a consumer drops a stale-holder replay of a log-resident effect |
| `agdx.mem.ns` | string | the logical memory namespace a record materializes under, so the read view is keyed by scope rather than by the physical topic |
| `agdx.mem.user` / `agdx.mem.app` | string | the user and app scope layers a memory record belongs to, materialized onto the read-view row so recall narrows by them |

Headers have a 1024-byte soft limit per record. Each value is limited to 255 bytes. Each header also uses 9 framing bytes, counted in the total.

One shared encoder writes the header block of both record families, so every SDK stamps identical bytes. The fixtures `header_block_envelope.json` and `header_block_generic.json` pin both blocks. Ids ride as canonical strings and numbers ride typed. A typed `AgentEnvelope` carries its own message fields. Its headers contain only content type, wire version, conversation, parent and root conversation, author, and addressee. `source`, `cause`, `correlation`, `deadline_micros`, and `idempotency_key` remain in the envelope. Generic messages without an envelope use the provenance header dictionary instead. Each field therefore has one authoritative carrier for that message form.

### B1.3 Versioning carriage

Agent records carry their version in `agdx.av`. Managed envelopes carry `v`, and `hello` also reports the supported versions (A12). The client uses discovery to reject unsupported operations before sending. The request version provides another check at the receiver.

The table defines the version carrier for each operation group. Hello slots and fenced-lease requests currently use version 1. Fenced leases also require their feature bit. If a later contract supports multiple simultaneous versions, it can use a minimum and maximum on the same slot.

| Surface | Mechanism | Carrier |
| --- | --- | --- |
| `query`, `control`, `checkpoint`, `kv`, `fork`, `agent`, `graph` | hello-negotiated | the `OpVersions` slot in the `hello` reply (A12), `0` or absent means not advertised |
| compare-and-swap, read-your-writes, strong consistency, fenced CAS, keyword search, watch, authz, consumer filters, group policy reads, sessions | feature-gated | a bit in the `hello` `features` bitset (A12), not a version |
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
| retired run registry (`agent.submit` / `cancel` / `status` / `list`), never reused | 1_000_700 .. 1_000_703 |
| `session.get` / `list` / `events` / `state` / `links` / `sources` / `changes` (A15.10) | 1_000_710 .. 1_000_716 |
| `filter.poll` / `ack` / `preview` / `test` / `validate` (served by the streaming server itself) | 1_000_800 .. 1_000_804 |
| `filter.mutate` / `get` / `list` / `list_revisions` / `get_binding` / `list_bindings` / `operation` | 1_000_810 .. 1_000_816 |
| `filter.resolve_policy` (internal: the streaming server resolves a read's policy, never client-facing) | 1_000_817 |
| `filter.watch_catalog` (internal: the streaming server watches the plane's catalog version, never client-facing) | 1_000_818 |

Authorization and system management use the first management block, `+100`, after internal and discovery commands. Feature blocks follow it. The base value of one million avoids collisions with ordinary Iggy codes. Blocks are 100 codes wide. These fixed numbers belong to this binding and its reference tests.

The server forwards CBOR requests to `laser-plane` through a local Unix socket. It attaches authenticated identity that the SDK cannot choose. `ForwardedQuery` carries the trusted user ID, client ID, audit correlation, and query envelope. Other operations use `ForwardedCommand`, with a command code and a retained field that no longer selects data. Session commands also require `ForwardedScope { stream_id, stream, stream_created_at_micros }`. The server resolves the named stream from its own metadata and stamps this scope on every forwarded request that names a stream: the seven session reads, session source registration, and a key-value or graph mutation that carries a `SessionRef`. The client never sets it. Numeric stream ID zero is valid, so a failed resolution is an error rather than zero. The managed backend refuses a session command without a trusted scope as unauthorized, refuses a payload stream that disagrees with the scope, and checks the stream creation time so a recreated stream never sees the former stream's sessions. The durable mutation envelope keeps the scope for replay and outcome lookup. A mixed-operation `batch` refuses any inner operation that bears a session, because one outer scope must not be copied to every item.

Socket frames use `[len: u32 little-endian][named-field CBOR payload]` and a 64 MiB limit. `laser-plane` dispatches queries, registry reads, KV, forks, graphs, session reads, and batches. Its projectors and state readers maintain models from the durable Iggy logs. Forwarded commands operate on those models.

Managed access uses grants independently of ordinary Apache Iggy permissions. A grant has the form `effect feature:action [on resource-pattern]`. A matching deny takes precedence over allow. Features include `kv`, `memory`, `projection`, `graph`, `query`, `fork`, `destination`, `checkpoint`, `authz`, `kv_lease`, `kv_fence`, `filter`, and `session`. `agent` and `workflow` are retired names that no command code maps to. They stay in the enum so stored grants keep decoding. A role that granted `agent:*` must be redefined with `session:*`. Actions are the closed set `read`, `write`, `delete`, and `admin`. New capability meanings belong to features rather than new actions.

Session requests are authorized per stream on the resource `stream:<name>`:

- The seven session reads need `session:read` and native read authority over the whole stream, either stream-level read or stream-level poll permission. Read permission on `agent.sessions` alone is not enough, because a session view reveals data from every registered topic of the stream.
- `RegisterSessionSource` and `RemoveSessionSource` need `session:admin` and the same stream-wide read authority. A `Named` topic set is also checked topic by topic.
- A key-value or graph mutation that carries a `SessionRef` needs its own grant, `session:write` on the named stream, and native send permission on that stream's `agent.sessions`.
- A prefixed resource `stream:<name>/...` is accepted only when the caller holds native permission on that stream. A prefix grant is a plain string prefix with no delimiter, so `stream:acme` also matches `stream:acme-staging`. Prefer literal stream grants.

Session lifecycle, state, and control records are ordinary appends. Native topic send permission gates them, and `session:write` or `session:admin` does not protect against a publisher that already holds that send permission. The `SESSIONS` capability bit is `1 << 12`. A server sets it only when it serves the session reads.

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
| writer schemas of one stream | the schema routes take `?stream=<name>`, and the register body takes `stream`, so a schema ID resolves in that stream's registry |
| apply / remove binding | `POST /bindings` / `DELETE /bindings` (control envelope) |
| `kv.get` / `set` / `delete` | `GET` / `PUT` / `DELETE /kv/{namespace}/{key}` (`GET` replies the value as the raw response body with the optional expiry in the `agdx-expires-at-micros` response header, or `404` when absent. `PUT` takes the value as the raw body with `?expires_at_micros`. A scan page instead carries the base64url `KvEntryView` JSON, since a JSON array cannot hold raw bytes.) |
| `kv.cas` | `PUT /kv/{namespace}/{key}/cas?expect_version=&expect_absent=` (the value rides the raw body, a `409` with the current version on a precondition miss) |
| `kv.scan` / `delete_many` / `namespaces` | `GET /kv/{namespace}?prefix=&start=&end=&key_contains=&conversation=&limit=&cursor=` / `DELETE /kv/{namespace}?...` / `GET /kv` |
| `fork.list` / `create` / `delete` / `promote` / `put` | `GET` / `POST /forks`, `DELETE /forks/{id}`, `POST /forks/{id}/promote`, `PUT /forks/{id}/rows`. The fork ID is percent-encoded, so a scoped ID `stream:<stream>/<local>` is one path segment |
| names in paths | a KV namespace, KV key, graph name, graph node, graph ID, projection ID, and fork ID are each percent-encoded as one path segment, so a stream-scoped name `stream:<stream>/<local>` stays one segment |
| `graph.query` / `neighbors` | `POST /graph/{name}/query` (a `GraphQuery` JSON body) / `GET /graph/{name}/neighbors/{node}?dir=&edge_type=&depth=&limit=&as_of=&conversation=` |
| `session.list` / `get` / `events` / `state` / `links` / `sources` / `changes` | `GET /sessions/{stream}?status=&agent=&text=&cursor=&limit=&total=` / `GET /sessions/{stream}/{id}` / `GET /sessions/{stream}/{id}/events?cursor=&limit=&fixed_frontier=` / `GET /sessions/{stream}/{id}/state?history_limit=` / `GET /sessions/{stream}/{id}/links?surface=` / `GET /sessions/{stream}/{id}/sources` / `GET /sessions/{stream}/changes?after=&limit=`. Each path segment is percent-encoded, and the static `changes` segment is matched before a session id. A deployment serves them when it announces the `sessions` capability |
| `registry.list_graphs` / `get` / register / drop graph projection | `GET /graphs?topic=&name_contains=&id_prefix=&search=` / `GET /graphs/{id}` / `POST /graphs` / `DELETE /graphs/{id}` (the projection listing narrowed to graph-kind projections, register and drop riding the control envelope) |
| `filter.list` / `get` / `list_revisions` | `GET /filters?name_contains=&state=&before_id=&page=&page_size=` / `GET /filters/{id}` / `GET /filters/{id}/revisions?page=&page_size=` |
| `filter.mutate` / `operation` | `POST /filters/mutations` (a `FilterMutationRequest` body with the `operation_id` as decimal text, `200` when applied, `202` while pending, the typed error status when rejected) / `GET /filters/operations/{operation_id}` |
| `filter.validate` / `test` / `preview` | `POST /filters/validate` / `POST /filters/test` / `POST /filters/preview` (a preview stores no offset and joins no group) |
| `filter.list_bindings` / `get_binding` | `GET /filter-bindings?filter_id=&stream=&topic=&page=&page_size=` / `GET /filter-bindings/{stream}/{topic}/{group}` |
| `authz.whoami` / `list_roles` / `get_role` / define / delete role / `get_bindings` / bind roles | `GET /authz/whoami` / `GET /authz/roles` / `GET /authz/roles/{name}` / `PUT /authz/roles/{name}` / `DELETE /authz/roles/{name}` / `GET /authz/users/{id}/roles` / `PUT /authz/users/{id}/roles` (gated by the `authz` capability, B1.4. `whoami` reads the caller's own bound roles and effective grants, `list_roles` a JSON array of `Role` and `get_role` one `Role` or `404`. A role `PUT`/`DELETE` and a user bind (`PUT` a bare JSON array of role names) journal to the server-side authorization band, the reads forward like any managed read.) |

The session routes carry the same requests and replies as the binary session reads (A15.10). The stream is the path segment. Query parameters fill the remaining request fields, and an absent or zero `limit` leaves the page size to the server. A reply is the JSON form of the `Ok` outcome, and a failure is the error body with the `ResultCode` of its `SessionError`. The wire fixtures pin the request and reply shapes. The former `/runs` routes are removed.

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
| signature activation and key registry (A9.5) | SHIPPED SDK-side (`sign` feature): the `Signature` envelope field activates with Ed25519 `SigningKey`/`KeyRegistry`, `Agent::builder().signing_key(..)` signs pickup and terminal replies, `LaserBuilder::verifier(..)` rejects an unsigned, unknown, or wrong-identity reply, and the managed `KvKeyRegistry` enrolls and snapshots verifying keys through the platform. All three SDKs share the same signing input and domain separator |
| content-block lifecycle and applied-through-offset ack | typed text, reasoning, data, tool-call blocks with start, delta, finish, and a reply naming the log offset a command took effect at (an offset, not a queue ack) |
| action governance hook | SHIPPED SDK-side (`ActionGovernor`): a pre-effect policy hook on agent sends, typed or raw topic publishes, requests and fan-out branches, the AGDX producer verbs (and through them the MCP/A2A bridges, `approval_gate`, and workflow dispatch), and memory writes. A vector-memory handle built from a governed `Laser` applies the hook before mutating its local index, and both log and vector item writes expose the proposed item body rather than their backend encoding. The hook sees the action kind, stream, topic, source and target, conversation, correlation, operation, tool, `on_behalf_of`, `purpose`, `data_classification`, the body, whether the record will be signed, and session counters. Its decision vocabulary is `allow`, `observe`, `block`, `step_up`, `modify` (applied before claim-check and signing), and `defer`. Chunk streams are exempt (per-chunk decisions are the wrong altitude). RBAC remains server-owned, the hook is defense in depth for regulated deployments |
| policy evidence capsule | the SDK emits each non-allow decision as a CBOR `event` (operation `policy_decision`) on the audit topic today: decision id, decision, mode, action attribution, reason, approved scope, policy pack/version/rules, risk score, a BLAKE3 receipt digest, the previous decision's digest (a per-conversation chain), outcome, and time. Python uses the Rust encoder. TypeScript mirrors its named fields and canonical digest input. What remains roadmap is pinning it into the fixture corpus as a capsule once a non-Rust-core port needs byte-identical evidence |
| enforcement modes | SHIPPED SDK-side (`GovernorMode`): `observe` records what enforcement would have done and never impacts the effect, `enforce` applies the verdict, and an evidence-write failure on a proceeding enforced decision fails the call (a governed effect is never unrecorded). The mode is configuration, not an envelope claim, because an agent must not self-authorize weaker enforcement. A `step_up` that expires unanswered fails closed unless the deployment explicitly configures that governor to fail open. `progressive` (observe first, promote selected rules) remains the policy engine's concern above the hook |
| policy context metadata | SHIPPED (A9.6): pinned advisory metadata keys `purpose`, `data_classification`, `task_context`, and `session_intent`, signed when they must be trusted. They give a policy engine stable inputs without inventing a new prompt or telemetry surface |

## C4. Conformance and fixtures

A conforming client decodes and checks the complete envelope. It can produce only the kinds that its application needs. Positive and negative reference cases cover decoding and encoding. The base implementation needs CBOR, the 16-byte ID codec, constants, validation, and operation behavior. Optional signing adds cryptographic requirements.

A binding adds fixtures for three things only: the identity-to-address mapping, the operation dispatch, and the out-of-band header encoding. The payload fixtures are shared across every binding.

Application IDs such as `conversation`, `record`, `correlation`, and `channel` differ from Iggy message IDs and offsets. CBOR stores them as 16-byte big-endian byte strings (A3). The conversation routing header stores a 26-character Crockford string. Readers also accept the older typed `Uint128` conversation header. Reference tests cover both forms.

## C5. Stability and evolution

The contract is pre-1.0 and permits breaking changes without backward compatibility. Update every affected client, service, specification, and reference file together. Part A and the Iggy binding define the current contract. Roadmap entries remain proposals until adopted. Reserved fields do not promise implementation. The appendix describes the SDK API separately.

### C5.1 Right to forget (erasure posture)

Erasure depends on the store that holds the bytes. The substrate log is append-only, so retention controls removal of log records. A referenced body can follow its object store deletion policy while the log retains the `BodyRef`. Deleting an object is not itself proof that every backup copy is erased.

The managed plane can hide a deleted or superseded row after applying its record. A deployment can rebuild a projection while excluding selected records. AGDX does not promise in-place edits to retained log history. Deployments with erasure requirements must define retention and external-object deletion policies. This section describes deployment policy and adds no erasure operation.

### C5.2 Schema evolution

`RegisterSchema` assigns a permanent ID and rejects collisions (A11.2). `SchemaDef { id, source, name, version }` retains optional name and version metadata. Only the ID selects the decoder. A record carries its writer ID in `agdx.sid`. Readers resolve that ID even when a topic contains records from several schemas.

Register a new ID for a changed schema, then move producers to it. Existing IDs are not overwritten. Dropping a schema removes it from active registration and browsing but retains decoding for existing records. The registry does not enforce a compatibility mode. Applications choose their schema migration policy.

### C5.3 Cross-surface timeline

A session timeline is a managed read model rebuilt from the log, like every other derived view (A1.1). The session index folds the registered topics of a stream and serves the timeline through `session.events` (A15.10), ordered by broker time and then by source address. It covers commands, responses, model and tool calls, state, context, memory, and lifecycle records. Key-value and graph writes appear as links without a timeline position. Without a managed deployment, a client reads one session's lane with bounded standard polls, and `ContextAssembler` merges selected topics for one conversation by timestamp. KV, memory, graph, and query remain separate views.

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

`laser-wire` defines authorization types and codes, reference encodings, and grant-decision helpers. A compile-time assertion keeps the `Feature` count multiplied by `ACTION_COUNT` within the 128 bits of the shared capability mask. Another assertion matches `ACTION_COUNT` to the `Action` variants. These prevent capability-bit overlap and omitted action rows.

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
- Accessors select scopes through `stream(name)`, `topic(name)`, `query(index)`, `kv(namespace)`, `fork(id)`, `graph(name)`, `memory(name)`, `context(conversation)`, `agent(id)`, and `sessions()`. Methods act on those objects. Accessors perform no I/O. Required arguments are positional, and optional configuration uses fluent methods. Boolean opt-ins use `.thing()` rather than `.thing(true)`.

The [client behavior guide](client-behavior.md) describes how the clients behave. Operation and envelope versions are 1.
