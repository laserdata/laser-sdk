# Agent Data Exchange Protocol (AGDX)

**Home: [agdxprotocol.ai](https://agdxprotocol.ai)**

AGDX is a substrate-neutral specification of how autonomous agents, and the conventional subsystems they exchange data with, move data over a durable log. It is a **data platform for agentic systems on a durable log**. Ultra-low-latency streaming is the foundation, but data is more than messages in flight: it is also materialized views to query and mutable state to coordinate on. AGDX treats all of it, streaming plus materialized views plus working state, as one data-exchange contract over one connection, with a binding per substrate. Agents are the motivating participants and the agent envelope is a first-class part of the model, but the model treats an agent and a service as peers. Nothing in the materialized-view or working-state surfaces is agent-specific.

AGDX is the lower-level layer: the efficient on-log message and data contract that agents and services speak natively. The edge agent standards (A2A, MCP, AG-UI) are not competitors at this layer. They sit above it and bridge into AGDX when an external client needs one, so an internal participant only ever speaks to the log while external clients keep their own contracts. The mappings are the [edge interoperability guide](interop.md).

The document is in three parts and an appendix. **Part A** is the core, which names no server. **Part B** is the bindings, where Apache Iggy is the normative binding, Kafka is the portability proof, and HTTP is the management surface. **Part C** records the design rationale and the roadmap. The **appendix** records the SDK API-stability policy.

The reader's test for the boundary: anything in Part A must be true on every substrate. Anything naming a header key, a command code, a byte order, or a frame layout belongs in Part B.

## TL;DR

AGDX is the wire protocol for agents and conventional services to exchange data over a durable log, on one connection. The log is the single source of truth, and everything else is a read model derived from it. Three surfaces share the connection:

- **Streaming** is the foundation: append typed records to topics, read them back by offset.
- **Materialized views** are projections declared per topic, with a query DSL over them, like a database index.
- **Working state** is a key-value store and copy-on-write forks, for coordination and speculative branches.

Agent messaging (commands, responses, token streams, status, errors) rides the same log as a typed CBOR envelope (A9). The protocol is substrate-neutral: a thin binding maps it onto a concrete log, Apache Iggy today (B1), Kafka and others possible (B2), so the data model outlives any single substrate.

```
            agents   .   services   .   edge bridges (A2A / MCP / AG-UI)
                                        |
                                 one connection
                                        |
   +=========================================================================+
   |                             AGDX data model                             |
   |                                                                         |
   |     streaming              materialized views      working state        |
   |     (the log)              (projections, query)    (key-value, forks)   |
   +=========================================================================+
                                        |
                                 binding (Part B)
                                        |
                       durable, partitioned, replayable log
                        Apache Iggy today, others possible
```

Read **Part A** for the substrate-neutral model, **Part B** for how it binds to a substrate, **Part C** for the rationale and roadmap.

## 0. Status and conventions

This is a design and specification document, not a frozen release. The repository is pre-1.0 and breaking wire changes are allowed.

- The normative body specifies the protocol as it stands. **Roadmap** marks a proposal that is not yet part of the contract: a draft that may evolve or break, recorded so the design space is visible, and pinned into the fixture corpus only once settled.
- Field tables give the logical type. The byte form is named-field CBOR (A2) unless a binding says otherwise.
- Requirement words (must, must not, should, may) are normative.
- Component names appear in prose. File paths do not.
- **Vendor-neutral and standardized.** The specification names nothing after any implementor. Its wire contract is standardized under the neutral `agdx` namespace: the header keys (`agdx.*`), the pinned dictionaries, and the signing domain are fixed, so independent implementations interoperate. Operational names that do not affect interop (a stream name, a topic name, a key-value namespace) are deployment-configurable with documented defaults. Substrate product names (Apache Iggy, Kafka) appear in binding chapters because a binding targets a named substrate. Keys drawn from OpenTelemetry use the `gen_ai.` namespace verbatim.

---
---

# Part A. The core (substrate-neutral)

Nothing in Part A names a server, a header key, a command code, or a byte order. The core assumes only an abstract substrate: an append-only, partitioned, offset-addressed log with replay and keyed ordering, able to carry a small set of attributes alongside a message body, and offering either a request and reply mechanism or a topic pair to emulate one.

## A1. Overview

### A1.1 What the SDK provides

The SDK provides three things over one authenticated connection.

1. Typed, batched message streaming over a durable, partitioned, replayable log.
2. A general data surface on that log: declared projections with a query DSL, a key-value store, and copy-on-write forks of the materialized read model.
3. An optional agentic layer: a reliable runtime and a typed agent envelope on the streaming layer.

These are not separate specifications. This document is one spec with one implementation. The agent envelope is the streaming layer's payload, not a protocol layered beside the data model, and there is one version story and one fixture corpus across the whole surface.

The log is the source of truth. Queries, projections, key-value, and agent coordination are read models on top of it, never a second store kept in sync by hand.

### A1.2 The thesis: specify the data, bind the transport

A wire that pins a frame layout and a connection handshake ages with the transport. A wire that pins a data model and its semantics survives, because the data is invariant when the substrate changes. So the specification splits into three layers, and only two are owned by the core.

| Layer | Owner | Content |
| --- | --- | --- |
| 1. Transport framing | the substrate | byte delimitation, segmentation, the substrate's own message or command frame |
| 2. Out-of-band metadata | the core names which attributes, the binding says how they ride | the attributes a reader or router acts on without decoding the body |
| 3. Payload | the core | the typed, versioned, named-field CBOR object or envelope, byte-identical everywhere |

> **The assignment rule.** An attribute is carried out of band if and only if a reader or router must act on it without decoding the body. Everything else is payload. Each binding states where the out-of-band attributes physically land.

### A1.3 The surfaces: a data platform on a log

The log is the single source of truth, in the lakehouse sense, and every other surface is a read model derived from it, never a second store kept in sync. Three surfaces share one connection.

| Surface | What it is | Nature |
| --- | --- | --- |
| Streaming | the log itself and the agent envelope | the foundation: append a record, read it back by offset, pull not push (A1.5) |
| Materialized views | projections and the query DSL | read models declared per topic, queried like a database index |
| Working state | key-value and copy-on-write forks | mutable state and speculative branches, addressed by key |

Streaming is the foundation and the others build on it, but the model is deliberately broader than streaming. A system speaks AGDX to query a materialized view, to coordinate on shared state, or to branch it speculatively, not only to push and consume messages.

### A1.4 What the core does not specify

The core carries no transport handshake, no flow-control window, no keepalive, and no multiplexing scheme. A log substrate already provides ordered delivery, retention, offsets, consumer groups, and back-pressure through the pull model, and the core must not duplicate any of it. This is the largest simplification over a transport-shaped protocol: the entire connection-management half lives in the substrate.

### A1.5 The streaming layer is a log, not a queue

This is the most important boundary in the model, and it decides which message-broker ideas apply and which do not. The streaming core is the foundation. The materialized-view and working-state surfaces and the agentic layer are read models and conventions on top of it. A great many primitives from the queue and broker world look relevant and are not, because a log is a different shape from a queue.

The log offers exactly two stream operations, both provided by the substrate: **append** a record (publish), and **read** a topic from an offset under a consumer group (consume). Reading is pull. A consumer polls at its own pace and back-pressure is the pull itself. There is no server push.

From that, the delivery semantics follow and are not configurable.

- Delivery is at-least-once with replay from any offset. Ordering is total within a partition key (the conversation).
- **Acknowledgement is an offset commit**, the consumer's own bookkeeping, not a wire message. Commit after processing for at-least-once, and a reader resumes from its last committed offset.
- **Exactly-once is a consumer-side property** (the business idempotency key plus the reliable consumer's processed-key store), never a mode negotiated with a broker.
- **Dead-lettering is a runtime convention**: after retry exhaustion the reliable consumer publishes a capsule to a dead-letter topic. It exists because the log lets anyone publish anywhere, not because a broker manages a dead-letter queue.

So the queue and broker primitives below have no place in the model. Each either does not map onto a log or names a need already met by offsets and replay.

| Broker or queue primitive | Why it does not apply on a log |
| --- | --- |
| server-push delivery (a DELIVER verb) | the log is pull. Consumers poll and replay by offset |
| ack and nack as settle verbs | acknowledgement is an offset commit, consumer-side. There is no negative-ack or requeue. A consumer simply does not advance its offset, or re-reads |
| delivery-mode negotiation (at-most / at-least / exactly-once) | the log is at-least-once with replay by construction. Exactly-once is consumer dedup, not a selectable mode |
| redelivery count, visibility timeout, ack deadline | queue bookkeeping. On a log, retry is the reliable consumer's local policy over re-read offsets |
| broker-managed dead-letter queue | dead-lettering is a runtime convention (a capsule on a DLQ topic), not a managed queue |
| subscribe versus consume as two modes | one primitive: read a topic from an offset under a consumer group |
| message priority | a log is ordered by offset within a partition, not reorderable by priority |

Telemetry is likewise not a separate primitive set. Logs, metrics, traces, and events are records published to topics with OTel-aligned provenance headers, materialized by projections (the trace view is one such projection). The observability vocabulary is a convention over publish, not new operations.

One consequence matters for interoperability. The edge bridges (A2A, MCP, AG-UI, and the candidate ATP and LangChain-streaming bridges) depend only on these log primitives: publish, offset-replay consume, and request-and-reply correlation. An edge protocol's settlement and reconnection concepts map onto offsets (a streaming client resumes from an offset, a task reply is matched by correlation), so interoperability is preserved without importing any queue semantics. The [edge interoperability guide](interop.md) covers the mappings.

## A2. Encoding rules

- **One encoding.** Every payload is named-field CBOR (RFC 8949), serialized through a single entry point so no surface drifts to a different encoding.
- **Declaration order, absent optionals skipped.** An unused optional field costs zero bytes. Unknown fields are ignored on decode, so every future field is additive and free.
- **No silent skip.** A payload is exactly one CBOR item. Trailing bytes are a decode error. Decoding must fail on corrupt or wrong-typed known data, and only fields foreign to the schema may be ignored.
- **Machine ids** ride as fixed-width 16-byte CBOR byte strings, not bignum-tagged integers, so a port needs only a byte-string reader.

How a message frame is delimited and how a managed request and reply is carried are binding concerns (B1.4), not core.

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

A record id is portable in a way a log position is not. A re-publish into another partition or a disaster-recovery cluster keeps the record id and gets a fresh position.

## A4. The out-of-band attribute set

The core defines which attributes must be carriable out of band and what they mean. It does not define how they are encoded.

| Attribute | Type | Why out of band |
| --- | --- | --- |
| wire version | u32 | a long-lived reader selects its decoder before decoding |
| content-type | u8 code | a consumer chooses the codec before parsing |
| routing / ordering key | 128-bit | the substrate partitions and orders without reading the body |
| operation identity | name or code | a request is dispatched without a parse |
| correlation | 128-bit | a reply is matched and a stream is filtered cheaply |

## A5. The operation registry

Operation **identity and semantics** are core. Operation **encoding** is binding-owned. The core registry names each operation and fixes its meaning. A binding maps the name to its own dispatch. This generalizes the content-type dictionary pattern (a logical variant with a per-wire code) from content types to operations.

| Op id | Surface | Semantics | Status |
| --- | --- | --- | --- |
| `hello` | control | capability and version probe |  |
| `query` | views | run a query IR, return rows or aggregates |  |
| `registry.get_projection` / `list_projections` | views | browse projections |  |
| `registry.get_schema` / `list_schemas` | views | browse writer schemas |  |
| `registry.register_schema` | views | allocate a writer-schema id |  |
| `registry.decode_record` | views | decode a body under a registered schema |  |
| `kv.get` / `set` / `delete` / `delete_many` / `scan` / `namespaces` | state | key-value operations |  |
| `kv.cas` | state | conditional set on a version token |  |
| `kv.exists` / `expire` / `patch` | state | metadata probe, in-place expiry, merge patch (formal object primitives, C6) |  |
| `kv.lease` / `release` | state | advisory lease on a key (unsupported error when the backend cannot serve it) |  |
| `fork.create` / `delete` / `promote` / `list` / `put` | state | copy-on-write branch operations |  |
| `graph.query` / `neighbors` / `upsert` | views | knowledge-graph traversal, one-hop neighbors, node/edge upsert (A13) |  |
| `watch` / `unwatch` | state/views | change notification over the read model | roadmap |

The four-verb agentic-memory API (`remember` / `recall` / `improve` / `forget`) is **not** a registry operation: it is an SDK facade that composes `publish`, `query`, and the `graph` ops above (A13), so it adds no wire op of its own.

The streaming layer does not use the registry. Its operations are the two substrate stream operations (append and offset-replay consume, A1.5) plus, for the agentic layer, the six agent-envelope kinds (A9), dispatched by the typed envelope rather than by a code. There is no subscribe, consume-mode, ack, nack, or deliver operation, because the log is not a queue (A1.5).

## A6. Dictionaries (pinned codes)

All code dictionaries are pinned small integers, permanent, never renumbered. An unknown code decodes to a pass-through unrecognized value rather than failing, so a new entry is additive.

**Content-type**:

```
raw=0  json=1  msgpack=2  cbor=3  bson=4  avro=5  protobuf=6  arrow=7  ref=8  any=255
```

`ref` marks the body as a claim-check capsule (A9.5). `any` is a best-effort sentinel.

**Task state**, the agentic lifecycle, A2A-aligned:

```
submitted=1  working=2  input-required=3  completed=4  canceled=5
failed=6  rejected=7  auth-required=8  unknown=9
```

Terminal set: completed, canceled, failed, rejected.

**Agentic error code**, the `error` body discriminator:

```
invalid_request=1  unauthorized=2  unsupported=3  deadline_exceeded=4
cancelled=5  tool_failure=6  internal=7
```

**Dead-letter reason**:

```
retry_exhausted=1  rejected=2  decode_failed=3  deadline_exceeded=4
```

**Consistency level.** A query carries one of three shipped levels as the snake-case string the `Consistency` enum serializes to:

```
eventual  read_your_writes  strong
```

`eventual` is the default and omitted on the wire. `bounded_staleness` and `linearizable` are reserved for a future revision and not yet defined. Rule: a substrate that cannot satisfy the requested level must fail with a `stale` (or unsupported) result, never serve a weaker guarantee.

## A7. The unified result-code space

One logical result code spans every managed surface (query, key-value, fork, browse). Each surface keeps its own typed error for the detail a caller needs, and every one of those errors also projects onto one `ResultCode`, so a generic client, the HTTP status mapper, and a cross-language port all dispatch on one small dictionary instead of parsing per-surface strings. The codes and the HTTP status each maps to are a pinned cross-repo contract.

| Code | Numeric | HTTP | Meaning |
| --- | --- | --- | --- |
| ok | 0 | 200 | success (no error to classify) |
| unsupported | 1 | 501 | the op, or the managed surface, is not available here |
| not-found | 2 | 404 | a named index, fork, or key does not exist |
| invalid-argument | 3 | 400 | malformed request or an out-of-range field |
| too-large | 4 | 413 | a result or value exceeded a cap |
| conflict | 5 | 409 | a precondition lost a race (compare-and-swap mismatch, fork conflict) |
| stale | 6 | 503 | a consistency level could not be met in time (the read model is catching up) |
| version-skew | 7 | 400 | the wire op version is not accepted |
| unauthorized | 8 | 401 | the credential is missing or invalid |
| backend | 9 | 502 | the managed backend failed or was unreachable |

An unknown code from a newer peer rides through as `unrecognized(code)` and re-encodes byte-for-byte, the same forward-compat shape the growable u8 dictionaries use, so an old build relays it rather than failing. Each binding maps the logical code to its own carriage (the HTTP binding to a status, B4).

A `CommandError` of `{ code, message }` is the surface-agnostic reply for a command code a server does not handle. Each managed surface has its own typed reply, so a server that receives an unhandled or unsupported code (a forwarded compare-and-swap on a build without it, or any future additive code) has no single surface to answer in, and a wrong-surface error fails to decode in a client awaiting a different reply. The server answers such a code with a `CommandError`, and a client that cannot decode the surface's typed reply decodes `CommandError` next, turning the reply into a typed code instead of an opaque transport failure. The two shapes are disjoint, so the fallback decode never misfires on a real reply.

## A8. Versioning, causality, idempotency, expiry, consistency

- **Versioning** is out of band by necessity for a durable record: a reader selects its decoder before reading the body, so the wire version is an out-of-band attribute (A4), never a body field. The managed surfaces additionally negotiate version at connect (A12) and fail fast before a round trip. Per-message strict handling beyond a whole-version bump rides the agent envelope's must-understand marker (A9.1).
- **Causality** rides a portable parent record id plus an optional log-position locator (A9.1). A cross-region happens-before token is a roadmap proposal (C3).
- **Idempotency** is the business key (A3), scoped to the authenticated identity so one peer cannot replay or suppress another's operation.
- **Expiry** is an absolute epoch-microsecond time. An expired object reads as absent.
- **Consistency** is a per-query `Consistency` level (`eventual`, `read_your_writes`, `strong`), fail-not-downgrade: a level that cannot be met returns a `stale` result rather than silently serving older data (A11.3, A11.4).

Wire compatibility rules for the named-field CBOR encoding:

- **Additive struct fields are compatible.** An unknown named field is ignored on decode, and an absent optional field is the default, so a new optional field is free and needs no version bump. The corollary is a hazard: an additive field whose meaning is "serve differently" (the `consistency` level is the example) is silently ignored by a peer that predates it, which then serves the old behavior and reports success. So a feature carried as an additive field must be capability-gated, and the client must refuse the level a peer does not advertise rather than let the field be silently dropped (A12).
- **Enum variant additions are not compatible in general.** Externally tagged decode fails on an unknown variant, so adding a variant a peer could receive unsolicited requires bumping that surface's op version. The exception is a variant only ever emitted in reply to a request a peer opts into: an old peer that never makes the request never receives the variant, so it is additive without a bump. The `query.stale`, `kv.committed`, and `kv.version-conflict` outcomes are this case (only a peer that sent a consistency query or a compare-and-swap sees them), which is why those shipped without a query/kv op-version bump.
- **Pinned u8 dictionaries are exempt from the enum rule** (content-type, task-state, the error and dead-letter codes). A raw u8 always decodes, an unknown code passes through as unrecognized, and codes are permanent and never renumbered, so a new code is additive.

## A9. The streaming layer: the agent envelope

One CBOR named-field envelope per agent message. It is the streaming layer's typed payload within this one spec, not a separate protocol. The wire version is carried out of band (A4), at version 1.

**An example.** A `command` asking a tool to run, and the `response` that answers it. The wire form is CBOR, and the opaque ids are 16-byte values shown here in their base32 display form:

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

A streamed answer instead of a single `response` is a run of `chunk` records sharing a `channel`, ordered by `sequence`, ended by `last = true` (A9.4). Absent optional fields cost zero bytes (A2), so a real envelope carries only what it needs.

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

### A9.2 The per-kind validity matrix

R required, O optional, X invalid. Enforced at three layers: the wire validate function, the SDK constructors, and receivers. Pinned by positive and negative fixtures.

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
| `operation` | O | O | O | R on opening chunk, X after | R (`task`\|`card`\|`progress`) | O |
| `tool` | O | O | O | O | X | O |
| `usage` | X | O | O | O (terminal chunk) | O | O |
| `body` | R | R | R | R (empty only with `last`) | O | R |

The command and event boundary is the correlation rule: a message expecting a reply or effect is a `command` and requires `correlation`, a message expecting nothing is an `event`. Fire-and-forget commands do not exist.

### A9.3 Closed sub-vocabularies

- `status` discriminator (`operation`): `task` (A2A lifecycle, requires `correlation` and `task_state`), `card` (liveness and capability), `progress` (advisory ticks).
- Chunk-stream purpose (`operation` on `sequence = 0`, required there, invalid after): `chat`, `reasoning`, `tool_args`.
- State sync convention (an `event`, never a new kind): `operation = state_snapshot` (body is the full state) or `state_delta` (body is an RFC 6902 JSON Patch).

### A9.4 Streaming and reassembly

A stream is `chunk` messages sharing a `channel`, ordered by `sequence` within one conversation partition, terminated by `last = true` (with `finish_reason`) or by an `error` carrying the channel. Offsets are the resume primitive. Reassembly is mechanical and every port mirrors it:

- chunks apply in `sequence` order from 0, each exactly once,
- a duplicate sequence is dropped and counted,
- a gap ends the stream with a reader-local synthetic terminal (`finish_reason = "gap"`),
- everything after a terminal is dropped and counted, the first terminal wins,
- the opening chunk carries the purpose and the abandonment deadline,
- whole-stream `usage` rides once, on the terminal chunk.

The synthetic finish reasons (`abandoned`, `gap`) are reader-local and never appear on the log. Replay always sees the raw truth.

### A9.5 Capsules (all CBOR, all fixtured)

- **BodyRef** (content-type `ref`): a claim-check naming where the content lives. Fields: `reference` (a URI, object key, or KV key, non-empty, at most 1024 bytes), `size_bytes`, `sha256` (exactly 32 bytes), and a dormant `encryption` scheme code (absent means plaintext). A consumer verifies the fetched bytes against the digest without trusting the store.
- **Dead-letter capsule**: `source` (the poison message's log position), `reason` (the dead-letter dictionary), `attempts`, optional `detail`, and `payload` (the original encoded envelope verbatim, for trivially correct redrive).
- **AgentCard** (the `card` status body): optional `name`, optional `version`, a capped `capabilities` list (at most 64), optional `ttl_micros`. A card older than its TTL means a dead agent.
- **Signature** (dormant): `scheme` (Ed25519 = 1), `key_id` (8 bytes), `bytes` (64 bytes). The signing input is domain-separated by the fixed spec constant `agdx.signature.v1` prefixed to the canonical envelope encoding with the signature field absent. The constant is one agreed value for the whole spec, so signatures verify across parties. No crypto enters the wire crate. Activation is one additive optional envelope field.

### A9.6 Pinned metadata keys

| Key | Type | Meaning |
| --- | --- | --- |
| `role` | string | chat role, recommended `user` / `assistant` / `system` / `tool` |
| `bridge_hops` | list of strings | the loop guard. A bridge appends its id and drops a message whose hop list already contains it |

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
| `value` | bytes, at most 8 MiB | opaque |
| `expires_at_micros` | u64, optional | absolute expiry, expired entries hidden on read |
| `version` | u64, optional | optimistic-concurrency token, store-assigned and bumped on every mutation. `0` (omitted on the wire) means an unversioned store |

### A10.2 Key-value operations

| Op | Request fields | Reply outcome |
| --- | --- | --- |
| `kv.get` | namespace, key, optional `if_none_match(version)` | `Value(Option<entry>)`, or `NotModified` when the conditional version matches |
| `kv.set` | namespace, key, value, optional expiry | `Written` |
| `kv.cas` | namespace, key, value, optional expiry, precondition (`match(version)` \| `absent`) | `Committed { version }`, or `VersionConflict { current }` on a precondition miss |
| `kv.delete` | namespace, key, optional `if_match(version)` | `Deleted(bool)`, or `VersionConflict` when the conditional version misses |
| `kv.delete_many` | namespace, composed bounds (prefix, range, substring) | `DeletedMany(count)` |
| `kv.scan` | namespace, composed bounds, limit, cursor | `Page { entries, cursor }` |
| `kv.namespaces` | none | `Namespaces([{ namespace, entries }])` |
| `kv.exists` | namespace, key | `Metadata(Option<{ version, expiry, size_bytes }>)`, the cheap presence/precondition probe without the value |
| `kv.expire` | namespace, key, optional expiry (none clears) | `Versioned { version }` (the value is untouched) |
| `kv.patch` | namespace, key, patch bytes, optional `if_match` | `Versioned { version }` (a merge patch on a structured value, no full-object transfer) |
| `kv.lease` | namespace, key, lease ttl | `Leased { lease_token, granted_ttl }`, or a clean unsupported error when the backend cannot serve leases |
| `kv.release` | namespace, key, lease token | `Released(bool)`, or `LeaseLost` when the token has expired |

Errors: unsupported, invalid key, too large, backend, version, version-conflict, lease-lost. Namespaces isolate keys, scope scans, and separate one user's keys from another's. The trusted user id is stamped by the binding, never by the SDK. Scan caps: page at most 1000, default 100. The `exists` / `expire` / `patch` / `lease` / `release` ops and the conditional `if_match` / `if_none_match` carriage realize the formal-spec object primitives (Section C6) on the key-value surface.

For example, writing a session flag with an expiry and reading it back:

```
kv.set { namespace: "sessions", key: "user:42", value: <bytes>, expires_at_micros: 1700000000000000 }
   -> Ok(Written)

kv.get { namespace: "sessions", key: "user:42" }
   -> Ok(Value(Some({ key: "user:42", value: <bytes>, expires_at_micros: 1700000000000000 })))
```

### A10.3 Compare-and-swap (optimistic concurrency)

Each entry carries a `version: u64`, assigned by the store and bumped on every successful mutation. A conditional write gives lock-free optimistic concurrency for agents contending on one key.

The operation:

```
kv.cas { namespace, key, value, expires_at_micros?, expect }
expect = match(version) | absent
```

Success returns `Committed { version }` (the new version, so a caller can chain a further conditional write without a re-read). A precondition miss returns `version-conflict` carrying the current version (`some(v)` when present, `none` when absent), so the caller re-reads and retries, or learns that an `absent` precondition lost a race.

Compare-and-swap is capability-gated by the `kv_cas` flag: a transactional row store (the embedded engine) serves it as a single conditional write, while a backend that cannot do a conditional write leaves the flag clear and returns a clean unsupported error. The version token rides on the entry either way, reading `0` on an unversioned store. The advisory lease (A10.4) builds on this: a lease is a key holding a token and an expiry, acquired and released by compare-and-swap, with the version as the fencing token.

### A10.4 Advisory lease

```
kv.lease { namespace, key, lease_ttl_micros } -> Leased { lease_token, granted_ttl_micros }
kv.release { namespace, key, lease_token } -> Released(bool)
```

A bounded-TTL distributed lock built on compare-and-swap (A10.3): a lease is a key holding a token and an expiry, acquired and released by conditional write, with the version as the fencing token. Acquiring a key already held returns `version-conflict` (a contended lock, not a backend failure), so a caller retries. A holder presents the `lease_token` on protected mutations. A lease not renewed before its TTL expires is released automatically, release is atomic at the held token, and a stale token fails with `lease-lost`. The lease lives under a reserved per-caller namespace, so it never surfaces in the caller's own scans or namespace listings. A backend that cannot serve advisory leases returns a clean unsupported error, never a fallback. This realizes the formal-spec `LEASE` / `RELEASE` primitives (C6).

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

A fork id is at most 128 bytes and is restricted to a strict charset safelist: ASCII letters, digits, `-`, `_`, and `.`. The bound is not only a length cap. A backend that overlays a fork inlines the id into a copy-on-write query as a quoted identifier, so the safelist is the one anti-injection rule, owned in the wire contract (`validate_fork_id`) and shared by every fork-serving backend. How many forks a deployment may hold is a managed-side resource policy surfaced as a fork error, not part of the wire contract.

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
| `EntitySchema` | node/edge extraction for a `graph` projection: node rules (label + RFC 6901 value pointer + optional embedding pointer) and edge rules (edge type + source/target pointers). Pointer-based and deterministic, so building the graph needs no model call (A13) |
| `ProjectionBinding` | source (stream, topic), allowed projection refs, default projection, target tables |
| `SchemaDef` | id (u32, permanent), source, optional name, optional version |
| `SchemaSource` | internally tagged on `kind`: `{kind: avro, schema}` \| `{kind: json_schema, schema}` \| `{kind: protobuf, descriptor_set, message_type}`. `descriptor_set` is a byte string. An unknown `kind` from a newer peer decodes to a forward-compatible `unknown` rather than failing the whole reply (the same shape `RetentionPolicy` uses), so an old client still reads a registry holding a source kind it cannot decode against. It must not re-register an `unknown`. |

Storage model: the log keeps the original bytes always, the indexed columns are extracted at materialize time and drive filters and ordering and aggregates, and the inline body is an optional copy alongside the row so typed fetches skip a log round trip. A record with zero indexed fields is dropped by the projector while the log keeps the bytes. At most 32 indexed fields per record. The inline body copy is capped at 8 MiB (the same ceiling as a key-value value): a larger payload still indexes and stays in the log, it is just not duplicated into the row, so a typed fetch decodes from the log or a claim-check `ref` body. An explicit indexed-field directive wins over schema extraction for the same field.

A materialized view is a read model built by a projector consuming the log, so by default it is eventually consistent: a record is queryable once the projector has materialized it, not at the instant it is appended. The lag is the projector's read-and-apply latency and depends on the backend. A query that needs to see its own prior writes sets the `read_your_writes` consistency level (A11.3), which waits for the projector to reach the source log head and fails with `stale` rather than serving older data if it cannot catch up in time. The log itself stays the synchronous source of truth, readable by offset the moment the append acknowledges.

The server obligation for a non-`eventual` level is one rule, owned in the wire contract as a small `ConsistencyGate { applied, required }` helper: serve only when the projector's applied offset for the queried source has reached the required offset (the source head at query time), otherwise return `stale`. `eventual` always passes. `strong` is read-your-writes plus cross-replica agreement, so a `strong` backend layers its own cross-replica check on a gate that has already passed. Every backend uses the one gate so the fail-not-downgrade rule is enforced identically.

### A11.2 Control commands (durable on the control topic)

The control envelope carries `{ v, timestamp_micros, command }`. The command is one of `RegisterProjection`, `DropProjection`, `ApplyBinding`, `RemoveBinding`, `RegisterSchema`, `DropSchema`, `RegisterGraph`, `DropGraph`. A graph projection (`kind = graph` with an entity schema, A11.1) registers through `RegisterGraph` rather than `RegisterProjection`, so a deployment can gate graph registration separately. Schema ids are permanent, collisions are rejected, and a dropped schema still decodes records already stamped with its id.

### A11.3 The query IR

A backend-neutral logical IR, compiled per backend on the managed side.

| Field | Meaning |
| --- | --- |
| `index` | the materialized index name |
| `by_key` | exact-match key constraints, AND-composed |
| `message_type`, `time_range` | sugar for equality on the type field and a closed range on the timestamp |
| `filter` | a predicate tree (`all` / `any` / `not` / `pred`) |
| `vector` | nearest-neighbour search (field, embedding, top_k), distance in the row score |
| `order` | sort clauses (field, direction) |
| `limit`, `offset` | paging, limit capped at 1000, 0 means a full page |
| `aggregate` | group-by keys, aggregate calls, optional tumbling window |
| `having` | a filter on aggregate output |
| `distinct` | distinct over the selected fields |
| `select` | projected fields and an inline-payload flag |
| `fork` | resolve against a fork's overlay |
| `raw_sql` | an opt-in escape hatch, SQL backends only, read-only single select |
| `consistency` | read-consistency level, absent on the wire for the default `eventual` |

| Vocabulary | Values |
| --- | --- |
| comparison op | `eq`, `ne`, `lt`, `lte`, `gt`, `gte`, `in`, `contains`, `prefix` |
| aggregate func | `count`, `count_distinct`, `sum`, `avg`, `min`, `max`, `percentile`, `stddev` |
| scalar value | string, int, uint, float, bool, null, list (untagged, variant order chosen so integers never become lossy floats) |
| consistency | `eventual` (serve as-is, the default), `read_your_writes` (wait for the projector to reach the source log head, else `stale`), `strong` (linearizable cross-replica). `read_your_writes` and `strong` are backend-gated |

### A11.4 Query reply

`Ok(QueryResult)` or `Err(QueryError)`. The error is one of `Unsupported`, `IndexNotFound`, `ForkNotFound`, `Backend`, `TooLarge { what, size, cap }`, `Version { expected, got }`, `Stale { what, applied, required }`. `TooLarge` is raised when a query asks for more than one reply can carry. A reply rides a single 64 MiB socket frame and is bounded by it, so larger result sets page via `limit` and `offset` rather than a bigger reply. `Stale` is raised when a `read_your_writes` (or `strong`) query cannot be served at the requested freshness within the deadline: the projector's applied offset (`applied`) had not reached what the level required (`required`), so the caller retries rather than reading older data. Every one of these errors also classifies into the unified result code (A7).

For example, the ten most recent high-latency calls for one model, newest first:

```
query {
  index:  "inferences",
  filter: { all: [ { pred: { field: "model", op: eq, value: "gpt-4o" } },
                   { pred: { field: "latency_ms", op: gt, value: 500 } } ] },
  order:  [ { field: "ts", dir: desc } ],
  limit:  10,
  select: { fields: ["model", "latency_ms", "user_id"], payload: false }
}
   -> Ok(QueryResult { rows: [ { fields, score, payload? }, ... ] })
```

## A12. Capability negotiation

A single connection negotiates what is available, and a managed feature works against a managed implementation or returns a clean unsupported error. There is no degraded path.

- At connect the client runs the `hello` operation. The reply advertises per-surface op versions (`query`, `control`, `kv`, `fork`, and optional `agent` and `graph` versions where 0 means not advertised) plus an optional `features` bitset for managed sub-capabilities that vary independently of the base surface: compare-and-swap, read-your-writes, and strong consistency. The `graph` op version is what gates the knowledge-graph surface (A13). A `0` bitset (skipped on the wire) means none advertised, so a pre-feature reply stays byte-identical and an old client simply lights up no extra capability. The capabilities reply additionally lists the materialization backends the server currently exposes: each is a descriptor of a stable `id` and an opaque engine `kind`, with optional advisory `label` and `version` strings for display and a set of opaque `capabilities` tags the backend declares about itself, identity only with no settings or secrets, so a client can show what it may route to and a new engine is advertised by name without a wire change. The `capabilities` tags let a consumer reason about what a backend is good for (e.g. `ingest`, `query`, a particular query-surface feature) and gate a decision before attempting an op: the wire pins no meaning to a tag, a producer emits what it supports and a consumer matches the tags it understands and ignores the rest, so a new capability is advertised by name with no wire change. An empty list (the default, skipped on the wire) means none advertised, and an absent `label`/`version`/`capabilities` (also skipped) means the client derives a label from `id`/`kind`, shows no version, and assumes no declared tags. The binary `hello` reply carries the same backend list as the HTTP capabilities surface: it is a `BackendAnnounce` that decodes byte-identically from a pre-backends versions-only reply, so an older server's reply still parses with an empty backend list.
- The SDK capability set is grouped by where a capability lives and what it depends on, not a flat list. A root `managed` flag says a managed plane is connected at all. The managed surfaces served off the log are `query`, `kv`, `graph`, `forks`, and the A2A gateway. The platform-native ones are `sessions` and `durable_dedup`. A surface's sub-features nest under it so a dependent feature cannot be advertised apart from its surface: `query.consistency` is the strongest read-consistency level the query surface serves (the `eventual < read_your_writes < strong` ladder, so a level implies the weaker ones, which makes the impossible "strong-but-not-read-your-writes" state unrepresentable), and `kv.cas` is the key-value conditional-write feature. Agentic memory composes the query and graph surfaces, so it has no capability of its own. On an open substrate with no managed surface every capability is off and the matching call returns unsupported. The wire `hello` reply still carries the flat `features` bitset (`kv_cas`, `read_your_writes`, `strong_consistency`). The SDK folds those bits into the grouped form (the consistency bits become the served level), and the HTTP capabilities reply mirrors the grouped shape (B4).
- When the advertised op version is not the SDK's pinned version, the call fails fast with the surface's typed version error before a round trip.
- A feature carried as an additive request field, rather than its own operation, is refused locally when unadvertised. A compare-and-swap rides its own command code, so an unaware server rejects it cleanly even without a local check, but a read-your-writes (or strong) query rides the additive `consistency` field that an unaware server silently drops, so the client refuses an unadvertised level before sending. This is what keeps fail-not-downgrade honest (A8).
- A server MUST keep its two capability carriages in agreement: the binary `hello` `features` bit and the HTTP capabilities boolean for the same feature say the same thing. The contract carries both because the two bindings are separate, and a divergence would let a feature look available on one surface and not the other.
- Every sub-feature defaults to **not advertised**: the HTTP `Capabilities` constructor leaves `kv.cas` off, `query.consistency` at `eventual`, and `graph` off, and a server opts each up only when it serves it, mirroring the skip-when-zero `features` bitset and the zero `graph` op version. A backend that advertised a feature it cannot honor would turn the clean unsupported error into a silent wrong answer, so the safe default is off and opt-in.
- When the streaming server and the managed backend are separate processes, the backend is the single source of its own capability truth. On connect it announces its `OpVersions` (versions plus feature bits) and the materialization backends it currently exposes (each a stable `id`, an opaque engine `kind`, and its opaque `capabilities` tags) to the streaming server over their private socket as a `BackendAnnounce` (`AGDX_BACKEND_HELLO_CODE`), and the streaming server caches and relays it verbatim when it answers a client `hello`, on both the binary and the HTTP carriage. This is what keeps the two carriages honest without the streaming server hardcoding bits the backend may or may not serve, and a backend that gains a feature lights it up everywhere by announcing one bitset.

## A13. Agentic memory and the knowledge graph

The agentic layer adds two things on top of the data platform: a knowledge graph as a new materialized view, and an agentic-memory API expressed entirely as a facade over the primitives already defined. Memory is not a wire surface of its own. Its four verbs compose `publish` (A1.5), `query` (A11), and the graph ops below, so every SDK gets the same semantics without a parallel command band.

**The memory verbs (SDK facade, no wire op).**

- `remember` appends an item to the log. With dedup it content-addresses the item id (below), so storing the same fact twice under the same durable owner stores it once. A graph binding on the source then extracts entities asynchronously, the way any projection materializes.
- `recall` reads back. Its `strategy` is `auto` by default and routes from the available state: `recent` / `temporal` fold the log or run a time-ranged query, `semantic` runs a vector query (A11.3), `graph` runs a traversal, `hybrid` blends. Routing authority sits where the state is: the managed plane routes when it knows the graph, otherwise a trivial client heuristic.
- `improve` is feedback and enrichment. Feedback rides the log as a typed record a ranking backend folds into recall order. The richer enrichment (triplet rebuild, session bridging, consolidation into summary nodes) is graph work.
- `forget` tombstones the source record. An opt-in cascade also deletes derived graph nodes, edges, and vectors.

A memory item carries a kind (`fact` / `message` / `summary` / `entity` / `feedback`) and a lifetime (`session`, conversation-scoped and prunable, or `durable`, shared across conversations and graph-backed). These are SDK labels, not wire-op fields.

**Content-addressed identity.** A deduped memory id and a graph node id are deterministic content hashes, the one canonical `content_id` in the wire crate (a dependency-free, fixtured FNV over byte segments, rendered as the 16-byte id, A3). A memory id hashes the durable owner, kind, and body, so the same fact in two conversations is one durable memory. A node id hashes the entity's label and value, so the same entity extracted from different messages converges on one node, which is what forms a graph rather than disconnected pairs. Every SDK reproduces the id from the same segments, pinned by a golden vector.

**The graph surface (managed wire surface).** A graph is a materialized view built by a `graph` projection (A11.1): the projector extracts nodes and edges from each record per its entity schema, persists them content-addressed, and indexes triplets (`source -> relationship -> target`) for semantic graph recall. It is gated by the `graph` op version (A12). The ops:

| Op | Request | Reply |
| --- | --- | --- |
| `graph.query` | graph name, start (`ids` \| predicate `match` \| vector `nearest`), hop spec (edge type + direction + max), optional node/edge filters (the same `Filter` predicate language as query, A11.3), return (`nodes` \| `edges` \| `paths` \| `triplets`), limit, optional fork, consistency | `nodes`, `edges`, `paths` |
| `graph.neighbors` | graph name, node id, direction (`out` \| `in` \| `both`), optional edge type, depth, limit | the reachable nodes and traversed edges |
| `graph.upsert` | graph name, nodes, edges (the projector path, idempotent on content-addressed ids) | written |

Caps (`wire/src/limits.rs`): traversal depth at most 8, at most 10000 nodes plus edges per reply, at most 16 labels per node. The depth and element caps are enforced server-side: an over-cap depth is clamped and an over-cap upsert is rejected with a too-large error, so one request cannot drive an unbounded walk or write. A query may resolve against a fork's overlay by naming the fork, the same copy-on-write the row views use (A10.5). A graph traversal reuses the query `Filter` / `Value` / `Consistency` types, so there is one predicate grammar across the row and graph views.

The contract defines the full return set and start modes, but a backend serves the subset it can. The shipped managed engine serves the `nodes` and `edges` returns and the `ids` and `match` starts. The `paths` and `triplets` returns and the vector `nearest` start return a clean unsupported error until per-path tracking and a vector-indexed graph backend land, never a silent or partial answer (A12, the same capability-gated discipline as every other surface).

This realizes the formal-spec collection and object primitives that suit a graph (C6) without a separate query language.

---
---

# Part B. Bindings

A binding owns exactly the substrate-specific concerns: the mapping from logical identity to physical address, the carriage of the out-of-band attributes, the operation dispatch encoding, the request and reply mechanism, and the packing of the opaque `cause_at` locator. Everything else is inherited from Part A unchanged. Within a binding, every mapping below is a hard, fixtured contract.

## B1. The Iggy binding (normative)

### B1.1 Identity to physical address

| Logical | Iggy address |
| --- | --- |
| streaming record | stream, topic, partition, offset |
| ordering key | the conversation id as the partition key |
| collection / topic | a topic on a data stream |
| managed ops | a reserved command range against the connection (B1.4), not a topic |
| `cause_at` locator packing | the four-level (stream, topic, partition, offset) address as 20 big-endian bytes in the opaque locator slot |

The binding uses a dedicated ops stream carrying a fixed set of logical ops channels: control commands and the dead-letter queue. The logical channels are the contract. Their names are deployment-configurable and carry no meaning to the protocol. The current implementation defaults to an `_agdx` stream with topics `control.commands` and `dlq`, neither of which is a wire constant. A consumer resolves the configured names, it does not hardcode them. Query and the other managed operations are not topics: they ride the reserved command range (B1.4), off the log.

Partitioning and isolation are separate concerns on Iggy, and conflating them is a mistake. Using the conversation id as the partition key buys total order within a conversation and lets independent conversations run in parallel across a topic's partitions, one conversation pinned to one partition (so a single very high-throughput conversation is bounded by one partition's, and on a shard-per-core server one core's, throughput, which is the cost of total order). Partitions are a throughput-and-ordering tool, never an access boundary: Iggy RBAC is enforced at the stream and topic level, not the partition level. So isolation between agents or workloads is a topology decision made above the protocol, by giving each its own stream or topic (and, across credentials, its own connection), not by partitioning. The stream and the ops-topic names are deployment-configurable for exactly this reason.

Iggy accepts a stream or topic reference as either a name or a resolved numeric id, so the binding passes the numeric id on every publish and consume to save addressing bytes. This is a binding optimization, not a core requirement. The core names a topic logically (A3), and a substrate that addresses by name (Kafka, B2) simply does not get this particular saving. It is the same kind of substrate-specific win as the typed headers (B1.5), and it costs no portability because the core never pinned the addressing form.

### B1.2 Out-of-band carriage: the header dictionary

Iggy carries the out-of-band attributes as typed headers, which is where it earns its place. Keys are short and values typed, so the header budget buys the most routing information per byte. The id routing-header duplicates use the typed 128-bit value, little-endian.

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
| `gen_ai.conversation.id` | u128 | conversation id (OpenTelemetry) |
| `gen_ai.agent.id` | string | producing agent (OpenTelemetry) |
| `gen_ai.usage.input_tokens` / `output_tokens` | u64 | token usage (OpenTelemetry) |
| `agdx.cause`, `agdx.parent_conv`, `agdx.root_conv`, `agdx.to`, `agdx.idem`, `agdx.deadline`, `agdx.cost` | mixed | provenance: causal parent, parent and root conversation, addressee, dedup key, deadline, cost |

Header caps: 1024-byte soft cap on total header bytes per record, 255-byte ceiling on a single value, 9 bytes of per-header framing counted toward the cap.

There is no duplication between these headers and the typed envelope when an agent envelope is present. The two carriers serve two cases. A message that carries a typed `AgentEnvelope` (the body) stamps only the minimized routing projection out of band: the content-type, the wire version, the conversation as the partitioning `Uint128`, and the addressee when targeted. The envelope is the single source of truth for everything else (`source`, `cause`, `correlation`, `deadline_micros`, `idempotency_key`), which is never copied to a header. The full provenance dictionary is used only for messages published without an envelope (the generic provenance path), where the headers are the sole carrier and there is nothing to duplicate. So a field is in exactly one place per message: the envelope for typed agent messages, the headers for generic provenance messages.

### B1.3 Versioning carriage

The durable agentic record carries its wire version as the `agdx.av` header, never a body field. The managed request envelopes carry a `v` first field, but the binding also negotiates version at connect through the `hello` reply (A12), which fails fast before a round trip. The in-band `v` is therefore redundant defense, not the primary mechanism.

### B1.4 Operation dispatch and the command range

The operation registry (A5) is realized as a `u32` command code. Raw Apache Iggy has no such range and rejects these, which enforces the open-versus-managed boundary.

| Op id | Code |
| --- | --- |
| `hello` | 1_000_000 |
| `backend_hello` (internal: the managed backend announces its `OpVersions` to the streaming server, not client-facing) | 1_000_001 |
| `query` | 1_000_100 |
| `registry.get_projection` / `list_projections` | 1_000_110 / 1_000_111 |
| `registry.get_schema` / `list_schemas` | 1_000_120 / 1_000_121 |
| `registry.register_schema` / `decode_record` | 1_000_122 / 1_000_123 |
| `kv.get` / `set` / `scan` / `delete` / `delete_many` / `namespaces` / `cas` | 1_000_200 .. 1_000_206 |
| `fork.create` / `delete` / `promote` / `list` / `put` | 1_000_300 .. 1_000_304 |

The base value is high (a million) only to avoid colliding with Apache Iggy's own low command codes, and the 100-wide blocks are organizational. Both are Iggy-local. They are pinned and fixtured here in the binding, not in the core.

The server forwards an opaque CBOR request to the managed side over a local channel, stamping the authenticated identity the SDK cannot set. A forwarded query carries the trusted user id, the client id, an audit correlation, and the opaque query envelope. A forwarded command additionally carries a read-all flag (which widens reads while writes stay scoped) and the command code. The socket frame is `[len: u32 little-endian][payload]`, 64 MiB ceiling.

### B1.5 Low-latency features exploited

The binding uses Iggy-specific fast paths because the core only requires that the out-of-band attributes be carriable, not how. The typed compact headers, the single multiplexed connection for publish and consume and managed commands together, and the low-latency local delivery paths are all used. A first-class zero-copy content-type is the next Iggy-leaning seam. None of this is a core requirement, so using it costs no portability.

## B2. The Kafka binding (illustrative, roadmap)

Kafka provides topics, partitions, offsets, consumer groups, retention, log compaction, an idempotent producer, and transactions. Only the binding concerns change.

| Concern | Kafka realization |
| --- | --- |
| identity to address | a collection or topic maps to a Kafka topic, the offset is the address, a namespace maps to a topic-name prefix |
| ordering key | the conversation id becomes the record key, hashed to a partition, ordered per key |
| `cause_at` locator packing | the slot is an opaque byte string, so a Kafka binding packs its own locator (topic name or UUID, partition, offset) into it with no envelope change. A consumer that cannot interpret it falls back to `cause` |
| out-of-band carriage | Kafka headers are name and byte-array pairs, untyped. The binding encodes the attributes into bytes (content-type one byte, version four big-endian bytes, conversation id sixteen bytes in a binding-declared order, names UTF-8) |
| operation dispatch | no command-code namespace. The operation name rides a header or the front of the payload, and the consumer dispatches on it |
| managed request and reply | no command channel. Managed ops ride a request topic and a reply topic correlated by the correlation id |
| state surface | a key-value store maps to a log-compacted topic keyed by the key, materialized into a state store. A tombstone is a null-value record. Compare-and-swap needs a single-writer-per-key processor or an external conditional store |
| dedup and exactly-once | the idempotent producer and transactions map onto the producer-dedup roadmap and the business idempotency key. The agent-id-to-group-id mapping is stated because Kafka group-id constraints differ from the 256-byte agent-name limit |
| query surface | not a Kafka primitive. Query, search, and aggregate are a managed layer above the log |

The payload, the envelope, the dictionaries, the validity matrix, and the result-code space are identical on Kafka and on Iggy. That identity is the proof the model is substrate-neutral.

## B3. Other substrates and the substrate requirement

- **NATS JetStream.** Subjects map to topics, the stream sequence is the offset, durable consumers are consumer groups, JetStream KV maps the state surface natively, headers are untyped. A good fit.
- **Apache Pulsar.** Topics, partitions, message ids, and subscriptions map closely, with native compaction for the state surface. A good fit.
- **A single-stream log broker.** Entry ids, consumer groups, and replay exist, but durability and partitioning are weaker and a single stream is the ordering unit. Possible for lighter deployments.

**The substrate requirement.** A substrate can host the model if it provides an append-only, partitioned, offset-addressed log with replay and keyed ordering, a way to carry the out-of-band attributes alongside a body, and either a request and reply mechanism or a topic pair to emulate one. Typed headers, low-latency local delivery, log compaction, and native transactions are optional accelerators a binding exploits when present. The query surface is always a managed layer above the log.

## B4. The HTTP binding (management and UI)

A management and UI surface that maps the same operation registry (A5) onto REST routes, for a browser or wasm client that has no binary substrate connection. It is a thin translation, not a second source of truth: a read forwards over the managed channel, a write publishes to the control topic, exactly as the binary binding does, so the two never diverge. There is no binary-SDK HTTP client.

| Operation | Route |
| --- | --- |
| capabilities probe (`hello`) | `GET /capabilities`, never gated, answers the managed flags and per-surface versions for UI feature-detection |
| `query` | `POST /query` (a `Query` JSON body) or `GET /query?index=&limit=&offset=&message_type=` for simple reads |
| `registry.list_projections` / `get_projection` | `GET /projections?topic=&name_contains=&id_prefix=&search=` / `GET /projections/{id}` |
| register / drop projection | `POST /projections` / `DELETE /projections/{id}` (control envelope) |
| `registry.list_schemas` / `get_schema` / `register_schema` / `decode_record` | `GET /schemas?name_contains=` / `GET /schemas/{id}` / `POST /schemas` / `POST /schemas/{id}/decode` |
| apply / remove binding | `POST /bindings` / `DELETE /bindings` (control envelope) |
| `kv.get` / `set` / `delete` | `GET` / `PUT` / `DELETE /kv/{namespace}/{key}` (`GET` replies the value as the raw response body with the optional expiry in the `agdx-expires-at-micros` response header, or `404` when absent. `PUT` takes the value as the raw body with `?expires_at_micros`. A scan page instead carries the base64url `KvEntryView` JSON, since a JSON array cannot hold raw bytes.) |
| `kv.cas` | `PUT /kv/{namespace}/{key}/cas?expect_version=&expect_absent=` (the value rides the raw body, a `409` with the current version on a precondition miss) |
| `kv.scan` / `delete_many` / `namespaces` | `GET /kv/{namespace}?prefix=&start=&end=&key_contains=&limit=&cursor=` / `DELETE /kv/{namespace}?...` / `GET /kv` |
| `fork.list` / `create` / `delete` / `promote` / `put` | `GET` / `POST /forks`, `DELETE /forks/{id}`, `POST /forks/{id}/promote`, `PUT /forks/{id}/rows` |
| `graph.query` / `neighbors` | `POST /graph/{name}/query` (a `GraphQuery` JSON body) / `GET /graph/{name}/neighbors/{node}?dir=&edge_type=&depth=&limit=` |
| `registry.list_graphs` / `get` / register / drop graph projection | `GET /graphs?topic=&name_contains=&id_prefix=&search=` / `GET /graphs/{id}` / `POST /graphs` / `DELETE /graphs/{id}` (the projection listing narrowed to graph-kind projections, register and drop riding the control envelope) |

A graph projection registers and drops through the control commands `RegisterGraph` / `DropGraph` on the control topic (A11.2), the same durable path as a row projection. The `/graphs` routes surface that path for a browser or wasm client: `POST` registers, `DELETE` drops, and `GET` lists or reads, narrowed to graph-kind projections so a graph explorer discovers the available graphs by name. The node and edge data is written by the projector or the binary `graph.upsert` op, not over HTTP.

JSON request and reply bodies mirror the CBOR wire types. Key-value keys are arbitrary bytes, so in a path or query parameter they are base64url-encoded, and a value rides the raw request body with the optional expiry carried as the `expires_at_micros` query parameter. The authenticated identity is the same trust boundary the binary binding stamps, so scoping is identical. The route prefix is a deployment-configurable operational name, not a wire constant (the current implementation defaults to `/agdx`, so `GET /capabilities` is served at `/agdx/capabilities`).

Each wire error maps to a precise status, the same mapping on every surface, so a client need not parse error strings:

| Condition | Status |
| --- | --- |
| missing index, fork, or key | 404 Not Found |
| unsupported op, or the managed surface disabled | 501 Not Implemented |
| result or value too large | 413 Payload Too Large |
| malformed input or version skew | 400 Bad Request |
| managed backend failure or unreachable | 502 Bad Gateway |
| missing or invalid credential | 401 Unauthorized |

The reply contract is uniform: a `2xx` carries the bare `Ok` payload, and every non-`2xx` carries a canonical **error body** `{ code, message, detail? }`. Bare means the inner value, never the binary band's reply wrapper. The browse routes serve a JSON array (`GET /projections`, `GET /schemas`) or a single object (`GET /projections/{id}`, `GET /schemas/{id}`, with `404` for absent), not the `BrowseReply`/`BrowseOutcome` envelope the CBOR socket multiplexes its ops through. A registration replies the bare allocated id. `code` is the unified `ResultCode` (A7) so a client dispatches on the classification rather than parsing the message text. `message` is human-facing, and `detail` is optional structured context (e.g. the conflicting version on a compare-and-swap miss). The status line is derived from `code` via the table above, so the two never disagree. The route constants, the path builders, the typed query-parameter structs (one field per parameter name above), this error body, and a typed client over a caller-injected transport (`gloo-net` on wasm, `reqwest` natively) are all owned in the wire crate's `http` / `http_client` modules, so a browser or native client carries no hand-rolled route, base64url, or query-string glue, and any drift is a compile or doc-test failure rather than a production `404`.

The capabilities reply carries the grouped shape (A12): `managed`, a `query` object (`available`, `projections`, `schemas`, and the served `consistency` level), a `kv` object (`available`, `cas`), `graph`, `fork`, the op `versions`, and the materialization `backends`. The sub-features default off (`kv.cas` off, `query.consistency` at `eventual`, `graph` off), and a server advertises one only when it genuinely serves it: over-advertising would turn a clean unsupported error into a silent wrong answer. Graph nodes and edges ride the JSON views (`GraphNodeView` / `GraphEdgeView` / `GraphResultView`) with ids as strings, since the CBOR id is bytes a JSON string cannot carry raw.

---

# Part C. Rationale and roadmap

## C1. Design rationale

### C1.1 Invariants

Every port preserves these, and they are what the fixture corpus pins.

- One CBOR encoding with named fields, and decoding fails on trailing bytes.
- The durable layer versions out of band, never with a body field.
- Dictionaries are pinned small integers, and an unknown code passes through rather than failing the record.
- Record identity is decoupled from log position, so a record keeps its id across replay, mirroring, and republish.
- The per-kind validity matrix is enforced at three layers (the wire validate function, the SDK constructors, and receivers) and pinned by positive and negative fixtures.
- A large or external body rides as a claim-check with a digest, verified against the fetched bytes.
- Capability negotiation returns a clean unsupported error and never silently downgrades.
- Framing is sans-io, pure functions over byte slices, with no transport of its own.

### C1.2 Why it is built this way

These decisions are settled. They are recorded here because they are not obvious from the field tables alone.

**The typed envelope is the single source of truth, and the binding stamps only a minimal routing projection out of band.** A message carrying an `AgentEnvelope` puts just the routing subset in headers (content-type, wire version, conversation, and the addressee when targeted). Everything else (`source`, `cause`, `correlation`, `deadline_micros`, `idempotency_key`) lives in the envelope and is never copied to a header. Three forces require this. Several envelope fields are structured or large and do not fit a flat capped header space. Headers are substrate-specific and lossy across hops, so they cannot survive mirroring or republish. And a router wants only a small subset. The generic provenance header dictionary is therefore used only for messages published without an envelope, where the headers are the sole carrier and nothing is duplicated (B1.2).

**One result space.** Every surface error projects onto a single `ResultCode` and its HTTP status while keeping its typed detail (A7). A generic client dispatches on the code, and a specialist still reads the typed variant.

**`cause_at` is an opaque, binding-defined byte string, and `cause` is the portable identity.** A foreign consumer that cannot interpret the locator falls back to `cause` (a portable id). This keeps the envelope substrate-neutral while letting each binding pack its native address. The Iggy binding packs its four-level position as 20 big-endian bytes.

**Optimistic concurrency is an entry version plus a conditional write.** The key-value store carries a version token, and `kv.cas` commits only against the expected version or absence (A10.3). It is gated by the `kv_cas` capability, so a backend that cannot do a conditional write returns unsupported rather than a wrong answer.

**Read consistency is a per-query level that fails rather than downgrades.** A query names `eventual`, `read_your_writes`, or `strong` (A11.3). A level the backend cannot serve returns `stale` or unsupported, never a silently weaker read, and is gated by the `read_your_writes` and `strong_consistency` capabilities.

**`must_understand` lets one message demand strict handling without a version bump.** The marker is a u64 bitset on the envelope (A9.1). A clear bit means ignore-if-unknown, and a set bit a receiver does not implement means reject. No bits are defined yet, so the mechanism is in place for the first feature that needs it.

## C2. What not to adopt

- **Queue and broker primitives.** Server-push delivery, ack and nack settle verbs, delivery-mode negotiation, redelivery counters and visibility timeouts, broker-managed dead-letter queues, and message priority. The streaming layer is a log (A1.5), and these are queue semantics that either do not map or are already provided by offsets, replay, and the reliable consumer. This is the single biggest filter applied to the broker idea-space.
- **Telemetry as a separate primitive set.** Logs, metrics, traces, and events are records on topics with OTel-aligned provenance headers and a trace projection, not new operations (A1.5).
- **A transport stack.** No frame layout, flow control, multiplexing, keepalive, or connection handshake of our own. The substrate owns transport. This is the deliberate inversion of a transport-shaped protocol into a data-shaped one.
- **Cross-substrate distributed transactions.** A transaction, if offered, is scoped to one connection and one responder.
- **Mutable objects as the primary store.** The state surface is a read model on the log. The log stays the source of truth.
- **Trust scores or admission control in the envelope.** Every agent-written field is a claim. Enforcement lives at the capability owner. Identity granularity equals credential granularity.

## C3. Roadmap

These are draft proposals. None is settled, and none is pinned into the fixture corpus. They are recorded so the design space is visible, and they evolve as real use sharpens the scope. Each is expressible in the wire contract independently of the runtime that serves it: the contract defines the shape, a capability flag gates the operation, and an unserved operation returns a clean unsupported error. The dormant signature, the dormant claim-check encryption code, and the agent version that reads zero until consumed follow the same pattern.

| Proposal | Shape (draft) |
| --- | --- |
| mixed-operation batch | several managed request-and-reply ops in one round trip to amortize it, each carrying its own result, not atomic |
| strong-consistency semantics | the `strong` level is wired (A11.3) but its linearizable cross-replica semantics past read-your-writes are still being pinned |
| lease renewal | the advisory lease and release ship (A10.4), the fencing token is the entry version, and re-acquire after expiry works; an explicit renewal op is still open |
| generalized causality token | an opaque happens-before token (a generalization of `cause_at`), recommended a hybrid logical clock, fail rather than reorder. Encoding still open until the cross-region backend proves it |
| watch and notify (A5) | change notification over the materialized read model, delivered log-natively as events on a change topic and consumed by offset, not a broker watch. Delivery semantics still open |
| signature activation and key registry (A9.5) | one additive optional envelope field, the registry runtime and the flag flip remain |
| content-block lifecycle and applied-through-offset ack | typed text, reasoning, data, tool-call blocks with start, delta, finish, and a reply naming the log offset a command took effect at (an offset, not a queue ack) |

## C4. Conformance and fixtures

A conformant port decodes and validates the complete envelope, produces whatever subset of kinds its use case needs, and is pinned by the positive and negative fixture corpus in both directions. It depends only on a CBOR library, the 16-byte id codec, the constants, the validate function, and the verb behaviors. No crypto, no proc-macros, no compression, and no transport beyond its substrate client.

A binding adds fixtures for three things only: the identity-to-address mapping, the operation dispatch, and the out-of-band header encoding. The payload fixtures are shared across every binding.

One byte-order rule is easy to get wrong. An application AGDX id (`conversation`, `record`, `correlation`, `channel`, minted SDK-side, distinct from the substrate's own message id or offset) rides the CBOR envelope as a 16-byte **big-endian** byte string (A3). When the same id is also stamped as an Iggy routing header (the conversation, for partitioning), that header copy is Iggy's typed `Uint128`, which is **little-endian** (B1.2). One id, two encodings by carrier. A port that reads the header copy as big-endian will mis-route, so the corpus pins both forms.

## C5. Stability and evolution

The contract is pre-1.0 and open to change. The normative surface is Part A as built plus the Iggy binding (B1). The roadmap (C3) is a sketch of the design space, and a proposal is pinned into the fixture corpus only once its shape is settled. The dormant slots already in the wire (the signature type, the claim-check encryption code, and the `must_understand` marker) activate additively under that same rule. The Rust SDK surface has its own stability contract in the appendix below, distinct from this wire contract.

## C6. Formal-spec conformance

The formal IETF-style draft (`draft-laserdata-agdx`) defines the protocol as named operations over a Data Object model. This implementation realizes that draft over Apache Iggy: the draft's own binary transport (its framing, opcodes, TLV options, and connection-control operations, draft Sections 4 through 7) is **deliberately not implemented**, because the SDK binds onto the Apache Iggy SDK's transport (binary command channel and HTTP API) per the thesis in A1.2. The draft's data operations map onto Iggy-binding command codes. This section is the conformance map, so the formal draft and this dev spec are provably the same protocol over two carriers.

**Operation map** (draft opcode to this implementation). Object primitives (draft 8) land on the key-value surface (A10), collection primitives (draft 9) on query (A11), messaging (draft 10) on the log (A1.5), schema (draft 13) on the registry (A11.2).

| Draft op | Realized as | State |
| --- | --- | --- |
| `PUT` / `GET` / `DELETE` / `CAS` | `kv.set` / `kv.get` / `kv.delete` / `kv.cas` | shipped |
| `EXISTS` / `EXPIRE` / `PATCH` / `LEASE` / `RELEASE` | `kv.exists` / `kv.expire` / `kv.patch` / `kv.lease` / `kv.release` (A10.2) | shipped |
| conditional `GET` / `DELETE` (`IF_MATCH` / `IF_NONE_MATCH`) | the `if_none_match` / `if_match` carriage on `kv.get` / `kv.delete` | shipped |
| `COPY` / `MOVE` | roadmap (composable from get + set + delete) | roadmap |
| `QUERY` / `AGGREGATE` / `COUNT` | the query IR (A11.3), `AggFunc` covering count/sum/min/max/avg/percentile/stddev/distinct | shipped |
| `SEARCH` | the query IR's vector + filter (hybrid); full-text is roadmap | partial |
| `SCAN` / `LIST` | `kv.scan` + the registry browse + `kv.namespaces` | shipped |
| `PUBLISH` / `CONSUME` | `publish` + the log reader/cursor | shipped |
| `REGISTER` / `RESOLVE` | `RegisterSchema` (A11.2) + schema browse | shipped |
| `BATCH` | roadmap (the mixed-operation batch, C3) | roadmap |
| `BEGIN` / `COMMIT` / `ABORT` (txn) | optional, managed-plane only (the draft marks transactions OPTIONAL) | roadmap |
| `EVENT` / `LOG` / `METRIC` / `TRACE` | telemetry is a published record plus the provenance OTel header dictionary (A6), not dedicated ops | convention |

**Status-code map.** The draft's statuses project onto the unified result-code space (A7): `CREATED`/`OK`/`NO_CONTENT` to `ok`, `VERSION_CONFLICT`/`ALREADY_EXISTS`/`LEASE_LOST`/`TXN_CONFLICT` to `conflict`, `NOT_FOUND` to `not-found`, `NOT_IMPLEMENTED` to `unsupported`, `RESOURCE_EXHAUSTED`/backend faults to `backend`, `INVALID_ARGUMENT` to `invalid-argument`, `PARTIAL` to the paging cursor rather than a status.

**Deliberately not adopted (broker semantics the log does not have).** The draft's push messaging (`SUBSCRIBE` / `DELIVER` / `ACK` / `NACK` / per-subscription `ACK_MODE`) is **not** implemented: Apache Iggy is an offset log, not a queue, so delivery guarantees (at-least-once, redelivery, dead-lettering, delivery-count) come from log replay plus idempotent dedup (A1.5, the reliable consumer and the dead-letter capsule A9.5), not from broker settlement ops. `WATCH` / `NOTIFY` change-capture is on the roadmap as a log-native change topic, not a broker watch (C3).

---
---

# Appendix. SDK API stability

This is about the Rust SDK surface, distinct from the wire contract above.

- **Builders are the contract.** Construct wire and data types through their builders and fluent methods. Public fields exist for reading results and for wire mirroring. New fields may appear in any minor release, so exhaustive struct literals are not supported.
- **Wire mirrors are wire-stability-bound, not API-stability-bound.** Types that mirror the wire keep their public fields because the wire defines them, and they change only when the wire does, per the compatibility rules in A8.
- **Terminal verb convention.** A fluent builder ends in `.send().await` for a write or `.fetch().await` for a read. Direct async methods are used only where there is nothing to build.
- **Errors are typed and forward-compatible.** Managed failures nest the wire error intact. Every public error enum and the capability structs (the hello reply, op-version set, and capability map) are `#[non_exhaustive]`, so a new variant or field is not a breaking change. Always keep a wildcard arm. The growable u8 dictionaries (task state, agent error code, dead-letter reason) instead carry an `Unrecognized(u8)` variant that decodes and re-encodes an unknown code byte-for-byte, so an old build relays a newer peer's code rather than failing. The internally tagged configuration enums that cross the JSON HTTP surface (`SchemaSource`, `RetentionPolicy`) carry a unit `Unknown` `#[serde(other)]` catch-all for the same reason: an unknown `kind` decodes rather than failing the whole reply, though it is lossy (the original kind and fields are dropped, so a decoder must not re-apply an `Unknown`). `ContentType` keeps its forward-compat at the byte level instead, through `from_code(u8) -> Option`, because the `agdx.ct` u8 code is its canonical wire form.
- **Facade growth lands on sub-facade handles**, not as new flat methods on the client. The handles are the only control surface.
