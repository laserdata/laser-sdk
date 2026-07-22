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

Five layers, from the ground up: **substrate** (the log itself), **wire** (the typed portable contract), **platform** (publish, views, state), **fabric** (the agent envelope, runtime, coordination, and memory), **edges** (the A2A, MCP, and AG-UI bridges). Every layer is independently adoptable: stream on the substrate alone, pin a port to the wire contract, use the platform with no agent concepts, or run the fabric with no edge bridge.

Read **Part A** for the substrate-neutral model, **Part B** for how it binds to a substrate, **Part C** for the rationale and roadmap.

## 0. Status and conventions

This is a design and specification document, not a frozen release. The repository is pre-1.0 and breaking wire changes are allowed.

- The normative body specifies the protocol as it stands. **Roadmap** marks a proposal that is not yet part of the contract: a draft that may evolve or break, recorded so the design space is visible, and pinned into the fixture corpus only once settled.
- Field tables give the logical type. The byte form is named-field CBOR (A2) unless a binding says otherwise.
- Requirement words (must, must not, should, may) are normative.
- Component names appear in prose. File paths do not.
- **Vendor-neutral and pinned for interoperability.** The specification names nothing after any implementor. Its wire contract uses the neutral `agdx` namespace: the header keys (`agdx.*`), pinned dictionaries, and signing domain are fixed so independent implementations interoperate. A binding explicitly identifies which operational names are pinned and which are deployment-configurable. Substrate product names (Apache Iggy, Kafka) appear in binding chapters because a binding targets a named substrate. Keys drawn from OpenTelemetry use the `gen_ai.` namespace verbatim.

---
---

# Part A. The core (substrate-neutral)

Nothing in Part A names a server, a header key, a command code, or a byte order. The core assumes only an abstract substrate: an append-only, partitioned, offset-addressed log with replay and keyed ordering, able to carry a small set of attributes alongside a message body, and offering either a request and reply mechanism or a topic pair to emulate one.

## A1. Overview

### A1.1 What the protocol provides

An implementation provides three things over one authenticated connection.

1. Typed, batched message streaming over a durable, partitioned, replayable log.
2. A general data surface on that log: declared projections with a query DSL, a key-value store, and copy-on-write forks of the materialized read model.
3. An optional agentic layer: a reliable runtime and a typed agent envelope on the streaming layer.

These are not separate specifications. This document is one spec with one implementation. The agent envelope is the streaming layer's payload, not a protocol layered beside the data model, and there is one version story and one fixture corpus across the whole surface.

The log is the source of truth. Queries, projections, key-value, and agent coordination are read models on top of it, never a second store kept in sync by hand.

The stack has five named layers, and the names are part of the contract's vocabulary because an architecture users can say out loud is one they adopt piecemeal. **Substrate**: the durable, partitioned, replayable log (Apache Iggy under the normative binding). **Wire**: the typed, runtime-free contract (codes, envelopes, dictionaries, caps, the fixture corpus) that any language can implement. **Platform**: the general data surface, publish and consume, projections and query, key-value and forks. **Fabric**: the agentic layer, the envelope, the reliable runtime, coordination, and memory. **Edges**: the bridges that map external agent protocols (A2A, MCP, AG-UI) onto the fabric. Each layer is adoptable without the ones above it.

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

- Delivery is at-least-once with replay from any offset. Ordering is total within a partition. Agent records use the conversation id as their partition key, generic streaming uses the caller's selected partitioning.
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

The client SDK instruments its own verbs and runtime loops with spans (target `laser`, hot-path verbs at debug so a default info filter never taxes them, lifecycle at info), and the span fields are drawn from the same pinned vocabulary the wire carries. The mapping is fixed, so a standard OpenTelemetry pipeline joins client spans with log-derived traces without custom translation: the log is the trace, made operational.

| Span field | Header key | Envelope field |
| --- | --- | --- |
| `conversation` | `gen_ai.conversation.id` | `AgentEnvelope.conversation` |
| `correlation` | `agdx.corr` | `AgentEnvelope.correlation` |
| `agent` | `gen_ai.agent.id` | `AgentEnvelope.source` |
| `topic` / `index` | (the address, not a header) | the topic the record rides / the materialized index queried |
| `operation` | (envelope-only, no header) | `AgentEnvelope.operation` (client verbs outside the envelope use the verb name: `publish`, `poll`, `send`, `ask`, `handle`, `contract`, `workflow`, `managed`) |
| `code` | (managed calls only) | the command code of the managed operation |

No exporter ships with the SDK. `tracing` is the seam and the deployment's subscriber bridges to OTel.

One consequence matters for interoperability. The edge bridges (A2A, MCP, AG-UI, and the candidate ATP and LangChain-streaming bridges) depend only on these log primitives: publish, offset-replay consume, and request-and-reply correlation. An edge protocol's settlement and reconnection concepts map onto offsets (a streaming client resumes from an offset, a task reply is matched by correlation), so interoperability is preserved without importing any queue semantics. The [edge interoperability guide](interop.md) covers the mappings.

Scoped to those external bridges only, the MCP and A2A edge applies the MCP-2025-11 authorization model: audience validation (a token is accepted only when minted for this server), no token passthrough (an inbound token is never forwarded to the log or an upstream), and step-up (a `403` naming the required scope when the caller is short one). The internal log path is unchanged, it authenticates on the server-stamped identity (B1.4).

## A2. Encoding rules

- **One encoding.** Every payload is named-field CBOR (RFC 8949), serialized through a single entry point so no surface drifts to a different encoding.
- **Declaration order, absent optionals skipped.** An unused optional field costs zero bytes. Unknown fields are ignored on decode, so every future field is additive and free.
- **No silent skip.** A payload is exactly one CBOR item. Trailing bytes are a decode error. Decoding must fail on corrupt or wrong-typed known data, and only fields foreign to the schema may be ignored.
- **Machine ids** ride as fixed-width 16-byte CBOR byte strings, not bignum-tagged integers, so a port needs only a byte-string reader.
- **Signing input is canonical by this encoding.** The envelope signature (A9.5) covers the domain separator `agdx.signature.v1`, the encoded `SignatureContext` when the signer binds one, then this same named-field encoding of the envelope with the signature field cleared. Because the encoding is deterministic (declaration order, absent optionals skipped), a decode-then-re-encode is byte-identical, so a verifier reconstructs the exact signing input. This round-trip property is pinned by fixtures.

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
| agent routing / ordering key | 128-bit | the substrate partitions an agent conversation without reading the body. Generic streaming may use another binding-supported key or partitioning mode |
| operation identity | name or code | a request is dispatched without a parse |
| correlation | 128-bit | a reply is matched and a stream is filtered cheaply |
| fence token | u64, optional | a consumer rejects a stale-holder replay before the effect runs, without reading the body |

### A4.1 Trusted versus advisory fields

Out-of-band fields split into two classes, and the split is load-bearing for anyone who wants to bill, meter, or audit on them. A **trusted** field is one a receiver can act on because something verifies it. An **advisory** field is a self-asserted hint any topic writer can stamp: useful for display and correlation, never a basis for billing or an access decision on its own. The table names, per field, its carrier, who can write it, what verifies it, and whether it is safe to bill or audit on. A deployment selects one of the security profiles in B1.1. A verified principal signature can establish authorship on a shared topic. An ACL-bound write-exclusive topic establishes it through topology. Unsigned records on a shared topic remain advisory.

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

Operation **identity and semantics** are core. Operation **encoding** is binding-owned. The core registry names each operation and fixes its meaning. A binding maps the name to its own dispatch. This generalizes the content-type dictionary pattern (a logical variant with a per-wire code) from content types to operations.

| Op id | Surface | Semantics | Status |
| --- | --- | --- | --- |
| `hello` | control | capability and version probe |  |
| `authz.whoami` | governance | the caller's own effective capabilities (roles + flattened grants), answered to any authenticated caller |  |
| `authz.list_roles` / `get_role` / `get_bindings` | governance | browse roles and a user's bound role set |  |
| `authz.define_role` / `delete_role` / `bind_roles` | governance | define/replace, delete, and bind roles, journalled. Role names must pass `validate_role_name` (64-byte charset safelist, B1.4). `bind_roles` takes an optional `expect_revision` compare-and-swap precondition (a mismatch is a `conflict`). Requires `authz:admin` or the near-root server-management permission (B1.4) |  |
| `authz.history` | governance | read the authorization change log for a role, a user's bindings, or all, paged by revision (who granted what, when). Requires `authz:read` |  |
| `query` | views | run a query IR, return rows or aggregates |  |
| `registry.get_projection` / `list_projections` | views | browse projections |  |
| `registry.get_schema` / `list_schemas` | views | browse writer schemas |  |
| `registry.register_schema` | views | allocate a writer-schema id |  |
| `registry.decode_record` | views | decode a body under a registered schema |  |
| `kv.get` / `set` / `delete` / `delete_many` / `scan` / `namespaces` | state | key-value operations |  |
| `kv.copy` / `move` | state | copy the value at one key to another key (optionally across namespaces) in one backend transaction. Move is copy plus delete of the source. `Committed` on success, `NotFound` when the source is absent, destination overwritten (a guarded copy composes `exists` + `cas`) |  |
| `kv.cas` | state | conditional set on a version token |  |
| `kv.cas_fenced` | state | conditional set applied only while the task fence sequence still holds |  |
| `kv.exists` / `expire` / `patch` | state | metadata probe, in-place expiry, merge patch (formal object primitives, C6) |  |
| `kv.lease` / `release` | state | advisory lease on a key (unsupported error when the backend cannot serve it) |  |
| `fork.create` / `delete` / `promote` / `list` / `put` | state | copy-on-write branch operations |  |
| `graph.query` / `neighbors` / `upsert` | views | knowledge-graph traversal, one-hop neighbors, node/edge upsert (A13). The graph name is at most 128 B, non-empty, control-character-free (`validate_graph_name`), enforced by the SDK edge and the serving plane |  |
| `batch` | control | the mixed-operation batch: up to `MAX_BATCH_OPS` (64) managed requests in one round trip, each item carrying its own command code and encoded request, each result that op's own reply bytes in order. Amortizes the round trip and nothing else: items execute independently, a failed item fails alone (explicitly NOT atomic), a nested batch is rejected. An old backend answers the unknown code with the surface-agnostic `CommandError`, decoded client-side as the typed unsupported, so no capability bit is needed |  |
| `agent.submit` / `cancel` / `status` / `list` | coordination | the run registry: submit records intent and mints the run identity (content-addressed, so a retried submit converges), delivery stays the envelope the SDK publishes, transitions are folded from the status records a registered run stamps with the `run` metadata key (A9.6), cancel records an intent flag the engine observes at a step boundary. `submit` MAY carry a multi-dimensional `RunBudget` (events, model calls, tool calls, patches, recursion depth, wall-clock, cost) the run fold accumulates, failing the run when a cap is crossed. It is a governance governor, not a grant. A managed read model over the log, never a second source of truth |  |
| change feed (no request op) | views | change notification over the read model: a projection binding opts in with `notify`, the projector publishes one change record per committed batch on the changes channel (A11.5, B1.1), and a consumer reads it by offset like any topic. Gated by the `watch` feature bit (A12), it adds no request op, so there is no `watch`/`unwatch` verb to register |

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
| unauthenticated | 8 | 401 | no credential, or an invalid one: the caller is not authenticated |
| backend | 9 | 502 | the managed backend failed or was unreachable |
| forbidden | 10 | 403 | authenticated, but the grant needed for the operation is missing |
| step-up-required | 11 | 403 | authenticated, but a stronger authentication (a step-up) is needed |

An unknown code from a newer peer rides through as `unrecognized(code)` and re-encodes byte-for-byte, the same forward-compat shape the growable u8 dictionaries use, so an old build relays it rather than failing. Each binding maps the logical code to its own carriage (the HTTP binding to a status, B4).

A `CommandError` of `{ code, message }` is the surface-agnostic reply for a command code a server does not handle. Each managed surface has its own typed reply, so a server that receives an unhandled or unsupported code (a forwarded compare-and-swap on a build without it, or any future additive code) has no single surface to answer in, and a wrong-surface error fails to decode in a client awaiting a different reply. The server answers such a code with a `CommandError`, and a client that cannot decode the surface's typed reply decodes `CommandError` next, turning the reply into a typed code instead of an opaque transport failure. The two shapes are disjoint, so the fallback decode never misfires on a real reply.

The key-value, fork, and agent-workflow error enums carry a dormant `NotLeader` variant: a clustered deployment's node that does not own an operation's mutation ordering declines the conditional write with it, the SDK classifies it as retryable and as `not_leader`, and the caller re-resolves the owner and retries. It is appended at each enum's tail and **no server emits it yet**: an externally tagged serde enum is not forward compatible for readers, so emission waits for a coordinated SDK-first rollout gated by a capability, never a silent server-side switch-on.

## A8. Versioning, causality, idempotency, expiry, consistency

- **Versioning** is out of band by necessity for a durable record: a reader selects its decoder before reading the body, so the wire version is an out-of-band attribute (A4), never a body field. The managed surfaces additionally negotiate version at connect (A12) and fail fast before a round trip. Per-message strict handling beyond a whole-version bump rides the agent envelope's must-understand marker (A9.1).
- **Causality** rides a portable parent record id plus an optional log-position locator (A9.1). A cross-region happens-before token is a roadmap proposal (C3).
- **Idempotency** is the business key (A3), scoped to the authenticated identity so one peer cannot replay or suppress another's operation.
- **Expiry** is an absolute epoch-microsecond time. An expired object reads as absent.
- **Consistency** is a per-query `Consistency` level (`eventual`, `read_your_writes`, `strong`), fail-not-downgrade: a level that cannot be met returns a `stale` result rather than silently serving older data (A11.3, A11.4).
- **Fencing** is the at-most-one-effective-writer guarantee for an exclusive effect. The producer holds a strictly-monotonic per-task fence (the lease grant returns it, A10.3), and carries it out of band (the fence token, A4). An effect that lands in the key-value store is gated by the fenced compare-and-swap (A10.3), and an effect that lands on the log is gated consumer-side: a record whose fence is below the highest the consumer has accepted for the task is a stale-holder replay and is dropped, ordered before idempotency dedup so it cannot consume the legitimate retry's slot.

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
| `signature` | Signature capsule, optional | a detached signature over the canonical encoding with the signature absent, domain-separated by `agdx.signature.v1` (A9.5). Absent means an unsigned record (the open-world default). Verified SDK-side, the wire crate stays crypto-free. May ride any kind, like `metadata` |

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
| `operation` | O | O | O | R on opening chunk, X after | R (`task`\|`card`\|`progress`\|`quarantine`\|`unquarantine`) | O |
| `tool` | O | O | O | O | X | O |
| `usage` | X | O | O | O (terminal chunk) | O | O |
| `body` | R | R | R | R (empty only with `last`) | O | R |
| `signature` | O | O | O | O | O | O |

The command and event boundary is the correlation rule: a message expecting a reply or effect is a `command` and requires `correlation`, a message expecting nothing is an `event`. Fire-and-forget commands do not exist.

### A9.3 Closed sub-vocabularies

- `status` discriminator (`operation`): `task` (A2A lifecycle, requires `correlation` and `task_state`), `card` (liveness and capability), `progress` (advisory ticks), `quarantine` (an operator marks an agent out of routing, body is the quarantined agent id, authorized by the registry topic's write access control), `unquarantine` (an operator lifts a prior quarantine, body is the agent id, same authorization, so quarantine is not a one-way door only retention expiry undoes).
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
- **AgentCard** (the `card` status body): optional `name`, optional `version`, a capped `capabilities` list (at most 64) of structured capability descriptors, optional `ttl_micros`. A card older than its TTL means a dead agent. Each capability descriptor names a `skill_id` (capped like every vocabulary string) and carries optional input/output content shape (a `ContentRef` naming either a content-type code or a registered schema id), optional advisory cost and latency classes, an optional max concurrency, an optional health (the pinned `healthy` / `degraded` / `unavailable` dictionary, unknown codes passing through like `TaskState`), and an optional load (per-mille of advertised capacity).
- **AgentPresence** (the live presence body, carried in the connection-metadata channel of the binding, not in an envelope): `v` (the body version, carried in-band because the metadata channel has no out-of-band version header), `agent` (which agent this connection is, the link from a connection to its card), and an optional `inbox` (the topic this agent currently consumes its work on, within the stream its connection is scoped to). It is the live counterpart to the durable card: it vanishes on disconnect and answers where to send agent work right now. Presence is connection-scoped and singular: a client MUST NOT advertise a second agent on the same connection, and an SDK rejects that attempt rather than overwriting the first claim. A registry retains the connection's server-authenticated principal beside the presence. Claim routes may resolve by agent alone. Principal-bound routes MUST match that retained principal and fail closed on a missing or foreign binding. An absent inbox is liveness-only presence, and a target with no inbox is a routing failure surfaced to the caller, never silently rerouted.
- **FoldSnapshot**: a periodic snapshot of a client-side fold (the conversation, a workflow journal) so replay resumes from the tail rather than offset zero. Fields: `conversation` (the fold this snapshots), `as_of` (a per-partition map of the last offset folded, inclusive), and `state` (the opaque folded bytes, the producer's codec). Resume seeds the cursor at `offset + 1` per partition, because the cursor takes the next offset to read while the snapshot records the last folded. It rides as a body under the existing content type, so A6 is unchanged. It bounds the conversation and the journal folds, not the registry (an incremental time-to-live-evicting fold over `AgentCard`/`AgentPresence`, above).
- **Signature** (active, the optional `signature` envelope field A9.1): `scheme` (Ed25519 = 1), `key_id` (8 bytes), `bytes` (64 bytes), and an optional `context` (a `SignatureContext` of `content_type`/`agent_version`). The signing input is domain-separated by the fixed spec constant `agdx.signature.v1`, followed by the encoded `context` when present, then the canonical envelope encoding with the signature field absent. Binding the `context` covers the out-of-band interpretation attributes (`agdx.ct`/`agdx.av`), so an intermediary cannot flip the codec or decoder on a signed record without invalidating it. A context-less signature reproduces the pre-context preimage and stays byte-identical. The constant is one agreed value for the whole spec, so signatures verify across parties. No crypto enters the wire crate, verification is SDK-side against a per-agent key registry. The key is enrolled bound to the authenticated principal (the server-stamped identity), not the self-asserted `source`, so a verified signature proves the enrolled principal signed. When a contract verifier is enrolled, an unsigned terminal, unknown key, bad signature, or signer other than the route identity MUST NOT complete the contract. Principal-bound routes use the same server-authenticated principal for discovery and reply verification. Accepted contract and fan-out results expose the verified principal. Absence means no verifier was configured, never that verification failed. A key carries a kind (agent vs operator) and a validity window: a privileged control fact (quarantine/unquarantine) folds only when signed by an operator key valid at fold time, and the fold dedups by record id so a captured fact republished verbatim is dropped.

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

An enveloped message MAY carry the fence token (A4, A8) as a pinned `agdx.fence` metadata key instead of the binding header, exactly one carrier per message (the single-place rule, B1.2).

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

### A10.2 Key-value operations

| Op | Request fields | Reply outcome |
| --- | --- | --- |
| `kv.get` | namespace, key, optional `if_none_match(version)` | `Value(Option<entry>)`, or `NotModified` when the conditional version matches |
| `kv.set` | namespace, key, value, optional expiry | `Written` |
| `kv.cas` | namespace, key, value, optional expiry, precondition (`match(version)` \| `absent`) | `Committed { version }`, or `VersionConflict { current }` on a precondition miss |
| `kv.cas_fenced` | namespace, key, value, optional expiry, precondition, fence key, fence token | `Committed { version }`, `LeaseLost` on a stale fence, or `VersionConflict { current }` on a precondition miss |
| `kv.delete` | namespace, key, optional `if_match(version)` | `Deleted(bool)`, or `VersionConflict` when the conditional version misses |
| `kv.delete_many` | namespace, composed bounds (prefix, range, substring) | `DeletedMany(count)` |
| `kv.scan` | namespace, composed bounds, limit, cursor | `Page { entries, cursor }` |
| `kv.namespaces` | none | `Namespaces([{ namespace, entries }])` |
| `kv.exists` | namespace, key | `Metadata(Option<{ version, expires_at_micros, size_bytes }>)`, the cheap presence/precondition probe without the value |
| `kv.expire` | namespace, key, optional expiry (none clears) | `Versioned { version }` (the value is untouched) |
| `kv.patch` | namespace, key, patch bytes, optional `if_match` | `Versioned { version }` (a merge patch on a structured value, no full-object transfer) |
| `kv.lease` | namespace, key, lease ttl | `Leased { lease_token, granted_ttl_micros }`, or a clean unsupported error when the backend cannot serve leases |
| `kv.release` | namespace, key, lease token | `Released(bool)`, or `LeaseLost` when the token has expired |

Errors: unsupported, invalid key, invalid namespace, too large, backend, version, version-conflict, lease-lost, not-found (an in-place `expire` / `patch` targeting an absent or expired key). Namespaces isolate and scope keys within one shared store: the managed key-value store is a single dataset, not partitioned per user, so any principal permitted by the substrate (B1.4) reads and writes every namespace, the same way it sees every stream it has rights to. The binding stamps the authenticated user id as identity for audit, not as a visibility boundary. Scan caps: page at most 1000, default 100. The `exists` / `expire` / `patch` / `lease` / `release` ops and the conditional `if_match` / `if_none_match` carriage realize the data-object operations in C6 on the key-value surface.

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

Compare-and-swap is capability-gated by the `kv_cas` flag: a transactional row store (the embedded engine) serves it as a single conditional write, while a backend that cannot do a conditional write leaves the flag clear and returns a clean unsupported error. The version token rides on the entry either way, reading `0` on an unversioned store. The advisory lease (A10.4) builds on this: a lease is a key holding a token and an expiry, acquired and released by compare-and-swap. The fencing token a holder presents is not the lease row's version (that version is TTL-bounded and resets when the lease expires and is re-acquired, so it is not monotonic), it is a dedicated never-expiring fence sequence (A10.3 fenced compare-and-swap).

A fenced compare-and-swap (`kv.cas_fenced`, capability-gated by the `kv_cas_fenced` flag) is the sibling of `kv.cas`: the identical write and precondition, applied under one write guard only if the task's fence sequence still equals the presented `fence_token`. The fence sequence is a dedicated, never-expiring per-task counter the lease grant bumps and returns as the token, strictly monotonic for the life of the task key across acquire, re-acquire inside a live window, and post-expiry re-acquire alike. A failed fence returns `lease-lost`, a failed target precondition returns `version-conflict`. This is the at-most-one-effective-writer gate for an effect that lands in the key-value store, additive over the current key-value op version.

### A10.4 Advisory lease

```
kv.lease { namespace, key, lease_ttl_micros } -> Leased { lease_token, granted_ttl_micros }
kv.release { namespace, key, lease_token } -> Released(bool)
```

A bounded-TTL distributed lock built on compare-and-swap (A10.3): a lease is a key holding a token and an expiry, acquired and released by conditional write. The fencing token a holder presents is a dedicated never-expiring fence sequence the grant bumps and returns, not the lease row's own version (which is TTL-bounded and resets on re-acquire after expiry, so it cannot fence). Acquiring a key already held returns `version-conflict` (a contended lock, not a backend failure), so a caller retries. A holder presents the `lease_token` on protected mutations. A lease not renewed before its TTL expires is released automatically, release is atomic at the held token, and a stale token fails with `lease-lost`. The lease lives under a reserved per-caller namespace, so it never surfaces in the caller's own scans or namespace listings. A backend that cannot serve advisory leases returns `unsupported`. This realizes the `LEASE` / `RELEASE` data-object operations in C6.

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
| `EntitySchema` | node/edge extraction for a `graph` projection: node rules (label + RFC 6901 value pointer + optional embedding pointer) and edge rules (edge type + source/target pointers + optional valid-from/valid-to pointers for a bitemporal extracted edge). Pointer-based and deterministic, so building the graph needs no model call (A13) |
| `ProjectionBinding` | source (stream, topic), allowed projection refs, default projection, target tables, `notify` flag (default off, skipped on the wire when false, so a pre-feed binding encodes byte-identically): opt this binding's committed batches into the change feed (A11.5), optional `retention` policy overriding the fleet-wide default for these rows, independent of the source topic's own retention |
| `SchemaDef` | id (u32, permanent), source, optional name, optional version |
| `SchemaSource` | internally tagged on `kind`: `{kind: avro, schema}` \| `{kind: json_schema, schema}` \| `{kind: protobuf, descriptor_set, message_type}`. `descriptor_set` is a byte string. An unknown `kind` from a newer peer decodes to a forward-compatible `unknown` rather than failing the whole reply (the same shape `RetentionPolicy` uses), so an old client still reads a registry holding a source kind it cannot decode against. It must not re-register an `unknown`. |

Storage model: the log keeps the original bytes always, the indexed columns are extracted at materialize time and drive filters and ordering and aggregates, and the inline body is an optional copy alongside the row so typed fetches skip a log round trip. Each row also carries its origin log position (numeric stream id, topic id, partition, offset), stamped per row since one target table can hold rows from several source topics, so a reader can jump a query row back to the record it was projected from. The ids, not names, keep the pointer compact and rename-proof. A record with zero indexed fields is dropped by the projector while the log keeps the bytes. At most 32 indexed fields per record. The inline body copy is capped at 8 MiB (the same ceiling as a key-value value): a larger payload still indexes and stays in the log, it is just not duplicated into the row, so a typed fetch decodes from the log or a claim-check `ref` body. An explicit indexed-field directive wins over schema extraction for the same field.

A materialized view is a read model built by a projector consuming the log, so by default it is eventually consistent: a record is queryable once the projector has materialized it, not at the instant it is appended. The lag is the projector's read-and-apply latency and depends on the backend. A query that needs to see its own prior writes sets the `read_your_writes` consistency level (A11.3), which waits for the projector to reach the source log head and fails with `stale` rather than serving older data if it cannot catch up in time. The log itself stays the synchronous source of truth, readable by offset the moment the append acknowledges.

The server obligation for a non-`eventual` level is one rule, owned in the wire contract as a small `ConsistencyGate { applied, required }` helper: serve only when the projector's applied offset for the queried source has reached the required offset (the source head at query time), otherwise return `stale`. `eventual` always passes. `strong` is read-your-writes plus cross-replica agreement, so a `strong` backend layers its own cross-replica check on a gate that has already passed. Every backend uses the one gate so the fail-not-downgrade rule is enforced identically.

### A11.2 Control commands (durable on the control topic)

The control envelope carries `{ v, timestamp_micros, command }`. The command is one of `RegisterProjection`, `DropProjection`, `ApplyBinding`, `RemoveBinding`, `RegisterSchema`, `DropSchema`, `RegisterGraph`, `DropGraph`, `RegisterRunSource`, `RemoveRunSource`. A graph projection (`kind = graph` with an entity schema, A11.1) registers through `RegisterGraph` rather than `RegisterProjection`, so a deployment can gate graph registration separately. Schema ids are permanent, collisions are rejected, and a dropped schema still decodes records already stamped with its id. `RegisterRunSource` and `RemoveRunSource` name a `{ stream, topic }` source of run-tagged agent records: the deployment folds a registered source into the run registry (the agent-workflow surface) without a restart, and both commands are idempotent by source. Both variants are additive at the tail of the command enum, so an older reader rejects them as an unknown command rather than misdecoding an existing one.

### A11.3 The query IR

A backend-neutral logical IR, compiled per backend on the managed side.

| Field | Meaning |
| --- | --- |
| `index` | the materialized index name |
| `by_key` | exact-match key constraints, AND-composed |
| `message_type`, `time_range` | sugar for equality on the type field and a closed range on the timestamp |
| `filter` | a predicate tree (`all` / `any` / `not` / `pred`) |
| `vector` | nearest-neighbour search (field, embedding, top_k), distance in the row score |
| `text` | lexical relevance search (the query text, optionally one indexed field), relevance in the row score, text capped at 1024 bytes. Capability-gated (`keyword_search`): an unaware backend would silently drop the additive field, so a client refuses an unadvertised `text` before sending, and a backend without a lexical index answers unsupported rather than a contains approximation |
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
| aggregate func | `count`, `count_distinct`, `sum`, `avg`, `min`, `max`, `percentile`, `std_dev` |
| scalar value | string, int, uint, float, bool, null, list (untagged, variant order chosen so integers never become lossy floats) |
| consistency | `eventual` (serve as-is, the default), `read_your_writes` (wait for the projector to reach the source log head, else `stale`), `strong` (linearizable cross-replica). `read_your_writes` and `strong` are backend-gated |

### A11.4 Query reply

`Ok(QueryResult)` or `Err(QueryError)`. The error is one of `Unsupported`, `IndexNotFound`, `ForkNotFound`, `Backend`, `TooLarge { what, size, cap }`, `Version { expected, got }`, `Stale { what, applied, required }`, `Unauthorized`. `TooLarge` is raised when a query asks for more than one reply can carry. A reply rides a single 64 MiB socket frame and is bounded by it, so larger result sets page via `limit` and `offset` rather than a bigger reply. `Stale` is raised when a `read_your_writes` (or `strong`) query cannot be served at the requested freshness within the deadline: the projector's applied offset (`applied`) had not reached what the level required (`required`), so the caller retries rather than reading older data. `Unauthorized` is the per-source DSL check refusing the query (the resource the filter names is outside the caller's grants, B1.4). Every one of these errors also classifies into the unified result code (A7).

`QueryResult` carries a `Page { offset, limit, total, has_more }`. The default reply costs one page: the server fetches `limit + 1` rows and sets `has_more` exactly from whether the probe row was there, never counting the rest. `total` is an optional exact match count present only when the request set `want_total`, which runs a real `COUNT(*)` over the filter, unbounded work on a large index. A caller paginating or driving a progress loop reads `has_more` (always exact, always free) and the rows seen so far, and asks for `total` only when the exact number itself is the point (rendering "page 3 of 12"). Because an unrequested total is absent rather than a page-bounded number, a caller can never mistake one for a real count.

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

### A11.5 The change feed

"Query after my data landed" should be await-then-query, not sleep-and-retry. The change feed is the log-native answer: a projection binding opts in with `notify` (A11.1), and after each committed projector batch that advanced a notifying binding's table the projector publishes one **change record** per advanced table on the changes channel of the ops stream (B1.1). A consumer reads the channel by offset like any topic, resumes from persisted offsets, and there is no server-push, subscription, or broker watch.

| Type | Fields |
| --- | --- |
| `ChangeRecord` | `v` (op version, 1), `index` (the materialized index that advanced), `partition_id`, `from_offset` / `to_offset` (the inclusive source-offset window the batch committed), `rows` (rows written) |

The record is a **wakeup, not truth**: it says the view advanced past an offset window, and the rows themselves are read through `query` (A11.3) or the log. Publishing is best-effort after the batch commits, a lost record costs one missed wakeup and never data, and a consumer that slept past the feed's retention re-reads the view directly. The feed composes with read-your-writes rather than replacing it: read-your-writes answers "has the view caught up to my write", the feed answers "tell me when the view moves". Availability is advertised by the `watch` feature bit (A12), and a client refuses to open the feed locally when the bit is absent, so a consumer never waits on a channel nothing writes to.

## A12. Capability negotiation

A single connection negotiates what is available. A managed feature works against a managed implementation or returns `unsupported`.

- At connect the client runs the `hello` operation. The reply advertises per-surface op versions (`query`, `control`, `kv`, `fork`, and optional `agent` and `graph` versions where 0 means not advertised) plus an optional `features` bitset for managed sub-capabilities that vary independently of the base surface: compare-and-swap, read-your-writes, strong consistency, fenced compare-and-swap, the agent-workflow run registry, lexical keyword search, the change feed (`watch`, A11.5), and the authorization control band (`authz`, B1.4). The `authz` bit is the one the streaming server itself adds (authorization is fork-native, journalled and enforced there), OR-ing it into the relayed announce rather than reading it from the backend. Every other bit is the backend's own truth. The `graph` op version is what gates the knowledge-graph surface (A13). A `0` bitset (skipped on the wire) means none advertised, so a pre-feature reply stays byte-identical and an old client simply lights up no extra capability. The capabilities reply additionally lists the materialization backends the server currently exposes: each is a descriptor of a stable `id` and an opaque engine `kind`, with optional advisory `label` and `version` strings for display and a set of opaque `capabilities` tags the backend declares about itself, identity only with no settings or secrets, so a client can show what it may route to and a new engine is advertised by name without a wire change. The `capabilities` tags let a consumer reason about what a backend is good for (e.g. `ingest`, `query`, a particular query-surface feature) and gate a decision before attempting an op: the wire pins no meaning to a tag, a producer emits what it supports and a consumer matches the tags it understands and ignores the rest, so a new capability is advertised by name with no wire change. An empty list (the default, skipped on the wire) means none advertised, and an absent `label`/`version`/`capabilities` (also skipped) means the client derives a label from `id`/`kind`, shows no version, and assumes no declared tags. The binary `hello` reply carries the same backend list as the HTTP capabilities surface: it is a `BackendAnnounce` that decodes byte-identically from a pre-backends versions-only reply, so an older server's reply still parses with an empty backend list.
- The SDK capability set is grouped by where a capability lives and what it depends on, not a flat list. A root `managed` flag says a managed plane is connected at all. The managed surfaces served off the log are `query`, `kv`, `graph`, `forks`, and the A2A gateway. The platform-native ones are `sessions` and `durable_dedup`. A surface's sub-features nest under it so a dependent feature cannot be advertised apart from its surface: `query.consistency` is the strongest read-consistency level the query surface serves (the `eventual < read_your_writes < strong` ladder, so a level implies the weaker ones, which makes the impossible "strong-but-not-read-your-writes" state unrepresentable), and `kv.cas` is the key-value conditional-write feature, with `kv.cas_fenced` the fenced sibling that gates a write on a live fence sequence. Agentic memory composes the query and graph surfaces, so it has no capability of its own. On an open substrate with no managed surface every capability is off and the matching call returns unsupported. The wire `hello` reply still carries the flat `features` bitset (`kv_cas`, `read_your_writes`, `strong_consistency`, `kv_cas_fenced`, `agent_workflow`, `keyword_search`, `watch`, `authz`). The SDK folds those bits into the grouped form (the consistency bits become the served level), and the HTTP capabilities reply mirrors the grouped shape (B4).
- When the advertised op version is not the SDK's pinned version, the call fails fast with the surface's typed version error before a round trip.
- A feature carried as an additive request field, rather than its own operation, is refused locally when unadvertised. A compare-and-swap rides its own command code, so an unaware server rejects it cleanly even without a local check, but a read-your-writes (or strong) query rides the additive `consistency` field that an unaware server silently drops, so the client refuses an unadvertised level before sending. This is what keeps fail-not-downgrade honest (A8).
- A server MUST keep its two capability carriages in agreement: the binary `hello` `features` bit and the HTTP capabilities boolean for the same feature say the same thing. The contract carries both because the two bindings are separate, and a divergence would let a feature look available on one surface and not the other.
- Every sub-feature defaults to **not advertised**: the HTTP `Capabilities` constructor leaves `kv.cas` off, `query.consistency` at `eventual`, and `graph` off, and a server opts each up only when it serves it, mirroring the skip-when-zero `features` bitset and the zero `graph` op version. A backend that advertised a feature it cannot honor would turn the clean unsupported error into a silent wrong answer, so the safe default is off and opt-in.
- When the streaming server and the managed backend are separate processes, the backend is the single source of its own capability truth. On connect it announces its `OpVersions` (versions plus feature bits) and the materialization backends it currently exposes (each a stable `id`, an opaque engine `kind`, and its opaque `capabilities` tags) to the streaming server over their private socket as a `BackendAnnounce` (`AGDX_BACKEND_HELLO_CODE`), and the streaming server caches and relays it verbatim when it answers a client `hello`, on both the binary and the HTTP carriage. This is what keeps the two carriages honest without the streaming server hardcoding bits the backend may or may not serve, and a backend that gains a feature lights it up everywhere by announcing one bitset.
- The announce optionally carries the deployment's `WireTopology`: the ops stream plus the control, dead-letter, change-feed, and the four managed mutation topic names (`kv`, `fork`, `run`, `graph`), so a client adopts the deployment's resolved names instead of hardcoding the defaults. Explicit client configuration always wins over an announced name. The field is skipped when absent, so a pre-topology announce stays byte-identical and an older reader falls back to the pinned default names. Every topology field has its own default, so a partial announce from an older peer still decodes to real names, never empty strings. Managed conditional writes ride those mutation topics as `MutationCommandEnvelope { v, timestamp_micros, command_code, payload }` records: the log is the sole ordering authority (log-first), each mutation topic is single-partition until the protocol defines cross-partition transactions, and only the deployment's own plane may publish to them.

## A13. Agentic memory and the knowledge graph

The agentic layer adds two things on top of the data platform: a knowledge graph as a new materialized view, and an agentic-memory API expressed entirely as a facade over the primitives already defined. Memory is not a wire surface of its own. Its four verbs compose `publish` (A1.5), the key-value store (A10), `query` (A11), and the graph ops below, so every SDK gets the same semantics without a parallel command band. There is one model: every write publishes to a memory topic, the source of truth and the full versioned audit of a scope's changes, and recall reads it back. The topic is configurable (stream, name, partition count, and message-expiry, each scope keyed to one partition), and the deployment materializes it into a versioned key-value read view whose retention is independent of the topic's, so a value pruned from the read view is still on the topic until its expiry passes. Recall reads the read view by default, so a large topic never folds to answer one recall. Folding the topic in process is an explicit opt-in for a small, serverless deployment with no read view, never the default. Similarity recall rides an in-process vector index over the same writes, relationship recall the graph. The on-topic record is the wire type `MemoryRecord` (item, forget, feedback), so a deployment folds the topic into the read view without importing the SDK. The fold materializes into a single shared read view, not a per-principal one: access to the managed surfaces (including this view) is gated by the capability layer at the command boundary (B1.4). A memory read is `kv:read` on the materialized namespace, a memory write is a streaming publish on the memory topic, and isolation is name-granular (resource pattern on the unforgeable subject), not a per-record ownership the fold infers. Each record carries its scope as headers, the logical namespace `agdx.mem.ns` the read view materializes under plus the `agdx.mem.user`, `agdx.mem.app`, agent, and conversation layers, so the fold keys each read-view row by that scope rather than by the physical topic. One shared stream and topic can then hold every conversation's memory and still resolve each context. The fold stamps the conversation that wrote each record onto its read-view row (from the record's `gen_ai.conversation.id` header), so a scan can narrow to one conversation's memory (the conversation lens below), a read-side filter over provenance, never the isolation boundary. The fold also stamps the origin log position (numeric stream id, topic id, partition, offset) onto the read-view row as a `SourceRef`, the same message-position provenance the graph carries below, so a reader can navigate a recalled item back to its source record while it is still on the log. The position carries ids, not names, so the pointer stays compact and survives a rename.

**The memory verbs (SDK facade, no wire op).**

- `remember` appends an item to the memory topic (the deployment materializes it into the read view). With dedup it content-addresses the item id (below), so storing the same fact twice under the same durable owner stores it once. Entities reach the graph either by an explicit `graph.upsert` or automatically when a graph projection is bound to the source, in which case the projector applies the projection's entity schema to each record and upserts the extracted nodes and edges.
- `recall` reads back over a multi-signal pipeline: candidate retrieval, score fusion, an optional rerank, then the top results. Its `strategy` routes which signals fire: `auto` (the default) uses the best available, `recent` / `temporal` fold the log or run a time-ranged query over the read view, `semantic` runs a vector query (A11.3), `keyword` runs a lexical relevance match (the `text` IR field, A11.3, where the shipped embedded engine ranks by token coverage then term frequency over its own inverted token index), `graph` runs a traversal, and `hybrid` fuses semantic and keyword by reciprocal rank client-side, each fused item keeping its per-signal attribution (which strategies surfaced it, at what rank and pre-fusion score), so a routed `auto` reports what it picked. The rerank stage is a seam (a cross-encoder, an LLM judge, or a hosted rerank API) the SDK leaves to the application, the same boundary as the embedder and the consolidation summarizer. Routing authority sits where the state is: the managed plane routes when it knows the graph, otherwise the client routes `auto` from the advertised capabilities plus the registered embedder (the graph and a similarity index when present, otherwise recency over the log and its read view).
- `improve` is feedback and enrichment. Feedback rides the log as a typed record a ranking backend folds into recall order. The richer, asynchronous enrichment (summarizing sessions, reweighting edges, pruning stale items, deriving facts) is **consolidation** (the "memify" / sleep-time pass), a seam a managed backend or an application fills.
- `forget` tombstones the item on the memory topic (a forget record the fold applies as a delete on the read view). An opt-in cascade also deletes derived graph nodes, edges, and vectors.

A memory item carries a kind and a lifetime, both SDK labels rather than wire-op fields. The kinds are `fact`, `message`, `summary`, `entity`, `feedback`, and `procedure` (a reusable skill or workflow), and each maps to one of the field's three classes: **episodic** (what happened: `message`), **procedural** (how things are done: `procedure`), and **semantic** (what is known: the rest). The lifetime is `session` (conversation-scoped and prunable) or `durable` (shared across conversations and graph-backed). A memory is scoped along the converging identity layers `user` / `agent` / `session` (the conversation) / `app`, plus the physical stream that is the isolation boundary. Any layer left unset widens recall across it.

The session layer is where the context accessor and memory meet. The SDK's context handle, scoped to one conversation, hands out a session-scoped memory view whose recall and remember carry that conversation implicitly, so one scope covers a task's messages and its working memory together. This is an SDK ergonomic over the same scoping fields, not a new wire surface. Durable memory and the graph deliberately stay cross-conversation (a fact learned in one session is worth recalling in the next, and a dependency graph holds no matter which task asked). The context handle can still reach the graph as a convenience so one scope covers a task's messages, memory, and dependency reads, but it returns the graph unnarrowed rather than filtering it to the conversation.

**Content-addressed identity.** A deduped memory id and a graph node id are deterministic content hashes, the one canonical `content_id` in the wire crate (a dependency-free, fixtured FNV over byte segments, rendered as the 16-byte id, A3). A memory id hashes the durable owner, kind, and body, so the same fact in two conversations is one durable memory. A node id hashes the entity's label and value, so the same entity extracted from different messages converges on one node, which is what forms a graph rather than disconnected pairs. Every SDK reproduces the id from the same segments, pinned by a golden vector.

The content id is a **convergence and dedup key, not a security boundary**. FNV is not second-preimage resistant, so it is deliberately not relied on to prevent a hostile writer from minting a colliding id. The applicable AGDX security profile is the trust boundary, and a principal allowed to write the memory or graph topic can already write any id directly, so a collision buys nothing a direct write does not. The hash's only job is that honest writers converge deterministically. A deployment that needs cross-writer integrity on these ids uses verified principal signatures or ACL-bound write-exclusive topology (A4.1, B1.1). Moving to a keyed cryptographic hash is a future option if a threat model ever requires adversarial collision resistance on the id itself.

**The graph surface (managed wire surface).** A graph is a materialized view named by a `graph` projection (A11.1): the projection's entity schema declares how nodes and edges are extracted from a payload (label and endpoint pointers), and the graph stores them content-addressed so the same entity converges on one node. Nodes and edges are written by the `graph.upsert` op below, idempotent on the content-addressed ids. The graph is gated by the `graph` op version (A12). The ops:

| Op | Request | Reply |
| --- | --- | --- |
| `graph.query` | graph name, start (`ids` \| predicate `match` \| vector `nearest`), hop spec (edge type + direction + max), optional node/edge filters (the same `Filter` predicate language as query, A11.3), return (`nodes` \| `edges` \| `paths` \| `triplets`), limit, optional fork, consistency, optional valid-time `as_of`, optional `conversation` lens | `nodes`, `edges`, `paths` |
| `graph.neighbors` | graph name, node id, direction (`out` \| `in` \| `both`), optional edge type, depth, limit, optional valid-time `as_of`, optional `conversation` lens | the reachable nodes and traversed edges |
| `graph.upsert` | graph name, nodes, edges (the projector path, idempotent on content-addressed ids) | written |

**Bitemporal edges.** An edge MAY carry a valid-time window (`valid_from` / `valid_to`, epoch micros, both optional and open-ended when unset), the time the relationship it records was true in the world. This is orthogonal to system time, which the substrate supplies for free as the log offset of the upsert (when the edge was observed). Carrying valid-time lets a fact be superseded without being destroyed: a changed relationship closes the old edge with a `valid_to` and opens a new edge, both retained, so the history is replayable. The window is metadata, not identity, so re-observing the same relationship updates the same content-addressed edge. The fields are absent on the wire when unset, so a pre-bitemporal edge encodes byte-identically. A traversal reads "as of" a past time with the `as_of` modifier (A13 ops), which keeps only edges whose valid-time window contains that instant. The system-time `as of` over the log offset is the remaining temporal axis.

**Provenance.** A node and an edge MAY each carry a `source`: the record an extraction came from, so a reader can navigate from a graph element back to its origin. A source is one of a message position (numeric stream id, topic id, partition, offset, and the `conversation` that asserted it when known), a key-value entry (namespace, key), or a memory item id. On an edge it is the record that most recently asserted the relationship (last-writer, since an edge is rewritten on each observation to keep its valid-time window current). On a node it is the first record the entity was seen in (first-writer): a re-observation keeps that first source, still applies any genuine field change (a later embedding, new attributes), and skips the write entirely when nothing changed, so a hot entity is not rewritten on every sighting. Provenance is metadata, not identity: it is excluded from the content-addressed id, so it never changes which node or edge an upsert targets. The field is absent on the wire when unknown, so a pre-provenance element encodes byte-identically. The complete history is the message log itself, which the deterministic projector can replay. The graph carries only the navigable pointer, bounded by `MAX_SOURCE_REF_BYTES`.

Caps (`wire/src/limits.rs`): traversal depth at most 8, at most 10000 nodes plus edges per reply, at most 16 labels per node. The depth and element caps are enforced server-side: an over-cap depth and an over-cap upsert are both rejected with a too-large error, so one request cannot drive an unbounded walk or write. A query may resolve against a fork's overlay by naming the fork, the same copy-on-write the row views use (A10.5). A graph traversal reuses the query `Filter` / `Value` / `Consistency` types, so there is one predicate grammar across the row and graph views. The traversal filters prune the walk, they do not post-filter a finished walk: `node_filter` gates frontier admission at every hop (a node that fails it is neither returned nor expanded), `edge_filter` gates which edges are followed (a filtered edge is neither traversed nor returned). Post-hoc filtering is expressible client-side, pruning is not, which is why the server owns it.

The contract defines the full return set and start modes, and the shipped managed engine serves all of them: the `nodes`, `edges`, `paths`, and `triplets` returns and the `ids`, `match`, and vector `nearest` starts. Nodes and edges are stored in dedicated relational tables keyed by `(graph, id)` with an adjacency index on each endpoint, so a hop is an index-driven range read over the whole frontier rather than a per-node scan, and a `nearest` start ranks node embeddings with the backend's native vector distance. A backend that cannot serve a mode still returns a clean unsupported error rather than a partial or silent answer (A12).

A traversal or neighbor read may carry a valid-time `as_of` (epoch micros): only edges whose valid-time window contains that instant are followed, so a read sees what was true then rather than only now. This is the read side of the bitemporal edges above. The system-time `as of` over the log offset is the remaining temporal axis.

A graph is populated two ways: by an explicit `graph.upsert`, and by projector-driven extraction. Binding a `graph` projection to a source topic (the same `ApplyBinding` the row projections use) makes the projector apply the projection's entity schema to each record as it lands and upsert the extracted nodes and edges, idempotent on their content-addressed ids. Extraction is at-least-once over the source: a re-processed record converges on the same nodes and edges rather than duplicating. The SDK's `link(from, relation, to)` / `unlink(..)` sugar is the same machinery one call tall: link upserts both content-addressed entity nodes and the typed edge (re-linking converges), unlink closes the edge bitemporally (`valid_to` at call time) so the fact is superseded, never destroyed.

This realizes the data-object collection primitives that suit a graph (C6) without a separate query language.

**The conversation lens.** Every managed read model that materializes from the log records the conversation that wrote each row, taken from the record's `gen_ai.conversation.id` header, so a read can narrow to one conversation server-side. Three surfaces carry it, each optional and absent on the wire when unset (so a pre-lens contract stays byte-identical). A `graph.query` and a `graph.neighbors` take a `conversation` filter, matched against each element's source conversation, so a traversal returns only what one conversation asserted. A projection materializes the header into an auto-projected `conversation_id` field on every row (a stable reserved field name), so `query` filters by conversation with an ordinary predicate on any projection, no producer-side index directive needed. A key-value scan takes a `conversation` filter, so a scan of the memory read view narrows to one conversation's memory (a generic key-value entry carries no conversation and is left out of a filtered scan). The lens is a read-side narrowing over provenance, not an isolation or authorship boundary. Those guarantees come from the selected security profile in B1.1.

---
---

# Part B. Bindings

A binding owns exactly the substrate-specific concerns: the mapping from logical identity to physical address, the carriage of the out-of-band attributes, the operation dispatch encoding, the request and reply mechanism, and the packing of the opaque `cause_at` locator. Everything else is inherited from Part A unchanged. Within a binding, every mapping below is a hard, fixtured contract.

## B1. The Iggy binding (normative)

### B1.1 Identity to physical address

| Logical | Iggy address |
| --- | --- |
| streaming record | stream, topic, partition, offset |
| agent ordering key | the conversation id as the partition key. Generic streaming preserves order within the caller-selected Iggy partition |
| collection / topic | a topic on a data stream |
| managed ops | a reserved command range against the connection (B1.4), not a topic |
| `cause_at` locator packing | the four-level (stream, topic, partition, offset) address as 20 big-endian bytes in the opaque locator slot |

The Iggy binding uses the `_agdx` ops stream with `control.commands`, `dlq`, and `changes` topics for projection control, dead-letter capsules, and the change feed (A11.5). These four names are wire constants a consumer uses from the shared dictionary rather than redeclaring literals. The reference SDK's `Laser` exposes a builder override for each (`ops_stream`, `control_topic`, `dlq_topic`, `changes_topic`), but today no managed backend reads a configured name back: LaserData Cloud's control plane imports these same four constants directly and answers on them unconditionally, so a client-side override has no deployment to coordinate with yet. The one place it does something today is per-test isolation against a raw, unmanaged Apache Iggy instance (a unique `ops_stream` per test avoids collisions on a shared container, with no control plane involved at all). Query and the other managed operations are not topics: they ride the reserved command range (B1.4), off the log.

Connection bootstrap and environment variables are SDK concerns documented in the tutorial, not part of this binding.

The reference SDK's optional `vsr` feature selects Apache Iggy's VSR client framing without changing any AGDX envelope or header bytes. The Laser producer and live consumer APIs continue to use standard append, poll, consumer-group, and offset commands, as do the reliable agent runtime, AGDX traffic, cursors, and folded log memory. The current upstream VSR command encoder rejects the custom command range in B1.4, so every managed operation riding that range - query, projection, key-value, fork, graph, run-registry, presence, and role/authorization - remains unavailable in that build until Iggy admits those codes. This is a transport implementation limit, not a new AGDX version or wire profile.

Partitioning and isolation are separate concerns on Iggy. Agent provenance uses the conversation id as the partition key, which buys total order within a conversation and lets independent conversations run in parallel across a topic's partitions. A single very high-throughput conversation is therefore bounded by one partition and, on a shard-per-core server, one core. Generic streaming does not acquire this rule: it uses balanced, explicit, or caller-keyed partitioning and preserves order within the selected partition. Partitions are a throughput-and-ordering tool, never an access boundary. Iggy RBAC is enforced at the stream and topic level, not the partition level.

Authorship uses an explicit deployment security profile:

| Profile | Authorship guarantee | Typical topology |
| --- | --- | --- |
| Advisory | `source` and provenance headers are self-asserted | shared topics for local development or mutually trusted writers |
| Signed principal | a verified envelope binds the enrolled signing key and authenticated principal to the record | shared agent topics with receiver-side verification |
| Topology isolated | Iggy ACLs bind one authenticated principal to a write-exclusive stream or topic, optionally with signatures for defense in depth | control and effect channels requiring an exclusive writer |

A receiver must know which profile applies and must not use advisory fields for billing or authorization. The reference SDK's shared command and response topics are valid under the advisory profile for trusted local deployments and under the signed-principal profile when verification is enrolled. A deployment that requires topology isolation creates write-exclusive routes and ACLs. Signatures and topology can be combined, but neither changes the streaming server's message body or adds a hot-path authorship header.

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
| `agdx.on_behalf_of` | string | reserved binding alias for a delegation subject. Signed on-behalf-of delegation uses the envelope metadata key `on_behalf_of` (A9.6, B1.4), not this header |
| `gen_ai.conversation.id` | u128 | conversation id (OpenTelemetry) |
| `gen_ai.agent.id` | string | producing agent (OpenTelemetry) |
| `gen_ai.usage.input_tokens` / `output_tokens` | u64 | token usage (OpenTelemetry) |
| `agdx.cause`, `agdx.parent_conv`, `agdx.root_conv`, `agdx.to`, `agdx.idem`, `agdx.deadline`, `agdx.cost` | mixed | provenance: causal parent, parent and root conversation, addressee, dedup key, deadline, cost |
| `agdx.fence` | u64 | the strictly-monotonic per-task fence the producer held, so a consumer drops a stale-holder replay of a log-resident effect |
| `agdx.mem.ns` | string | the logical memory namespace a record materializes under, so the read view is keyed by scope rather than by the physical topic |
| `agdx.mem.user` / `agdx.mem.app` | string | the user and app scope layers a memory record belongs to, materialized onto the read-view row so recall narrows by them |

Header caps: 1024-byte soft cap on total header bytes per record, 255-byte ceiling on a single value, 9 bytes of per-header framing counted toward the cap.

There is no duplication between these headers and the typed envelope when an agent envelope is present. The two carriers serve two cases. A message that carries a typed `AgentEnvelope` (the body) stamps only the minimized routing projection out of band: the content-type, the wire version, the conversation as the partitioning `Uint128`, and the addressee when targeted. The envelope is the single source of truth for everything else (`source`, `cause`, `correlation`, `deadline_micros`, `idempotency_key`), which is never copied to a header. The full provenance dictionary is used only for messages published without an envelope (the generic provenance path), where the headers are the sole carrier and there is nothing to duplicate. So a field is in exactly one place per message: the envelope for typed agent messages, the headers for generic provenance messages.

### B1.3 Versioning carriage

The durable agentic record carries its wire version as the `agdx.av` header, never a body field. The managed request envelopes carry a `v` first field, but the binding also negotiates version at connect through the `hello` reply (A12), which fails fast before a round trip. The in-band `v` is therefore redundant defense, not the primary mechanism.

Each surface is versioned by exactly one mechanism, named here so an implementer knows where to look. Every op version is `1` in this era (the RC policy breaks shapes in place rather than bumping), so the `hello`-negotiated slots advertise a single accepted version today. When simultaneous versions are ever needed they become a min/max range on the same slot rather than a flag day.

| Surface | Mechanism | Carrier |
| --- | --- | --- |
| `query`, `control`, `kv`, `fork`, `agent`, `graph` | hello-negotiated | the `OpVersions` slot in the `hello` reply (A12), `0`/absent means not advertised |
| compare-and-swap, read-your-writes, strong consistency, fenced CAS, agent-workflow, keyword search, watch, authz | feature-gated | a bit in the `hello` `features` bitset (A12), not a version |
| `batch`, `authz`, `client-metadata`, `presence`, `change` | body-versioned | the request/reply's own `v` first field, checked on decode (no hello slot) |
| the agent envelope | header-versioned | the `agdx.av` header, read before decode to select the decoder |
| the signature scheme, content-type, dead-letter reason, task state, agent error | not negotiated | a growable dictionary whose unknown values pass through (A7-style), so a new value never needs a version |

### B1.4 Operation dispatch and the command range

The operation registry (A5) is realized as a `u32` command code. Raw Apache Iggy has no such range and rejects these, which enforces the open-versus-managed boundary.

| Op id | Code |
| --- | --- |
| `hello` | 1_000_000 |
| `backend_hello` (internal: the managed backend announces its `OpVersions` to the streaming server, not client-facing) | 1_000_001 |
| `set_client_metadata` / `get_clients_metadata` (the connection-scoped discovery channel: a connection advertises an opaque metadata blob, an `AgentPresence` for an agent, and a filtered, paginated read lists live connections with their metadata) | 1_000_002 / 1_000_003 |
| `batch` (mixed-operation, A5) | 1_000_020 |
| `authz.whoami` / `list_roles` / `get_role` / `get_bindings` | 1_000_100 .. 1_000_103 |
| `authz.define_role` / `delete_role` / `bind_roles` | 1_000_104 .. 1_000_106 |
| `authz.history` | 1_000_107 |
| `query` | 1_000_200 |
| `registry.get_projection` / `list_projections` | 1_000_210 / 1_000_211 |
| `registry.get_schema` / `list_schemas` | 1_000_220 / 1_000_221 |
| `registry.register_schema` / `decode_record` | 1_000_222 / 1_000_223 |
| `kv.get` / `set` / `scan` / `delete` / `delete_many` / `namespaces` / `cas` | 1_000_300 .. 1_000_306 |
| `kv.exists` / `expire` / `patch` / `lease` / `release` | 1_000_307 .. 1_000_311 |
| `kv.cas_fenced` | 1_000_312 |
| `kv.copy` / `move` | 1_000_313 / 1_000_314 |
| `fork.create` / `delete` / `promote` / `list` / `put` | 1_000_400 .. 1_000_404 |
| `graph.query` / `upsert` / `neighbors` | 1_000_600 .. 1_000_602 |
| `agent.submit` / `cancel` / `status` / `list` | 1_000_700 .. 1_000_703 |

Authorization and system management is the first management band (`+100`), after the internal/handshake block, and the feature bands sit one block down accordingly. The base value is high (a million) only to avoid colliding with Apache Iggy's own low command codes, and the 100-wide blocks are organizational. Both are Iggy-local. They are pinned and fixtured here in the binding, not in the core.

The server forwards an opaque CBOR request to the managed side over a local channel, stamping the authenticated identity the SDK cannot set. A forwarded query carries the trusted user id, the client id, an audit correlation, and the opaque query envelope. A forwarded command additionally carries the command code (and a legacy read-all flag, retained on the wire but no longer meaningful now that the managed store is one shared dataset rather than per-user, A10). The socket frame is `[len: u32 little-endian][payload]`, 64 MiB ceiling.

The managed surfaces are gated by a **capability layer** orthogonal to the substrate's own permissions, which are never touched. A grant is `effect feature:action [on resource-pattern]`: `effect` allow or deny (deny wins), `feature` maps to the command bands (`kv`, `memory`, `projection`, `graph`, `query`, `fork`, `agent`, `workflow`, plus `authz` for administering the layer), `action` is `read`/`write`/`delete`/`admin`, and the resource pattern is `all`, a `literal` name, or a `prefix`. `all` is the only pattern that matches a request with no keyed resource selector. `literal` and `prefix` grants require a concrete resource string and never widen into list or whole-surface operations. Grants are assembled through **roles** bound to a subject, and the subject is the server-stamped, unspoofable user id. A role name is at most 64 bytes and restricted to the same strict charset safelist as a fork id: ASCII letters, digits, `-`, `_`, and `.`. The rule is owned in the wire contract (`validate_role_name`) and enforced on define and bind by the SDK, the server edge, and the console alike, never on journal replay, so tightening the rule cannot strand existing state. A user's effective capability is the union of the grants of every bound role, minus any matching deny. With the layer enabled and no bound role, the default is **deny**. The `(feature, action)` a command authorizes against is a pure function of its code (shared by every enforcer so they cannot drift), and the resource is the leading keyed field of the request (kv namespace, fork id, projection/schema id). The server derives both and checks them before forwarding, rejecting with the unauthorized result code, and a `batch` is decomposed and every inner op checked. Query and graph DSL depth (the per-source check) is enforced by the managed side where the envelope is fully parsed. Grants live in the substrate's durable, boot-replayed state journal (the same path as create-user), so the check reads a precomputed, resident capability set with no round trip. Isolation is name-granular (resource pattern on the unforgeable subject), so two principals are separated by distinct namespaces or prefixes plus scoped grants, not by per-record ownership inside a shared name. The physical shared pool (A10, A13) is correct once access is name-gated at the edge. Administering the layer (defining roles, binding them) requires the `authz:admin` capability or the near-root server-management permission (the bootstrap backstop, so an operator is never locked out before any grant exists). `whoami` is answered to the authenticated caller, while role catalog, binding browse, and history reads require `authz:read` or the near-root server-management permission. On-behalf-of delegation carries the invoking user in the signed envelope metadata key `on_behalf_of` (A9.6). The effective grant is then the agent's capabilities intersected with that user's, so the agent can never exceed the user it acts for. Permission intersection, not substitution.

### B1.5 Low-latency features exploited

The binding uses Iggy-specific fast paths because the core only requires that the out-of-band attributes be carriable, not how. The typed compact headers, the single multiplexed connection for publish and consume and managed commands together, and the low-latency local delivery paths are all used. Client-side batch assembly is exploited the same way: the SDK's opt-in batching producer and buffered chunk writer accumulate records and hand them to one substrate batch append, amortizing the per-message cost without touching what any record carries. A first-class zero-copy content-type remains a possible Iggy-specific extension, not a shipped protocol guarantee. None of this is a core requirement, so using it costs no portability.

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

- **NATS JetStream.** Subjects map to topics, the stream sequence is the offset, durable consumers are consumer groups, JetStream KV maps the state surface natively, headers are untyped. A good fit.
- **Apache Pulsar.** Topics, partitions, message ids, and subscriptions map closely, with native compaction for the state surface. A good fit.
- **A single-stream log broker.** Entry ids, consumer groups, and replay exist, but durability and partitioning are weaker and a single stream is the ordering unit. Possible for lighter deployments.

**The substrate requirement.** A substrate can host the model if it provides an append-only, partitioned, offset-addressed log with replay and keyed ordering, a way to carry the out-of-band attributes alongside a body, and either a request and reply mechanism or a topic pair to emulate one. Typed headers, low-latency local delivery, log compaction, and native transactions are optional accelerators a binding exploits when present. The query surface is always a managed layer above the log.

## B4. The HTTP binding (management and UI)

A management and UI surface that maps the same operation registry (A5) onto REST routes, for a browser or wasm client that has no binary substrate connection. It is a thin translation, not a second source of truth: a read forwards over the managed channel, a write publishes to the control topic, exactly as the binary binding does, so the two never diverge. There is no binary-SDK HTTP client.

| Operation | Route |
| --- | --- |
| capabilities probe (`hello`) | `GET /capabilities`, never gated, answers the managed flags and per-surface versions for UI feature-detection |
| `query` | `POST /query` (a `Query` JSON body) |
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
| `authz.whoami` / `list_roles` / `get_role` / define / delete role / `get_bindings` / bind roles | `GET /authz/whoami` / `GET /authz/roles` / `GET /authz/roles/{name}` / `PUT /authz/roles/{name}` / `DELETE /authz/roles/{name}` / `GET /authz/users/{id}/roles` / `PUT /authz/users/{id}/roles` (gated by the `authz` capability, B1.4. `whoami` reads the caller's own bound roles and effective grants, `list_roles` a JSON array of `Role` and `get_role` one `Role` or `404`. A role `PUT`/`DELETE` and a user bind (`PUT` a bare JSON array of role names) journal to the fork-native authorization band, the reads forward like any managed read.) |

A graph projection registers and drops through the control commands `RegisterGraph` / `DropGraph` on the control topic (A11.2), the same durable path as a row projection, and lands in the one projection registry alongside row projections. The `/graphs` and `/projections` routes are the two browse views that partition that one registry by kind: `/graphs` keeps only graph-kind projections and `/projections` only row-kind (non-graph) ones, and each id route returns `404` for the other kind. Both list routes forward the same `list_projections` read and filter by kind, so the registry stays a single source of truth and the two views never diverge. On the `/graphs` routes a `POST` registers, a `DELETE` drops, and a `GET` lists or reads, so a graph explorer discovers the available graphs by name without a graph projection ever surfacing in the query/index browse. The node and edge data is written content-addressed by the binary `graph.upsert` op (the `/graph/{name}/query` and `/neighbors` routes read it), not over the registration routes.

JSON request and reply bodies mirror the CBOR wire types. Key-value keys are arbitrary bytes, so in a path or query parameter they are base64url-encoded, and a value rides the raw request body with the optional expiry carried as the `expires_at_micros` query parameter. The authenticated identity is the same trust boundary the binary binding stamps, so scoping is identical. The route prefix is a deployment-configurable operational name, not a wire constant (the current implementation defaults to `/agdx`, so `GET /capabilities` is served at `/agdx/capabilities`).

Each wire error maps to a precise status, the same mapping on every surface, so a client need not parse error strings:

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

The reply contract is uniform: a `2xx` carries the bare `Ok` payload, and every non-`2xx` carries a canonical **error body** `{ code, message, detail? }`. Bare means the inner value, never the binary band's reply wrapper. The browse routes serve a JSON array (`GET /projections`, `GET /schemas`) or a single object (`GET /projections/{id}`, `GET /schemas/{id}`, with `404` for absent), not the `BrowseReply`/`BrowseOutcome` envelope the CBOR socket multiplexes its ops through. A registration replies the bare allocated id. `code` is the unified `ResultCode` (A7) so a client dispatches on the classification rather than parsing the message text. `message` is human-facing, and `detail` is optional structured context (e.g. the conflicting version on a compare-and-swap miss). The status line is derived from `code` via the table above, so the two never disagree. The route constants, the path builders, the typed query-parameter structs (one field per parameter name above), this error body, and a typed client over a caller-injected transport (`gloo-net` on wasm, `reqwest` natively) are all owned in the wire crate's `http` / `http_client` modules, so a browser or native client carries no hand-rolled route, base64url, or query-string glue, and any drift is a compile or doc-test failure rather than a production `404`.

The capabilities reply carries the grouped shape (A12): `managed`, a `query` object (`available`, `projections`, `schemas`, the served `consistency` level, and `keyword` for lexical search), a `kv` object (`available`, `cas`, `cas_fenced`), `graph`, `fork`, `agent_workflow`, `watch`, `authz`, the op `versions`, and the materialization `backends`. The sub-features default off (`kv.cas` and `kv.cas_fenced` off, `query.consistency` at `eventual`, `query.keyword` off, `graph`/`agent_workflow`/`watch`/`authz` off), and a server advertises one only when it genuinely serves it: over-advertising would turn a clean unsupported error into a silent wrong answer. Graph nodes and edges ride the JSON views (`GraphNodeView` / `GraphEdgeView` / `GraphResultView`) with ids as strings, since the CBOR id is bytes a JSON string cannot carry raw.

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
- Capability negotiation returns `unsupported` for an unavailable surface.
- Framing is sans-io, pure functions over byte slices, with no transport of its own.

### C1.2 Why it is built this way

These decisions are settled. They are recorded here because they are not obvious from the field tables alone.

**The typed envelope is the single source of truth, and the binding stamps only a minimal routing projection out of band.** A message carrying an `AgentEnvelope` puts just the routing subset in headers (content-type, wire version, conversation, and the addressee when targeted). Everything else (`source`, `cause`, `correlation`, `deadline_micros`, `idempotency_key`) lives in the envelope and is never copied to a header. Three forces require this. Several envelope fields are structured or large and do not fit a flat capped header space. Headers are substrate-specific and lossy across hops, so they cannot survive mirroring or republish. And a router wants only a small subset. The generic provenance header dictionary is therefore used only for messages published without an envelope, where the headers are the sole carrier and nothing is duplicated (B1.2).

**One result space.** Every surface error projects onto a single `ResultCode` and its HTTP status while keeping its typed detail (A7). A generic client dispatches on the code, and a specialist still reads the typed variant.

**`cause_at` is an opaque, binding-defined byte string, and `cause` is the portable identity.** A foreign consumer that cannot interpret the locator falls back to `cause` (a portable id). This keeps the envelope substrate-neutral while letting each binding pack its native address. The Iggy binding packs its four-level position as 20 big-endian bytes.

**Optimistic concurrency is an entry version plus a conditional write.** The key-value store carries a version token, and `kv.cas` commits only against the expected version or absence (A10.3). It is gated by the `kv_cas` capability, so a backend that cannot do a conditional write returns unsupported rather than a wrong answer.

**Read consistency is a per-query level.** A query names `eventual`, `read_your_writes`, or `strong` (A11.3). A level the backend cannot serve returns `stale` or `unsupported` and is gated by the `read_your_writes` and `strong_consistency` capabilities.

**`must_understand` lets one message demand strict handling without a version bump.** The marker is a u64 bitset on the envelope (A9.1). A clear bit means ignore-if-unknown, and a set bit a receiver does not implement means reject. No bits are defined yet, so the mechanism is in place for the first feature that needs it.

## C2. What not to adopt

- **Queue and broker primitives.** Server-push delivery, ack and nack settle verbs, delivery-mode negotiation, redelivery counters and visibility timeouts, broker-managed dead-letter queues, and message priority. The streaming layer is a log (A1.5), and these are queue semantics that either do not map or are already provided by offsets, replay, and the reliable consumer. This is the single biggest filter applied to the broker idea-space.
- **Telemetry as a separate primitive set.** Logs, metrics, traces, and events are records on topics with OTel-aligned provenance headers and a trace projection, not new operations (A1.5).
- **A transport stack.** No frame layout, flow control, multiplexing, keepalive, or connection handshake of our own. The substrate owns transport. This is the deliberate inversion of a transport-shaped protocol into a data-shaped one.
- **Cross-substrate distributed transactions.** A transaction, if offered, is scoped to one connection and one responder.
- **Mutable objects as the primary store.** The state surface is a read model on the log. The log stays the source of truth.
- **Trust scores or admission control in the envelope.** Every agent-written field is a claim. Enforcement lives at the capability owner. Identity granularity equals credential granularity.
- **Prompt guardrails as the security boundary.** A model-level filter is advice to a probabilistic component, not enforcement. The enforcement points are the command edge (the capability layer, B1.4) and the effect boundary (the governance hook, C3), and every decision leaves evidence as a record on the log rather than in a second audit store.

## C3. Roadmap

These are draft proposals. None is settled, and none is pinned into the fixture corpus. They are recorded so the design space is visible, and they evolve as real use sharpens the scope. Each is expressible in the wire contract independently of the runtime that serves it: the contract defines the shape, a capability flag gates the operation, and an unserved operation returns a clean unsupported error. The dormant claim-check encryption code and the agent version that reads zero until consumed follow the same pattern, as the signature slot already did before the reference SDK activated it (see the roadmap row below).

| Proposal | Shape (draft) |
| --- | --- |
| strong-consistency semantics | the `strong` level is wired (A11.3) but its linearizable cross-replica semantics past read-your-writes are still being pinned |
| lease renewal | the advisory lease and release ship (A10.4), the fencing token is the entry version, and re-acquire after expiry works, an explicit renewal op is still open |
| generalized causality token | an opaque happens-before token (a generalization of `cause_at`), recommended a hybrid logical clock, fail rather than reorder. Encoding still open until the cross-region backend proves it |
| signature activation and key registry (A9.5) | SHIPPED SDK-side (`sign` feature): the `Signature` envelope field activates with Ed25519 `SigningKey`/`KeyRegistry`, `Agent::builder().signing_key(..)` signs pickup and terminal replies, `LaserBuilder::verifier(..)` rejects an unsigned, unknown, or wrong-identity reply, and the managed `KvKeyRegistry` enrolls and snapshots verifying keys through the platform. Both SDKs share the same signing input and domain separator |
| content-block lifecycle and applied-through-offset ack | typed text, reasoning, data, tool-call blocks with start, delta, finish, and a reply naming the log offset a command took effect at (an offset, not a queue ack) |
| action governance hook | SHIPPED SDK-side (`ActionGovernor`): a pre-effect policy hook on agent sends, typed or raw topic publishes, requests and fan-out branches, the AGDX producer verbs (and through them the MCP/A2A bridges, `approval_gate`, and workflow dispatch), and memory writes. A vector-memory handle built from a governed `Laser` applies the hook before mutating its local index, and both log and vector item writes expose the proposed item body rather than their backend encoding. The hook sees the action kind, stream, topic, source and target, conversation, correlation, operation, tool, `on_behalf_of`, `purpose`, `data_classification`, the body, whether the record will be signed, and session counters. Its decision vocabulary is `allow`, `observe`, `block`, `step_up`, `modify` (applied before claim-check and signing), and `defer`. Chunk streams are exempt (per-chunk decisions are the wrong altitude). RBAC remains server-owned, the hook is defense in depth for regulated deployments |
| policy evidence capsule | the SDK emits each non-allow decision as a CBOR `event` (operation `policy_decision`) on the audit topic today: decision id, decision, mode, action attribution, reason, approved scope, policy pack/version/rules, risk score, a BLAKE3 receipt digest, the previous decision's digest (a per-conversation chain), outcome, and time. Both SDKs share the one encoder, so the shape is consistent without a wire pin. What remains roadmap is pinning it into the fixture corpus as a capsule once a non-Rust-core port needs byte-identical evidence |
| enforcement modes | SHIPPED SDK-side (`GovernorMode`): `observe` records what enforcement would have done and never impacts the effect, `enforce` applies the verdict, and an evidence-write failure on a proceeding enforced decision fails the call (a governed effect is never unrecorded). The mode is configuration, not an envelope claim, because an agent must not self-authorize weaker enforcement. A `step_up` that expires unanswered fails closed unless the deployment explicitly configures that governor to fail open. `progressive` (observe first, promote selected rules) remains the policy engine's concern above the hook |
| policy context metadata | SHIPPED (A9.6): pinned advisory metadata keys `purpose`, `data_classification`, `task_context`, and `session_intent`, signed when they must be trusted. They give a policy engine stable inputs without inventing a new prompt or telemetry surface |

## C4. Conformance and fixtures

A conformant port decodes and validates the complete envelope, produces whatever subset of kinds its use case needs, and is pinned by the positive and negative fixture corpus in both directions. It depends only on a CBOR library, the 16-byte id codec, the constants, the validate function, and the verb behaviors. No crypto, no proc-macros, no compression, and no transport beyond its substrate client.

A binding adds fixtures for three things only: the identity-to-address mapping, the operation dispatch, and the out-of-band header encoding. The payload fixtures are shared across every binding.

One byte-order rule is easy to get wrong. An application AGDX id (`conversation`, `record`, `correlation`, `channel`, minted SDK-side, distinct from the substrate's own message id or offset) rides the CBOR envelope as a 16-byte **big-endian** byte string (A3). When the same id is also stamped as an Iggy routing header (the conversation, for partitioning), that header copy is Iggy's typed `Uint128`, which is **little-endian** (B1.2). One id, two encodings by carrier. A port that reads the header copy as big-endian will mis-route, so the corpus pins both forms.

## C5. Stability and evolution

The contract is pre-1.0 and open to change. The normative surface is Part A as built plus the Iggy binding (B1). The roadmap (C3) is a sketch of the design space, and a proposal is pinned into the fixture corpus only once its shape is settled. The dormant slots already in the wire (the signature type, the claim-check encryption code, and the `must_understand` marker) activate additively under that same rule. The Rust SDK surface has its own stability contract in the appendix below, distinct from this wire contract.

### C5.1 Right to forget (erasure posture)

Erasure is split by which store owns the bytes. The raw log is append-only and owned by the substrate: a record is not edited in place, so "forget" on the log is retention expiry (the topic's message-expiry drops the segment once its window passes) plus, for a bounded set of subjects, crypto-shredding: a claim-checked body (A9.5) is dropped by deleting its blob-store object, and the log keeps only the now-dangling `BodyRef` capsule whose bytes are unrecoverable. The read models are owned by the managed plane: a tombstone-hide is immediate (the projector applies a delete/supersede and the row stops being returned), and a hard erase-by-subject rebuilds the affected projection from the retained log with the subject's records filtered out. What the platform does *not* promise is retroactive edit of the immutable log within its retention window. A deployment that must guarantee subject erasure inside that window configures a shorter window or routes those subjects through claim-check so the shred is a single object delete. This is a documented posture, not an operation: no `forget` verb ships, because erasure is a deployment-and-retention decision, not a per-call one.

### C5.2 Schema evolution

The schema registry (A11.2) is the evolution seam, but it carries no compatibility-mode contract: `RegisterSchema` allocates a permanent, collision-rejected id (`SchemaDef { id, source, name, version }`), and `name`/`version` are optional caller-tracked metadata the registry stores and returns but never dispatches on, only `id` selects the decoder. Readers decode against a specific id: a record carries its writer schema id (`agdx.sid`), and the versioned-decode path resolves that id, so a topic carrying several live ids decodes each record under the schema it was written with rather than assuming the latest. Migration is therefore additive by construction rather than by a declared mode: a change registers as a new id (ids are never overwritten), producers move to the new id when ready, and readers span as many ids as the topic has ever used. Dropping a schema only retires it from new registrations and browse listings, and a record already stamped with its id keeps decoding. A producer that wants a compatible evolution keeps it compatible by discipline (e.g. only widening what a new id's readers already tolerate), not by a mode the registry checks.

### C5.3 Cross-surface timeline

"Everything that happened for one context" is a read recipe, not a new verb. A conversation's timeline is the log filtered to one `ConversationId` across the surfaces it touched (commands, responses, tool calls, memory writes, run status), ordered by the substrate's single timestamp clock, exactly what the reader/cursor plus a conversation filter already produce, and what the Conversations lens in the management UI renders. Global stores (kv, memory, graph, query) stay separate views, honestly scoped, rather than being folded into one synthetic timeline. The SDK documents the recipe (a `ContextAssembler` over the context's topics) instead of growing a `timeline()` verb, so there is one context model, not two.

## C6. Data-object operation map

AGDX expresses its data model as named operations rather than defining a second transport. The Iggy binding maps those operations onto Apache Iggy's streaming APIs and the managed command band per A1.2. This table records implementation coverage without claiming publication or adoption by an external standards body.

| Data-object op | Realized as | State |
| --- | --- | --- |
| `PUT` / `GET` / `DELETE` / `CAS` | `kv.set` / `kv.get` / `kv.delete` / `kv.cas` | shipped |
| `EXISTS` / `EXPIRE` / `PATCH` / `LEASE` / `RELEASE` | `kv.exists` / `kv.expire` / `kv.patch` / `kv.lease` / `kv.release` (A10.2) | shipped |
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

Governance validation is split by ownership. `laser-wire` pins the authz bytes and command codes with fixtures and constants tests, and unit-tests the deny-wins/default-deny/delegation decision helpers. The shared coarse-capability bitmask (`action_index`) is guarded by a compile-time assert that `Feature` count times `ACTION_COUNT` fits the 64-bit mask and that `ACTION_COUNT` equals the true `Action` variant count, so adding a feature or action can never silently alias two capabilities onto one bit or skip a row. The Rust, Python, and TypeScript SDKs test the typed client surface and shared scenarios. The Iggy fork owns full enforcement integration: journal replay, root `admin` seed, per-command resource selection, batch decomposition, and edge rejection before forwarding. The managed plane owns the per-source query/graph depth checks on forwarded grants, and gates the role catalog and role-binding browse reads behind `authz:read` (listing who-can-do-what is itself privileged, so an unprivileged caller cannot enumerate the policy). The `Feature` and `Action` enums are `#[non_exhaustive]` so a newer peer's added capability does not force a breaking match, and an unknown authz `feature`/`action` string an enforcer cannot classify is treated as matching no grant (default-deny), never as a silent allow. The mirrored `governance` examples are live smoke tests against a deployment that advertises `authz`. Raw Apache Iggy correctly skips those phases because it does not serve the fork-native authorization band. Above the command edge, the SDK adds the effect-boundary governance hook (C3): a deployment-enrolled `ActionGovernor` decides before an agent's side effect runs and every non-allow decision leaves a digest-chained evidence event on the audit topic, verified by the cross-SDK governance scenarios. The hook cannot widen server-side RBAC, only narrow what an agent does before it reaches the edge.

**Status-code map.** The draft's statuses project onto the unified result-code space (A7): `CREATED`/`OK`/`NO_CONTENT` to `ok`, `VERSION_CONFLICT`/`ALREADY_EXISTS`/`LEASE_LOST`/`TXN_CONFLICT` to `conflict`, `NOT_FOUND` to `not-found`, `NOT_IMPLEMENTED` to `unsupported`, `RESOURCE_EXHAUSTED`/backend faults to `backend`, `INVALID_ARGUMENT` to `invalid-argument`, `PARTIAL` to the paging cursor rather than a status.

**Deliberately not adopted (broker semantics the log does not have).** The draft's push messaging (`SUBSCRIBE` / `DELIVER` / `ACK` / `NACK` / per-subscription `ACK_MODE`) is **not** implemented: Apache Iggy is an offset log, not a queue, so delivery guarantees (at-least-once, redelivery, dead-lettering, delivery-count) come from log replay plus idempotent dedup (A1.5, the reliable consumer and the dead-letter capsule A9.5), not from broker settlement ops. `WATCH` / `NOTIFY` change-capture ships as the log-native change feed (A11.5): records on a change topic consumed by offset, not a broker watch.

---
---

# Appendix. SDK API stability

This is about the Rust SDK surface, distinct from the wire contract above.

- **Builders are the contract.** Construct wire and data types through their builders and fluent methods. Public fields exist for reading results and for wire mirroring. New fields may appear in any minor release, so exhaustive struct literals are not supported.
- **Wire mirrors are wire-stability-bound, not API-stability-bound.** Types that mirror the wire keep their public fields because the wire defines them, and they change only when the wire does, per the compatibility rules in A8.
- **Terminal verb convention.** A fluent builder ends in `.send().await` for a write or `.fetch().await` for a read. Direct async methods are used only where there is nothing to build.
- **Errors are typed and forward-compatible.** Managed failures nest the wire error intact. Every public error enum and the capability structs (the hello reply, op-version set, and capability map) are `#[non_exhaustive]`, so a new variant or field is not a breaking change. Always keep a wildcard arm. The growable u8 dictionaries (task state, agent error code, dead-letter reason) instead carry an `Unrecognized(u8)` variant that decodes and re-encodes an unknown code byte-for-byte, so an old build relays a newer peer's code rather than failing. The internally tagged configuration enums that cross the JSON HTTP surface (`SchemaSource`, `RetentionPolicy`) carry a unit `Unknown` `#[serde(other)]` catch-all for the same reason: an unknown `kind` decodes rather than failing the whole reply, though it is lossy (the original kind and fields are dropped, so a decoder must not re-apply an `Unknown`). `ContentType` keeps its forward-compat at the byte level instead, through `from_code(u8) -> Option`, because the `agdx.ct` u8 code is its canonical wire form.
- **Facade growth lands on sub-facade handles**, not as new flat methods on the client. The handles are the only control surface.
- **The accessor grammar.** Every primitive is reached through an accessor that takes its scope word (`stream(name)` then `topic(name)` mirroring the substrate's stream-then-topic hierarchy, `topic(name)` against the default stream, `query(index)`, `kv(namespace)`, `fork(id)`, `graph(name)`, `memory(name)`, `context(conversation)`, `agent(id)`, `runs()`), and every action is a verb on that object. Accessors are free and synchronous, IO happens at the terminal verb, required arguments are positional, options are always fluent, and binary opt-ins are `.thing()` never `.thing(true)`.
