# LaserData - Laser Wire

[![crates.io](https://img.shields.io/crates/v/laser-wire.svg)](https://crates.io/crates/laser-wire) [![docs.rs](https://docs.rs/laser-wire/badge.svg)](https://docs.rs/laser-wire)

The wire contract by [LaserData, Inc.](https://laserdata.com) for [Apache Iggy](https://iggy.apache.org), as one typed, runtime-free crate: managed command codes, CBOR envelopes, logical schemas and tagged values, operational and lakehouse query IR, destination and checkpoint declarations, Arrow IPC policy, key-value, fork, knowledge-graph, and agent surfaces, HTTP views and routes, dictionaries, caps, and the golden fixture corpus that pins every byte.

The Rust SDK re-exports this crate as `laser_sdk::wire`, Python binds the same types, and the native TypeScript client round-trips every file in this crate's fixture corpus. LaserData Cloud consumes the definitions directly. No IO, async runtime, clock, or randomness is required, so the crate compiles unchanged for native servers and `wasm32-unknown-unknown`.

## Features

Modules carve the API surface. Features gate dependencies. No wire-contract type is ever feature-gated.

| Feature | Adds | wasm |
|---|---|---|
| `cbor` (default) | named-field CBOR (RFC 8949) encode/decode + the `[len: u32 LE]` socket framing, sans-io | yes |
| `codecs` | `Codec`/`Decoder` traits, `Json`/`Msgpack`/`Cbor` marker types, `Row`/`KvEntry` decode helpers | yes |
| `bson` | the BSON codec | no (its tree pulls `getrandom`) |
| `fixtures` | the golden corpus embedded via `include_bytes!` + assert helpers | yes |
| `builders` | the fluent `Query::builder()` (bon-derived), non-default so a type-only consumer that struct-literals `Query` does not pull `bon` | yes |
| `http-client` | a typed `/agdx/*` client (`http_client`) over a caller-injected `Transport` (gloo-net on wasm, reqwest natively), the crate's one async surface, runtime-agnostic | yes |

## Module map

| Module | Owns |
|---|---|
| `codes` | managed command codes + per-surface op versions |
| `headers` | the `agdx.*` / `gen_ai.*` header dictionaries + header caps + `CONVERSATION_FIELD` (the auto-projected `conversation_id` field name the conversation lens filters on) |
| `topics` | the `_agdx` ops stream + topic names |
| `limits` | page, query, KV, frame, and agent-envelope caps |
| `content` | `ContentType` + the `agdx.ct` u8 code dictionary |
| `hello` | `HelloReply`, independently negotiated `OpVersions`, structured backend descriptors and capabilities, and the backend announcement consumed during capability negotiation |
| `authz` | the capability layer: `Effect`/`Feature`/`Action`/`ResourcePattern`/`Grant`/`Role`/`RoleBinding`, the `feature_action(code)` classifier + `action_index` coarse-bitmask layout, the whoami/role/binding/history request+reply types, revision-guarded binding writes, and `AuthzReply`/`AuthzError`. Orthogonal to the substrate's own permissions, the fork-native authorization band (`AGDX_AUTHZ_*`) |
| `batch` | the mixed-operation batch (`BatchRequest`/`BatchItem`/`BatchReply`, `MAX_BATCH_OPS = 64`): several managed requests in one round trip, per-op results, never a transaction |
| `change` | `ChangeRecord`, the change-feed frame the projector publishes per committed notifying batch (a wakeup carrying the index and its committed offset window, never the rows) |
| `schema` | logical schemas, canonical SHA-256 fingerprints, all logical type and tagged value variants, reserved provenance fields, and value-to-schema validation |
| `source` | stable source scope and source-incarnation identity used by destinations and checkpoints |
| `destination` | materialization destination declarations, backend and physical-table bindings, start policy, and explicit operational or lakehouse query routes |
| `checkpoint` | revision-guarded public destination mutations, separately typed replicated transitions, checkpoint reads, lifecycle, progress, repair, and snapshot evidence |
| `arrow` | Arrow IPC stream metadata, acceptance policy, limits, and typed rejection codes |
| `query` | Queries with explicit operational or lakehouse targets, typed predicates and SQL parameters, cursor paging, status and cancellation, positional typed results, execution evidence, consistency, and errors |
| `result` | the unified `ResultCode` space + HTTP status mapping with `From` projections off every surface error, and `CommandError` (the surface-agnostic fallback reply) |
| `browse` | registry browse requests + `BrowseReply`, including `DecodeRecord` |
| `control` | `Projection` (incl. `ProjectionKind::Graph` + the `EntitySchema` node/edge extraction plan), `ProjectionBinding`, `SchemaDef`, `ControlEnvelope` |
| `kv` | the key-value requests (incl. `KvCas`/`CasExpect` and the single-transaction `KvCopy`/`KvMove`), `KvReply` (incl. `Committed`), `KvError` (incl. `VersionConflict`), the entry `version` token, and an optional `conversation` on `KvScan`/`KvDeleteMany` that narrows a memory-view scan to one conversation (additive, omitted on the wire when unset) |
| `fork` | the Iggy server requests, `ForkReply`, `ForkError`, and `validate_fork_id` (the shared id charset safelist) |
| `graph` | the knowledge-graph ops (`GraphQuery`/`GraphNeighbors`/`GraphUpsert`), `GraphResult`, `GraphError`, `NodeId`/`EdgeId`, and the content-addressed constructors `NodeId::content`/`EdgeId::content` + `GraphNode::entity`/`GraphEdge::relate`. A node and an edge carry an optional `source` ([`SourceRef`]: a message position, key-value entry, or memory id) so a graph element links back to its origin, skip-none and excluded from the content-addressed id (`GraphEdge::with_source`). `SourceRef::Message` carries an optional `conversation`, and `GraphQuery`/`GraphNeighbors` an optional `conversation`, so a read narrows a traversal to one conversation (additive, omitted on the wire when unset) |
| `hashing` | the one canonical `content_id` (a dependency-free FNV over byte segments) every content-addressed id shares, pinned by a golden vector |
| `agent` | the Agent Data Exchange Protocol: `AgentEnvelope`, ids, dictionaries, `validate`, `BodyRef`, the pinned operation/metadata vocabularies |
| `forward` | the forwarded managed-request frames (`ForwardedQuery`/`ForwardedCommand`, the command carrying the optional stable `operation_id` a mutation forwards under) |
| `mutation` | the managed mutation identity contract: `ManagedRequestEnvelope` (the client wrapper carrying one nonzero `operation_id` per logical mutation) and `MutationCommandEnvelope` (the durable record a deployment appends and folds). Which codes must carry the identity is the `codes` module's shared `is_idempotent_managed_request` classifier |
| `keys` | the versioned managed key record (`KeyRecord`, `KeyKind`, `KEY_RECORD_VERSION`): the one storage representation every port's managed key registry reads and writes |
| `commands` | the `Command` trait pairing each code with its request/reply types |
| `http` | `/agdx/*` route constants, path builders, typed query parameters, JSON views for query, destinations, snapshots, schemas, files and metrics, and the canonical `ErrorBody` reply contract |
| `http_client` | feature `http-client`, a typed `/agdx/*` client over an injected runtime-agnostic `Transport`, available on native and wasm targets |
| `validate` | the `Validate` trait used both before client I/O and after server decode, including recursive logical schemas and values, query requests and replies, destinations, checkpoints, Arrow metadata, batch, key-value, graph, memory, and client metadata |
| `framing` | `encode_named`/`decode_named` + `frame_encode`/`frame_decode` |
| `codecs` | the payload codec traits and marker types |
| `fixtures` | the embedded golden corpus |

## Compatibility rules

Query results are schema-first. `QueryResult.fields` defines one ordered logical schema and every `Row.values` array aligns positionally with it. Use the language SDK accessors to read a value by field name. Inline query pages stay bounded at 1000 rows. Publish a self-contained Arrow IPC stream when transferring analytical batches.

Operational and lakehouse targets are different variants. An operational result proves its backend resource generation and runtime configuration revision. A lakehouse result additionally proves destination generation, table UUID, snapshot, schema and partition-spec IDs, materialization boundary, checkpoint revision, and global state revision.

Destination declarations are public desired state. Public checkpoint mutations carry bounded client intent and worker evidence, while Plane-promoted transitions add authenticated actors, committed time, authoritative source cuts, absolute lease deadlines, lifecycle evidence, and server-certified repair records. Their types and decoders remain separate so a client body cannot be mistaken for a committed managed-log mutation.

Named-field CBOR ignores unknown fields, so additive optional fields are compatible. Enum growth rides the per-surface op versions advertised by the capability handshake. One family departs from version 1: the fenced-lease ops (`KvLease`, `KvLeaseRenew`, `KvRelease`, `KvCasFenced`) ride `KV_LEASE_OP_VERSION = 2` after their holder-identity reshape, with required fields and no decode defaults so a v1 payload fails decode and `Validate` rejects any other `v`, both fail-closed, gated by the `KV_FENCED_LEASES` capability bit. The reply, outcome, and error enums are `#[non_exhaustive]`, so a consumer keeps compiling when a variant is added. The u8-code dictionaries (task state, agent error code, dead-letter reason) go further: an unknown code decodes to an `Unrecognized(u8)` variant and re-encodes byte-for-byte, so an old build relays a newer peer's code instead of failing. The internally tagged config enums that cross the JSON HTTP surface (`SchemaSource`, `RetentionPolicy`) carry a unit `Unknown` `#[serde(other)]` catch-all in the same spirit (lossy and read-only, so never re-apply an `Unknown`). A few executor-dispatched vocabularies (the query comparison and aggregate operators) are deliberately exhaustive, so adding one is a compile error that forces every backend to implement it rather than silently mis-handling it.

Every `Vec<u8>` byte field rides as a CBOR byte string via the shared `encoding::bin_bytes`/`opt_bin_bytes` helpers (compact, unambiguous), never as a bare `Vec<u8>` (which would encode as a CBOR array of integers). The forwarded frames (`ForwardedQuery`/`ForwardedCommand`) and `SchemaSource::Protobuf.descriptor_set` follow this like every other surface.

The fixture corpus pins the encoding behavior of the crate itself: regenerate with `AGDX_WIRE_FIXTURES_REGEN=1` only on an intentional wire change. Decoding hostile bytes never panics, and that guarantee is held by a deterministic robustness suite (`wire/tests/robustness.rs`) and a `cargo-fuzz` crate under `fuzz/`.

## License

Apache-2.0. Copyright LaserData, Inc.

Apache and Apache Iggy are trademarks of the Apache Software Foundation. Use of these marks does not imply endorsement by the Apache Software Foundation.
