# LaserData - Laser Wire

[![crates.io](https://img.shields.io/crates/v/laser-wire.svg)](https://crates.io/crates/laser-wire) [![docs.rs](https://docs.rs/laser-wire/badge.svg)](https://docs.rs/laser-wire)

The `laser-wire` crate defines the data contract shared by LaserData clients and services. [LaserData, Inc.](https://laserdata.com) maintains it for the [Apache Iggy](https://iggy.apache.org) binding. It defines command codes, envelopes, schemas, queries, destinations, checkpoints, and state operations. It also defines agent messages, HTTP representations, limits, and reference test data.

Context manifests, compaction, retrieval, state deltas, and state snapshots use named CBOR fixtures that Rust, Python, and TypeScript re-encode byte for byte. The shared JSON Patch cases check application behavior in all three languages. A state JSON integer must fit JavaScript's exact integer range so the clients preserve the same value.

Rust exposes these types as `laser_sdk::wire`. Python calls the same Rust code. TypeScript decodes and re-encodes the same reference test data. LaserData Cloud uses the definitions directly. The crate needs no I/O, clock, randomness, or asynchronous runtime. It supports native targets and `wasm32-unknown-unknown`.

## Features

Modules group related types and operations. Features select optional dependencies. Wire contract types remain available independently of feature selection.

| Feature | Adds | wasm |
|---|---|---|
| `cbor` (default) | named-field CBOR (RFC 8949) encode/decode + the `[len: u32 LE]` socket framing, sans-io | yes |
| `codecs` | `Codec`/`Decoder` traits, `Json`/`Msgpack`/`Cbor` marker types, `Row`/`KvEntry` decode helpers | yes |
| `bson` | the BSON codec | no (its tree pulls `getrandom`) |
| `fixtures` | the golden corpus embedded via `include_bytes!` + assert helpers | yes |
| `builders` | the fluent `Query::builder()` (bon-derived), non-default so a type-only consumer that struct-literals `Query` does not pull `bon` | yes |
| `http-client` | a typed `/agdx/*` client (`http_client`) over a caller-injected `Transport` (gloo-net on wasm, reqwest natively), the crate's one async surface, runtime-agnostic | yes |
| `filter-eval` | the consumer-filter evaluator: compile a `ConsumerFilter` once and judge JSON, CBOR, Avro, and Protobuf records with the same verdicts the server gives | yes |

## Module map

| Module | Owns |
|---|---|
| `codes` | managed command codes + per-surface op versions, including the session read band `AGDX_SESSION_*_CODE` (1_000_710 to 1_000_716). Codes 1_000_700 to 1_000_703 belonged to the retired run registry and are never reused |
| `headers` | the `agdx.*` / `gen_ai.*` header dictionaries, header caps, `CONVERSATION_FIELD` (the auto-projected `conversation_id` field name the conversation lens filters on), and the one header encoder `RecordHeaders` both record families share. `BROADCAST` is the `*` addressee, which a reader never parses as an agent id |
| `topics` | the `_agdx` ops stream + topic names, and the agent topic names `AGENT_SESSIONS`, `AGENT_STREAMS`, `AGENT_HEARTBEATS`, `AGENT_CONTROL`, `AGENT_MEMORY`, `AGENT_DLQ`, `AGENT_AUDIT`, `AGENT_JOURNAL`, `AGENT_REGISTRY` |
| `limits` | page, query, KV, frame, agent-envelope, session, and consumer-filter caps (`MAX_SESSION_LABEL_BYTES`, `MAX_HEARTBEAT_SESSIONS`, `MAX_STATE_PATCH_OPS`, `MAX_STATE_DOCUMENT_BYTES`, `MAX_MANIFEST_FRAGMENTS`, `MAX_FILTER_BYTES`, `MAX_FILTERED_PAGE_RECORDS`, `MAX_FILTERED_PAGE_BYTES`, the catalog and preview bounds) |
| `filter` | consumer filters: `ConsumerFilter` and `FilterExpr` builders (`expr`), field paths, text matching, coercions, the filtered poll, acknowledgment, preview, and test request and reply types (`read`), the saved-filter catalog and group bindings (`catalog`), the typed header dictionary (`headers`), the codec decoders (`codecs`), and the evaluator (`eval`, feature `filter-eval`), with `fixtures/filter_eval_cases.json` and `fixtures/filter_codec_cases.json` as the cross-language corpus |
| `content` | `ContentType` + the `agdx.ct` u8 code dictionary |
| `hello` | `HelloReply`, independently negotiated `OpVersions`, structured backend descriptors and capabilities, and the backend announcement consumed during capability negotiation |
| `authz` | the capability layer: `Effect`/`Feature`/`Action`/`ResourcePattern`/`Grant`/`Role`/`RoleBinding`, the `feature_action(code)` classifier + `action_index` coarse-bitmask layout, the whoami/role/binding/history request+reply types, revision-guarded binding writes, and `AuthzReply`/`AuthzError`. `Feature::Session` covers the session reads and source control on `stream:<name>` (`stream_resource`). `Agent` and `Workflow` are retired names no code maps to. Orthogonal to the substrate's own permissions, the server-native authorization band (`AGDX_AUTHZ_*`) |
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
| `control` | `Projection` (including `ProjectionKind::Graph` + the `EntitySchema` node/edge extraction plan), `ProjectionBinding`, `SchemaDef`, `ControlEnvelope`, and the stream-scoped `RegisterSessionSource { stream, topics: SessionTopics }` and `RemoveSessionSource { stream }` |
| `kv` | the key-value requests (including `KvCas`/`CasExpect` and the single-transaction `KvCopy`/`KvMove`), `KvReply` (including `Committed`), `KvError` (including `VersionConflict`), the entry `version` token, an optional `conversation` on `KvScan`/`KvDeleteMany` that narrows a memory-view scan to one conversation, and an optional `session: SessionRef` on `KvSet`, `KvCas`, `KvDelete`, and `KvPatch` that links the write to a session |
| `fork` | the fork requests, `ForkReply`, `ForkError`, and `validate_fork_id` (the shared id charset safelist) |
| `graph` | the knowledge-graph ops (`GraphQuery`/`GraphNeighbors`/`GraphUpsert`), `GraphResult`, `GraphError`, `NodeId`/`EdgeId`, and the content-addressed constructors `NodeId::content`/`EdgeId::content` + `GraphNode::entity`/`GraphEdge::relate`. A node and an edge carry an optional `source` (`SourceRef`: a message position, key-value entry, or memory id) so a graph element links back to its origin, skip-none and excluded from the content-addressed id (`GraphEdge::with_source`). `SourceRef::Message` carries an optional `conversation`, and `GraphQuery`/`GraphNeighbors` an optional `conversation`, so a read narrows a traversal to one conversation. `GraphUpsert` carries an optional `session: SessionRef`, and nodes and edges an optional `producer: ProducerInfo`, also excluded from the content-addressed id |
| `hashing` | the one canonical `content_id` (a dependency-free FNV over byte segments) every content-addressed id shares, pinned by a golden vector |
| `agent` | the Agent Data Exchange Protocol: `AgentEnvelope` (with `parent` and `root`), ids, dictionaries, `validate`, `BodyRef`, the pinned operation/metadata vocabularies, the session bodies `SessionStart`/`SessionTransition`/`SessionEnd`, `SessionStatus`, `SessionRef`, `Budget`, `SdkInfo`, the context records `ContextManifest`/`Fragment`/`ContextCompaction`/`ContextRetrieval`, the state records `StateDelta`/`StateSnapshot` with `apply_json_patch`, and the shared `estimate_tokens` |
| `session` | the seven session read requests (each naming its stream first), `SessionReply` with `SessionInfo`, `SessionPage`, `SessionEventsPage`, `SessionStateView`, `SessionLinksView`, `SessionSources`, and `SessionChanges`, the frontier and gap types, `SessionError`, and the process heartbeat body `SessionHeartbeat` |
| `dispatch` | the timeline `display_type` mapping, the reliable-consumer `classify` and `classify_generic` dispatch classification, the control operation names, and the addressee filters `addressee_filter(me)` and `broadcast_filter()`, pinned by `fixtures/dispatch_cases.json` |
| `snapshot` | `FoldSnapshot` names one stream and fold. Sorted `SnapshotOffset` entries pin each topic creation time, partition, and last folded offset. |
| `forward` | the forwarded managed-request frames (`ForwardedQuery`/`ForwardedCommand`, the command carrying the optional stable `operation_id` a mutation forwards under) and the server-stamped `ForwardedScope { stream_id, stream, stream_created_at_micros }` a session command needs |
| `mutation` | the managed mutation identity contract: `ManagedRequestEnvelope` (the client wrapper carrying one nonzero `operation_id` per logical mutation) and `MutationCommandEnvelope` (the durable record a deployment appends and folds, carrying the trusted `scope` into replay). Which codes must carry the identity is the `codes` module's shared `is_idempotent_managed_request` classifier |
| `keys` | the versioned managed key record (`KeyRecord`, `KeyKind`, `KEY_RECORD_VERSION`): the one storage representation every port's managed key registry reads and writes |
| `commands` | the `Command` trait pairing each code with its request/reply types |
| `http` | `/agdx/*` route constants, path builders, typed query parameters, JSON views for query, destinations, snapshots, schemas, files and metrics, the session routes under `/agdx/sessions/{stream}` with their query types, and the canonical `ErrorBody` reply contract |
| `http_client` | feature `http-client`, a typed `/agdx/*` client over an injected runtime-agnostic `Transport`, available on native and wasm targets |
| `validate` | the `Validate` trait used both before client I/O and after server decode, including recursive logical schemas and values, query requests and replies, destinations, checkpoints, Arrow metadata, batch, key-value, graph, memory, and client metadata |
| `framing` | `encode_named`/`decode_named` + `frame_encode`/`frame_decode` |
| `codecs` | the payload codec traits and marker types |
| `fixtures` | the embedded golden corpus |

## Compatibility rules

`QueryResult.fields` defines the ordered schema for a result. Each `Row.values` entry matches the field at the same position. Use SDK accessors to read values by field name. A page contains at most 1000 rows. For analytical batches, publish a self-contained Arrow IPC stream.

Operational and lakehouse targets use separate variants. Operational results identify the backend resource generation and runtime configuration revision. Lakehouse results also identify the destination generation, table UUID, snapshot, schema, and partition-spec IDs. They include the materialization boundary, checkpoint revision, and global state revision.

Destination declarations describe the state that the client requests. Public checkpoint mutations carry client intent and worker evidence. Plane-promoted transitions add authenticated actors, commit time, source boundaries, lease deadlines, lifecycle evidence, and certified repair records. Separate types and decoders prevent a client request from impersonating a committed transition.

Named-field CBOR can ignore unknown fields. Optional additions are compatible when their meaning allows old readers to ignore them safely. Operation versions describe support for each command group. During this contract phase, all operation versions remain 1, including `KvLease`, `KvLeaseRenew`, `KvRelease`, and `KvCasFenced`. These requests require their holder-identity fields and the `KV_FENCED_LEASES` capability. `Validate` rejects an unsupported `v`.

The reply, outcome, and error enums use `#[non_exhaustive]`. Code that matches them must handle later variants. The u8 dictionaries preserve unknown values through `Unrecognized(u8)`, so a relay can re-encode them without changing the bytes. `SchemaSource` and `RetentionPolicy` use `Unknown` with `#[serde(other)]` for unknown HTTP variants. This representation loses the original fields and is read-only. Never submit an `Unknown` value as new configuration.

Query comparison and aggregate operators are exhaustive. An added operator causes compilation errors until each backend handles it.

Byte fields use the shared `encoding::bin_bytes` and `opt_bin_bytes` helpers. These helpers encode a `Vec<u8>` as a CBOR byte string. Plain `Vec<u8>` serialization produces an integer array. `ForwardedQuery`, `ForwardedCommand`, and `SchemaSource::Protobuf.descriptor_set` follow the same byte-string rule.

Reference files define the expected encoded bytes. If a wire change is intentional, regenerate them with `AGDX_WIRE_FIXTURES_REGEN=1`. The decode tests in `wire/tests/robustness.rs` reject malformed input without a panic. The `cargo-fuzz` project under `fuzz/` also tests malformed input.

## Consumer-group reads

Consumer-group reads use `FilterRef::Group`. A bound group runs its saved revision, while an explicitly unbound group returns original records without evaluating payloads. Pages, continuations and acknowledgments carry execution mode, policy generation and source identity. `count` limits delivered records and `max_examined` independently limits source records scanned. `CatalogPosition` carries an optional `operation_id` as durable proof of configuration across control-log recreation. JSON encodes this 128-bit ID as decimal text, while CBOR retains the integer. `GROUP_POLICIES_PATH` and `group_policy_path` address the confirmed HTTP group-policy endpoint.

## License

Apache-2.0. Copyright LaserData, Inc.

Apache and Apache Iggy are trademarks of the Apache Software Foundation. Use of these marks does not imply endorsement by the Apache Software Foundation.
