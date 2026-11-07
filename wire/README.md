# LaserData - Laser Wire

[![crates.io](https://img.shields.io/crates/v/laser-wire.svg)](https://crates.io/crates/laser-wire) [![docs.rs](https://docs.rs/laser-wire/badge.svg)](https://docs.rs/laser-wire)

The `laser-wire` crate defines the data contract shared by LaserData clients and services. [LaserData, Inc.](https://laserdata.com) maintains it for the [Apache Iggy](https://iggy.apache.org) binding. It defines command codes, envelopes, schemas, queries, and state operations. It also defines agent messages, HTTP representations, limits, and reference test data.

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
| `schema-decode` | the one writer-schema lowering (`writer_schema`): compile a registered Avro or Protobuf writer schema within the 1 MiB and depth 64 bounds and decode a body into the shared model, which `filter-eval`, the SDK codecs and the managed projector all use | yes |

## Module map

| Module | Owns |
|---|---|
| `codes` | managed command codes + per-surface op versions, including the session read band `AGDX_SESSION_*_CODE` (1_000_710 to 1_000_716). Codes 1_000_700 to 1_000_703 belonged to the retired run registry and are never reused |
| `headers` | the `agdx.*` / `gen_ai.*` header dictionaries, header caps, `CONVERSATION_FIELD` (the auto-projected `conversation_id` field name the conversation lens filters on), and the one header encoder `RecordHeaders` both record families share. `BROADCAST` is the `*` addressee, which a reader never parses as an agent id |
| `topics` | the `_agdx` ops stream + topic names, and the agent topic names `AGENT_SESSIONS`, `AGENT_STREAMS`, `AGENT_HEARTBEATS`, `AGENT_CONTROL`, `AGENT_MEMORY`, `AGENT_DLQ`, `AGENT_AUDIT`, `AGENT_JOURNAL`, `AGENT_REGISTRY` |
| `limits` | page, query, KV, frame, agent-envelope, session, and consumer-filter caps (`MAX_SESSION_LABEL_BYTES`, `MAX_SESSION_TEXT_QUERY_BYTES`, `MAX_HEARTBEAT_SESSIONS`, `MAX_STATE_PATCH_OPS`, `MAX_STATE_DOCUMENT_BYTES`, `MAX_MANIFEST_FRAGMENTS`, `MAX_FILTER_BYTES`, `MAX_FILTERED_PAGE_RECORDS`, `MAX_FILTERED_PAGE_BYTES`, `MAX_FILTERED_WAIT_CANCEL_IDS`, the catalog and preview bounds), and the caps on caller-chosen names (`MAX_PROJECTION_ID_BYTES`, `MAX_PROJECTION_NAME_BYTES`, `MAX_INDEX_FIELD_BYTES`, `MAX_SCHEMA_NAME_BYTES`, `MAX_GRAPH_LABEL_BYTES`, `MAX_MEMORY_ID_BYTES`) |
| `filter` | consumer filters: `ConsumerFilter` and `FilterExpr` builders (`expr`), field paths, text matching, coercions, the filtered poll, its page and owner context (`AckContext`), the filtered wait and its cancel by `wait_id` (`FilteredWaitRequest`, `FilteredWaitPartition`, `FilteredWaitResult`, `FilteredWaitCancel`, `FilteredWaitCancelled`), preview, and test request and reply types (`read`), the saved-filter catalog and group bindings with named group policies, filter owners and `validate_filter_name` (`catalog`), the typed header dictionary (`headers`), the codec decoders (`codecs`), and the evaluator (`eval`, feature `filter-eval`), with `fixtures/filter_eval_cases.json` and `fixtures/filter_codec_cases.json` as the cross-language corpus |
| `content` | `ContentType` + the `agdx.ct` u8 code dictionary |
| `hello` | `HelloReply`, independently negotiated `OpVersions`, structured backend descriptors and capabilities, and the backend announcement consumed during capability negotiation |
| `authz` | the capability layer: `Effect`/`Feature`/`Action`/`ResourcePattern`/`Grant`/`Role`/`RoleBinding`, the `feature_action(code)` classifier + `action_index` coarse-bitmask layout, the whoami/role/binding/history request+reply types, revision-guarded binding writes, and `AuthzReply`/`AuthzError`. `Feature::Session` covers the session reads and source control on `stream:<name>` (`stream_resource`). `Agent` and `Workflow` are retired names no code maps to. Orthogonal to the substrate's own permissions, the server-native authorization band (`AGDX_AUTHZ_*`) |
| `batch` | the mixed-operation batch (`BatchRequest`/`BatchItem`/`BatchReply`, `MAX_BATCH_OPS = 64`): several managed requests in one round trip, per-op results, never a transaction |
| `change` | `ChangeRecord`, the change-feed frame the projector publishes per committed notifying batch (a wakeup carrying the index and its committed offset window, never the rows) |
| `schema` | logical schemas, canonical SHA-256 fingerprints, all logical type and tagged value variants, reserved provenance fields, and value-to-schema validation |
| `destination` | the backend binding a projection writes to (`BackendBinding`, `BackendResourceId`) |
| `arrow` | Arrow IPC stream metadata, acceptance policy, limits, and typed rejection codes |
| `query` | Queries with an explicit operational target, typed predicates and SQL parameters, offset paging up to `MAX_QUERY_OFFSET`, positional typed results, execution evidence, consistency, and errors |
| `result` | the unified `ResultCode` space + HTTP status mapping with `From` projections off every surface error, and `CommandError` (the surface-agnostic fallback reply) |
| `browse` | registry browse requests + `BrowseReply`, including `DecodeRecord` |
| `control` | `Projection` (including `ProjectionKind::Graph` + the `EntitySchema` node/edge extraction plan), `ProjectionBinding`, `SchemaDef`, `ControlEnvelope`, and the stream-scoped `RegisterSessionSource { stream, topics: SessionTopics }` and `RemoveSessionSource { stream }`. A projection id never starts with `RESERVED_PROJECTION_ID_PREFIX` (`index:`) |
| `kv` | the key-value requests (including `KvCas`/`CasExpect` and the single-transaction `KvCopy`/`KvMove`), `KvReply` (including `Committed`), `KvError` (including `VersionConflict`), the entry `version` token, an optional `conversation` on `KvScan`/`KvDeleteMany` that narrows a memory-view scan to one conversation, and an optional `session: SessionRef` on `KvSet`, `KvCas`, `KvDelete`, and `KvPatch` that links the write to a session |
| `fork` | the fork requests, `ForkReply`, `ForkError`, and `validate_fork_id` (the shared id charset safelist) |
| `graph` | the knowledge-graph ops (`GraphQuery`/`GraphNeighbors`/`GraphUpsert`), `GraphResult`, `GraphError`, `NodeId`/`EdgeId`, and the content-addressed constructors `NodeId::content`/`EdgeId::content` + `GraphNode::entity`/`GraphEdge::relate`. A node and an edge carry an optional `source` (`SourceRef`: a message position, key-value entry, or memory id) so a graph element links back to its origin, skip-none and excluded from the content-addressed id (`GraphEdge::with_source`). `SourceRef::Message` carries an optional `conversation`, and `GraphQuery`/`GraphNeighbors` an optional `conversation`, so a read narrows a traversal to one conversation. `GraphUpsert` carries an optional `session: SessionRef`, and nodes and edges an optional `producer: ProducerInfo`, also excluded from the content-addressed id |
| `hashing` | the one canonical `content_id` (a dependency-free FNV over byte segments) every content-addressed id shares, pinned by a golden vector |
| `agent` | the Agent Data Exchange Protocol: `AgentEnvelope` (with `parent` and `root`), ids, dictionaries, `validate`, `BodyRef`, the pinned operation/metadata vocabularies, the session bodies `SessionStart`/`SessionTransition`/`SessionEnd`, `SessionStatus`, `SessionRef`, `Budget`, `SdkInfo`, the context records `ContextManifest`/`Fragment`/`ContextCompaction`/`ContextRetrieval`, the state records `StateDelta`/`StateSnapshot` with `apply_json_patch`, and the shared `estimate_tokens` |
| `session` | the seven session read requests (each naming its stream first), `SessionReply` with `SessionInfo`, `SessionPage` (an offset page with `total_pages`, `total_results`, `page`, and the walk's bound `as_of` and `series`, whose `rebuilding` says the index is rebuilding or catching up, so the page can be incomplete), `SessionEventsPage` (the same bounded offset page over one timeline), `SessionStateView`, `SessionLinksView`, `SessionSources`, and `SessionChanges` (an offset page over the dense change sequence with `floor`, the oldest retained sequence, and the index's `series`), the frontier and gap types, `SessionError`, and the process heartbeat body `SessionHeartbeat` |
| `dispatch` | the timeline `display_type` mapping, the reliable-consumer `classify` and `classify_generic` dispatch classification, the control operation names, and the addressee filters `addressee_filter(me)` and `broadcast_filter()` with `ADDRESSEE_FILTER_NAME`, the name an agent runtime saves its addressee policy under, pinned by `fixtures/dispatch_cases.json` |
| `snapshot` | `FoldSnapshot` names one stream and fold. Sorted `SnapshotOffset` entries pin each topic creation time, partition, and last folded offset. |
| `forward` | the forwarded managed-request frames (`ForwardedQuery`/`ForwardedCommand`, the command carrying the optional stable `operation_id` a mutation forwards under and the `caller` the server stamps, absent on the server's own requests), the server-stamped `ForwardedScope { stream_id, stream, stream_created_at_micros }` a session command needs, and `Caller { user_id, agent, on_behalf_of }`, the only actor that forwarded commands, control stamps and mutation envelopes name, present only when a client asked |
| `mutation` | the managed mutation identity contract: `ManagedRequestEnvelope` (the client wrapper carrying one nonzero `operation_id` per logical mutation) and `MutationCommandEnvelope` (the durable record a deployment appends and folds, carrying the trusted `scope` into replay). Which codes must carry the identity is the `codes` module's shared `is_idempotent_managed_request` classifier |
| `keys` | the versioned managed key record (`KeyRecord`, `KeyKind`, `KEY_RECORD_VERSION`): the one storage representation every port's managed key registry reads and writes |
| `commands` | the `Command` trait pairing each code with its request/reply types |
| `http` | `/agdx/*` route constants, path builders, typed query parameters, JSON views for query results, projections, schemas, key-value pages and forks, the session routes under `/agdx/sessions/{stream}` with their query types, and the canonical `ErrorBody` reply contract |
| `http_client` | feature `http-client`, a typed `/agdx/*` client over an injected runtime-agnostic `Transport`, available on native and wasm targets. Each managed write has an `_as` variant that sends the caller's `operation_id`, so a re-sent write applies once. A non-2xx reply is `ClientError::Api` when its body is an `ErrorBody`, and `ClientError::Status` with the HTTP status, a status-derived code, and the reply text otherwise, such as a gateway's plain-text 502 or 504. `ClientError::code` covers both |
| `validate` | the `Validate` trait used both before client I/O and after server decode, including recursive logical schemas and values, query requests and replies, Arrow metadata, batch, key-value, graph, memory, client metadata, projections, bindings, writer schemas, control commands, and session lists |
| `framing` | `encode_named`/`decode_named` + `frame_encode`/`frame_decode`, `encode_named_framed` (one buffer behind the length prefix) and `NamedFields` (a borrowed one-pass scan of a named map's top-level fields) |
| `codecs` | the payload codec traits and marker types |
| `writer_schema` | feature `schema-decode`: `WriterSchema`, `AvroWriterSchema` (logical annotations removed, so values are physical), `compile_protobuf`, `decode_protobuf`, `lower_avro`, `lower_protobuf`, `DecodeLimits` and `RECORD_DECODE_LIMITS`. Schema field names, numeric enums, exact 64-bit integers, bytes as integer arrays, proto3 defaults present, pinned by `fixtures/writer_schema_cases.json` |
| `fixtures` | the embedded golden corpus |

## Compatibility rules

`QueryResult.fields` defines the ordered schema for a result. Each `Row.values` entry matches the field at the same position. Use SDK accessors to read values by field name. A page contains at most 1000 rows. For analytical batches, publish a self-contained Arrow IPC stream.

Operational results identify the index, the backend resource generation and the runtime configuration revision. Pages are addressed by offset, and `has_more` says whether rows follow the page.

Named-field CBOR can ignore unknown fields. Optional additions are compatible when their meaning allows old readers to ignore them safely. Operation versions describe support for each command group. During this contract phase, all operation versions remain 1, including `KvLease`, `KvLeaseRenew`, `KvRelease`, and `KvCasFenced`. These requests require their holder-identity fields and the `KV_FENCED_LEASES` capability. `Validate` rejects an unsupported `v`.

The reply, outcome, and error enums use `#[non_exhaustive]`. Code that matches them must handle later variants. The u8 dictionaries preserve unknown values through `Unrecognized(u8)`, so a relay can re-encode them without changing the bytes. `SchemaSource` and `RetentionPolicy` use `Unknown` with `#[serde(other)]` for unknown HTTP variants. This representation loses the original fields and is read-only. Never submit an `Unknown` value as new configuration.

Query comparison and aggregate operators are exhaustive. An added operator causes compilation errors until each backend handles it.

Byte fields use the shared `encoding::bin_bytes` and `opt_bin_bytes` helpers. These helpers encode a `Vec<u8>` as a CBOR byte string. Plain `Vec<u8>` serialization produces an integer array. `ForwardedQuery`, `ForwardedCommand`, and `SchemaSource::Protobuf.descriptor_set` follow the same byte-string rule.

Reference files define the expected encoded bytes. The `session_http_*.json` files pin the HTTP JSON body of every session read, the change feed's start, page, pruned, and legacy replies with a series above 2^53, and the session error body, all generated from typed values. If a wire change is intentional, regenerate them with `AGDX_WIRE_FIXTURES_REGEN=1`. The decode tests in `wire/tests/robustness.rs` reject malformed input without a panic. The `cargo-fuzz` project under `fuzz/` also tests malformed input.

## Consumer-group reads

Consumer-group reads use `FilterRef::Group`. A bound group runs its saved revision, while an explicitly unbound group returns original records without evaluating payloads. Pages and continuations carry execution mode, policy generation and source identity, and a page also carries the owner context its reader stores the offset under with the standard `StoreConsumerOffset`. `count` limits delivered records and `max_examined` independently limits source records scanned. `CatalogPosition` carries an optional `operation_id` as durable proof of configuration across control-log recreation. JSON encodes this 128-bit ID as decimal text, while CBOR retains the integer. `GROUP_POLICIES_PATH` and `group_policy_path` address the confirmed HTTP group-policy endpoint. `GroupFilterSpec::Definition { name, filter }` configures a group with a definition saved under the caller's `name`, a label checked by `validate_filter_name` that need not be unique. `FilterSummary.owner` names the owning group of a group's own filter, `ListFilters.owner` selects it by identity, and `FilterListQuery` carries the same selector as the five `owner_*` HTTP parameters, all or none. `AGDX_FILTERED_WAIT_CODE` holds a reader's idle round until one of its partitions has a record, answered with `FilterOutcome::Ready`, and `FilterAnnounce.max_wait_ms` announces the longest wait a server holds, zero for none.

## License

Apache-2.0. Copyright LaserData, Inc.

Apache and Apache Iggy are trademarks of the Apache Software Foundation. Use of these marks does not imply endorsement by the Apache Software Foundation.

## Materialization and operational SQL

Materialization setup is available in Rust, Python, and TypeScript through `topic.materialize().apply()`. It registers a whole-record projection by default. A typed projection selects extracted fields. Reuse the setup operation identity after an uncertain response. The resulting handle reports binding status and waits against a captured source target. Readiness and complete historical coverage are separate results. A wait also ends early as failed, blocked or not found, and `topic.materialization(projection_id)` returns the handle again in a later process, see [client behavior](../docs/client-behavior.md#materialization-and-operational-sql).

Projection history defaults to append. Keyed history selects a typed header key or a payload JSON pointer and can name a delete header. Without one, a keyed view never deletes a key. A key stays in one partition per topic incarnation. Older updates cannot replace a newer value or restore a deleted value. Keyed history does not claim complete append history. Projection and binding history cannot change in place.

Bindings use `invalid_records` and `oversize` policies. Both default to block. Dead-letter handling permits automatic skips of rejected records. Reference handling omits an oversized body while retaining its source location. A record that fails its declared writer schema follows the invalid-record policy.

Raw SQL requires an explicit dialect in every client. Rust uses `raw_sql(dialect, sql)` or `raw_sql_with(dialect, sql, params)`. Python uses `raw_sql(dialect, sql, params=None)`. TypeScript uses `rawSql(dialect, sql, params?)`. Every referenced index must be authorized for the caller and belong to the same stream and backend. Unsupported managed capabilities fail explicitly on original Apache Iggy. Standard streaming remains available.
