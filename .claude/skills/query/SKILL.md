---
name: query
description: The managed query, logical schema, destination, checkpoint, and Arrow IPC surfaces across Rust, Python, and TypeScript. Use for typed results, operational or lakehouse targets, projection control, cursor paging, execution control, or destination APIs.
---

# Query and data surfaces

Load [laser-sdk-overview](../laser-sdk-overview/SKILL.md) first. The normative wire contract is [docs/agdx.md](../../../docs/agdx.md). Repository rules and the verification order are in [AGENTS.md](../../../AGENTS.md).

## Ownership boundary

Laser SDK owns client builders, validation, codecs, capability gates, command transport, HTTP routes and views, and cross-language fixtures. Query execution, projection materialization, checkpoints, Iceberg commits, and backend adapters run in managed deployments. Do not add an executor or storage backend to `sdk/`.

The binary path is a managed Apache Iggy command. It does not use a request topic. Rust sends through `send_raw_with_response`, TypeScript through `ManagedTransport`, and Python binds the Rust client. The optional `laser-wire` HTTP client uses the same contract through `/agdx/*` routes over an injected transport.

## Source map

- `wire/src/query.rs` owns the query contract, paging and execution envelopes, typed results, result context, and query errors.
- `wire/src/schema.rs` owns logical types, schemas, fingerprints, tagged values, provenance field reservations, and value-to-schema validation.
- `wire/src/source.rs` owns stable source identity and incarnation types.
- `wire/src/destination.rs` owns materialization destination declarations and explicit query routes.
- `wire/src/checkpoint.rs` separates bounded public mutations from replicated checkpoint transitions and owns checkpoint reads.
- `wire/src/arrow.rs` owns Arrow IPC metadata, the frozen acceptance policy, and rejection codes.
- `wire/src/http.rs` and `wire/src/http_client.rs` own JSON views, routes, and the runtime-agnostic HTTP client.
- `sdk/src/query/` owns the Rust query builder and command client.
- `sdk/src/destinations.rs` owns Rust destination and checkpoint accessors.
- `foreign/python/src/query.rs` and `foreign/python/src/destinations.rs` bind the same Rust surfaces.
- `foreign/typescript/src/wire/{query,schema,source,destination,checkpoint,arrow}.ts` implement the contract natively.
- `foreign/typescript/src/managed/{query,destinations}.ts` and `src/client/laser.ts` own the TypeScript public builders and capability gates.

## Query contract

Every query carries a nonzero `execution_id`, an absolute nonzero `deadline_micros`, an explicit target, a bounded page request, and a requested consistency. `QUERY_OP_VERSION` is `1`.

Targets are explicit and mutually exclusive:

- `operational { index }` addresses the current materialized operational view and may use a fork.
- `lakehouse { destination_id, destination_generation, snapshot? }` addresses one declared destination generation and may select a positive snapshot ID or timestamp. It cannot use an operational fork.

Rust uses `laser.query(index)` and `laser.query_lakehouse(destination_id, generation)`. Python uses `laser.query(...)` and `laser.query_lakehouse(...)`. TypeScript uses `laser.query(...)` and `laser.queryLakehouse(...)`.

The structured DSL includes exact matches, recursive filters, message type, a half-open time range, lexical and vector search, ordering, selection, aggregation, having, distinct, and typed raw SQL parameters. Raw SQL has an explicit dialect and cannot be mixed with the structured expression. Validation caps names, fields, predicates, parameters, SQL bytes, vector dimensions, cursor bytes, page size, and recursive depth before transport I/O.

## Typed results

`QueryResult.fields` is the ordered logical result schema. Each `Row.values` array is positionally aligned with it. Query rows do not have header maps. Every value is a tagged `TypedValue`, including null, numeric widths, decimal, date and time, UUID, bytes, struct, list, and map.

Use the result accessor by logical field name rather than manually searching the schema:

- Rust: `result.value(row, "amount")`, `value_text`, `value_u64`, and `value_i64`.
- Python: `result.value(row, "amount")` and `value_text`.
- TypeScript: `queryResultValue(result, row, "amount")` and `typedValueDiagnosticText(value)`.

Reject replies with invalid field structure, reserved-field pairs, row width, value types, nullability, row count, or page cursors. Also require valid engine identity, sufficient consistency, target evidence, and lakehouse checkpoint evidence. Do not use partially decoded success data.

The reserved provenance field IDs and names may appear only as exact top-level result-field pairs. User schemas and nested structs cannot claim them.

## Paging and execution control

The first request may use an offset for initial positioning. Continuation uses only the opaque `next_cursor` returned by the server. `has_more` is true exactly when `next_cursor` is present. Cursors are bounded, nonempty, and cannot contain control characters.

`AGDX_QUERY_PAGE_CODE`, `AGDX_QUERY_STATUS_CODE`, and `AGDX_QUERY_CANCEL_CODE` all use query version 1 envelopes. Cursor paging, execution status, and cancellation have separate negotiated capability flags and must be rejected locally when unavailable. Terminal execution states require a finish time. Only a failed state carries an error.

Bounded row iteration follows server cursors. Aggregate and vector requests use one page. Exact totals are optional because they can require a full count. Use Arrow IPC for bulk analytical data rather than increasing the 1000-row page limit.

## Result evidence

Every result page carries `QueryContext` with execution identity, engine name and version, resolved target, requested and delivered consistency, resource counters, and row count.

Operational context proves the index, backend resource generation, and runtime configuration revision. It cannot carry lakehouse checkpoint evidence.

Lakehouse context proves destination generation, backend generation, table UUID, namespace and table, snapshot, schema ID, partition spec ID, a 32-byte materialization boundary, checkpoint revision, and global state revision. Missing evidence invalidates the reply.

Consistency is fail-not-downgrade. The delivered level must be at least the requested level. A deployment that cannot meet the request returns a typed unsupported or stale error.

## Destinations and checkpoints

`laser.destinations()` provides destination and checkpoint operations. Clients can register a destination, change desired state with revision guards, and register or remove query routes. Reads use bounded pages and explicit checkpoint consistency.

`CHECKPOINT_OP_VERSION` is negotiated independently. Every Rust and TypeScript destination command must compare it with `OpVersions.checkpoint` before sending.

Public checkpoint requests can describe lease, progress, completion, and repair intent. They cannot supply authenticated actors, commit timestamps, source cuts, absolute lease deadlines, or certified repair evidence. The server adds those fields to a separate committed type. Reference tests keep the representations distinct.

## Arrow IPC publishing

`PublishRequest::arrow_ipc`, Python `PublishRequest.arrow_ipc`, and TypeScript `PublishRequest.arrowIpc` publish one complete self-contained Arrow IPC stream per Apache Iggy message. Batch builders have matching add methods.

Metadata carries contract version, 32-byte logical schema fingerprint, encoded byte count, field count, record-batch count, row count, and dictionary count. SDKs validate metadata and exact payload length before I/O. Managed ingestion must parse the stream and enforce the full policy.

The accepted policy requires stream format, self-containment, microsecond timestamps, decimals no wider than 128 bits, stable dictionaries, and no unions or extension types. Limits are defined once in `wire/src/arrow.rs` and ported exactly to TypeScript.

## Compatibility and verification

Wire types use named fields. The hello-negotiated surface slots and fenced-lease KV family remain version 1. The fenced-lease family separately requires its feature gate. A breaking contract change replaces its consumers, fixtures, and implementations together rather than preserving an older shape or assigning a migration version.

An intentional wire change must update Rust fixtures, the TypeScript fixture manifest and codecs, Python bindings and stubs, HTTP JSON fixtures, API reports, robustness coverage, and `bdd/scenarios/data_stack.feature`. Run focused tests while implementing, then the complete repository gate from `AGENTS.md` before release.

The data-stack BDD scenarios compare Rust, Python, and TypeScript. They cover schemas, targets, typed values, destinations, checkpoints, Arrow metadata, paging, execution status, and cancellation.
