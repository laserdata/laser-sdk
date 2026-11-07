---
name: query
description: The managed query, logical schema, and Arrow IPC surfaces across Rust, Python, and TypeScript. Use for typed results, operational targets, projection control, or offset paging.
---

# Query and data surfaces

Load [laser-sdk-overview](../laser-sdk-overview/SKILL.md) first. The normative wire contract is [docs/agdx.md](../../../docs/agdx.md). Repository rules and the verification order are in [AGENTS.md](../../../AGENTS.md).

## Ownership boundary

Laser SDK owns client builders, validation, codecs, capability gates, command transport, HTTP routes and views, and cross-language fixtures. Query execution, projection materialization, and backend adapters run in managed deployments. Do not add an executor or storage backend to `sdk/`.

The binary path is a managed Apache Iggy command. It does not use a request topic. Rust sends through `send_raw_with_response`, TypeScript through `ManagedTransport`, and Python binds the Rust client. The optional `laser-wire` HTTP client uses the same contract through `/agdx/*` routes over an injected transport.

## Source map

- `wire/src/query.rs` owns the query contract, offset paging, typed results, result context, and query errors.
- `wire/src/schema.rs` owns logical types, schemas, fingerprints, tagged values, provenance field reservations, and value-to-schema validation.
- `wire/src/destination.rs` owns the backend binding a projection writes to.
- `wire/src/arrow.rs` owns Arrow IPC metadata, the frozen acceptance policy, and rejection codes.
- `wire/src/http.rs` and `wire/src/http_client.rs` own JSON views, routes, and the runtime-agnostic HTTP client.
- `sdk/src/query/` owns the Rust query builder and command client.
- `foreign/python/src/query.rs` binds the same Rust surface.
- `foreign/typescript/src/wire/{query,schema,destination,arrow}.ts` implement the contract natively.
- `foreign/typescript/src/managed/query.ts` and `src/client/laser.ts` own the TypeScript public builder and capability gates.

## Query contract

Every query carries a nonzero `execution_id`, an absolute nonzero `deadline_micros`, an explicit target, a bounded page request, and a requested consistency. `QUERY_OP_VERSION` is `1`.

The target is `operational { index }`, the current materialized operational view, which may use a fork. Rust, Python, and TypeScript all use `laser.query(index)`.

Python builds predicate trees with `Filter` (`pred`, `all`, `any`, `negate`) and passes them to `filter` and `having`. The aggregate alias is `agg_as` in Rust and Python and `aggAs` in TypeScript. Rust `deadline(Duration)` is relative and `deadline_at_micros` absolute, Python `deadline(deadline_ms)` is relative and `deadline_at_micros` absolute, and TypeScript `deadline(deadlineMs)` is relative and `deadlineAtMicros` absolute. `window(field, every)` takes a `Duration` in Rust and milliseconds in Python (`every_ms`) and TypeScript (`everyMs`). The structured DSL includes exact matches, recursive filters, message type, a time range with both bounds inclusive, lexical and vector search, ordering, selection, aggregation, having, distinct, and typed raw SQL parameters. Raw SQL has an explicit dialect and cannot be mixed with the structured expression. Validation caps names, fields, predicates, parameters, SQL bytes, vector dimensions, page size, page offset, and recursive depth before transport I/O.

## Typed results

`QueryResult.fields` is the ordered logical result schema. The server builds it from the query and the projections' declared fields, never from a page's rows (`docs/agdx.md` A11.4): declared types map `text`, `int`, `float`, `bool` to string, long, double, boolean, an untyped field, an undeclared selected field and every raw SQL column are `LogicalType::Variant`, and a stored value that does not fit is null. Each `Row.values` array is positionally aligned with it. Query rows do not have header maps. Every value is a tagged `TypedValue`, including null, numeric widths, decimal, date and time, UUID, bytes, struct, list, and map.

Walks (`rows()`, `fetch_all()`) yield `QueryRow`, a row bound to its page's fields with the same name accessors, in all three clients. `fetch_all()` returns `QueryRowSet { rows, frontier_moved }`, and `QueryRows` and `TypedQueryRows` expose `frontier_moved`: a later page's `QueryContext.frontier` differs from the first page's. TypeScript filter, `whereEq` and raw SQL values also take native values (`QueryValue`), inferred like Python.

Use the result accessor by logical field name rather than manually searching the schema:

- Rust: `result.value(row, "latency_ms")`, `value_text`, `value_u64`, `value_i64`, and `field_index`.
- Python: `result.value(row, "latency_ms")`, `value_text`, `value_u64`, `value_i64`, and `field_index`. Paging fields live on `result.page`.
- TypeScript: `queryResultValue(result, row, "latency_ms")`, `queryResultValueText`, `queryResultValueU64`, `queryResultValueI64`, `queryResultFieldIndex`, and `typedValueDiagnosticText(value)`.

Reject replies with invalid field structure, reserved-field pairs, row width, value types, nullability, or row count. Also require valid engine identity, sufficient consistency, and target evidence. Do not use partially decoded success data.

The reserved provenance field IDs and names may appear only as exact top-level result-field pairs. User schemas and nested structs cannot claim them.

## Paging

Queries page by offset and limit only. `offset(n)` skips the first `n` matching rows, up to `MAX_QUERY_OFFSET` (1,000,000), and the limit is `1..=1000`. `has_more` says whether rows follow the page. Bounded row iteration repeats the query with the offset moved past the rows it read and stops on an empty page or `has_more = false`. Aggregate and vector requests use one page. Exact totals are optional because they can require a full count. Use Arrow IPC for bulk analytical data rather than increasing the 1000-row page limit.

## Result evidence

Every result page carries `QueryContext` with execution identity, engine name and version, resolved target, requested and delivered consistency, resource counters, and row count.

Operational context proves the index, backend resource generation, and runtime configuration revision.

Consistency is fail-not-downgrade. The delivered level must be at least the requested level. A deployment that cannot meet the request returns a typed unsupported or stale error.

## Arrow IPC publishing

`PublishRequest::arrow_ipc`, Python `PublishRequest.arrow_ipc`, and TypeScript `PublishRequest.arrowIpc` publish one complete self-contained Arrow IPC stream per Apache Iggy message. Batch builders have matching add methods.

Metadata carries contract version, 32-byte logical schema fingerprint, encoded byte count, field count, record-batch count, row count, and dictionary count. SDKs validate metadata and exact payload length before I/O. Managed ingestion must parse the stream and enforce the full policy.

The accepted policy requires stream format, self-containment, microsecond timestamps, decimals no wider than 128 bits, stable dictionaries, and no unions or extension types. Limits are defined once in `wire/src/arrow.rs` and ported exactly to TypeScript.

## Compatibility and verification

Wire types use named fields. The hello-negotiated surface slots and fenced-lease KV family remain version 1. The fenced-lease family separately requires its feature gate. A breaking contract change replaces its consumers, fixtures, and implementations together rather than preserving an older shape or assigning a migration version.

An intentional wire change must update Rust fixtures, the TypeScript fixture manifest and codecs, Python bindings and stubs, HTTP JSON fixtures, API reports, robustness coverage, and the shared `bdd/scenarios/query.feature`. Run focused tests while implementing, then the complete repository gate from `AGENTS.md` before release.

A zero row ceiling performs no query. Every page rejects a reply for another execution ID. Version skew keeps its typed error classification. See [client behavior](../../../docs/client-behavior.md).

Materialization setup lives in `sdk/src/materialize.rs`, `foreign/python/src/materialize.rs`, and `foreign/typescript/src/managed/materialize.ts`. The shared scenarios are `bdd/scenarios/materialization.feature` and `bdd/scenarios/operational_sql.feature`. Keep append and keyed history, rejection policies, captured-target readiness, and explicit raw SQL dialects aligned in every client. Read `docs/client-behavior.md` from the repository root for the public contract.
