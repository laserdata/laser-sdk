---
name: wire-contract
description: The laser-wire crate, its command codes, typed CBOR and JSON contracts, validation, HTTP routes and client, and golden fixture corpus. Use for any change under wire or a native TypeScript wire port.
---

# laser-wire contract

Load [laser-sdk-overview](../laser-sdk-overview/SKILL.md) first. [docs/agdx.md](../../../docs/agdx.md) is the normative prose contract. [wire/README.md](../../../wire/README.md) is the public module reference.

## Contract boundary

`laser-wire` owns data and pure validation. It has no I/O, clock, randomness, Apache Iggy client, or async runtime. The optional `http-client` feature adds an async client over a caller-injected runtime-agnostic transport and still supports `wasm32-unknown-unknown`.

Rust is the executable source of wire truth. Python binds it. TypeScript implements it natively under `foreign/typescript/src/wire` and proves parity against the Rust fixture corpus.

The contract includes binary managed commands, JSON HTTP views, headers, topics, command codes, version negotiation, structured backend capabilities, result codes, and limits. Application behavior and backend execution do not belong in this crate.

## Module map

- `codes` owns permanent command numbers, code classification, and independent operation versions.
- `hello` owns `OpVersions`, structured backend descriptors and capability sets, readiness, and topology announcement.
- `schema` owns logical schemas, SHA-256 fingerprints, tagged values, provenance field reservations, and recursive validation.
- `source` owns stable source scope and source-incarnation identity.
- `destination` owns destination declarations, physical table and backend bindings, start policies, and query routes.
- `checkpoint` owns revision-guarded public mutations, separately typed replicated transitions, reads, lifecycle, progress, snapshots, and repair evidence.
- `arrow` owns Arrow IPC metadata, the frozen policy, limits, and rejection codes.
- `query` owns query targets, DSL, typed parameters, paging, execution status and cancellation, positional typed results, execution evidence, and errors.
- `control` and `browse` own projection, binding, writer-schema, and registry contracts.
- `kv`, `fork`, `graph`, `batch`, `runs`, and `authz` own their managed surfaces.
- `agent`, `change`, `keys`, `mutation`, and `forward` own AGDX envelopes, change notifications, managed key records, mutation identity, and forwarded commands.
- `headers`, `topics`, `content`, `limits`, and `result` own permanent dictionaries and shared result classification.
- `http` owns `/agdx/*` routes, parameters, JSON views, and `ErrorBody`.
- `http_client` owns the feature-gated typed client over an injected `Transport`.
- `framing`, `encoding`, and `codecs` own CBOR framing, byte-string helpers, and payload codec traits.
- `fixtures` embeds the golden corpus.

## Hard invariants

Bytes are the binary contract. Field names, serde attributes, enum representation, byte-string encoding, integer width, and command numbers cannot drift accidentally. Intentional pre-1.0 breaking changes update every consumer, fixture, binding, and implementation in the same stage while operation versions remain 1.

All payload byte fields use the shared CBOR byte-string adapters. A bare `Vec<u8>` serialized as an integer array is a contract bug.

Decode of untrusted bytes must return a typed error and never panic. The crate forbids unsafe code. Every new envelope belongs in deterministic robustness tests and the fuzz decoder.

Logical result rows are positional. `QueryResult.fields` is the schema and every `Row.values` array must have the same width and validate against its field type and nullability.

Provenance result fields are reserved exact ID and name pairs. They are allowed only at the top level of query result schemas. User schemas and nested structs cannot claim them.

Public destination mutations and replicated checkpoint transitions remain different types and decoders. A public request must never be decoded as a worker lease, progress transition, snapshot commit, or repair transition.

Arrow IPC metadata validation in an SDK is the local preflight. Managed ingestion must also parse the stream and enforce stream format, self-containment, schema fingerprint, stable dictionaries, microsecond timestamps, decimal width, unsupported type rejection, and all caps.

Consistency is fail-not-downgrade. Query reply validation rejects a delivered consistency weaker than requested and rejects missing target evidence.

## Versioning

Named fields allow additive optional growth when readers can safely ignore it. Changed meaning, removed fields, or new executor-dispatched grammar is breaking and requires a coordinated package release across every consumer. The hello-negotiated surface slots remain version 1. The fenced-lease request family is the explicit payload-version exception below.

Query, control, KV, fork, graph, checkpoint, agent, and other surfaces negotiate independently through `OpVersions`. A client checks the version associated with the command it is about to send. Destination and checkpoint commands use `versions.checkpoint`, not the query version. The fenced-lease KV family (`KvLease`, `KvLeaseRenew`, `KvRelease`, `KvCasFenced`) additionally rides its own payload version `KV_LEASE_OP_VERSION = 2` and must only be sent to a server advertising the `KV_FENCED_LEASES` feature bit. A pre-fencing server would decode the reshaped payloads under the old contract instead of rejecting them.

Permanent u8 dictionaries use unknown-code passthrough where relay compatibility matters. Executor vocabularies such as comparison and aggregate functions remain exhaustive so every backend must handle a new variant explicitly.

## Fixture workflow

Every new public contract type and enum discriminant needs a Rust round-trip assertion. Cross-language public shapes also need TypeScript decode and byte-identical re-encode coverage. JSON HTTP views need canonical JSON fixtures.

Regenerate with `just fixtures-regen` only for an intentional wire change. Review the byte diff, update `wire/src/fixtures.rs`, `wire/tests/wire_fixtures.rs`, the TypeScript fixture manifest and tests, robustness decoders, AGDX docs, Python bindings, TypeScript codecs, and API reports.

The fixture manifest is closed. Added or removed files fail TypeScript tests until explicitly classified and consumed.

## New command checklist

1. Allocate the code in `codes.rs` without renumbering an existing command.
2. Add request, reply, validation, and typed error shapes in the owning module.
3. Register the command pairing in `commands.rs`.
4. Add binary fixtures, constants assertions, robustness decode coverage, and TypeScript parity.
5. Add HTTP routes and views only when that surface is served over HTTP.
6. Update AGDX prose and the relevant language SDK accessors.
7. Run native, wasm, deny, fuzz, fixture, BDD, and package verification gates.

## Review focus

Reject silent defaulting that broadens a query, accepts malformed success data, downgrades consistency, confuses public and replicated state, trusts caller-declared Arrow metadata without server parsing, or makes TypeScript use lossy `number` for wire u64 or u128 values.
