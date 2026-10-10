---
name: consumer-filters
description: Consumer-group-owned server filtering, normal and advanced group readers, safe progress, revisions and previews across Rust, Python and TypeScript. Use for Topic and ConsumerGroup filter APIs, wire/filter and the CDC examples.
---

# Consumer-group filters

Load the SDK overview first. The public hierarchy is `Laser → stream → topic → producer` and `Laser → stream → topic → consumer group → filter`. There is no public top-level filter service. Pure filter values remain reusable types.

## Implementation map

- `sdk/src/stream/consumer_group.rs` owns group handles, optional setup policies, creation outcomes and group-scoped administration.
- `sdk/src/stream/transport.rs` owns normal streaming, group-aware delivery, commit policies, cancellation and shutdown.
- `sdk/src/filters/` holds internal catalog clients, routing, membership, advanced readers, bounded outstanding pages, progress and optional local guards.
- `sdk/src/filters/mod.rs` re-exports the policy and evaluator types. The saved-filter catalog frames (`FilterRef`, `FilterMutation`, `FilterPage`, `FilterSummary`, `FilteredPage`, `RecordFault`, and the rest) and `ExactDecimal` are reachable only as `laser_sdk::wire::filter`, and TypeScript keeps them under `wire` in `@laserdata/laser-sdk/full`. `FilterAnnounce` lives in `laser_sdk::capabilities`.
- `wire/src/filter/` defines policies, codecs, requests, replies, revisions and durable operation results. `read.rs` includes automatic group execution, scan ceilings, source history and policy generations.
- `foreign/python/src/consumer_group.rs`, `filters.rs` and `transport.rs` bind the same behavior through Rust. Regenerate the Python stubs.
- `foreign/typescript/src/stream/consumer-group.ts`, `consumer.ts`, `managed/filters.ts` and the wire modules implement the native TypeScript peer. Regenerate both API reports.

## Invariants

A normal group consumer executes its group's policy. The advanced group reader does too. An unbound group returns all records without payload evaluation. Explicitly selected saved revisions stay strict. The agent runtime binds each agent's group (one per agent id) to the headers-only addressee filter `agdx.to In [<agent>, "*"]` on `agent.sessions` and `agent.control` when the server resolves group policies and serves native filters and the catalog, and the `Laser` was built from a connection string (`DeliveryEngine` in `sdk/src/agent/consumer.rs`, `resolveEngine` and `bindAddressee` in `foreign/typescript/src/agent/reliable-consumer.ts`). Otherwise the group stays unbound and the runtime classifies records on the client. A failed catalog or capability lookup never becomes an unfiltered success.

Normal batch length limits examined source records per partition request. Advanced `count` limits matches, and `max_examined` is independent. Empty scan results keep progress and distinguish remaining work from end of visible data.

Acknowledge only completed contiguous work under the delivered group/source identity, mode and policy generation. Do not commit a record before it is delivered unless an explicitly documented pre-delivery mode requires that behavior. Cancellation must retain recovered deliveries. Do not treat offset zero as an unset checkpoint.

A reader in `ReadMode::Primary` asks the coordinator for the partition route and, once, for the cluster topology (a single node is dialed through the caller's own address). Both are idempotent reads that Iggy does not retry itself, so `Routes::connection` (Rust `sdk/src/filters/route.rs`, TypeScript `Routes.connection` in `src/managed/filters.ts`) retries a transient refusal with the connection's publish retry count and backoff (`PublishOptions::retry_transient_read`, TypeScript `retryTransientRead`). A failed topology probe is never cached.

A reused name or offset is not proof of the same history. Catalog freshness uses primary history verification and durable configuration receipts. Group bindings remain immutable per used incarnation. New draft revisions do not activate themselves. Use separate groups for A/B policies.

Rust defines behavior. Python and TypeScript must implement equivalent limits, errors, setup outcomes, commit timing and lifecycle. Public changes update exports, stubs, API reports and tests together, then regenerate `docs/parity.md` with `python3 scripts/check-parity.py --write` and run `just parity-check`. Keep operation versions at 1.

## Validation and documentation

TypeScript runs regex predicates with `foreign/typescript/src/wire/regex.ts`, a port of the regex-syntax 0.8 grammar run as a Thompson NFA over code points. Each character class compiles to a one-character V8 regex with the `v` flag (`iv` when case-insensitive, which matches Rust's simple case folding), so V8 never backtracks over input. `src/wire/regex-unicode.ts` copies the regex-syntax property alias tables so loose spellings such as `\p{greek}` resolve as in Rust. Known differences: Unicode data comes from Node's ICU (Unicode 16.0 on Node 22.14, the same version as regex-syntax 0.8.11), `\p{ASCII}` is spelled as an explicit range because V8 omits the long s and the Kelvin sign from its case-folded form, `Age` and the segmentation break properties and the binary properties V8 lacks (`Other_*`, `Hyphen`, `InCB`, and a few others) are refused, and the 256 KiB size limit is checked as a lower-bound cost estimate, so TypeScript never refuses a pattern Rust accepts but may accept one Rust refuses.

Use the shared evaluator/codec corpus for verdict parity. Add wire fixtures for intentional shape changes and shared BDD cases for normal and advanced group reads. Verify unbound groups, sparse/empty batches, setup retries, cancellation, multiple members, restart, pause, policy changes and native Iggy compatibility. Cluster claims require actual cluster evidence.

Update root/crate/language READMEs, API documentation, `docs/agdx.md` A14, the tutorial, contributor guidance, focused skills, all three CDC examples and the public Consumer Filters guide. Never describe a proposed endpoint or a skipped test as shipped or passing.
