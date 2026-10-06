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
- `wire/src/filter/` defines policies, codecs, requests, replies, revisions and durable operation results. `read.rs` includes automatic group execution, scan ceilings, source history and policy generations.
- `foreign/python/src/consumer_group.rs`, `filters.rs` and `transport.rs` bind the same behavior through Rust. Regenerate the Python stubs.
- `foreign/typescript/src/stream/consumer-group.ts`, `consumer.ts`, `managed/filters.ts` and the wire modules implement the native TypeScript peer. Regenerate both API reports.

## Invariants

A normal group consumer executes its group's policy. The advanced group reader does too. An unbound group returns all records without payload evaluation. Explicitly selected saved revisions stay strict. A failed catalog or capability lookup never becomes an unfiltered success.

Normal batch length limits examined source records per partition request. Advanced `count` limits matches, and `max_examined` is independent. Empty scan results keep progress and distinguish remaining work from end of visible data.

Acknowledge only completed contiguous work under the delivered group/source identity, mode and policy generation. Do not commit a record before it is delivered unless an explicitly documented pre-delivery mode requires that behavior. Cancellation must retain recovered deliveries. Do not treat offset zero as an unset checkpoint.

A reused name or offset is not proof of the same history. Catalog freshness uses primary history verification and durable configuration receipts. Group bindings remain immutable per used incarnation. New draft revisions do not activate themselves. Use separate groups for A/B policies.

Rust defines behavior. Python and TypeScript must implement equivalent limits, errors, setup outcomes, commit timing and lifecycle. Public changes update exports, stubs, API reports and tests together, then regenerate `docs/parity.md` with `python3 scripts/check-parity.py --write` and run `just parity-check`. Keep operation versions at 1.

## Validation and documentation

Use the shared evaluator/codec corpus for verdict parity. Add wire fixtures for intentional shape changes and shared BDD cases for normal and advanced group reads. Verify unbound groups, sparse/empty batches, setup retries, cancellation, multiple members, restart, pause, policy changes and native Iggy compatibility. Cluster claims require actual cluster evidence.

Update root/crate/language READMEs, API documentation, `docs/agdx.md` A14, the tutorial, contributor guidance, focused skills, all three CDC examples and the public Consumer Filters guide. Never describe a proposed endpoint or a skipped test as shipped or passing.
