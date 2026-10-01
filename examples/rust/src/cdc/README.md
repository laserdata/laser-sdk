# cdc - the Consumer filters primitive

**Read a small subset of a shared topic without downloading the rest.** This example reads a change feed through a consumer filter. The server selects the matching records, so the reader receives only those records, with their original offsets, and never downloads the rest.

A satellite fleet streams the change feed of its mission-ops database into `fleet_changes`: battery readings, orbit maneuvers, and ground-station status flips. The anomaly desk wants the satellites that enter safe mode or leave the fleet, four records out of 240. The server evaluates the filter next to the data. **Only 424 of 27,953 payload bytes reach the reader: 98.5% less payload transfer.** These are the example feed's measured totals.

This example requires a server that serves consumer filters, as the LaserData Iggy fork in Laser Stack and LaserData Cloud does. Saved filters and consumer group bindings also require `laser-plane`. Without filters, the example explains the requirement and exits successfully. Without `laser-plane`, it skips the catalog phases.

## What it shows

- Model the feed as typed records: `FleetChange` is either a `RowChange`, tagged by its `table`, or a `TelemetryEvent`, all `#[derive(Serialize, Deserialize)]`. Publish 240 of them keyed by satellite with `topic.publish().partition_key(change.key()).json(change)?.send()`.
- Read with an inline filter that needs no catalog: `laser.filters().reader(&stream, TOPIC).consumer(..).inline(filter).start(FilteredStart::First).build()`. Only the four safe-mode or decommission events arrive. Decode each one back into its type with `record.json::<FleetChange>()`, read with `reader.next_record()`, acknowledge after processing with `reader.ack(&record)`, and print how many records and bytes stayed on the broker.
- Test the strict filter and the values-only filter against a battery update of a satellite already in safe mode with `filters.test(..)`. The strict filter rejects it because the mode did not change. The values-only filter selects it.
- Preview every partition with `filters.preview(..).max_records(10).send()`. A preview reports what it examined and matched, stores no offset, and joins no group.
- Route binary alert frames on their own `fleet_alerts` topic with `ConsumerFilter::headers_only(..)` on a custom `priority` header stored as a one-byte `uint8`. Numeric `2` selects critical alerts. The server never decodes the payload, so it can be in any format.
- With `laser-plane`: `register` both filters, create and bind the `anomaly-desk` group with `create_consumer_group`, then read with `.group_id(binding.identity.group_id)`. A delete of a bound filter fails with `Conflict`. Then release the exact binding with `unbind_binding(binding)`, and `archive` and `delete` both filters, also when a step failed.

**Filter binary payload fields too.** The codec phase publishes typed fleet readings in CBOR, Avro, and Protobuf. Each reader selects one of three records, decodes the original bytes, and acknowledges after processing. Avro and Protobuf register writer schemas through plane and stamp each record with its schema ID. Shared schema files are in `examples/shared/`. CBOR runs without plane.

The record API buffers bounded poll results internally. Use `next_page()` when your handler works on batches. Both use the optional filtered-poll command over standard Iggy transport. A saved group binding does not change ordinary consumer polling.

**Create the filtered group once and consume by its numeric ID.** The managed phase also creates a second revision for an A/B variant in a separate group. It pauses that revision, acknowledges already-delivered work while paused, and resumes. The two groups independently process four broad events and two mode transitions. One group cannot mix policies because its members share offsets.

## Run it

Run from `examples/rust`:

```sh
LASER_CONNECTION_STRING=user:pwd@your-laserdata-cloud-host \
  cargo run --example cdc
```

## Learn more

- Docs: https://docs.laserdata.cloud/laser-sdk/consumer-filters
- Related primitive: [`watch`](../watch/README.md), the change feed that reports view progress instead of filtering the log.
