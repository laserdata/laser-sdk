# cdc - read only the change-feed records you care about

**Read a small subset of a shared topic without downloading the rest.** This example reads a change feed through a consumer filter. The server selects the matching records, so the reader receives only those records, with their original offsets, and never downloads the rest.

A satellite fleet streams the change feed of its mission-ops database into `fleet_changes`: battery readings, orbit maneuvers, and ground-station status flips. The anomaly desk wants the satellites that enter safe mode or leave the fleet, four records out of 240. The server evaluates the filter next to the data. **Only 424 of 27,953 payload bytes reach the reader: 98.5% less payload transfer.** These are the example feed's measured totals.

## What it shows

- Models the feed as typed records: `FleetChange` is a `RowChange` union discriminated by `table` and `op`, or a `TelemetryEvent`. Publishes 240 of them keyed by satellite with `topic.publish().partitionKey(key).json(change).send()`.
- Reads with an inline filter that needs no catalog: `laser.filters().reader(stream, TOPIC).consumer(..).inline(filter).start({ kind: "first" }).build()`. Only the four safe-mode or decommission events arrive. Each one is parsed back into a `FleetChange`, `reader.nextRecord()` reads one record at a time, each record is acknowledged after processing with `reader.ack(record)`, and the example prints how many records and bytes stayed on the broker.
- Tests the strict filter and the values-only filter against a battery update of a satellite already in safe mode with `filters.test(..)`. The strict filter rejects it because the mode did not change. The values-only filter selects it.
- Previews every partition with `filters.preview(stream, TOPIC, partition, filter, { maxRecords: 10 })`. A preview reports what it examined and matched, stores no offset, and joins no group.
- Routes binary alert frames on their own `fleet_alerts` topic with `ConsumerFilter.headersOnly(..)` on a custom `priority` header stored as a one-byte `uint8`. Numeric `2` selects critical alerts. The server never decodes the payload, so it can be in any format.
- With `laser-plane`: registers both filters, creates and binds the `anomaly-desk` group with `createConsumerGroup`, then reads with `.groupId(binding.identity.groupId)`. A delete of a bound filter fails with a conflict. Then it releases the exact binding with `unbindBinding(binding)`, and archives and deletes both filters, also when a step failed. Readers close in `finally` blocks.

**Filter binary payload fields too.** The codec phase publishes typed fleet readings in CBOR, Avro, and Protobuf. Each reader selects one of three records, decodes the original bytes, and acknowledges after processing. Avro and Protobuf register writer schemas through plane and stamp each record with its schema ID. Shared schema files are in `examples/shared/`. CBOR runs without plane.

The record API buffers bounded poll results internally. Use `nextPage()` when your handler works on batches. Both use the optional filtered-poll command over standard Iggy transport. A saved group binding does not change ordinary consumer polling.

**Create the filtered group once and consume by its numeric ID.** The managed phase also creates a second revision for an A/B variant in a separate group. It pauses that revision, acknowledges already-delivered work while paused, and resumes. The two groups independently process four broad events and two mode transitions. One group cannot mix policies because its members share offsets.

## Run it

Run `npm run setup` once, then run from `examples/typescript`:

```sh
npm run example:cdc
```

This needs a server that serves consumer filters, as the LaserData Iggy fork in Laser Stack or LaserData Cloud does. Saved filters and group bindings also need `laser-plane`. Without filters, the example prints a skip notice and returns. Without `laser-plane`, it skips the catalog phases.

```sh
LASER_CONNECTION_STRING=user:pwd@your-host npm run example:cdc
```

## Learn more

- Docs: https://docs.laserdata.cloud/laser-sdk/consumer-filters
- Related primitive: [`watch`](../watch/README.md), the change feed that reports view progress instead of filtering the log.
