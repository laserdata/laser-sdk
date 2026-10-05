# cdc - consumer-group filters

This example publishes a satellite change feed and gives each consumer group its own server-side policy. Consumers use the group name or ID without repeating the filter definition.

**The anomaly desk receives 4 of 240 records and 424 of 27,953 payload bytes, saving 98.5% of payload transfer.** The feed includes battery readings, maneuvers and status changes. The policy selects safe-mode and decommission events.

## What it does

- Publishes the complete typed feed to `fleet_changes` in one batch per satellite or station key and prints the time of each batch.
- Creates the anomaly desk group with its policy, then reads through a normal group consumer and commits after processing.
- Reads the same matches through the advanced group reader with separate match and scan limits.
- Tests change evidence against a battery update and previews stored records without changing consumer progress.
- Routes binary alerts through a one-byte numeric `priority` header without decoding their payloads.
- Publishes and filters typed records in CBOR, Avro and Protobuf. Schema-first formats use registered writer schemas.
- Uses separate groups for A/B policies and demonstrates draft revisions, pause, acknowledgment while paused, and resume.

## Run it

Run from `examples/rust` against Laser Stack or LaserData Cloud:

```sh
LASER_CONNECTION_STRING=user:pwd@your-laserdata-cloud-host \
  cargo run --example cdc
```

The example requires managed group configuration and group-policy reads. Unsupported deployments print the requirement and skip the showcase.

## Highlights

- **Configure once, consume by group ID.** The group owns its policy and multiple members share native partition assignments.
- **An unbound group receives all records.** Catalog failures and paused policies never become an unfiltered read.
- Normal batch length bounds examined source records per partition request. Advanced match and scan limits remain separate.
- Typed headers, original offsets and payload bytes survive filtering. Acknowledgments include skipped records through completed work.

## Learn more

- [Consumer Filters guide](https://docs.laserdata.cloud/laser-sdk/consumer-filters)
- Related example: [`watch`](../watch/README.md), the change feed that reports view progress.
