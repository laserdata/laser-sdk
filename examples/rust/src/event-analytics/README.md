# event-analytics - one clickstream, every read model

This example publishes clickstream events and reads them through a live consumer and a replay cursor. Managed deployments also project the events for analytics and schema checks.

## What it does

1. Read the log through a consumer group while the producer sends batches. `CommitPolicy::Polling` enables automatic commits during polling. A lost response can require recovery from explicit offsets. A timeout reports a stalled read.
2. On a managed deployment, query the projected `clickstream` table. The queries use `message_type`, `ts`, and `time_range` for counts and time windows.
3. Export records through a separate `Cursor` and save offsets in a `StateStore`. Recreate the reader from the saved offsets.
4. Register a JSON Schema through `laser.schemas().register(source).send()`. Attach its ID with `.schema_id(id)`. A matching record enters the index. A mismatched record increments `schema_decode_failures.mismatch` and follows the configured dead-letter policy.

Projection pointers extract typed columns from each JSON body. The example does not duplicate those values in `agdx.idx.*` headers. Records include `message_type` and `ts` for the reserved query fields. A managed deployment supplies projection and analytics. Apache Iggy can run the live consumer and resumable export without those phases.

## Run it

Run from `examples/rust`:

```sh
# local server: live stream plus resumable export
just up && cargo run --release --example event-analytics

# against Laser Stack or LaserData Cloud (enables the schema coda)
LASER_CONNECTION_STRING=user:pwd@your-laserdata-cloud-host \
  cargo run --release --example event-analytics

# soak: millions of events
LASER_MESSAGES=2000000 LASER_BATCH=1000 cargo run --release --example event-analytics
```

## Where to look (LaserData Cloud)

- Query: indexes `clickstream` (the main tape) and `clickstream_guarded` (the schema-guarded one, exactly one row).
- Writer schemas: the JSON Schema guard the run registered, with the LaserData-Cloud-allocated id.
- Messages: the raw events with their compact `agdx.*` headers.

## Highlights

- `topic.consumer_group(group)` with `CommitPolicy::Polling` for server-side commit-on-poll delivery.
- `laser.topic(topic).publish_batch()` chunked indexed publishing (each chunk one `send_messages` call, spread across partitions by the balanced partitioner).
- `query(..)` aggregates: `count` / `group_by` / `time_range` windows over the `message_type` and `ts` convention fields.
- `Cursor` + `StateStore` checkpointing for resumable downstream jobs.
- The binding enables `notify`, and `laser.watch()` reports view progress. The wait helper recounts after a notification. Without a feed, it uses bounded query polling.
- Writer schemas: synchronous register returning the allocated id, JSON Schema validation guarding an index.
