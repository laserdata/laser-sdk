# event-analytics - one clickstream, every read model

The general-purpose example. One topic of clickstream events and every read model the platform offers layered over it, in one run. Scaled by the shared volume knobs (`LASER_MESSAGES`, `LASER_BATCH`), the same binary is a smoke test or a multi-million-event soak. Layer: generic. AGDX surfaces: streaming plus materialized views and the query DSL, with writer-schema validation on a managed deployment.

## What it does

1. **Hot path.** A Laser consumer-group reader tails the raw log live while the producer streams in chunks, folding a rolling ops ticker (events seen, checkouts). `CommitPolicy::Polling` stores offsets on the server before each poll, so the reader resumes safely after a restart. A timeout turns any stall into a diagnostic instead of a hang.
2. **Analytics.** LaserData Cloud materializes the indexed events into a queryable `clickstream` table and answers what a dashboard asks: the funnel by `message_type`, the slowest routes, per-window counts over the `ts` convention field with `time_range`.
3. **Resumable export.** An independent reader tails the same log with a `Cursor` and a `StateStore` checkpoint, then restarts and resumes exactly where it stopped instead of re-reading from zero.
4. **Validated ingest (LaserData Cloud only).** A registered JSON Schema (draft 2020-12) guards a second index (the binary schema-first path lives in the order-book example's Avro tape): `laser.schemas().register(source).send()` returns LaserData Cloud-allocated id, producers stamp it with `.schema_id(id)`, a well-formed event materializes and a malformed one (a string where an integer must be) never reaches the index. It shows up in LaserData Cloud's `/health` `schema_decode_failures.mismatch` counter and the DLQ when the policy says so.

Indexing is body-first: the projection's pointers extract every column out of the decoded JSON event, typed, so the index and the event body can never disagree and no `agdx.idx.*` headers duplicate the payload. Every record carries the `message_type` + `ts` convention fields so the reserved columns fill and the query sugar works. Point it at LaserData Cloud to register the projection and run analytics. On raw Apache Iggy, the live consumer and resumable export still run and the managed phase prints one skip pointer.

## Run it

```sh
# local server: live stream plus resumable export
just up && cargo run --release --example event-analytics

# against LaserData Cloud (enables the schema coda)
LASER_CONNECTION_STRING=user:pwd@your-laserdata-cloud-host \
  cargo run --release --example event-analytics

# soak: millions of events
LASER_MESSAGES=2000000 LASER_BATCH=1000 cargo run --release --example event-analytics
```

## Where to look (LaserData Cloud)

- **Query**: indexes `clickstream` (the main tape) and `clickstream_guarded` (the schema-guarded one, exactly one row).
- **Writer schemas**: the JSON Schema guard the run registered, with the LaserData-Cloud-allocated id.
- **Messages**: the raw events with their compact `agdx.*` headers.

## Highlights

- `topic.consumer_group(group)` with `CommitPolicy::Polling` for server-side commit-on-poll delivery.
- `laser.topic(topic).publish_batch()` chunked indexed publishing (each chunk one `send_messages` call, spread across partitions by the balanced partitioner).
- `query(..)` aggregates: `count` / `group_by` / `time_range` windows over the `message_type` and `ts` convention fields.
- `Cursor` + `StateStore` checkpointing for resumable downstream jobs.
- `laser.watch()` await-then-query: the binding opts into `notify`, and the projection wait re-counts only when the change feed reports the view advanced (falling back to the plain bounded poll where the feed is not published).
- Writer schemas: synchronous register returning the allocated id, JSON Schema validation guarding an index.
