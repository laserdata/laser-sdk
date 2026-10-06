# fleet-tape - live fleet view + reading-tape analytics

This example sends host CPU readings to a live fleet-view reader and a reading-history topic. Managed deployments also support analytics and schema-based publication.

## What it does

- Generate deterministic readings in timed bursts. Publish them to the live feed and queryable tape.
- Run a Laser producer with a live consumer-group reader. The reader reports last CPU, rolling sample-weighted mean CPU, cumulative samples, and degraded readings per host.
- Project the tape on a managed deployment, then query samples and the sample-weighted mean CPU. Extract typed columns from JSON bodies. Retain `message_type` and `ts` for query helpers without duplicating columns in `agdx.idx.*` headers.
- Read the tape with `laser.topic(topic).json::<Reading>()` and `records(reader_name)`. Make sure that recomputed weighted CPU totals match the session totals. Decode errors include the record position.
- On a managed deployment, register `HostReading` through `laser.schemas().register(SchemaSource::Avro { .. }).send()`. Compile it with `schema-codecs`, then publish through `.add_avro(&compiled, id, &reading)`. Records carry `agdx.sid` for schema selection. Make sure that Avro weighted CPU totals match the JSON tape. An open server skips this phase.

## Run it

Run from `examples/rust`:

```sh
# a local server: live fleet view plus typed tape replay
just up && cargo run --example fleet-tape

# a LaserData Cloud deployment (enables the Avro schema-first tape)
LASER_CONNECTION_STRING=user:pwd@your-laserdata-cloud-host cargo run --example fleet-tape
```

## Where to look (LaserData Cloud)

- Query: the reading-tape index `readings_<token>` (the run token keeps a rerun, or another language's example on the same deployment, out of this run's rows), queried for per-host samples and mean CPU.
- Writer schemas: the `fleet_reading` Avro schema the run registered, with its LaserData-Cloud-allocated id.

## Highlights

- `topic.producer()` tuned for a feed (balanced routing, bounded retries) and `topic.consumer_group(group)` with a tight `poll_interval` for low reading-to-view latency.
- The feed runs concurrently with the view reader, so the view updates in real time as readings arrive rather than after a batch lands.
- `laser.topic(topic).publish_batch()` feeds the tape in batches of indexed records with inline bodies, each batch one `send_messages` call, spread across partitions by the balanced partitioner.
- `query(topic).sum(field).group_by([..])`, with the mean CPU derived from two grouped sums (weighted CPU total over samples).
- The typed handle: `topic.json::<Reading>()` then `records("fleet-tape-audit")`, the typed rung of the replay ladder, offsets caller-owned like the raw `Cursor`.
- CPU values and weighted totals are integers end to end, so index ordering and aggregation stay exact.
- Writer schemas: synchronous Avro register returning the allocated id, with client-side validation before publish.
