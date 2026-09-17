# order-book - live book + trade-tape analytics

This example sends market fills to a live order-book reader and a trade-history topic. Managed deployments also support analytics and schema-based publication.

## What it does

- Generate deterministic fills in timed bursts. Publish them to the live feed and queryable tape.
- Run a Laser producer with a live consumer-group reader. The reader reports last price, rolling volume-weighted average price, and cumulative volume per symbol.
- Project the tape on a managed deployment, then query volume and volume-weighted average price. Extract typed columns from JSON bodies. Retain `message_type` and `ts` for query helpers without duplicating columns in `agdx.idx.*` headers.
- Read the tape with `laser.topic(topic).json::<Trade>()` and `records(reader_name)`. Make sure that recomputed notionals match the session totals. Decode errors include the record position.
- On a managed deployment, register `Fill` through `laser.schemas().register(SchemaSource::Avro { .. }).send()`. Compile it with `schema-codecs`, then publish through `.add_avro(&compiled, id, &fill)`. Records carry `agdx.sid` for schema selection. Make sure that Avro notionals match the JSON tape. An open server skips this phase.

## Run it

Run from `examples/rust`:

```sh
# a local server: live book plus typed tape replay
just up && cargo run --example order-book

# a LaserData Cloud deployment (enables the Avro schema-first tape)
LASER_CONNECTION_STRING=user:pwd@your-laserdata-cloud-host cargo run --example order-book
```

## Where to look (LaserData Cloud)

- Query: the trade-tape index, queried for per-symbol volume and VWAP.
- Writer schemas: the `Fill` Avro schema the run registered, with its LaserData-Cloud-allocated id.

## Highlights

- `topic.producer()` tuned for a feed (balanced routing, bounded retries) and `topic.consumer_group(group)` with a tight `poll_interval` for low tick-to-book latency.
- The feed runs concurrently with the book reader, so the book updates in real time as fills arrive rather than after a batch lands.
- `laser.topic(topic).publish_batch()` feeds the tape in batches of indexed records with inline bodies, each batch one `send_messages` call, spread across partitions by the balanced partitioner.
- `query(topic).sum(field).group_by([..])`, with VWAP derived from two grouped sums (notional over quantity).
- The typed handle: `topic.json::<Trade>()` then `records("tape-audit")`, the typed rung of the replay ladder, offsets caller-owned like the raw `Cursor`.
- Prices and notionals are integer cents end to end, so index ordering and aggregation stay exact. The float is display-only.
- Writer schemas: synchronous Avro register returning the allocated id, with client-side validation before publish.
