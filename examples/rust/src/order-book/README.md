# order-book - live book + trade-tape analytics

A market-data workload with two readers over one stream, the shape a trading stack actually runs. Layer: generic. AGDX surfaces: streaming (the hot feed) and materialized views with the query DSL (the analytics tape), plus the writer-schema registry on a managed deployment, all on one connection. The query layer sits on top of the raw streaming path, it does not replace it.

## What it does

- **Live feed.** A deterministic matching engine random-walks prices and streams thousands of fills in bursts over wall-clock time, written raw to the hot feed and, in the same pass, indexed onto a queryable tape.
- **Hot path.** A tuned raw producer writes fills as they happen. A consumer-group reader, async-iterated as a `Stream`, folds them into a live order book that prints a rolling snapshot (last price, rolling VWAP, cumulative volume per symbol) as the market moves. Latency-critical, straight off the log, nothing materialized.
- **Analytics path.** LaserData Cloud materializes the indexed tape into a queryable trade tape, and once the feed drains we compute per-symbol volume and VWAP over every fill. Indexing is body-first: the projection's pointers extract every column out of the JSON fill, typed (integer cents stay integers), and the fills carry the `message_type` and `ts` convention fields so the reserved columns and the query sugar work. No `agdx.idx.*` headers duplicate the payload.
- **Schema-first tape (LaserData Cloud only).** Real feeds are binary, not JSON. On a managed deployment the same fills replay onto a second tape as raw Avro datums. `laser.schemas().register(SchemaSource::Avro { .. }).send()` registers the `Fill` writer schema synchronously: LaserData Cloud validates that it compiles, allocates a collision-free id, and returns it. The `schema-codecs` feature compiles the schema client-side so `.add_avro(&compiled, id, &fill)` fails before publish if a body stops matching, and the records carry no headers at all (LaserData Cloud resolves `agdx.sid` and decodes each binary body). The per-symbol notionals come out identical to the JSON tape's. On an open server this coda prints how to point at a deployment and skips.

## Run it

```sh
# a local server
just up && cargo run --example order-book

# a LaserData Cloud deployment (enables the Avro schema-first tape)
LASER_CONNECTION_STRING=iggy://user:pwd@your-laserdata-cloud-host cargo run --example order-book
```

## Where to look (LaserData Cloud)

- **Query**: the trade-tape index, queried for per-symbol volume and VWAP.
- **Writer schemas**: the `Fill` Avro schema the run registered, with its LaserData-Cloud-allocated id.

## Highlights

- `laser.iggy_producer(..)` tuned for a feed (balanced partitioning, bounded retries) and `laser.iggy_consumer_group(..)` with a tight `poll_interval` for low tick-to-book latency, the bare producer and consumer the SDK exposes directly.
- The feed runs concurrently with the book reader, so the book updates in real time as fills arrive rather than after a batch lands.
- `laser.publish_batch(topic)` feeds the tape in batches of indexed records with inline bodies, each batch one `send_messages` call, spread across partitions by the balanced partitioner.
- `query(topic).sum(field).group_by([..])`, with VWAP derived from two grouped sums (notional over quantity).
- Prices and notionals are integer cents end to end, so index ordering and aggregation stay exact. The float is display-only.
- Writer schemas: synchronous Avro register returning the allocated id, with client-side validation before publish.
