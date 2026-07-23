# order-book - live book and materialized trade tape

> A deterministic market workload with a low-latency feed, a body-first analytics tape, typed replay, and a schema-first binary tape.

## What it does

1. Generates the Rust and Python fill model from one deterministic random walk: symbol, integer-cent price, quantity, side, exact notional, `message_type`, and timestamp.
2. Publishes the fills to `md_feed` in paced batches while a named typed reader folds the live book.
3. Maintains last price, cumulative volume, notional, and VWAP per symbol without floating-point accounting drift.
4. Publishes the identical fills to the durable `trades` tape in bounded JSON batches.
5. Registers the index-only `trades.v1` projection before publishing when query is available. Every tape batch explicitly calls `inlinePayload()`, so the managed row stores the body as well as the extracted columns.
6. Queries per-symbol quantity and notional sums, derives VWAP, and fetches one materialized payload through `FILL_CODEC`.
7. Replays only this run's durable tape as typed `Fill` values and verifies every symbol's notional against the generated session.
8. Registers the complete seven-field Avro writer schema, validates before transport I/O, publishes up to 500 fills to `trades_avro`, waits for materialization, and queries per-symbol notionals on LaserData Cloud.

The feed, durable JSON tape, and typed replay run on Apache Iggy. Projection, query, schema registry, and Avro materialization are managed phases and print one skip reason on an open server.

The JSON tape is body-first. Projection pointers extract queryable scalars from the fill itself, and no duplicate index headers can disagree with that body. Because each tape record opts into inline payload, a query with payload selection, including the stream UI DSL `include_payload` flag, returns the original JSON bytes.

## Run it

```sh
npm run example:order-book
```

Use the shared workload controls for a larger tape.

```sh
LASER_MESSAGES=100000 LASER_BATCH=1000 \
  npm run example:order-book
```

Run every phase against LaserData Cloud with a bare target.

```sh
LASER_CONNECTION_STRING=user:pwd@your-laserdata-cloud-host \
  npm run example:order-book
```

## Where to look (LaserData Cloud)

- **Query**: the `trades` materialized tape, per-symbol volume and VWAP inputs, and decodable inline payloads.
- **Writer schemas**: the allocated Avro schema used by `trades_avro`.
- **Messages**: hot JSON fills on `md_feed`, durable JSON fills on `trades`, and validated Avro datums on `trades_avro`.
- **Bindings**: the projection binding from the streaming topic to the managed table.

## Highlights

- The explicit `Codec<Fill>` validates JSON values after TypeScript types disappear.
- Integer cents and bigint accumulators keep ordering, totals, and VWAP inputs exact.
- The typed reader carries the source partition and offset with every decoded record.
- `publishBatch().inlinePayload()` makes the durable tape body available to query payload selection without changing the index-only projection default.
- `sum("qty").groupBy(["symbol"])` and `sum("notional_cents").groupBy(["symbol"])` derive managed VWAP from exact materialized columns.
- The compiled Avro schema is reused for the entire binary tape and rejects invalid values before publish.
