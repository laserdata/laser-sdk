# fleet-tape: live fleet view and reading-tape analytics

This example sends host CPU readings to a live fleet-view reader and a reading-history topic. Managed deployments also support analytics and schema-based publication.

## What it does

1. Generates the same reading model as the Rust and Python examples from one deterministic random walk: host, integer CPU percentage, sample count, health level, exact weighted CPU total, `message_type`, and timestamp.
2. Publishes the readings to `metrics_feed` in paced batches while a named typed reader folds the live fleet view.
3. Maintains last CPU, cumulative samples, weighted CPU total, sample-weighted mean CPU, and degraded readings per host without floating-point drift.
4. Publishes the identical readings to the durable `readings` tape in bounded JSON batches.
5. Registers the index-only `readings_<token>.v1` projection before publishing when query is available. Every tape batch explicitly calls `inlinePayload()`, so the managed row stores the body as well as the extracted columns.
6. Queries per-host sample and weighted CPU sums, derives the mean CPU, and fetches one materialized payload through `READING_CODEC`.
7. Replays only this run's durable tape as typed `Reading` values and verifies every host's weighted CPU total against the generated session.
8. Registers the complete seven-field Avro writer schema, validates before transport I/O, publishes up to 500 readings to `readings_avro`, waits for materialization, and queries per-host weighted CPU totals on LaserData Cloud.

The feed, durable JSON tape, and typed replay run on Apache Iggy. Projection, query, schema registry, and Avro materialization are managed phases and print one skip reason on an open server.

The JSON tape is body-first. Projection pointers extract queryable scalars from the reading itself, and no duplicate index headers can disagree with that body. Because each tape record opts into inline payload, a query with payload selection returns the original JSON bytes.

## Run it

Run `npm run setup` once, then run from `examples/typescript`:

```sh
npm run example:fleet-tape
```

Use the shared workload controls for a larger tape.

```sh
LASER_MESSAGES=100000 LASER_BATCH=1000 \
  npm run example:fleet-tape
```

Run every phase against Laser Stack or LaserData Cloud with a bare target.

```sh
LASER_CONNECTION_STRING=user:pwd@your-laserdata-cloud-host \
  npm run example:fleet-tape
```

## Where to look (LaserData Cloud)

- Query: the `readings_<token>` materialized tape (the run token keeps a rerun, or another language's example on the same deployment, out of this run's rows), per-host sample and weighted CPU sums, and decodable inline payloads.
- Writer schemas: the allocated Avro schema used by `readings_avro`.
- Messages: hot JSON readings on `metrics_feed`, durable JSON readings on `readings`, and validated Avro datums on `readings_avro`.
- Bindings: the projection binding from the streaming topic to the managed table.

## Highlights

- The explicit `Codec<Reading>` validates JSON values after TypeScript types disappear.
- Integer CPU values and bigint accumulators keep ordering, totals, and mean CPU inputs exact.
- The typed reader carries the source partition and offset with every decoded record.
- `publishBatch().inlinePayload()` makes the durable tape body available to query payload selection without changing the index-only projection default.
- `sum("samples").groupBy(["host"])` and `sum("cpu_total").groupBy(["host"])` derive the managed mean CPU from exact materialized columns.
- The compiled Avro schema is reused for the entire binary tape and rejects invalid values before publish.
