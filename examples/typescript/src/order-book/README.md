# order-book - one market tape, live and materialized

A deterministic random-walk fill stream feeds a live typed fold and a durable
trade tape on one connection. The fold reports last price, volume, notional,
and VWAP with integer arithmetic.

## What it does

1. Publishes JSON fills to `trades` in bounded batches.
2. Reads the tape concurrently through a named typed cursor.
3. Registers the JSON projection on LaserData Cloud.
4. Registers an Avro writer schema, validates fills before publish, and writes
   the same market to `trades_avro`.

## Run it

```sh
npm run build
node dist/src/order-book/main.js

LASER_MESSAGES=100000 LASER_BATCH=1000 node dist/src/order-book/main.js
```

The streaming fold runs on Apache Iggy. Projection, query, and Avro registry
work run only when the managed capabilities are available.

## Highlights

- Integer cents and bigint notionals avoid floating-point accounting drift.
- The typed reader reports exact source positions and advances past failures.
- Avro schema compilation happens once for the typed topic.
