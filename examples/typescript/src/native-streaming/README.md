# native-streaming - live producers and consumer groups

> The focused Apache Iggy streaming path through Laser, with no managed service required.

## What it does

1. Creates the `events` topic with four partitions.
2. Builds a direct producer with three bounded retries and a one-second retry interval.
3. Sends one keyed record with an exact `uint16` header, then publishes the remaining configurable workload in batches.
4. Drains the full topic through an `auto-workers` consumer group with automatic server-side commits.
5. Drains it again through an independent `manual-workers` group that commits only after each record is handled.
6. Prints partition, offset, decoded payload, and bounded progress so a stalled reader fails with a useful timeout instead of hanging.

## Run it

```sh
npm run example:native-streaming
```

Scale the same program without changing code.

```sh
LASER_MESSAGES=100000 LASER_BATCH=1000 \
  npm run example:native-streaming
```

The example runs against stock Apache Iggy or LaserData Cloud. To use another server, pass a bare target.

```sh
LASER_CONNECTION_STRING=user:pwd@your-host \
  npm run example:native-streaming
```

## Highlights

- `topic.producer()` exposes direct sends, batch sends, bounded retry, and balanced, keyed, or explicit-partition routing.
- `topic.consumerGroup()` supports named groups, start positions, polling intervals, automatic commits, explicit commits, stored offsets, and bounded `nextWithin()` waits.
- `HeaderValue.uint16(7)` preserves the exact Apache Iggy header width instead of lowering every number to a generic JavaScript value.
- `Consumer` and `Producer` implement `AsyncDisposable`, so `await using` owns their lifecycle without manual cleanup scaffolding.
- Delivery remains at least once. The manual group demonstrates the commit-after-success pattern an idempotent handler uses for external effects.
