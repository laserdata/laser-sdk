# native-streaming - live producer and consumer groups

This example uses the Laser producer and consumer APIs for ordinary Apache Iggy streaming. It demonstrates batching, headers, routing, and automatic or explicit offset commits.

## What it does

- Builds a direct producer with batching, linger, retries, topic creation, and default balanced routing.
- Sends a keyed record with an exact-width header, then 1000 messages total in batches matching the producer's own batch length.
- Reads all 1000 records through a live async consumer group with automatic server-side offset commits.
- Reads them again through an independent group and commits only after each record is handled successfully.

## Run it

Run from `examples/rust`:

```sh
just up && cargo run --example native-streaming
```

The SDK uses Iggy's native VSR transport. Point it at the desired Iggy deployment:

```sh
LASER_CONNECTION_STRING='user:pwd@iggy-host:8090' \
  cargo run --example native-streaming
```

## Highlights

- `topic.producer()` exposes direct batching, linger, retries, topology, and per-send key or partition routing.
- `topic.consumer_group()` returns a `futures::Stream` with configurable start, polling, replay, retries, group creation, and commit policy.
- Iterate with `while let Some(message) = consumer.next().await`, or Python `async for message in consumer`. For a bounded single-record wait, use `Consumer::next_within(timeout)`.
- Use `CommitPolicy::Disabled` with `consumer.commit(&message)` to store an offset after handling. Shutdown does not commit an unhandled record. Each explicit commit requires a `store_offset` round trip, so it can be slower than batched commits.
- `ConsumerMessage` preserves the raw payload, typed headers, timestamps, partition, and exact log offset.
