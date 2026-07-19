# native-streaming - live producer and consumer groups

A focused ordinary message-streaming example over Apache Iggy using only the Laser-facing producer and consumer APIs. Layer: generic. No managed surface is required.

## What it does

- Builds a direct producer with batching, linger, retries, topic creation, and default balanced routing.
- Sends a keyed record with an exact-width header, then 1000 messages total in batches matching the producer's own batch length.
- Reads all 1000 records through a live async consumer group with automatic server-side offset commits.
- Reads them again through an independent group and commits only after each record is handled successfully.

## Run it

```sh
just up && cargo run --example native-streaming
```

The same code runs against a VSR cluster:

```sh
LASER_CONNECTION_STRING='iggy+tcp://user:pwd@vsr-host:3000' \
  cargo run --example native-streaming --features vsr
```

## Highlights

- `topic.producer()` exposes direct batching, linger, retries, topology, and per-send key or partition routing.
- `topic.consumer_group()` returns a `futures::Stream` with configurable start, polling, replay, retries, group creation, and commit policy.
- `while let Some(message) = consumer.next().await` (Python: `async for message in consumer`) is the ordinary way to drain a live consumer: keep iterating for as long as records keep arriving, no per-call timeout. `Consumer::next_within(timeout)` exists in the SDK for a caller that wants a bounded single-record wait instead, but this example does not need one.
- `CommitPolicy::Disabled` plus `consumer.commit(&message)` implements commit-after-success delivery. Shutdown does not advance an uncommitted record. This is one `store_offset` network round-trip per message, by design (crash-safe to the exact last handled record, not just the last batch), so it is visibly slower than the batched auto-commit path above on any connection with real latency, most noticeably against a remote or rate-limited deployment.
- `ConsumerMessage` preserves the raw payload, typed headers, timestamps, partition, and exact log offset.
