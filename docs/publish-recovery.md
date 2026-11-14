# Publish timeouts and recovery

Rust, Python, and TypeScript give each publish attempt a 60-second timeout and allow three additional attempts. Retry delays start at 250 milliseconds, double after each failure, and stop increasing at 30 seconds. Each wait is drawn at random from the upper half of that delay, so clients that failed together do not retry together. A timeout limits how long an attempt can take. The transport can report a failure sooner.

Each producer setup attempt and each send attempt uses the configured timeout. Reconnection before a retry uses time from that attempt. TypeScript spends at most half of that time on reconnection, which leaves time to send. After an attempt times out, Rust can use one additional timeout period to close the old connection and reconnect.

| Configuration | Rust builder | Python `Laser.connect` | TypeScript builder | Environment variable | Default |
| --- | --- | --- | --- | --- | --- |
| Attempt timeout | `publish_timeout` | `publish_timeout_ms` | `publishTimeout` | `LASER_PUBLISH_TIMEOUT_MS` | `60000` |
| Additional attempts | `publish_max_retries` | `publish_max_retries` | `publishMaxRetries` | `LASER_PUBLISH_MAX_RETRIES` | `3` |
| Initial retry delay | `publish_retry_backoff` | `publish_retry_backoff_ms` | `publishRetryBackoff` | `LASER_PUBLISH_RETRY_BACKOFF_MS` | `250` |

Explicit builder or connect configuration overrides the corresponding environment variable. To disable automatic resends, set retries to zero. Timeout and retry delay must be positive and at most 2147483647 milliseconds. Invalid configuration fails before connection.

Environment defaults apply to normal connection builders and convenience connect methods. Rust `Laser::from_client` uses the built-in defaults because this wrapper cannot return an error. To configure a supplied Rust client, use `Laser::builder().client(client)`.

```rust
use laser_sdk::prelude::*;
use std::time::Duration;

# async fn example() -> Result<(), LaserError> {
let laser = Laser::builder()
    .connection_string("iggy:iggy@127.0.0.1:8090")
    .publish_timeout(Duration::from_secs(90))
    .publish_max_retries(5)
    .publish_retry_backoff(Duration::from_millis(500))
    .build()
    .await?;
# Ok(())
# }
```

```python
laser = await ls.Laser.connect(
    "iggy:iggy@127.0.0.1:8090",
    publish_timeout_ms=90000,
    publish_max_retries=5,
    publish_retry_backoff_ms=500,
)
```

```typescript
const laser = await Laser.builder()
  .connectionString("iggy:iggy@127.0.0.1:8090")
  .publishTimeout(90_000)
  .publishMaxRetries(5)
  .publishRetryBackoff(500)
  .connect()
```

This configuration covers fluent, typed, agent, and direct streaming publishes. Explicit retry configuration on a direct producer takes precedence. An unset retry count or delay inherits the connection configuration in all three SDKs: Rust `ProducerBuilder::retries(None, None)`, Python `retries=None` and `retry_interval_ms=None`, and TypeScript `retries` and `retryBackoffMs` left out. Set the count to zero to disable resends. Rust `ProducerBuilder::retry_backoff` changes the delay without changing the inherited retry count. Background producers in all three SDKs keep Iggy queue and worker behavior. Their send result acknowledges that the queue accepted the records. The publish response timeout does not cover that queue operation. Their retry count and delay use the connection defaults unless explicitly configured. Direct sends wait the initial delay before the first resend and double it for each later one, up to 30 seconds, and resend only after a transient error. A background worker resends a failed write at once, then waits the configured delay before each later resend without doubling it, so a shard keeps its order. It resends after any error except a lost confirmation of a batch that already committed.

Timeouts and temporary connection failures trigger retries within the configured limits. An `Unauthenticated` reply also triggers recovery when the connection previously authenticated. Reconnection or a leader change can move the client to a node without its session. The SDK reconnects with the connection string credentials before retrying. Permission errors, rejected credentials, invalid requests, and malformed commit confirmations fail immediately.

A group reader that reads from partition primaries uses the same retry count and delay for its two routing reads, the cluster topology probe and the partition route. Rust and TypeScript retry a transient refusal of either read before failing the reader. Python reads through the Rust reader.

Rust reconnects the existing shared Iggy client. Consumers and reply readers keep their connection and diagnostic subscriptions. Recovery applies only to the connection that the failed attempt used. If another publish already replaces that connection, the failed publish uses the replacement. Concurrent publishes cannot repeatedly close each other's new connection during a leader change.

TypeScript closes a failed socket and rejoins registered consumer groups on the replacement. Apache Iggy can replace its own socket during a leader change or while trying the cluster nodes. TypeScript does not treat that replacement as a lost connection, so the current send can finish.

A TypeScript send whose outcome the client could not prove is never resent and keeps the connection. The next send to that partition resolves it first and then goes out, as the Apache Iggy producer does for Rust. The transport hands the connection string's reconnection policy (`reconnection_interval`, `reconnection_retries`) to the Apache Iggy client, which redials each replica link and resumes the session.

TypeScript runs one publish attempt per connection at a time. The Apache Iggy client resends every queued command when it changes nodes. A second queued send causes two authentication attempts per node change. The second login returns the connection to the metadata leader. One queued send lets the connection stay on the partition primary that accepts the message. The client already runs commands one at a time, so this restriction does not reduce throughput.

A supplied TypeScript client has no connection recipe. After a connection failure, its owner must replace it. The SDK closes a socket that times out even when the client belongs to the caller. Its pending response prevents safe reuse.

Retries keep message IDs, payloads, headers, and the selected routing. Rust and TypeScript retry batches in chunks of 1000 records and never resend an earlier confirmed chunk. Without an idempotent producer, delivery remains at-least-once, so a record can arrive more than once. A lost acknowledgement can cause a duplicate even when the message ID stays the same. Effects must be idempotent, which means that repeating them does not change the result. An agent reply deadline, such as incident-desk's `DESK_TIMEOUT`, is separate from the publish timeout.

## Idempotent producers

An idempotent producer writes each batch once per partition, also when a lost reply makes the producer send the batch again. The option is off by default. Turn it on with Rust `ProducerBuilder::idempotent()`, Python `Topic.producer(idempotent=True)`, or TypeScript `ProducerOptions.idempotent`.

The producer takes a producer id, a number that identifies its batches. Rust and Python take the id from the Apache Iggy client, which builds it from the session number. TypeScript draws a random id from the same 63-bit range. Each batch carries the producer id, an epoch, and the sequence number of its first record. The sequence counts records per partition. The producer sends the batches of one partition one at a time, also in background mode, so the sequence order is the send order.

The partition keeps the last five batches of each producer. When a batch arrives again, the partition does not write it, and the reply gives the offset of the first write. A resend after a lost reply, a timeout, or a reconnect keeps the producer section and the message IDs of the first attempt. The retry count and delay of the producer apply. If retries are zero, the producer does not resend.

A partition forgets a producer after one day without a write, or when it must make room for other producers. If a partition does not know the producer, the producer starts a new epoch at sequence 0 and sends the batch again. If an earlier send of that batch had no reply, the send fails instead, because that earlier send can have landed. After a batch fails for good, the next batch starts a new epoch, so it never follows a gap. If a newer epoch of the same producer wrote to the partition, the server answers Apache Iggy error code 93, and the producer refuses every later send.

The server must support producer sections. A server without this support refuses the batch with an error. All three SDKs read records that an idempotent producer wrote. A reader on an older Apache Iggy client can refuse such batches. The producer id lives in the producer. A new producer, or a restarted process, gets a new id, so the protection covers resends of one producer only. Keep the business idempotency keys that the rest of this page describes.

## Longer outages

Exhausted retries return `Result::Err` in Rust, raise a typed exception in Python, and reject a promise in TypeScript. This path does not panic or terminate the process. Applications must handle the returned failure. The client can try again after the cluster recovers.

An unhandled Python exception or JavaScript promise rejection can terminate an application. Rust applications can exit when they propagate an error from `main`, as the incident-desk example does. They can also panic through `unwrap` or `expect`.

Consumer and agent tasks can also finish with an error. Monitor their run or join result and restart them when appropriate. Publish retries do not replace application supervision.

A long-running service must retain failed work in a durable application queue. During an outage, pause work or slow new requests. Retry later with the same business idempotency key, which identifies repeated attempts at one operation. Keep retry limits finite so one request cannot occupy resources forever. The SDK cannot guarantee progress while the cluster is unavailable. It does not provide a durable queue for offline publishes.

Every fluent or direct publish that gives up returns Rust `LaserError::PublishFailed`. Its message names the stream, the topic, and the cause. It carries the confirmed ranges in `committed` and the records without a confirmation in `unconfirmed`, with the message ids the attempts used, so a resend stays deduplicated on the server. `publish_cause()` returns the original error, and every classifier answers for that cause. Python raises `PublishFailedError` with `stream`, `topic`, `committed`, and `unconfirmed`, a list of `IggyMessage`, and the original error as its `__cause__`. TypeScript throws `PublishFailedError` from every publish path, including agent sends, AGDX verbs, and memory writes, with `stream`, `topic`, `committed`, and `unconfirmed`, and `publishCause()` (or the free `publishCause(error)`) returns the original error. The TypeScript classifiers such as `isRetryable` and `isPermissionDenied` answer for that cause. Unconfirmed records can already exist on the server if their acknowledgement was lost. Do not replay the confirmed prefix. Permanent transport errors, including missing resources and denied permissions, set `retryable` to false. An expired session can still trigger reauthentication.

Batching producers serialize flushes. A failed timer flush never stops the timer. Its failure is kept until the next `send()`, `flush()`, or `close()` reports it. A `send()` that finds it does not queue its record and returns the kept failure with that record added to the unconfirmed records. `flush()` and `close()` report it after they drain the queue. A TypeScript `send()` or `flush()` whose signal aborts sends no retry of its flush and keeps that flush's failure the same way ([client behavior](client-behavior.md#durations-deadlines-and-cancellation)). Several kept failures arrive as one publish failure that lists the records of every failed batch. Inspect a publish failure before retrying because some records can already be committed.

A background producer hands each failed write to its error callback: the `error_callback` of the Rust `BackgroundConfig`, the `error_callback` of the Python `BackgroundConfig`, or the TypeScript `onError` option, which can return a promise. Without a callback the producer keeps the failures, and the next `flush()` or `shutdown()` reports them as one publish failure that lists the records of every failed write. A TypeScript callback that throws leaves its failure kept as well. `flush()` waits until every record a background send queued is written, after the sends already in flight, and `shutdown()` flushes and stops the producer. Both act for every clone of the producer, and a second `shutdown()` returns at once.
