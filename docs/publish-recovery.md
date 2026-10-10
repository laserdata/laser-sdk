# Publish timeouts and recovery

Rust, Python, and TypeScript give each publish attempt a 60-second timeout and allow three additional attempts. Retry delays start at 250 milliseconds, double after each failure, and stop increasing at 30 seconds. A timeout limits how long an attempt can take. The transport can report a failure sooner.

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

This configuration covers fluent, typed, agent, and direct streaming publishes. Explicit retry configuration on a direct producer takes precedence. Python `retries=None` and `retry_interval_ms=None` inherit the connection configuration. Set `retries=0` to disable resends. Rust `ProducerBuilder::retry_backoff` changes the delay without changing the inherited retry count. Background producers in all three SDKs keep Iggy queue and worker behavior. Their send result acknowledges that the queue accepted the records. The publish response timeout does not cover that queue operation. Their retry count and delay use the connection defaults unless explicitly configured. Direct sends wait the initial delay before the first resend and double it for each later one, up to 30 seconds, and resend only after a transient error. A background worker resends a failed write at once, then waits the configured delay before each later resend without doubling it, so a shard keeps its order. It resends after any error except a lost confirmation of a batch that already committed.

Timeouts and temporary connection failures trigger retries within the configured limits. An `Unauthenticated` reply also triggers recovery when the connection previously authenticated. Reconnection or a leader change can move the client to a node without its session. The SDK reconnects with the connection string credentials before retrying. Permission errors, rejected credentials, invalid requests, and malformed commit confirmations fail immediately.

A group reader that reads from partition primaries uses the same retry count and delay for its two routing reads, the cluster topology probe and the partition route. Rust and TypeScript retry a transient refusal of either read before failing the reader. Python reads through the Rust reader.

Rust reconnects the existing shared Iggy client. Consumers and reply readers keep their connection and diagnostic subscriptions. Recovery applies only to the connection that the failed attempt used. If another publish already replaces that connection, the failed publish uses the replacement. Concurrent publishes cannot repeatedly close each other's new connection during a leader change.

TypeScript closes a failed socket and rejoins registered consumer groups on the replacement. Apache Iggy can replace its own socket during a leader change or while trying the cluster nodes. TypeScript does not treat that replacement as a lost connection, so the current send can finish.

TypeScript runs one publish attempt per connection at a time. The Apache Iggy client resends every queued command when it changes nodes. A second queued send causes two authentication attempts per node change. The second login returns the connection to the metadata leader. One queued send lets the connection stay on the partition primary that accepts the message. The client already runs commands one at a time, so this restriction does not reduce throughput.

A supplied TypeScript client has no connection recipe. After a connection failure, its owner must replace it. The SDK closes a socket that times out even when the client belongs to the caller. Its pending response prevents safe reuse.

Retries keep message IDs, payloads, headers, and the selected routing. Rust and TypeScript retry batches in chunks of 1000 records and never resend an earlier confirmed chunk. Delivery remains at-least-once, so a record can arrive more than once. A lost acknowledgement can cause a duplicate even when the message ID stays the same. Effects must be idempotent, which means that repeating them does not change the result. An agent reply deadline, such as incident-desk's `DESK_TIMEOUT`, is separate from the publish timeout.

## Longer outages

Exhausted retries return `Result::Err` in Rust, raise a typed exception in Python, and reject a promise in TypeScript. This path does not panic or terminate the process. Applications must handle the returned failure. The client can try again after the cluster recovers.

An unhandled Python exception or JavaScript promise rejection can terminate an application. Rust applications can exit when they propagate an error from `main`, as the incident-desk example does. They can also panic through `unwrap` or `expect`.

Consumer and agent tasks can also finish with an error. Monitor their run or join result and restart them when appropriate. Publish retries do not replace application supervision.

A long-running service must retain failed work in a durable application queue. During an outage, pause work or slow new requests. Retry later with the same business idempotency key, which identifies repeated attempts at one operation. Keep retry limits finite so one request cannot occupy resources forever. The SDK cannot guarantee progress while the cluster is unavailable. It does not provide a durable queue for offline publishes.

Every fluent or direct publish that gives up returns Rust `LaserError::PublishFailed`. Its message names the stream, the topic, and the cause. It carries the confirmed ranges in `committed` and the records without a confirmation in `unconfirmed`, with the message ids the attempts used, so a resend stays deduplicated on the server. `publish_cause()` returns the original error, and every classifier answers for that cause. Python raises `PublishFailedError` with `stream`, `topic`, `committed`, and `unconfirmed`, a list of `IggyMessage`, and the original error as its `__cause__`. TypeScript throws `PublishFailedError` from every publish path, including agent sends, AGDX verbs, and memory writes, with `stream`, `topic`, `committed`, and `unconfirmed`, and `publishCause()` (or the free `publishCause(error)`) returns the original error. The TypeScript classifiers such as `isRetryable` and `isPermissionDenied` answer for that cause. Unconfirmed records can already exist on the server if their acknowledgement was lost. Do not replay the confirmed prefix. Permanent transport errors, including missing resources and denied permissions, set `retryable` to false. An expired session can still trigger reauthentication.

Batching producers serialize flushes. A failed timer flush never stops the timer. Its failure is kept until the next `send()`, `flush()`, or `close()` reports it. A `send()` that finds it does not queue its record and returns the kept failure with that record added to the unconfirmed records. `flush()` and `close()` report it after they drain the queue. Several kept failures arrive as one publish failure that lists the records of every failed batch. Inspect a publish failure before retrying because some records can already be committed.

A Python background producer hands each failure to the `error_callback` of its `BackgroundConfig`. Without one, the failure is logged on the `laser_sdk` logger and dropped, as in Rust. A TypeScript background producer hands each failure to its `onError` callback and awaits it. Without a callback, or when the callback throws, the producer keeps the original failure and reports it from `shutdown()`.
