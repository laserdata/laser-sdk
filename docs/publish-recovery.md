# Publish timeouts and recovery

Rust, Python, and TypeScript give each publish attempt a 60-second timeout and allow three additional attempts. Retry delays start at 250 milliseconds, double after each failure, and stop increasing at 30 seconds. A timeout limits how long an attempt can take. The transport can report a failure sooner.

Producer setup and reconnection use time from the same attempt. TypeScript spends at most half of that time on reconnection, which leaves time to send. After an attempt times out, Rust can use one additional timeout period to close the old connection and reconnect.

| Configuration | Environment variable | Default |
| --- | --- | --- |
| Attempt timeout | `LASER_PUBLISH_TIMEOUT_MS` | `60000` |
| Additional attempts | `LASER_PUBLISH_MAX_RETRIES` | `3` |
| Initial retry delay | `LASER_PUBLISH_RETRY_BACKOFF_MS` | `250` |

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
laser = await Laser.connect(
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

This configuration covers fluent, typed, agent, and direct streaming publishes. Explicit retry configuration on a direct producer takes precedence. Rust background producers keep Iggy queue and worker behavior. Their send result acknowledges that the queue accepted the records. The publish response timeout does not cover that queue operation. Their retry count and delay use the connection defaults unless explicitly configured.

Timeouts and temporary connection failures trigger retries within the configured limits. An `Unauthenticated` reply also triggers recovery when the connection previously authenticated. Reconnection or a leader change can move the client to a node without its session. The SDK reconnects with the connection string credentials before retrying. Permission errors, rejected credentials, invalid requests, and malformed commit confirmations fail immediately.

Rust reconnects the existing shared Iggy client. Consumers and reply readers keep their connection and diagnostic subscriptions. Recovery applies only to the connection that the failed attempt used. If another publish already replaces that connection, the failed publish uses the replacement. Concurrent publishes cannot repeatedly close each other's new connection during a leader change.

TypeScript closes a failed socket and rejoins registered consumer groups on the replacement. Apache Iggy can replace its own socket during a leader change or while trying the cluster nodes. TypeScript does not treat that replacement as a lost connection, so the current send can finish.

TypeScript runs one publish attempt per connection at a time. The Apache Iggy client resends every queued command when it changes nodes. A second queued send causes two authentication attempts per node change. The second login returns the connection to the metadata leader. One queued send lets the connection stay on the partition primary that accepts the message. The client already runs commands one at a time, so this restriction does not reduce throughput.

A supplied TypeScript client has no connection recipe. After a connection failure, its owner must replace it. The SDK closes a socket that times out even when the client belongs to the caller. Its pending response prevents safe reuse.

Retries keep message IDs, payloads, headers, and the selected routing. Rust retries batches in chunks and never resends an earlier confirmed chunk. Delivery remains at-least-once, so a record can arrive more than once. A lost acknowledgement can cause a duplicate even when the message ID stays the same. Effects must be idempotent, which means that repeating them does not change the result. An agent reply deadline, such as concierge's `DESK_TIMEOUT`, is separate from the publish timeout.

## Longer outages

Exhausted retries return `Result::Err` in Rust, raise a typed exception in Python, and reject a promise in TypeScript. This path does not panic or terminate the process. Applications must handle the returned failure. The client can try again after the cluster recovers.

An unhandled Python exception or JavaScript promise rejection can terminate an application. Rust applications can exit when they propagate an error from `main`, as the concierge example does. They can also panic through `unwrap` or `expect`.

Consumer and agent tasks can also finish with an error. Monitor their run or join result and restart them when appropriate. Publish retries do not replace application supervision.

A long-running service must retain failed work in a durable application queue. During an outage, pause work or slow new requests. Retry later with the same business idempotency key, which identifies repeated attempts at one operation. Keep retry limits finite so one request cannot occupy resources forever. The SDK cannot guarantee progress while the cluster is unavailable. It does not provide a durable queue for offline publishes.
