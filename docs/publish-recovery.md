# Publish timeouts and recovery

Rust, Python, and TypeScript use a 60-second publish attempt timeout and allow three additional attempts. Retries start after 250 milliseconds, double after each failure, and cap the delay at 30 seconds. A timeout is an upper bound. The underlying transport can report a failure sooner. Producer setup and reconnect work consume the attempt budget. TypeScript spends at most half of an attempt on reconnect work, so the send that follows always keeps a usable window. Rust may spend one additional timeout budget retiring and reconnecting a connection after a timed-out attempt.

| Setting | Environment variable | Default |
| --- | --- | --- |
| Attempt timeout | `LASER_PUBLISH_TIMEOUT_MS` | `60000` |
| Additional attempts | `LASER_PUBLISH_MAX_RETRIES` | `3` |
| Initial retry delay | `LASER_PUBLISH_RETRY_BACKOFF_MS` | `250` |

Explicit builder or connect options override the corresponding environment variable. Set retries to zero to disable automatic resends. Timeout and backoff must be positive and at most 2147483647 milliseconds. Invalid settings fail before connecting. Environment defaults apply to normal connection builders and convenience connect methods. Rust `Laser::from_client` uses the built-in defaults because it is an infallible wrapper. Use `Laser::builder().client(client)` to configure an injected Rust client.

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

These settings cover fluent, typed, agent, and direct streaming publishes. Direct producer retry overrides remain authoritative. Rust background producers keep Iggy's queue and worker semantics. Their send result acknowledges queueing, so the publish response timeout does not wrap that queue operation. Their retry count and delay inherit the connection defaults unless overridden.

Timeouts and transient connection failures trigger bounded retries. So does an `Unauthenticated` reply on a connection that already authenticated once: the socket was re-dialed underneath the client, by its own reconnect or by a leader move, and the new node holds no session for it, so the SDK re-dials through the connection string's auto-login credentials before the retry. Permission, authentication, validation, and malformed commit-confirmation errors fail immediately. Rust reconnects the existing shared Iggy client so consumers and reply readers retain their connection and diagnostic subscriptions. Recovery applies to the connection an attempt actually used. A publish that failed on a connection another publish has already replaced skips recovery and sends on the replacement, so concurrent publishes during a leadership change cannot keep retiring each other's fresh connection. TypeScript retires the failed socket and rejoins registered consumer groups on its replacement. The Apache Iggy client swaps its own socket when it follows a leader move or walks the cluster roster, and TypeScript does not count that swap as a lost connection, so the send being relocated is left to finish. TypeScript also runs one publish attempt at a time per connection. The client relocates by re-issuing every queued command, and a second queued send makes it authenticate twice per hop with the second login settling back on the metadata leader, so a lone command is what lets a send stay on the partition primary that admits it. The client already runs commands one at a time, so this costs no throughput. An injected TypeScript client has no connection recipe, so its owner must replace it after a connection failure. A timed-out socket is closed even when that client was borrowed, because its pending response cannot safely be reused.

Retry attempts retain message IDs, payloads, headers, and the selected routing. Rust retries batches in chunks and never resends an earlier confirmed chunk. Delivery remains at-least-once. A lost acknowledgement can cause a duplicate even when the message ID is unchanged. Effects must be idempotent. An agent reply deadline, such as concierge's `DESK_TIMEOUT`, is separate from the publish timeout.

## Longer outages

Exhausting the retry budget returns `Result::Err` in Rust, raises a typed exception in Python, and rejects a promise in TypeScript. This path does not panic or terminate the process. The client can be used for a later attempt after the cluster recovers. Applications must handle the returned failure. An unhandled Python exception or JavaScript promise rejection can terminate an application. Rust applications can also choose to exit by propagating the error from `main`, as the concierge example does, or panic by calling `unwrap` or `expect`.

Consumer and agent tasks can also finish with an error. Monitor their run or join result and restart them when appropriate. A publish retry policy does not replace application supervision.

A long-running service should retain failed work in a durable application queue, pause or apply backpressure during an outage, and retry later with the same business idempotency key. Keep retry budgets finite so a request cannot occupy resources forever. The SDK cannot guarantee progress while the cluster is unavailable and does not provide a durable offline outbox.
