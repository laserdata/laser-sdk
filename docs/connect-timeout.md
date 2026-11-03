# Connect timeout and cleanup

Rust, Python, and TypeScript give the initial connect a 30-second budget. The budget covers the TCP dial, the TLS handshake, the login, and the managed capability probe. The Iggy client retries a failed dial by itself. The budget stops those retries.

| Configuration | Rust builder | Python `Laser.connect` | TypeScript builder | Environment variable | Default |
| --- | --- | --- | --- | --- | --- |
| Connect budget | `connect_timeout` | `connect_timeout_ms` | `connectTimeout` | `LASER_CONNECT_TIMEOUT_MS` | `30000` |

Explicit configuration overrides the environment variable. The budget must be positive and at most 2147483647 milliseconds. Invalid configuration fails before any connection attempt. `Laser::connect`, `Laser.connect`, and the other convenience connect methods use the environment variable or the default.

An expired budget returns a timeout error that names the stage that stalled.

| Message | Meaning |
| --- | --- |
| `the Iggy server to accept the connection` (Rust, Python), `Iggy server to accept the connection` (TypeScript) | The TCP or TLS handshake never completed. The server may be down, unreachable, or out of resources to accept new connections. |
| `the Iggy login reply` (Rust, Python), `Iggy login reply` (TypeScript) | The transport was up but the server never answered the login. |

If the capability probe has not answered when the budget expires, the connect still succeeds. The client starts with the open capability set, as after any failed probe. A later capability call probes again.

The budget applies only to the initial connect. Reconnection after a lost connection stays unlimited by default, so long-running consumers survive a server restart. Publish reconnection is bounded by the publish timeout, see [publish recovery](publish-recovery.md).

```rust
use laser_sdk::prelude::*;
use std::time::Duration;

# async fn example() -> Result<(), LaserError> {
let laser = Laser::builder()
    .connection_string("iggy:iggy@127.0.0.1:8090")
    .connect_timeout(Duration::from_secs(10))
    .build()
    .await?;
# Ok(())
# }
```

```python
laser = await ls.Laser.connect("iggy:iggy@127.0.0.1:8090", connect_timeout_ms=10_000)
```

```typescript
const laser = await Laser.builder()
  .connectionString("iggy:iggy@127.0.0.1:8090")
  .connectTimeout(10_000)
  .connect()
```

## Closing and deleting

`close` ends the shared connection for every clone of a `Laser` in all three SDKs. Rust `Laser::close` also closes the dedicated fenced-lease connection when one was opened. Repeating it is safe.

`stream(name).delete()` removes a stream with every topic and message in it. It returns `false` when the stream did not exist. Each partition costs the server open files and memory, and `bootstrap(partitions)` alone creates seven topics with `partitions` partitions each: `agent.sessions`, `agent.heartbeats`, `agent.streams`, `agent.memory`, `agent.dlq`, `agent.audit`, and `agent.workflow_journal`. Delete streams you no longer need, especially on small tiers. The examples delete the previous run's `laser-<example>-<language>` stream when they start and keep their own result afterwards, and never delete a stream supplied through `LASER_STREAM`.
