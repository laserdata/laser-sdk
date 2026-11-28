# Connect timeout and cleanup

Rust, Python, TypeScript, and Go give the initial connect a 30-second budget. The budget covers the dial over TCP, QUIC or WebSocket, the TLS handshake, the login, and the managed capability probe. The Iggy client retries a failed dial by itself. The budget stops those retries.

| Configuration | Rust builder | Python `Laser.connect` | TypeScript builder | Go `Options` | Environment variable | Default |
| --- | --- | --- | --- | --- | --- | --- |
| Connect budget | `connect_timeout` | `connect_timeout_ms` | `connectTimeout` | `Timeout` | `LASER_CONNECT_TIMEOUT_MS` | `30000` |

Explicit configuration overrides the environment variable. The budget must be positive and at most 2147483647 milliseconds. Invalid configuration fails before any connection attempt. `Laser::connect`, `Laser.connect`, and the other convenience connect methods use the environment variable or the default.

An expired budget returns a timeout error that names the stage that stalled, with the same text in every client.

| Message | Meaning |
| --- | --- |
| `timed out waiting for the Iggy server to accept the connection` | The TCP or TLS handshake never completed. The server may be down, unreachable, or out of resources to accept new connections. |
| `timed out waiting for the Iggy server to accept the connection: the deployment admits only addresses its access rules allow: add an access rule for this address` | The same stage for a `*.laserdata.cloud` or `*.laserdata.com` host. A LaserData deployment drops connections from an address no access rule allows, so a missing access rule is the usual cause. |
| `timed out waiting for the Iggy login reply` | The transport was up but the server never answered the login. |

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

A scoped block closes what it opened. Python `async with await Laser.connect(...) as laser:` and TypeScript `await using laser = await Laser.connect(...)` close the connection when the block ends, and Go closes it with a deferred `laser.Close()`. A handle from `with_default_stream` or another `with_*` method only borrows the connection, so ending its block leaves it open. Producers, consumers, batching producers, and filtered readers close the same way: Python `async with` and TypeScript `await using` call `shutdown()` or `close()` on exit, and Rust callers call them before dropping the handle. A filtered reader that is iterated stops yielding once it is closed: Python `async for` and TypeScript `for await` end without an error, and the Rust `FilteredReader::stream()` acknowledges through the same handle and closes with `close()`.

`stream(name).delete()` removes a stream with every topic and message in it. It returns `false` when the stream did not exist. A scratch stream lives for one program run: Rust `laser.scratch_stream(prefix)`, Python `async with laser.scratch_stream(prefix) as scratch:`, TypeScript `await using scratch = await laser.scratchStream(prefix)`, and Go `laser.ScratchStream(ctx, prefix)` create a stream named `{prefix}-` and eight lowercase hex characters, refuse a name that already exists, and hand back a handle whose default stream it is. Python deletes it when the block ends, also when the block raised, and TypeScript when the scope disposes it. Go deletes it with `scratch.Delete(ctx)`, usually deferred. Rust `ScratchStream::delete` deletes it and dropping the handle deletes it best effort, so a Rust program awaits `delete` for a guaranteed cleanup. A second delete does nothing. Each partition costs the server open files and memory, and `bootstrap(partitions)` alone creates seven topics with `partitions` partitions each: `agent.sessions`, `agent.heartbeats`, `agent.streams`, `agent.memory`, `agent.dlq`, `agent.audit`, and `agent.workflow_journal`. Delete streams you no longer need, especially on small tiers. The examples delete the previous run's `laser-<example>-<language>` stream when they start and keep their own result afterwards, and never delete a stream supplied through `LASER_STREAM`.
