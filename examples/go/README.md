# LaserData - Go examples

Runnable examples of the Go client. Each is a standalone program that does one
thing and says why, so they are worth reading as well as running.

They are a module of their own, replacing the client with the checkout beside
them, so an example runs against the working tree rather than a published
version.

## Running

Every example reads `LASER_CONNECTION_STRING`, so the same binary runs against
a local container, a staging stack, or LaserData Cloud without being rebuilt:

```sh
export LASER_CONNECTION_STRING='iggy:iggy@127.0.0.1:8090'
export LASER_STREAM=examples

go run ./kv
go run ./agent
go run ./memory
```

`just up` from the repository root starts the local Apache Iggy the examples
talk to.

## The examples

| Example | What it shows |
|---|---|
| `kv` | The managed key-value store, and a compare-and-swap that two writers race on. The loser is told it lost rather than silently overwriting the winner. |
| `agent` | Two agents exchanging work on one conversation, and the gates a running agent applies: expiry, addressing, and a fence that turns away a stale holder. |
| `memory` | Remembering facts, recalling them by word overlap, and rendering the result into a prompt under a token budget. |

`kv` needs a managed backend (Laser Stack or LaserData Cloud) and says so
rather than failing partway through. `agent` and `memory` run against plain
Apache Iggy.

## What is not here yet

The Go client is in progress, so there is no example of a streaming consumer
group, a query, or a bridge served over HTTP. The [Rust
examples](../rust/README.md) cover those today.
