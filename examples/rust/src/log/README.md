# log - the Log primitive

A topic is an append-only record of every message in your system. Services write to it and read from it like a group chat that never loses a message. New readers start from the beginning or jump straight to now.

## What it shows

- Connect once with `laser_examples::laser(..)`.
- Create a topic (`laser.stream("shop").topic("orders")`) with `ensure(2)`.
- Publish two JSON messages (`topic.publish().json(&order)?.send()`).
- Read them back through one typed handle (`topic.json::<Order>().records(..)`), draining from offset 0 until caught up.
- Run it twice and the second run replays four orders: the log keeps every record, and a fresh reader starts at offset 0. That is the primitive, not a bug.

## Run it

Run from `examples/rust`:

```sh
just up && cargo run --example log
```

## Learn more

- Docs: https://docs.laserdata.cloud/laser-sdk/log
- Full system built on this primitive: [`native-streaming`](../native-streaming/README.md)
