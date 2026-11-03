# log: the Log primitive

A topic stores records in append order under its retention policy. This example publishes typed host readings and reads them by offset.

## What it shows

- Connect once with `laser_examples::laser(..)` on the example's own `laser-log-rust` stream.
- Create the `readings` topic on that stream with `laser.topic("readings").ensure(2)`.
- Publish two JSON messages with `topic.publish().json(&reading)?.send()`.
- Read them back through one typed handle, `topic.json::<Reading>().records(..)`, from offset 0 until the reader catches up.

Each run deletes and re-creates the example stream, so it reads back exactly its own two readings.

## Run it

Run from `examples/rust`:

```sh
just up && cargo run --example log
```

## Learn more

- Docs: https://docs.laserdata.cloud/laser-sdk/log
- Full system built on this primitive: [`native-streaming`](../native-streaming/README.md)
