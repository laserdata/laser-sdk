# log - the Log primitive

A topic stores records in append order under its retention policy. This example publishes typed host readings and reads them by offset.

## What it shows

- Connect once with `laser_examples::laser(..)`.
- Create a topic (`laser.stream("fleet").topic("readings")`) with `ensure(2)`.
- Publish two JSON messages (`topic.publish().json(&reading)?.send()`).
- Read them back through one typed handle (`topic.json::<Reading>().records(..)`), draining from offset 0 until caught up.
- Run it twice against the same retained topic to read four readings on the second run. Each new reader starts at offset 0.

## Run it

Run from `examples/rust`:

```sh
just up && cargo run --example log
```

## Learn more

- Docs: https://docs.laserdata.cloud/laser-sdk/log
- Full system built on this primitive: [`native-streaming`](../native-streaming/README.md)
