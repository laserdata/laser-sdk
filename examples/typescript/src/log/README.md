# log - every message, written once, readable forever

> A topic is an append-only record of every message in your system. Services write to it and read from it like a group chat that never loses a message. New readers start from the beginning or jump straight to now.

## What it shows

- Ensures the `shop/orders` topic with two partitions.
- Publishes two JSON orders with `topic.publish().json(value).send()`.
- Opens one typed, codec-bound reader (`topic.json(codec).records(readerName)`) and drains it from offset 0 until both orders are back.
- Run it twice and the second run replays four orders: the log keeps every record, and a fresh reader starts at offset 0. That is the primitive, not a bug.

## Run it

Run `npm run setup` once, then run from `examples/typescript`:

```sh
npm run example:log
```

Runs against Apache Iggy - no LaserData Cloud needed. To use another server, pass a bare target.

```sh
LASER_CONNECTION_STRING=user:pwd@your-host npm run example:log
```

## Learn more

- Docs: https://docs.laserdata.cloud/laser-sdk/log
- Full system built on this primitive: [`native-streaming`](../native-streaming) - the same log, wired into producers, batches, and consumer groups.
