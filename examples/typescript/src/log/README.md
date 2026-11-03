# log: the Log primitive

A topic stores records in append order under its retention policy. This example publishes typed host readings and reads them by offset.

## What it shows

- Ensures the `readings` topic with two partitions on the example's own `laser-log-typescript` stream, through `laser.topic("readings")`.
- Publishes two JSON readings with `topic.publish().json(value).send()`.
- Opens one typed reader bound to a codec, `topic.json(codec).records(readerName)`, and reads it from offset 0 until both readings are back.

Each run deletes and re-creates the example stream, so it reads back exactly its own two readings.

## Run it

Run `npm run setup` once, then run from `examples/typescript`:

```sh
npm run example:log
```

It runs on Apache Iggy without LaserData Cloud. To use another server, pass a bare target.

```sh
LASER_CONNECTION_STRING=user:pwd@your-host npm run example:log
```

## Learn more

- Docs: https://docs.laserdata.cloud/laser-sdk/log
- Full system built on this primitive: [`native-streaming`](../native-streaming): the same log, wired into producers, batches, and consumer groups.
