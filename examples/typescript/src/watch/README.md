# watch - stop re-querying blind

> Poll a lightweight advancement feed, then query only when the view has moved. The feed rides the connection you already have.

## What it shows

- Declares the same notify-enabled view shape as `query` under this run's own `orders_v1_<token>` name, so this entry point runs on its own with no shared state.
- Opens a change feed reader with `laser.watch().index(INDEX).records()`. The per-run view starts empty, so the first change it reports is this run's own publish.
- Publishes an order and polls for the lightweight change record, reading the batch it reports: rows landed, and the source offsets they came from.

## Run it

Run `npm run setup` once, then run from `examples/typescript`:

```sh
npm run example:watch
```

This is managed by `laser-plane` in Laser Stack or LaserData Cloud. On Apache Iggy without `laser-plane`, the example prints a skip notice and returns.

```sh
LASER_CONNECTION_STRING=user:pwd@your-host npm run example:watch
```

## Learn more

- Docs: https://docs.laserdata.cloud/laser-sdk/change-feed
- Full system built on this primitive: [`event-analytics`](../event-analytics) - the same change feed driving live analytics instead of a one-shot print.
