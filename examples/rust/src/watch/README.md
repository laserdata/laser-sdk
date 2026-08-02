# watch - the Change feed primitive

Poll a lightweight advancement feed, then query only when the view has moved. The feed rides the connection you already have and replaces repeated blind queries with a small change record.

Managed by `laser-plane` in Laser Stack or LaserData Cloud, using the same view shape as the `query` example under this run's own `orders_v1_<token>` name. On Apache Iggy without `laser-plane`, this prints one pointer and exits clean.

## What it shows

- Declare the same view shape the `query` example builds under this run's own name, so this binary runs on its own with no shared state.
- Open a change-feed reader: `laser.watch().index(&index).records()?`. The per-run view starts empty, so the first change it reports is this run's own publish.
- Publish an order that advances the view.
- Poll until the advance arrives, and read the batch it reports: rows landed, and the source offsets they came from.

## Run it

Run from `examples/rust`:

```sh
LASER_CONNECTION_STRING=user:pwd@your-laserdata-cloud-host \
  cargo run --example watch
```

## Learn more

- Docs: https://docs.laserdata.cloud/laser-sdk/change-feed
- Full system built on this primitive: [`event-analytics`](../event-analytics/README.md)
