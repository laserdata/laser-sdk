# query - the Views primitive

A projector turns topic records into a queryable view. This example waits for projection, then reads orders that match a filter.

Managed by `laser-plane` in Laser Stack or LaserData Cloud. On Apache Iggy without `laser-plane`, this prints one pointer and exits clean.

## What it shows

- Create `orders` and this run's `orders_v1_<token>` view through `laser_examples::ensure_view` and `index_for`. Unique view names keep repeated and concurrent runs separate.
- Publish three orders with a `status` field.
- Wait for the projector to materialize them (`laser_examples::wait_for_rows`).
- Query the view with `laser.query(&index).where_eq("status", "paid").limit(10).fetch()`. `where_eq` uses indexed keys. `filter_eq` and related methods cover other predicates.

## Run it

Run from `examples/rust`:

```sh
LASER_CONNECTION_STRING=user:pwd@your-laserdata-cloud-host \
  cargo run --example query
```

## Learn more

- Docs: https://docs.laserdata.cloud/laser-sdk/views
- Full system built on this primitive: [`order-book`](../order-book/README.md)
