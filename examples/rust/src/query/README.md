# query - the Views primitive

A projection watches your topics and keeps an always-current table you can query. Filter, aggregate, window, paginate, even search by meaning. Like a materialized view, except you never refresh it.

Managed by `laser-plane` in Laser Stack or LaserData Cloud. On Apache Iggy without `laser-plane`, this prints one pointer and exits clean.

## What it shows

- Ensure the `orders` topic, then declare this run's `orders_v1_<token>` view over it (`laser_examples::ensure_view` with `index_for`), so repeat and concurrent runs never count each other's rows. Naming the index apart from its source topic is what lets a view be versioned without renaming the topic.
- Publish three orders with a `status` field.
- Wait for the projector to materialize them (`laser_examples::wait_for_rows`).
- Query the maintained view: `laser.query(&index)`.where_eq("status", "paid").limit(10).fetch()`. `where_eq` matches an indexed key, the cheap path a projection's key columns answer directly, and `filter_eq` and its siblings cover the rest.

## Run it

Run from `examples/rust`:

```sh
LASER_CONNECTION_STRING=user:pwd@your-laserdata-cloud-host \
  cargo run --example query
```

## Learn more

- Docs: https://docs.laserdata.cloud/laser-sdk/views
- Full system built on this primitive: [`order-book`](../order-book/README.md)
