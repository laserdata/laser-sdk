# query - queries that already ran

> A projection watches your topics and keeps an always-current table you can query - filter, aggregate, window, paginate, even search by meaning. Like a materialized view, except you never refresh it.

## What it shows

- Declares this run's `orders_v1_<token>` view over the `orders` topic (`ensureView` with `indexFor`), so repeat and concurrent runs never count each other's rows, bound to an embedded managed table. Naming the index apart from its source topic is what lets a view be versioned without renaming the topic.
- Publishes three orders with a `status` field and waits for the projector to materialize them.
- Queries the maintained view with `laser.query(INDEX).whereEq("status", "paid").limit(10).fetch()` and reads the matching rows. `whereEq` matches an indexed key, the cheap path a projection's key columns answer directly, and `filterEq` and its siblings cover the rest.

## Run it

Run `npm run setup` once, then run from `examples/typescript`:

```sh
npm run example:query
```

This is managed by `laser-plane` in Laser Stack or LaserData Cloud. On Apache Iggy without `laser-plane`, the example prints a skip notice and returns.

```sh
LASER_CONNECTION_STRING=user:pwd@your-host npm run example:query
```

## Learn more

- Docs: https://docs.laserdata.cloud/laser-sdk/views
- Full system built on this primitive: [`order-book`](../order-book) - the same projection pattern powering a live order book and materialized trade tape.
