# kv - fast keyed state, with an undo button

This example reads and writes key-value state. It also demonstrates conditional updates, revocable leases, and copy-on-write forks where the deployment supports them.

## What it shows

- Sets a JSON service config under `config/service:auth` with a 24-hour TTL (`kv.set(key).json(value).ttl(micros).send()`) and reads it back with `kv.get(key)`.
- Changes the same key under compare-and-swap: reads the version with `kv.getEntry(key)`, then `set(key).json(value).expectVersion(version).commit()`, so the write lands only if nobody moved first.
- Acquires a lease as `worker-a` through `kv.lease(leaseKey, holder, ttlMicros)`, reads at the grant position with `kv.getEntryAtLeast`, then writes with its fence through `kv.casFenced`. It renews and releases the lease, then shows that the released token is refused.
- Creates a severed fork named `experiment-1`, writes one speculative row with `putRow(..).field(..).send()`, and promotes it back onto the trunk.

Compare-and-swap, the fenced-lease contract, and forks are separately advertised capabilities, so each act runs only where the deployment serves it.

## Run it

Run `npm run setup` once, then run from `examples/typescript`:

```sh
npm run example:kv
```

This is managed by `laser-plane` in Laser Stack or LaserData Cloud. On Apache Iggy without `laser-plane`, the example prints a skip notice and returns.

```sh
LASER_CONNECTION_STRING=user:pwd@your-host npm run example:kv
```

## Learn more

- Docs: https://docs.laserdata.cloud/laser-sdk/state
- Full system built on this primitive: [`incident-desk`](../incident-desk) - keyed state and forks used for real quota and what-if branching.
