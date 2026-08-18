# kv - fast keyed state, with an undo button

> A key-value store living next to your log: point reads, compare-and-set, TTLs. Forks give you git-like copies of your data - branch it, try something, then promote it or throw it away.

## What it shows

- Sets a JSON profile under `profiles/user:42` with a 24-hour TTL (`kv.set(key).json(value).ttl(micros).send()`) and reads it back with `kv.get(key)`.
- Upgrades the same key under compare-and-swap: reads the version with `kv.getEntry(key)`, then `set(key).json(value).expectVersion(version).commit()`, so the write lands only if nobody moved first.
- Holds a revocable lease as `worker-a` (`kv.lease(leaseKey, holder, ttlMicros)`), reads behind its barrier with `kv.getEntryAtLeast(key, lease.position)` so a fresh holder never plans against its predecessor's state, writes under its fence with `kv.casFenced(key, fenceNamespace, fenceKey, lease.token).expectVersion(version).commit()`, renews at the same fence, releases, and shows the released fence refused as `lease-lost` - the at-most-one-effective-writer gate.
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
- Full system built on this primitive: [`concierge`](../concierge) - keyed state and forks used for real session and what-if branching.
