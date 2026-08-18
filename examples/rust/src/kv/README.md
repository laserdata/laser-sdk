# kv - the State primitive

A key-value store living next to your log: point reads, compare-and-set, TTLs. Forks give you git-like copies of your data. Branch it, try something, then promote it or throw it away.

Managed by `laser-plane` in Laser Stack or LaserData Cloud. On Apache Iggy without `laser-plane`, this prints one pointer and exits clean. Compare-and-swap, the fenced-lease contract, and forks are separately advertised capabilities, so each act runs only where the deployment serves it.

## What it shows

- Set a JSON value with a TTL: `laser.kv("profiles").set("user:42").json(&profile)?.ttl(..).send()`.
- Read it back typed: `kv.get_typed::<Profile>("user:42")`.
- Upgrade the same key under compare-and-swap: read the version with `kv.get_entry(..)`, then `set(..).expect_version(version).commit()`, so the write lands only if nobody moved first.
- Hold a revocable lease as `worker-a` (`kv.lease(lease_key, holder, ttl)`), read behind its barrier with `kv.get_entry_at_least(key, lease.position)` so a fresh holder never plans against its predecessor's state, write under its fence with `kv.cas_fenced(key, fence_namespace, fence_key, lease.token).expect_version(version).commit()`, renew at the same fence, release, and see the released fence refused as `lease-lost` - the at-most-one-effective-writer gate.
- Open a severed fork, write one speculative row with `put_row(..).field(..).send()`, and promote it, keeping the change.

## Run it

Run from `examples/rust`:

```sh
LASER_CONNECTION_STRING=user:pwd@your-laserdata-cloud-host \
  cargo run --example kv
```

## Learn more

- Docs: https://docs.laserdata.cloud/laser-sdk/state
- Full system built on this primitive: [`concierge`](../concierge/README.md)
