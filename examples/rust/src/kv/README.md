# kv: the State primitive

This example reads and writes key-value state. It also demonstrates conditional updates, revocable leases, and copy-on-write forks where the deployment supports them.

This example requires `laser-plane` in Laser Stack or LaserData Cloud. Without it, the example explains the requirement and exits successfully. Conditional writes, fenced leases, and forks require their own reported capabilities.

Key-value entries, leases, and forks live under the stream's scoped names and belong to the example stream. A rerun recreates the stream, so its entries start again at version 1. Fence tokens keep counting across runs, so a token from an earlier run never matches a new lease.

## What it shows

- Set a JSON value with a TTL: `laser.kv("config").set("service:auth").json(&config)?.ttl(..).send()`.
- Read it back typed: `kv.get_typed::<ServiceConfig>("service:auth")`.
- Read its version with `kv.get_entry(..)`, then use `set(..).expect_version(version).commit()`. The write succeeds only if the version still matches.
- Acquire a lease as `worker-a` through `kv.lease(lease_key, holder, ttl)`. Read with `kv.get_entry_at_least(key, lease.position)`. Write through `kv.cas_fenced(key, fence_namespace, fence_key, lease.token).expect_version(version).commit()`. Renew and release the lease, then make sure that the released token returns `lease-lost`.
- Open a severed fork, write one speculative row with `put_row(..).field(..).send()`, and promote it, keeping the change.

## Run it

Run from `examples/rust`:

```sh
LASER_CONNECTION_STRING=user:pwd@your-laserdata-cloud-host \
  cargo run --example kv
```

## Learn more

- Docs: https://docs.laserdata.cloud/laser-sdk/state
- Full system built on this primitive: [`incident-desk`](../incident-desk/README.md)
