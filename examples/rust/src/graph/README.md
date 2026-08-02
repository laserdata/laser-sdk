# graph - the Graph primitive

Nodes and edges built from what flows through your log - who bought what, which agent said what, what depends on what. Traverse it, search it by meaning, and ask what was true at any point in time.

Managed by `laser-plane` in Laser Stack or LaserData Cloud. On Apache Iggy without `laser-plane`, this prints one pointer and exits clean.

## What it shows

- Relate entities in one call: `laser.graph("kg").link("customer:42", "purchased", "product:7")`, which upserts both content-addressed nodes and the typed edge, so re-linking the same triple converges instead of growing.
- Rebuild the same node id locally (`GraphNode::entity("customer", "42").id`), because a node is addressed by its content and never by a server-assigned key.
- Traverse one relation out of it: `.neighbors(customer, EdgeDir::Out, Some("purchased".to_owned()), 1)`.

## Run it

Run from `examples/rust`:

```sh
LASER_CONNECTION_STRING=user:pwd@your-laserdata-cloud-host \
  cargo run --example graph
```

## Learn more

- Docs: https://docs.laserdata.cloud/laser-sdk/graph
- Full system built on this primitive: [`memory`](../memory/README.md)
