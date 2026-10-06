# graph - the Graph primitive

A graph connects entities through named relationships. This example writes a relationship and reads the neighboring entities.

Managed by `laser-plane` in Laser Stack or LaserData Cloud. On Apache Iggy without `laser-plane`, this prints one pointer and exits clean.

## What it shows

- Use `laser.graph("kg").link("host:node-7", "runs", "service:auth")` to write two entity nodes and their relationship. Repeating the same link preserves their content IDs.
- Use `GraphNode::entity("host", "node-7").id` to calculate the same node ID locally.
- Traverse one relation out of it: `.neighbors(host, EdgeDir::Out, Some("runs".to_owned()), 1)`.

## Run it

Run from `examples/rust`:

```sh
LASER_CONNECTION_STRING=user:pwd@your-laserdata-cloud-host \
  cargo run --example graph
```

## Learn more

- Docs: https://docs.laserdata.cloud/laser-sdk/graph
- Full system built on this primitive: [`memory`](../memory/README.md)
