# graph - the relationships your messages imply

A graph connects entities through named relationships. This example writes a relationship and reads the neighboring entities.

## What it shows

- Links entities in the `kg` graph with `graph.link("customer:42", "purchased", "product:7")`, upserting both content-addressed nodes and the typed edge, so re-linking the same triple converges instead of growing.
- Rebuilds the same node id locally with `graphNodeEntity("customer", "42")`, because a node is addressed by its content and never by a server-assigned key.
- Traverses one relation out of it with `graph.neighbors(nodeId, "out", "purchased", 1)` and prints each neighbor as `kind:value`.

## Run it

Run `npm run setup` once, then run from `examples/typescript`:

```sh
npm run example:graph
```

This is managed by `laser-plane` in Laser Stack or LaserData Cloud. On Apache Iggy without `laser-plane`, the example prints a skip notice and returns.

```sh
LASER_CONNECTION_STRING=user:pwd@your-host npm run example:graph
```

## Learn more

- Docs: https://docs.laserdata.cloud/laser-sdk/graph
- Full system built on this primitive: [`memory`](../memory) - the same graph primitive used alongside durable memory in one woven scenario.
