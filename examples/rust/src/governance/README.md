# governance

Capability RBAC and agent governance over the managed surfaces.

## What it does

`governance` installs a small role set when the connected deployment advertises `authz`, binds those roles to an Iggy user, and reads back `whoami`, role browse, and bindings through the SDK.

It also demonstrates the governance decisions that do not need a server: deny-wins grant matching, on-behalf-of permission intersection, and external-edge audience plus step-up checks. When the managed run registry is advertised, it submits a run with a multi-dimensional `RunBudget`.

## Run it

```sh
cargo run --release --example governance
```

On raw Apache Iggy the live RBAC and run-registry phases skip cleanly. Point the same binary at LaserData Cloud to exercise the full managed path:

```sh
LASER_CONNECTION_STRING='iggy+tcp://user:pwd@your-host' \
LASER_GOVERNANCE_USER_ID=42 \
  cargo run --release --example governance
```

## Where to look (LaserData Cloud)

In LaserData Cloud, open the access or roles view to see:

- `support-reader`, `projection-operator`, `agent-runner`, and `safety-deny`
- the configured binding for `LASER_GOVERNANCE_USER_ID`
- the effective grant preview with the deny applied

## Highlights

- Managed surfaces use `effect feature:action [on resource-pattern]`, assembled through roles bound to the server-stamped Iggy user.
- An agent acting on behalf of a user is allowed only where both grant sets allow the operation.
- External MCP/A2A edge requests distinguish a wrong audience from a missing scope that can step up.
- Run budgets are a governor, not a grant.
