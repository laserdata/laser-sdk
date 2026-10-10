# governance: roles, delegation, and budgets

This example defines access roles on a managed deployment and shows the policy decisions the SDK makes without a server. It ends by submitting a session with a token budget.

## What it does

1. Capability RBAC. When the deployment advertises `authz`, the example defines four roles: `support-reader`, `projection-operator`, `agent-runner`, and `safety-deny`. It binds them to a dedicated `governance-demo` Iggy user that it creates on the first run through `laser.client.user`, or to `LASER_GOVERNANCE_USER_ID` when set. It never binds them to the caller, because `safety-deny` holds a deny-wins grant that would restrict every later run. It then reads back `whoami`, the roles that start with `support`, and the user's bindings.
2. Permission intersection. An agent acting for a user may do only what both grant sets allow. The example checks a read, a write, and a delete with `delegatedAllow`, and a direct delete with `grantsAllow`.
3. External edge. `authorizeEdge` accepts a token with the right audience and scope, asks for step-up when the scope is missing, and rejects a foreign audience.
4. Session governor. `laser.sessions().submit(..).budget({ tokens: 4_000n })` submits a session to `governance-auditor` with a token ceiling.

The decision checks and the session submission run on Apache Iggy. The RBAC phase prints the missing capability there and skips.

## Run it

Run `npm run setup` once, then run from `examples/typescript`:

```sh
npm run example:governance
```

Run the RBAC phase against Laser Stack or LaserData Cloud:

```sh
LASER_CONNECTION_STRING=user:pwd@your-laserdata-cloud-host \
  npm run example:governance
```

## Where to look (LaserData Cloud)

- Roles: `support-reader`, `projection-operator`, `agent-runner`, and `safety-deny`.
- Bindings: the four roles on `governance-demo`, or on `LASER_GOVERNANCE_USER_ID`.
- Sessions: the submitted `governance-auditor` session with its token budget.

## Highlights

- Grants use one `effect`, `feature`, `action`, and resource grammar for client checks and managed RBAC.
- Deny wins whenever an allow and a deny match the same operation.
- Delegation never exceeds the agent's grants or the user's grants.
- Edge authorization tells a wrong audience apart from a missing scope that can step up.
- A session budget is a ceiling the runtime compares usage with. It does not grant permission.
