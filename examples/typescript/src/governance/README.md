# governance - permissions at every boundary

> Pure grant decisions run locally, while managed RBAC and budgeted runs use the same policy vocabulary on LaserData Cloud.

## What it does

1. Proves that a matching deny overrides a broader allow.
2. Proves that an agent acting on behalf of a user receives only the intersection of both grant sets.
3. Proves default deny when the invoking user has no matching grant.
4. Checks an MCP edge request and renders the step-up challenge for a missing scope.
5. Defines the `support-reader` role when the deployment advertises authorization.
6. Binds that role to `LASER_GOVERNANCE_USER_ID`.
7. Reads back the server-stamped identity, role listing, and user bindings.
8. Submits a run with event, model-call, and wall-clock budgets when the run registry is available.

## Run it

```sh
npm run example:governance
```

The pure grant and edge checks run on Apache Iggy. Managed authorization and run submission are capability-gated.

```sh
LASER_CONNECTION_STRING=user:pwd@your-laserdata-cloud-host \
LASER_GOVERNANCE_USER_ID=42 \
  npm run example:governance
```

## Where to look (LaserData Cloud)

- **Roles**: the `support-reader` role with its allow and explicit deny.
- **Bindings**: the role assignment for `LASER_GOVERNANCE_USER_ID`.
- **Identity**: the effective roles returned by `whoami`.
- **Runs**: the `governed-agent` run and its multidimensional budget.

## Highlights

- Grants use one `effect`, `feature`, `action`, and resource grammar across client checks and managed RBAC.
- Deny wins whenever both an allow and deny match the same operation.
- Delegation cannot exceed either the agent's grants or the invoking user's grants.
- Edge authorization distinguishes a wrong audience from a missing scope that can be stepped up.
- A run budget constrains execution volume and time. It does not grant permission.
