# governance - permissions at every boundary

The local phase proves deny-wins matching, delegated permission intersection,
default deny, and edge scope step-up without a server. On a deployment that
advertises authorization, the example defines and binds a role, reads the
effective identity, lists the role prefix, and submits a budgeted run when the
run registry is available.

## Run it

```sh
npm run build
node dist/src/governance/main.js

LASER_GOVERNANCE_USER_ID=1 \
LASER_CONNECTION_STRING=iggy://user:password@your-laserdata-cloud-host \
  node dist/src/governance/main.js
```

The pure policy phase runs on Apache Iggy. Managed RBAC and run operations are
capability-gated and failures are not converted into successful skips.
