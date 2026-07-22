# concierge - an AI support desk on the log

The full-stack example combines a queryable ticket world model, semantic
resolution memory, four agents, durable credit deduplication, approval, a
copy-on-write plan, and conversation replay. The SDK transports model context
and results. The deterministic `MockLlm` remains an example-owned dependency.

## What it does

1. Publishes deterministic support tickets into a body-extracted projection.
2. Seeds and recalls semantic resolution notes.
3. Runs triage, specialist, resolver, and approver agents over AGDX topics.
4. Applies each remediation credit once even when every command is sent twice.
5. Holds large credits for a correlated approval response.
6. Stages a bulk resolution row in `bulk-resolve-plan` and optionally promotes it.
7. Remembers the diagnosis and rebuilds the incident from its conversation log.

## Run it

```sh
npm run build
node dist/src/concierge/main.js

LASER_CONNECTION_STRING=iggy://user:password@your-laserdata-cloud-host \
  node dist/src/concierge/main.js

LASER_MESSAGES=200000 LASER_BATCH=1000 node dist/src/concierge/main.js
LASER_APPLY_PLAN=1 node dist/src/concierge/main.js
```

Query, KV CAS, and forks are required for the full desk. On Apache Iggy the
example reports those missing capabilities and exits before provisioning the
managed phase.
