# orchestra - contracts, panels, workflows, and recovery

Six long-lived workers demonstrate directed contracts, capability scatter,
journalled workflow steps, quarantine routing, recovery, and deadline-bound
redispatch. An unavailable advertised diagnostic target is excluded from every
panel. All coordination runs over Apache Iggy.

## Run it

```sh
npm run build
node dist/src/orchestra/main.js

# CI and unattended runs
LASER_NON_INTERACTIVE=1 node dist/src/orchestra/main.js
```

The example keeps every worker alive for the full run and shuts handles down in
reverse startup order. Each phase asserts its routing or lifecycle outcome
before continuing.
