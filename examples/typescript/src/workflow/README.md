# workflow: three plain functions as durable steps

This program runs a durable workflow of three steps in the calling process, then prints the run's status, the completed steps in order, and the workflow's value as JSON. It imports only `@laserdata/laser-sdk` and Node's own `node:timers/promises`, so it runs when copied alone into any project that installed the package.

## What it shows

- `step(name, fn)` makes a function a step and `workflow(name, fn)` makes a function a workflow. The workflow calls its steps in ordinary code.
- `pipeline.run()` reads the connection string from `LASER_CONNECTION_STRING`, runs the function, and closes its connection when the run ends.
- The run uses the stream named by `LASER_STREAM`, or a stream named `pipeline` after the workflow. It creates the stream and its topics when they are missing and keeps them after the run, so the run can be resumed.
- A step takes and returns what JSON holds. A step name holding `#` is refused, because the journal numbers repeated calls with it.
- The run journals each completed step, so `pipeline.resume(runId)` returns the recorded values without running those steps again. A resumed run must call the same steps in the same order. A failed run returns no outcome, so a caller that wants to resume one names the run ID first with `pipeline.resume(runId)`.
- `run.status`, `run.completedSteps`, and `run.value` report the run, and `run.json(label)` and `run.text(label)` read one step's output.
- For steps routed to agents, verifiers, compensation and budgets, use the builder, `laser.workflow(name).step(...)`, with `stepLocal` for its in-process steps.

## Run it

```sh
LASER_CONNECTION_STRING='<connection string>' npm run example:workflow
```

It prints `{"status":"completed","steps":["ingest","analyze","report"],"report":"report on analyzed frames from sat-7"}` and runs against plain Apache Iggy, Laser Stack, and LaserData Cloud.
