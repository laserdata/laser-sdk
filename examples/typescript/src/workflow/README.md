# workflow: three in-process steps, one of them async

This program runs a durable workflow of three steps whose bodies run in the calling process, then prints the run id, its end status, the completed steps in order, and the last step's output as JSON. It imports only `@laserdata/laser-sdk` and Node's own `node:timers/promises`, so it runs when copied alone into any project that installed the package.

## What it shows

- `Laser.connectEnv()` reads the connection string from `LASER_CONNECTION_STRING`.
- `root.scratchStream("workflow")` creates a stream with a unique name, and `await using` deletes it when the program ends.
- `scratch.laser.workflow("pipeline").stepLocal(label, run)` adds a step whose body runs here. `run` gets `{ outputs }`, the outputs of the steps before it by label as bytes, and returns a string. `analyze` is an async function that awaits one second. `.after(label)` orders the steps.
- The first run on the new stream creates the agent topics with the `bootstrap` defaults, so the program needs no `bootstrap`, agent, or handler.
- `outcome.status`, `outcome.completedSteps`, and `outcome.text(label)` report the run. The program prints `run_id`, `status`, `completed_steps` and `report`, the same fields in the same order as the Python and Rust programs. Running the same workflow again with `.runId(outcome.runId)` resumes it and skips every step the journal recorded.

## Run it

```sh
LASER_CONNECTION_STRING='<connection string>' npm run example:workflow
```

It runs against plain Apache Iggy, Laser Stack, and LaserData Cloud.
