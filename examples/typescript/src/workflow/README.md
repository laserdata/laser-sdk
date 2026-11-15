# workflow: three in-process steps, journaled and resumable

This program runs a durable workflow of three steps whose bodies run in the calling process, then prints the run id, its end status, the completed steps in order, and each step's output as JSON. It imports only `@laserdata/laser-sdk`, so it runs when copied alone into any project that installed the package.

## What it shows

- `Laser.connectEnv()` reads the connection string from `LASER_CONNECTION_STRING`.
- `laser.scratchStream("workflow-demo")` creates a stream with a unique name, and `await using` deletes it when the program ends.
- `scratch.laser.workflow("job-runner").stepLocal(label, run)` adds a step whose body runs here. `run` gets the outputs of the steps before it and returns a string. `.after(label)` orders the steps.
- The first run on the new stream creates the agent topics with the `bootstrap` defaults, so the program needs no `bootstrap`, agent, or handler.
- `outcome.status`, `outcome.completedSteps`, and `outcome.text(label)` report the run. Running the same workflow again with `.runId(outcome.runId)` resumes it and skips every step the journal recorded.

## Run it

```sh
LASER_CONNECTION_STRING='<connection string>' npm run example:workflow
```

It runs against plain Apache Iggy, Laser Stack, and LaserData Cloud.
