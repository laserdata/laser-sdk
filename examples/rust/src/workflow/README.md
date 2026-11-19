# workflow: three steps in an async closure

This example runs a workflow of three steps in the calling process and prints the outcome as JSON. It copies alone: it uses only `laser-sdk`, `tokio` and `serde_json`, and no helper from this crate.

## What it shows

- `Laser::connect_env()` reads `LASER_CONNECTION_STRING`.
- `laser.workflow("pipeline").run_with(async |run| ...)` runs the closure as the workflow. The run uses the stream named by `LASER_STREAM`, or a stream named `pipeline` after the workflow. It creates the stream when it is missing and keeps it after the run, so the run can be resumed.
- `run.step(label, async || ...)` runs one step. Its body can await, and its value is any type that implements `Serialize` and `DeserializeOwned`. A step name holding `#` is refused, because the journal numbers repeated calls with it.
- The run journals each completed step, so `laser.workflow(name).run_id(id).run_with(...)` returns the recorded values without running those steps again. A resumed run must call the same steps in the same order. A failed run returns no outcome, so a caller that wants to resume one names the run ID first with `run_id(id)`.
- The run creates its own topics, so the program needs no `bootstrap`, agent or handler.
- `WorkflowOutcome` carries `run_id`, `status`, `completed_steps` in completion order, `value`, `outputs` by label as bytes, `json::<T>(label)` and `text(label)`. The program prints `status`, `steps` and `report`.
- For steps routed to agents, verifiers, compensation and budgets, use the builder, `laser.workflow(name).step(...)`, with `step_local` for its in-process steps.

## Run it

Run from `examples/rust`:

```sh
LASER_CONNECTION_STRING='<connection string>' cargo run --example workflow
```

It prints `{"report":"report on analyzed frames from sat-7","status":"completed","steps":["ingest","analyze","report"]}` and runs on Apache Iggy, Laser Stack, and LaserData Cloud.

## Learn more

- Docs: https://docs.laserdata.com/laser-sdk/workflow
- A workflow of routed steps with agents, a budget and a verifier: [`orchestra`](../orchestra/README.md)
