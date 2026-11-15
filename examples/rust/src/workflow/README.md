# workflow: three in-process steps

This example runs a three-step workflow whose steps run in the calling process and prints the outcome as JSON. It copies alone: it uses only `laser-sdk`, `tokio`, `serde` and `serde_json`, and no helper from this crate.

## What it shows

- `Laser::connect_env()` reads `LASER_CONNECTION_STRING`.
- `laser.scratch_stream("workflow-demo")` creates a uniquely named stream, and `scratch.delete()` removes it whatever the run returned.
- `workflow("job-runner").step_local(label, run)` adds a step whose body gets the prior outputs through `StepContext::outputs`. `.after(label)` orders it. The run journals each completion, so a resumed run keeps a recorded output instead of running the step again.
- The run creates its own topics, so the program needs no `bootstrap`, agent or handler.
- `WorkflowOutcome` carries `run_id`, `status`, `completed_steps` in completion order, and `text(label)` for each output.

## Run it

Run from `examples/rust`:

```sh
LASER_CONNECTION_STRING='<connection string>' cargo run --example workflow
```

It runs on Apache Iggy, Laser Stack, and LaserData Cloud.

## Learn more

- Docs: https://docs.laserdata.com/laser-sdk/workflow
- A workflow of routed steps with agents, a budget and a verifier: [`orchestra`](../orchestra/README.md)
