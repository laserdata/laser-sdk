# workflow: three in-process steps, one of them async

This example runs a three-step workflow whose steps run in the calling process and prints the outcome as JSON. It copies alone: it uses only `laser-sdk`, `tokio`, `serde`, `serde_json` and `async-trait`, and no helper from this crate.

## What it shows

- `Laser::connect_env()` reads `LASER_CONNECTION_STRING`.
- `root.scratch_stream("workflow")` creates a uniquely named stream, and `scratch.delete()` removes it whatever the run returned.
- `workflow("pipeline").step_local(label, run)` adds a step whose body gets the prior outputs as bytes through `StepContext::outputs`. `.after(label)` orders it. The run journals each completion, so a resumed run keeps a recorded output instead of running the step again.
- A closure step cannot await, so `analyze` is a type implementing `StepFn` under `#[async_trait::async_trait]` that sleeps one second, passed in place of the closure.
- The run creates its own topics, so the program needs no `bootstrap`, agent or handler.
- `WorkflowOutcome` carries `run_id`, `status`, `completed_steps` in completion order, `outputs` by label as bytes, and `text(label)`. The program prints `run_id`, `status`, `completed_steps` and `report`, the same fields in the same order as the Python and TypeScript programs.

## Run it

Run from `examples/rust`:

```sh
LASER_CONNECTION_STRING='<connection string>' cargo run --example workflow
```

It runs on Apache Iggy, Laser Stack, and LaserData Cloud.

## Learn more

- Docs: https://docs.laserdata.com/laser-sdk/workflow
- A workflow of routed steps with agents, a budget and a verifier: [`orchestra`](../orchestra/README.md)
