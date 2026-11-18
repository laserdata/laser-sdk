// cargo add laser-sdk --features agent,kv
// cargo add tokio --features macros,rt-multi-thread,time
// cargo add serde --features derive
// cargo add serde_json
// cargo add async-trait
// Needs only LASER_CONNECTION_STRING, which connect_env() reads. No server to start when it is set.
// Check: echo "${LASER_CONNECTION_STRING:+configured}"
use laser_sdk::prelude::*;
use serde::Serialize;
use std::time::Duration;

#[derive(Serialize)]
struct Report {
    run_id: String,
    status: SessionStatus,
    completed_steps: Vec<String>,
    report: Option<String>,
}

struct Analyze;

#[async_trait::async_trait]
impl StepFn for Analyze {
    async fn build(&self, context: &StepContext<'_>) -> Result<Vec<u8>, LaserError> {
        let ingested = context.outputs.get("ingest").cloned().unwrap_or_default();
        tokio::time::sleep(Duration::from_secs(1)).await;
        Ok([b"analyzed ".to_vec(), ingested].concat())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = Laser::connect_env().await?;
    let scratch = root.scratch_stream("workflow").await?;
    let result = run_pipeline(scratch.laser()).await;
    scratch.delete().await?;
    let outcome = result?;

    let report = Report {
        run_id: outcome.run_id.to_string(),
        report: outcome.text("report"),
        status: outcome.status,
        completed_steps: outcome.completed_steps,
    };
    println!("{}", serde_json::to_string(&report)?);
    Ok(())
}

async fn run_pipeline(laser: &Laser) -> Result<WorkflowOutcome, LaserError> {
    laser
        .workflow("pipeline")
        .step_local("ingest", |_: &StepContext<'_>| {
            b"frames from sat-7".to_vec()
        })
        .step_local("analyze", Analyze)
        .after("ingest")
        .step_local("report", |context: &StepContext<'_>| {
            let analyzed = context.outputs.get("analyze").cloned().unwrap_or_default();
            [b"report on ".to_vec(), analyzed].concat()
        })
        .after("analyze")
        .run()
        .await
}
