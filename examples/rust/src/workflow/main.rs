use laser_sdk::agent::StepContext;
use laser_sdk::prelude::*;
use laser_sdk::wire::agent::SessionStatus;
use serde::Serialize;
use std::collections::BTreeMap;

#[derive(Serialize)]
struct Report {
    run_id: ConversationId,
    status: SessionStatus,
    completed_steps: Vec<String>,
    outputs: BTreeMap<String, String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let laser = Laser::connect_env().await?;
    let scratch = laser.scratch_stream("workflow-demo").await?;
    let report = run(scratch.laser()).await;
    scratch.delete().await?;
    println!("{}", serde_json::to_string_pretty(&report?)?);
    Ok(())
}

async fn run(laser: &Laser) -> Result<Report, LaserError> {
    let outcome = laser
        .workflow("job-runner")
        .step_local("create", |_: &StepContext<'_>| b"job-42".to_vec())
        .step_local("process", |context: &StepContext<'_>| {
            let job = context.outputs.get("create").cloned().unwrap_or_default();
            [b"processed ".to_vec(), job].concat()
        })
        .after("create")
        .step_local("finish", |_: &StepContext<'_>| b"completed".to_vec())
        .after("process")
        .run()
        .await?;
    let outputs = outcome
        .completed_steps
        .iter()
        .map(|label| (label.clone(), outcome.text(label).unwrap_or_default()))
        .collect();
    Ok(Report {
        run_id: outcome.run_id,
        status: outcome.status,
        completed_steps: outcome.completed_steps,
        outputs,
    })
}
