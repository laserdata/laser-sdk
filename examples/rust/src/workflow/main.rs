// cargo add laser-sdk
// cargo add tokio --features macros,rt-multi-thread,time
// cargo add serde_json
// Needs only LASER_CONNECTION_STRING, which connect_env() reads. No server to start when it is set.
use laser_sdk::prelude::*;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), LaserError> {
    let laser = Laser::connect_env().await?;
    let run = laser
        .workflow("pipeline")
        .run_with(async |run| {
            let frames: String = run
                .step("ingest", async || Ok("frames from sat-7".to_owned()))
                .await?;
            let analysis: String = run
                .step("analyze", async || {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    Ok(format!("analyzed {frames}"))
                })
                .await?;
            run.step("report", async || Ok(format!("report on {analysis}")))
                .await
        })
        .await?;
    let report = serde_json::json!({
        "status": run.status,
        "steps": run.completed_steps,
        "report": run.json::<String>("report")?,
    });
    println!("{report}");
    laser.close().await
}
