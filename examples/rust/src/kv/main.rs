use laser_examples::{init_tracing, laser, managed_feature_ready, phase, stream_for};
use laser_sdk::prelude::full::*;
use serde::{Deserialize, Serialize};
use std::time::Duration;

// The State primitive: fast keyed state next to the log (KV), with
// git-like copy-on-write forks to try changes before keeping them. Both
// surfaces are managed by laser-plane in Laser Stack or LaserData Cloud.
const NAMESPACE: &str = "profiles";
const KEY: &str = "user:42";
const FORK: &str = "experiment-1";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Profile {
    plan: String,
}

#[tokio::main]
async fn main() -> Result<(), LaserError> {
    init_tracing();
    let laser = laser(&stream_for("kv"), Capabilities::OPEN).await?;
    let capabilities = laser.capabilities().await;
    if !capabilities.kv.available {
        managed_feature_ready(false, "state (kv)", "kv");
        return Ok(());
    }

    phase("set and get keyed state");
    let kv = laser.kv(NAMESPACE);
    kv.set(KEY)
        .json(&Profile {
            plan: "pro".to_owned(),
        })?
        .ttl(Duration::from_secs(86_400))
        .send()
        .await?;
    let profile = kv.get_typed::<Profile>(KEY).await?;
    println!("  {KEY} is on {}", plan_of(profile.as_ref()));

    if capabilities.kv.cas {
        phase("compare-and-swap: the write lands only if nobody moved first");
        let entry = kv
            .get_entry(KEY)
            .await?
            .ok_or_else(|| LaserError::Invalid(format!("{KEY} vanished")))?;
        kv.set(KEY)
            .json(&Profile {
                plan: "enterprise".to_owned(),
            })?
            .expect_version(entry.version)
            .commit()
            .await?;
        let upgraded = kv.get_typed::<Profile>(KEY).await?;
        println!(
            "  version {} accepted the upgrade, {KEY} is now on {}",
            entry.version,
            plan_of(upgraded.as_ref())
        );
    }

    if capabilities.forks {
        phase("fork: a branch of the same state, promoted or thrown away");
        let fork = laser.fork(FORK);
        fork.squash().await?;
        fork.create().severed().tables([NAMESPACE]).send().await?;
        fork.put_row(NAMESPACE, 0, 0)
            .field("plan", "enterprise-preview")
            .send()
            .await?;
        let applied = fork.promote().await?;
        println!("  fork `{FORK}` promoted, {applied} row(s) applied");
    }
    Ok(())
}

fn plan_of(profile: Option<&Profile>) -> &str {
    profile.map_or("no plan", |profile| profile.plan.as_str())
}
