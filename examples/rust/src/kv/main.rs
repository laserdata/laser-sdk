use laser_examples::{
    PARTITIONS, ensure_view, index_for, init_tracing, laser, managed_feature_ready, phase,
    stream_for,
};
use laser_sdk::prelude::full::*;
use serde::{Deserialize, Serialize};
use std::time::Duration;

// The State primitive: fast keyed state next to the log (KV), with
// git-like copy-on-write forks to try changes before keeping them. Both
// surfaces are managed by laser-plane in Laser Stack or LaserData Cloud.
const NAMESPACE: &str = "profiles";
const KEY: &str = "user:42";
const FORK: &str = "experiment-1";
const LEASE_KEY: &str = "lease:user:42";
const HOLDER: &str = "worker-a";
const LEASE_TTL: Duration = Duration::from_secs(30);

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

    if capabilities.kv.fenced_leases {
        phase("lease and fenced write: at most one effective writer");
        let lease = kv.lease(LEASE_KEY, HOLDER, LEASE_TTL).await?;
        println!("  {HOLDER} holds {LEASE_KEY} at fence {}", lease.token);
        // Barriered read: the answering fold has applied at least the grant, so
        // a holder that just took over never plans against its predecessor's
        // state.
        let held = kv
            .get_entry_at_least(KEY, lease.position)
            .await?
            .ok_or_else(|| LaserError::Invalid(format!("{KEY} vanished")))?;
        let fenced = kv
            .cas_fenced(KEY, NAMESPACE, LEASE_KEY, lease.token)
            .json(&Profile {
                plan: "enterprise-plus".to_owned(),
            })?
            .ttl(Duration::from_secs(86_400))
            .expect_version(held.version)
            .commit()
            .await?;
        let seen: Profile = serde_json::from_slice(&held.value)
            .map_err(|error| LaserError::Invalid(error.to_string()))?;
        println!(
            "  barriered read saw {}, the fenced write landed as version {fenced}",
            plan_of(Some(&seen))
        );
        let renewed = kv
            .renew_lease(LEASE_KEY, HOLDER, lease.token, LEASE_TTL)
            .await?;
        kv.release(LEASE_KEY, HOLDER, renewed.token).await?;
        println!(
            "  lease renewed at the same fence {}, then released",
            renewed.token
        );
        // The gate holds without waiting for a successor: a released fence is
        // already dead, so a zombie holder cannot commit through it.
        let zombie = kv
            .cas_fenced(KEY, NAMESPACE, LEASE_KEY, lease.token)
            .json(&Profile {
                plan: "zombie".to_owned(),
            })?
            .expect_version(fenced)
            .commit()
            .await;
        match zombie {
            Err(error) if error.is_lease_lost() => {
                println!("  after release the same fence is refused: lease-lost");
            }
            Err(error) => return Err(error),
            Ok(_) => {
                return Err(LaserError::Invalid(
                    "a released fence was accepted".to_owned(),
                ));
            }
        }
    }

    if capabilities.forks {
        phase("fork: a branch of the same state, promoted or thrown away");
        let table = index_for(NAMESPACE);
        laser.topic(NAMESPACE).ensure(PARTITIONS).await?;
        ensure_view(&laser, NAMESPACE, &table, ContentType::Json, &["plan"]).await?;
        let fork = laser.fork(FORK);
        fork.squash().await?;
        fork.create()
            .severed()
            .tables([table.as_str()])
            .send()
            .await?;
        fork.put_row(&table, 0, 0)
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
