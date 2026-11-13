use crate::common::world::LaserWorld;
use cucumber::{given, then, when};
use iggy::prelude::IggyTimestamp;
use laser_sdk::agent::{AgentHandle, WarmDedupPolicy};
use laser_sdk::prelude::full::*;
use laser_sdk::types::AgentId;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

// How far ahead a resend's deadline lies, far enough that it never expires
// in the scenario.
const RESEND_DEADLINE: Duration = Duration::from_secs(60);

/// The worker of the warm dedup scenarios: its id and policy, the running
/// incarnation, the bodies it handled, and the deadline each key was sent with.
#[derive(Default)]
pub struct Warm {
    agent: Option<AgentId>,
    policy: WarmDedupPolicy,
    worker: Option<AgentHandle>,
    handled: Arc<Mutex<Vec<String>>>,
    deadlines: BTreeMap<String, u64>,
}

#[given(regex = r#"^a warm-dedup worker "([^"]+)" with policy "([^"]+)"$"#)]
async fn warm_worker(world: &mut LaserWorld, name: String, policy: String) {
    world.warm.agent = Some(name.parse().expect("a valid agent id"));
    world.warm.policy = policy.parse().expect("a known warm dedup policy");
    start(world).await;
}

#[when(regex = r#"^the worker is sent a command under key "([^"]+)" due in (\d+) seconds$"#)]
async fn send_due(world: &mut LaserWorld, key: String, seconds: u64) {
    let deadline = now_micros() + seconds * 1_000_000;
    send(world, &key, Some(deadline)).await;
}

#[when(regex = r#"^the worker is sent a command under key "([^"]+)" already past its deadline$"#)]
async fn send_expired(world: &mut LaserWorld, key: String) {
    let deadline = now_micros() - 1_000_000;
    send(world, &key, Some(deadline)).await;
}

#[when(regex = r#"^the worker is sent a command under key "([^"]+)" without a deadline$"#)]
async fn send_without_deadline(world: &mut LaserWorld, key: String) {
    send(world, &key, None).await;
}

#[then(regex = r#"^the worker handled the keys "([^"]+)"$"#)]
async fn worker_handled(world: &mut LaserWorld, keys: String) {
    assert_handled(world, &keys).await;
}

#[when(regex = r#"^the worker stops and the deadline of key "([^"]+)" passes$"#)]
async fn stop_after_deadline(world: &mut LaserWorld, key: String) {
    let worker = world.warm.worker.take().expect("the worker runs");
    worker.shutdown().await.expect("the worker stops");
    let deadline = world.warm.deadlines[&key];
    loop {
        let now = now_micros();
        if now > deadline {
            break;
        }
        tokio::time::sleep(Duration::from_micros(deadline + 1 - now)).await;
    }
}

#[when(regex = r#"^resends arrive under the keys "([^"]+)" and then "([^"]+)"$"#)]
async fn resends(world: &mut LaserWorld, keys: String, last: String) {
    let due = now_micros() + u64::try_from(RESEND_DEADLINE.as_micros()).expect("fits u64");
    for key in keys.split(", ").chain([last.as_str()]) {
        send(world, key, Some(due)).await;
    }
}

#[when("the worker restarts")]
async fn restart(world: &mut LaserWorld) {
    world.warm.handled.lock().expect("handled bodies").clear();
    start(world).await;
}

#[then(regex = r#"^the restarted worker handled the keys "([^"]+)"$"#)]
async fn restarted_handled(world: &mut LaserWorld, keys: String) {
    assert_handled(world, &keys).await;
    let worker = world.warm.worker.take().expect("the worker runs");
    worker.shutdown().await.expect("the worker stops");
}

// Records the body of every record it handles.
struct RecordsBodies {
    handled: Arc<Mutex<Vec<String>>>,
}

impl AgentHandler for RecordsBodies {
    async fn handle(&self, message: &AgentMessage, _ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        self.handled
            .lock()
            .expect("handled bodies")
            .push(String::from_utf8_lossy(&message.payload).into_owned());
        Ok(())
    }
}

async fn start(world: &mut LaserWorld) {
    let mut worker = Agent::builder()
        .id(world.warm.agent.clone().expect("the worker has an id"))
        .listen_on(AgentTopic::Sessions)
        .warm_dedup(true)
        .warm_dedup_policy(world.warm.policy)
        .handler(RecordsBodies {
            handled: Arc::clone(&world.warm.handled),
        })
        .build()
        .spawn(world.laser().clone());
    worker.ready().await.expect("the worker is ready");
    world.warm.worker = Some(worker);
}

async fn send(world: &mut LaserWorld, key: &str, deadline: Option<u64>) {
    let provenance = Provenance::builder()
        .conversation_id(world.conversation())
        .agent("warm-sender".parse().expect("a valid agent id"))
        .target_agent_id(world.warm.agent.clone().expect("the worker has an id"))
        .idempotency_key(key.to_owned())
        .maybe_deadline_at(deadline.map(IggyTimestamp::from))
        .build();
    world
        .laser()
        .send_agent(AgentTopic::Sessions, key.as_bytes().to_vec(), &provenance)
        .await
        .expect("the command is sent");
    if let Some(deadline) = deadline {
        world.warm.deadlines.insert(key.to_owned(), deadline);
    }
}

// Waits until the worker handled the last listed key, then compares the
// whole list. Every record rides one partition and the worker is serial, so
// the records before the last one were decided before it.
async fn assert_handled(world: &LaserWorld, keys: &str) {
    let expected: Vec<String> = keys.split(", ").map(str::to_owned).collect();
    let last = expected.last().expect("at least one key").clone();
    for _ in 0..200 {
        if world
            .warm
            .handled
            .lock()
            .expect("handled bodies")
            .contains(&last)
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        *world.warm.handled.lock().expect("handled bodies"),
        expected
    );
}

fn now_micros() -> u64 {
    IggyTimestamp::now().as_micros()
}
