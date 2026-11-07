use crate::common::test_iggy::fresh_connected_laser;
use crate::common::world::LaserWorld;
use cucumber::{given, then, when};
use laser_sdk::LaserError;
use laser_sdk::agent::TopicRetention;
use laser_sdk::kv::Lease;
use std::time::{Duration, Instant};

const COORDINATION: &str = "coordination";
const STATE: &str = "state";
const TAKEOVER_DEADLINE: Duration = Duration::from_secs(15);

// The fenced-lease verbs open dedicated connections, which only a `Laser`
// built from a connection string can do, and the stream the namespaces are
// scoped to must exist.
#[given("a fresh stream bootstrapped on a connection-string client")]
async fn connected_stream(world: &mut LaserWorld) {
    let fresh = fresh_connected_laser().await;
    fresh
        .laser
        .bootstrap(1, TopicRetention::expire_after(Duration::from_secs(86_400)))
        .await
        .expect("bootstrap the stream");
    world.platform = fresh.iggy;
    world.laser = Some(fresh.laser);
}

#[when(regex = r#"^"([^"]+)" acquires the lease "([^"]+)" for (\d+) ms$"#)]
async fn acquire(world: &mut LaserWorld, holder: String, key: String, ttl_ms: u64) {
    let lease = world
        .laser()
        .kv(COORDINATION)
        .lease(key.as_bytes(), &holder, Duration::from_millis(ttl_ms))
        .await
        .expect("the lease is granted");
    world.leases.insert(holder, (key, lease));
}

// The earlier grant has to run out, so the step polls until the store grants
// the key rather than sleeping a fixed time.
#[when(regex = r#"^"([^"]+)" acquires the lease "([^"]+)" once it is free$"#)]
async fn acquire_when_free(world: &mut LaserWorld, holder: String, key: String) {
    let deadline = Instant::now() + TAKEOVER_DEADLINE;
    let lease = loop {
        match world
            .laser()
            .kv(COORDINATION)
            .lease(key.as_bytes(), &holder, Duration::from_secs(5))
            .await
        {
            Ok(lease) => break lease,
            Err(_) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(error) => panic!("the lease never became free: {error}"),
        }
    };
    world.leases.insert(holder, (key, lease));
}

#[when(regex = r#"^"([^"]+)" renews the lease "([^"]+)"$"#)]
async fn renew(world: &mut LaserWorld, holder: String, key: String) {
    let token = held(world, &holder).token;
    let renewed = world
        .laser()
        .kv(COORDINATION)
        .renew_lease(key.as_bytes(), &holder, token, Duration::from_secs(5))
        .await
        .expect("the holder renews its own lease");
    world.renewed_lease = Some(renewed);
}

#[then(regex = r#"^the renewal of "([^"]+)" keeps its lease token$"#)]
fn renewal_keeps_token(world: &mut LaserWorld, holder: String) {
    let renewed = world.renewed_lease.expect("a renewal ran");
    assert_eq!(renewed.token, held(world, &holder).token);
}

#[when(regex = r#"^"([^"]+)" releases the lease "([^"]+)"$"#)]
async fn release(world: &mut LaserWorld, holder: String, key: String) {
    let token = held(world, &holder).token;
    let released = world
        .laser()
        .kv(COORDINATION)
        .release(key.as_bytes(), &holder, token)
        .await
        .expect("the holder releases its own lease");
    world.lease_released = Some(released);
}

#[then("the release reports the lease was held")]
fn release_was_held(world: &mut LaserWorld) {
    assert_eq!(world.lease_released, Some(true));
}

#[when(regex = r#"^"([^"]+)" writes "([^"]+)" to "([^"]+)" fenced by its lease$"#)]
async fn fenced_write(world: &mut LaserWorld, holder: String, value: String, key: String) {
    let (fence_key, lease) = world
        .leases
        .get(&holder)
        .cloned()
        .expect("the holder took a lease");
    let request = world
        .laser()
        .kv(STATE)
        .cas_fenced(
            key.as_bytes(),
            COORDINATION,
            fence_key.as_bytes(),
            lease.token,
        )
        .bytes(value.as_bytes());
    let request = match &world.last_entry {
        Some(entry) => request.expect_version(entry.version),
        None => request.expect_absent(),
    };
    let result = request.commit().await;
    world.last_code = result.as_ref().err().map(LaserError::code);
    world.last_lease_lost = result.as_ref().err().map(LaserError::is_lease_lost);
    world.last_result = Some(result.map(drop).map_err(|error| format!("{error:?}")));
}

#[then("the fenced write fails as lease lost")]
fn fenced_write_lost(world: &mut LaserWorld) {
    assert!(
        matches!(world.last_result, Some(Err(_))),
        "the stale write must be refused: {:?}",
        world.last_result
    );
    assert_eq!(world.last_lease_lost, Some(true), "{:?}", world.last_result);
}

#[then(regex = r#"^a read of "([^"]+)" at the lease grant of "([^"]+)" returns "([^"]+)"$"#)]
async fn read_at_grant(world: &mut LaserWorld, key: String, holder: String, value: String) {
    let position = held(world, &holder).position;
    let entry = world
        .laser()
        .kv(STATE)
        .get_entry_at_least(key.as_bytes(), position)
        .await
        .expect("the barriered read answers")
        .expect("the earlier holder's write is visible at the new grant");
    assert_eq!(entry.value, value.as_bytes());
    world.last_entry = Some(entry);
}

#[then(regex = r#"^reading "([^"]+)" returns "([^"]+)"$"#)]
async fn read_back(world: &mut LaserWorld, key: String, value: String) {
    let stored = world
        .laser()
        .kv(STATE)
        .get(key.as_bytes())
        .await
        .expect("the read answers")
        .expect("the key holds a value");
    assert_eq!(stored, value.as_bytes());
}

fn held(world: &LaserWorld, holder: &str) -> Lease {
    world
        .leases
        .get(holder)
        .map(|(_, lease)| *lease)
        .expect("the holder took a lease")
}
