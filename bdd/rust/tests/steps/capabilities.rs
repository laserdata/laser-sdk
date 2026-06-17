use crate::common::world::LaserWorld;
use cucumber::{given, then, when};
use laser_sdk::prelude::Capabilities;
use laser_sdk::query::ResultCode;

#[given("a managed-query connection that does not advertise read-your-writes")]
async fn managed_query_without_read_your_writes(world: &mut LaserWorld) {
    // Force the base query surface on while leaving the read-your-writes
    // capability off, so a read-your-writes query exercises the consistency
    // pre-gate rather than the managed-query gate.
    let laser = world
        .laser
        .take()
        .expect("a Laser is connected")
        .with_capabilities(Capabilities::OPEN.with_managed_query(true));
    world.laser = Some(laser);
}

#[when("I read the negotiated capabilities")]
async fn read_capabilities(world: &mut LaserWorld) {
    let capabilities = world.laser().capabilities().await;
    world.managed_query = Some(capabilities.managed_query);
    world.managed_kv = Some(capabilities.managed_kv);
    world.forks = Some(capabilities.forks);
    world.kv_cas = Some(capabilities.kv_cas);
    world.read_your_writes = Some(capabilities.read_your_writes);
    world.strong_consistency = Some(capabilities.strong_consistency);
}

#[then("managed query is unavailable")]
async fn no_managed_query(world: &mut LaserWorld) {
    assert_eq!(
        world.managed_query,
        Some(false),
        "managed query should be off"
    );
}

#[then("managed key-value is unavailable")]
async fn no_managed_kv(world: &mut LaserWorld) {
    assert_eq!(world.managed_kv, Some(false), "managed kv should be off");
}

#[then("forks are unavailable")]
async fn no_forks(world: &mut LaserWorld) {
    assert_eq!(world.forks, Some(false), "forks should be off");
}

#[then("the coordination features are unavailable")]
async fn no_coordination_features(world: &mut LaserWorld) {
    assert_eq!(world.kv_cas, Some(false), "kv_cas should be off");
    assert_eq!(
        world.read_your_writes,
        Some(false),
        "read_your_writes should be off"
    );
    assert_eq!(
        world.strong_consistency,
        Some(false),
        "strong_consistency should be off"
    );
}

#[when(regex = r#"^I run a query against topic "([^"]+)"$"#)]
async fn run_query(world: &mut LaserWorld, topic: String) {
    let result = world.laser().query(&topic).fetch().await;
    world.last_code = result.as_ref().err().map(|error| error.code());
    world.last_result = Some(result.map(|_| ()).map_err(|error| format!("{error:?}")));
}

#[when(regex = r#"^I run a read-your-writes query against topic "([^"]+)"$"#)]
async fn run_read_your_writes_query(world: &mut LaserWorld, topic: String) {
    let result = world.laser().query(&topic).read_your_writes().fetch().await;
    world.last_code = result.as_ref().err().map(|error| error.code());
    world.last_result = Some(result.map(|_| ()).map_err(|error| format!("{error:?}")));
}

#[when(regex = r#"^I compare-and-swap key "([^"]+)" in namespace "([^"]+)" expecting it absent$"#)]
async fn compare_and_swap(world: &mut LaserWorld, key: String, namespace: String) {
    let result = world
        .laser()
        .kv(&namespace)
        .set(&key)
        .bytes(b"held")
        .expect_absent()
        .commit()
        .await;
    world.last_code = result.as_ref().err().map(|error| error.code());
    world.last_result = Some(result.map(|_| ()).map_err(|error| format!("{error:?}")));
}

#[then("the call fails as unsupported")]
async fn fails_unsupported(world: &mut LaserWorld) {
    match world
        .last_result
        .as_ref()
        .expect("a managed call was attempted")
    {
        Err(error) => assert!(
            error.contains("Unsupported"),
            "expected an Unsupported error, got: {error}"
        ),
        Ok(()) => panic!("expected the managed call to fail as unsupported"),
    }
}

#[then("the unified result code is unsupported")]
async fn result_code_unsupported(world: &mut LaserWorld) {
    assert_eq!(
        world.last_code,
        Some(ResultCode::Unsupported),
        "the failure should classify as Unsupported in the unified result space"
    );
}
