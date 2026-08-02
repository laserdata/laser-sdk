use crate::common::world::LaserWorld;
use cucumber::{given, then, when};
use laser_bdd::kv_engine::KvEngine;
use laser_sdk::kv::{CasExpect, KvError};

// A fixed logical clock for the deterministic-expiry scenarios. Steps that care
// about expiry pass an absolute micros value relative to this base.
const NOW: u64 = 1_000;

fn engine(world: &mut LaserWorld) -> &mut KvEngine {
    world.kv_engine.as_mut().expect("a KV store was opened")
}

#[given("an empty KV store")]
async fn open_store(world: &mut LaserWorld) {
    world.kv_engine = Some(KvEngine::new());
}

#[given(regex = r#"^key "([^"]+)" holds "([^"]+)"$"#)]
async fn seed_key(world: &mut LaserWorld, key: String, value: String) {
    engine(world).set(key.as_bytes(), value.into_bytes(), None, NOW);
}

#[given(regex = r#"^key "([^"]+)" holds "([^"]+)" expiring at (\d+)$"#)]
async fn seed_key_expiring(world: &mut LaserWorld, key: String, value: String, expiry: u64) {
    engine(world).set(key.as_bytes(), value.into_bytes(), Some(expiry), NOW);
}

#[when(regex = r#"^I create "([^"]+)" with "([^"]+)" if absent$"#)]
async fn cas_absent(world: &mut LaserWorld, key: String, value: String) {
    let outcome = engine(world).cas(
        key.as_bytes(),
        value.into_bytes(),
        CasExpect::Absent,
        None,
        NOW,
    );
    world.last_cas = Some(outcome);
}

#[when(regex = r#"^I create "([^"]+)" with "([^"]+)" if absent at (\d+)$"#)]
async fn cas_absent_at(world: &mut LaserWorld, key: String, value: String, now: u64) {
    let outcome = engine(world).cas(
        key.as_bytes(),
        value.into_bytes(),
        CasExpect::Absent,
        None,
        now,
    );
    world.last_cas = Some(outcome);
}

#[when(regex = r#"^I swap "([^"]+)" to "([^"]+)" expecting version (\d+)$"#)]
async fn cas_match(world: &mut LaserWorld, key: String, value: String, version: u64) {
    let outcome = engine(world).cas(
        key.as_bytes(),
        value.into_bytes(),
        CasExpect::Match(version),
        None,
        NOW,
    );
    world.last_cas = Some(outcome);
}

#[when(regex = r#"^I swap "([^"]+)" to "([^"]+)" expecting version (\d+) at (\d+)$"#)]
async fn cas_match_at(world: &mut LaserWorld, key: String, value: String, version: u64, now: u64) {
    let outcome = engine(world).cas(
        key.as_bytes(),
        value.into_bytes(),
        CasExpect::Match(version),
        None,
        now,
    );
    world.last_cas = Some(outcome);
}

#[then(regex = r#"^the swap commits version (\d+)$"#)]
async fn then_commits(world: &mut LaserWorld, version: u64) {
    match world.last_cas.as_ref().expect("a swap was attempted") {
        Ok(committed) => assert_eq!(*committed, version, "committed version"),
        Err(error) => panic!("expected a commit, got {error:?}"),
    }
}

#[then("the swap conflicts because the key is absent")]
async fn then_conflict_absent(world: &mut LaserWorld) {
    match world.last_cas.as_ref().expect("a swap was attempted") {
        Err(KvError::VersionConflict { current: None }) => {}
        other => panic!("expected a conflict with no current version, got {other:?}"),
    }
}

#[then(regex = r#"^the swap conflicts with current version (\d+)$"#)]
async fn then_conflict_current(world: &mut LaserWorld, current: u64) {
    match world.last_cas.as_ref().expect("a swap was attempted") {
        Err(KvError::VersionConflict {
            current: Some(version),
        }) => assert_eq!(*version, current, "conflict current version"),
        other => panic!("expected a conflict with current {current}, got {other:?}"),
    }
}
