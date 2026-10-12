use crate::common::world::LaserWorld;
use cucumber::{then, when};

#[when(regex = r#"^I write "([^"]+)" to the managed key "([^"]+)" in namespace "([^"]+)"$"#)]
async fn write_managed_key(world: &mut LaserWorld, value: String, key: String, namespace: String) {
    world
        .laser()
        .kv(namespace)
        .set(key)
        .bytes(value.into_bytes())
        .send()
        .await
        .expect("the managed write applies");
}

#[then("the client has seen a key-value position")]
fn seen_kv_position(world: &mut LaserWorld) {
    assert!(
        world.laser().seen_positions().kv.is_some(),
        "the write reply carried the fold's position"
    );
}

#[when("I note the client's key-value position")]
fn note_kv_position(world: &mut LaserWorld) {
    world.noted_position = world.laser().seen_positions().kv;
}

#[then("the client's key-value position moved past the noted one")]
fn kv_position_moved(world: &mut LaserWorld) {
    let noted = world.noted_position.expect("a position was noted");
    let seen = world
        .laser()
        .seen_positions()
        .kv
        .expect("the client has a position");
    assert!(
        (seen.topic_generation, seen.offset) > (noted.topic_generation, noted.offset),
        "{seen:?} is past {noted:?}"
    );
}

#[then(regex = r#"^the managed key "([^"]+)" in namespace "([^"]+)" reads "([^"]+)"$"#)]
async fn managed_key_reads(world: &mut LaserWorld, key: String, namespace: String, value: String) {
    let read = world
        .laser()
        .kv(namespace)
        .get(key)
        .await
        .expect("the read is served");
    assert_eq!(read.as_deref(), Some(value.as_bytes()));
}
