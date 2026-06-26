use crate::common::world::LaserWorld;
use cucumber::{given, then, when};
use laser_bdd::memory_engine::MemoryEngine;
use laser_sdk::memory::{Lifetime, MemoryKind, MemoryScope};

fn engine(world: &mut LaserWorld) -> &mut MemoryEngine {
    world
        .memory_engine
        .as_mut()
        .expect("a memory store was opened")
}

#[given("an empty memory store")]
async fn open_store(world: &mut LaserWorld) {
    let owner = MemoryScope::builder()
        .agent("agent".parse().expect("a valid agent id"))
        .lifetime(Lifetime::Durable)
        .build();
    world.memory_engine = Some(MemoryEngine::new(owner));
    world.memory_ids.clear();
}

#[when(regex = r#"^I remember "([^"]+)" with dedup$"#)]
async fn remember_dedup(world: &mut LaserWorld, body: String) {
    let id = engine(world).remember(MemoryKind::Fact, body.as_bytes(), true);
    world.memory_ids.insert(body, id);
}

#[when(regex = r#"^I remember "([^"]+)"$"#)]
async fn remember(world: &mut LaserWorld, body: String) {
    let id = engine(world).remember(MemoryKind::Fact, body.as_bytes(), false);
    world.memory_ids.insert(body, id);
}

#[when(regex = r#"^I give "([^"]+)" a feedback weight of (-?\d+)$"#)]
async fn feedback(world: &mut LaserWorld, body: String, weight: f32) {
    let id = *world
        .memory_ids
        .get(&body)
        .expect("the item was remembered");
    engine(world).improve(id, weight);
}

#[when(regex = r#"^I forget "([^"]+)"$"#)]
async fn forget(world: &mut LaserWorld, body: String) {
    let id = *world
        .memory_ids
        .get(&body)
        .expect("the item was remembered");
    engine(world).forget(id);
}

#[then(regex = r#"^the memory holds (\d+) items?$"#)]
async fn holds(world: &mut LaserWorld, count: usize) {
    assert_eq!(engine(world).len(), count, "live item count");
}

#[then(regex = r#"^recalling (\d+) items? returns "([^"]+)" then "([^"]+)"$"#)]
async fn recall_two(world: &mut LaserWorld, limit: usize, first: String, second: String) {
    let recalled = engine(world).recall(limit);
    assert_eq!(recalled, vec![first, second], "recall order");
}

#[then(regex = r#"^recalling (\d+) items? returns "([^"]+)"$"#)]
async fn recall_one(world: &mut LaserWorld, limit: usize, only: String) {
    let recalled = engine(world).recall(limit);
    assert_eq!(recalled, vec![only], "recall result");
}
