use crate::common::world::LaserWorld;
use crate::common::world::TokenEmbedder;
use cucumber::{given, then, when};
use laser_bdd::memory_engine::MemoryEngine;
use laser_sdk::memory::{
    Lifetime, Memory, MemoryKind, MemoryQuery, MemoryScope, RecallStrategy, VectorMemory,
};

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

#[given("an empty semantic memory")]
async fn open_semantic_memory(world: &mut LaserWorld) {
    world.semantic_memory = Some(VectorMemory::new(TokenEmbedder));
    world.semantic_conversation = Some(laser_sdk::types::ConversationId::new());
}

#[when(regex = r#"^I remember the fact "([^"]+)"$"#)]
async fn remember_fact(world: &mut LaserWorld, body: String) {
    let conversation = world
        .semantic_conversation
        .expect("a semantic memory was opened");
    let scope = MemoryScope::builder().conversation(conversation).build();
    world
        .semantic_memory
        .as_ref()
        .expect("a semantic memory was opened")
        .remember(&scope, body.into_bytes())
        .await
        .expect("remember succeeds");
}

#[then(regex = r#"^keyword recall for "([^"]+)" returns "([^"]+)" first$"#)]
async fn keyword_recall_first(world: &mut LaserWorld, query: String, expected: String) {
    assert_first(world, RecallStrategy::Keyword, &query, &expected).await;
}

#[then(regex = r#"^hybrid recall for "([^"]+)" returns "([^"]+)" first$"#)]
async fn hybrid_recall_first(world: &mut LaserWorld, query: String, expected: String) {
    assert_first(world, RecallStrategy::Hybrid, &query, &expected).await;
}

async fn assert_first(
    world: &mut LaserWorld,
    strategy: RecallStrategy,
    query: &str,
    expected: &str,
) {
    let conversation = world
        .semantic_conversation
        .expect("a semantic memory was opened");
    let scope = MemoryScope::builder().conversation(conversation).build();
    let recall = MemoryQuery::builder()
        .limit(10)
        .semantic(query.to_owned())
        .strategy(strategy)
        .build();
    let items = world
        .semantic_memory
        .as_ref()
        .expect("a semantic memory was opened")
        .recall(&scope, &recall)
        .await
        .expect("recall succeeds");
    let first = items.first().expect("recall returned items");
    assert_eq!(
        String::from_utf8_lossy(&first.payload),
        expected,
        "the exact-term match must rank first"
    );
}
