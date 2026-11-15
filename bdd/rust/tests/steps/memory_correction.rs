use crate::common::world::LaserWorld;
use cucumber::{then, when};
use laser_sdk::memory::{MemoryHandle, MemoryId, MemoryItem};
use laser_sdk::prelude::{AgentTopic, ConversationId};

const NAMESPACE: &str = "prefs";
const USER: &str = "ana";

/// The memory correction scenarios' state: one handle on memory "prefs", the
/// conversation each label names, and the last fact remembered.
#[derive(Default)]
pub struct Correction {
    handle: Option<MemoryHandle>,
    conversations: std::collections::HashMap<String, ConversationId>,
    last: Option<MemoryId>,
}

impl Correction {
    fn handle(&mut self, world_laser: &laser_sdk::prelude::Laser) -> &MemoryHandle {
        self.handle
            .get_or_insert_with(|| world_laser.memory(NAMESPACE))
    }

    fn conversation(&mut self, label: &str) -> ConversationId {
        *self.conversations.entry(label.to_owned()).or_default()
    }
}

impl std::fmt::Debug for Correction {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Correction")
            .field("conversations", &self.conversations)
            .field("last", &self.last)
            .finish_non_exhaustive()
    }
}

#[when(regex = r#"^the user states "([^"]+)"$"#)]
async fn remember(world: &mut LaserWorld, fact: String) {
    let laser = world.laser().clone();
    let id = world
        .correction
        .handle(&laser)
        .remember(fact)
        .user(USER)
        .send()
        .await
        .expect("the fact is remembered");
    world.correction.last = Some(id);
}

#[when(regex = r#"^the user states "([^"]+)" in conversation "([^"]+)"$"#)]
async fn remember_in(world: &mut LaserWorld, fact: String, label: String) {
    let laser = world.laser().clone();
    let conversation = world.correction.conversation(&label);
    let id = world
        .correction
        .handle(&laser)
        .remember(fact)
        .scope(conversation)
        .user(USER)
        .send()
        .await
        .expect("the fact is remembered");
    world.correction.last = Some(id);
}

#[when(regex = r#"^the user corrects it to "([^"]+)" in conversation "([^"]+)"$"#)]
async fn supersede(world: &mut LaserWorld, fact: String, label: String) {
    let laser = world.laser().clone();
    let conversation = world.correction.conversation(&label);
    let old = world.correction.last.expect("a fact to correct");
    let id = world
        .correction
        .handle(&laser)
        .supersede(old, fact)
        .scope(conversation)
        .user(USER)
        .send()
        .await
        .expect("the correction is stored");
    world.correction.last = Some(id);
}

#[then("the memory topic exists on the stream")]
async fn memory_topic_exists(world: &mut LaserWorld) {
    let laser = world.laser();
    let stream = laser.default_stream().expect("a default stream");
    assert!(
        laser
            .stream(stream)
            .topic(AgentTopic::Memory.topic_string())
            .exists()
            .await
            .expect("the topic reads")
    );
}

#[then(regex = r#"^a folded recall for the user returns "([^"]+)" only$"#)]
async fn folded_recall_only(world: &mut LaserWorld, fact: String) {
    let items = recall(world, None, None, true).await;
    assert_eq!(texts(&items), [fact]);
}

#[then(regex = r#"^a recall for the user returns "([^"]+)" only$"#)]
async fn recall_only(world: &mut LaserWorld, fact: String) {
    let items = recall(world, None, None, false).await;
    assert_eq!(texts(&items), [fact]);
}

#[then(regex = r#"^a folded semantic recall for "([^"]+)" returns (".+")$"#)]
async fn folded_semantic_recall(world: &mut LaserWorld, query: String, facts: String) {
    let items = recall(world, Some(query), Some(false), true).await;
    let expected: Vec<String> = facts
        .split('"')
        .skip(1)
        .step_by(2)
        .map(str::to_owned)
        .collect();
    assert_eq!(texts(&items), expected);
}

#[then(regex = r#"^a folded keyword recall for "([^"]+)" returns "([^"]+)" only$"#)]
async fn folded_keyword_recall(world: &mut LaserWorld, query: String, fact: String) {
    let items = recall(world, Some(query), Some(true), true).await;
    assert_eq!(texts(&items), [fact]);
}

// Recall for the user across conversations: `keyword` picks the keyword
// strategy for the query text, otherwise it is semantic.
async fn recall(
    world: &mut LaserWorld,
    query: Option<String>,
    keyword: Option<bool>,
    folded: bool,
) -> Vec<MemoryItem> {
    let laser = world.laser().clone();
    let handle = world.correction.handle(&laser);
    let mut recall = handle.recall(None).user(USER);
    if let Some(query) = query {
        recall = match keyword {
            Some(true) => recall.keyword(query),
            _ => recall.semantic(query),
        };
    }
    if folded {
        recall = recall.folded();
    }
    recall.fetch().await.expect("recall")
}

fn texts(items: &[MemoryItem]) -> Vec<String> {
    items.iter().map(MemoryItem::text).collect()
}
