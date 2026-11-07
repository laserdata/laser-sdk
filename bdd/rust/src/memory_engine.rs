// The reference agent-memory engine: the executable specification of the recall
// semantics every backend and every SDK port must reproduce. Pure and
// transport-free (no Iggy, no client), so the shared Gherkin pins one cross-
// language contract: content-addressed dedup, recency order, feedback re-ranking,
// and forget. The dedup id is the SDK's own `MemoryId::content`, so the Rust and
// Python ports agree on it byte for byte.

use laser_sdk::memory::{MemoryId, MemoryKind, MemoryScope};
use laser_sdk::types::ConversationId;
use std::collections::HashMap;

struct Entry {
    id: MemoryId,
    body: Vec<u8>,
}

/// An in-memory store of remembered items under one durable owner. Items keep
/// arrival order, so the most recent are the tail.
#[derive(Default)]
pub struct MemoryEngine {
    owner: MemoryScope,
    items: Vec<Entry>,
    feedback: HashMap<MemoryId, f32>,
    counter: u128,
}

impl MemoryEngine {
    /// A store for `owner` (the durable scope that content-addresses ids).
    pub fn new(owner: MemoryScope) -> Self {
        Self {
            owner,
            ..Self::default()
        }
    }

    /// Remember `body`. With `dedup`, the id is content-addressed, so the same
    /// body is stored once and returns the existing id. Without it, each call is
    /// a distinct item.
    pub fn remember(&mut self, kind: MemoryKind, body: &[u8], dedup: bool) -> MemoryId {
        self.remember_in(kind, body, dedup, None)
    }

    /// [`remember`](Self::remember) in `conversation`, which is part of the
    /// content-addressed id, so the same body in two conversations is two items.
    pub fn remember_in(
        &mut self,
        kind: MemoryKind,
        body: &[u8],
        dedup: bool,
        conversation: Option<ConversationId>,
    ) -> MemoryId {
        let mut owner = self.owner.clone();
        owner.conversation = conversation;
        let id = if dedup {
            let id = MemoryId::content(&owner, kind, body);
            if self.items.iter().any(|entry| entry.id == id) {
                return id;
            }
            id
        } else {
            // A distinct deterministic id: the body salted with a counter.
            self.counter += 1;
            let mut seed = body.to_vec();
            seed.extend_from_slice(&self.counter.to_be_bytes());
            MemoryId::content(&owner, kind, &seed)
        };
        self.items.push(Entry {
            id,
            body: body.to_vec(),
        });
        id
    }

    /// The number of live items.
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Whether the store holds no live items.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Record feedback on `target`, accumulating its weight.
    pub fn improve(&mut self, target: MemoryId, weight: f32) {
        *self.feedback.entry(target).or_insert(0.0) += weight;
    }

    /// Forget `id`, removing it from recall. Remembering it again later
    /// brings it back, as a write after a tombstone does.
    pub fn forget(&mut self, id: MemoryId) {
        self.items.retain(|entry| entry.id != id);
    }

    /// Recall up to `limit` items. With feedback present, items rank by weight
    /// (promoted first). Otherwise the most recent come first. Returns the bodies
    /// as text, the form the scenarios assert against.
    pub fn recall(&self, limit: usize) -> Vec<String> {
        let mut live: Vec<&Entry> = self.items.iter().collect();
        if self.feedback.is_empty() {
            live.reverse();
        } else {
            let weight = |entry: &Entry| self.feedback.get(&entry.id).copied().unwrap_or(0.0);
            // Stable sort by descending weight over the most-recent-first order.
            live.reverse();
            live.sort_by(|a, b| weight(b).total_cmp(&weight(a)));
        }
        live.into_iter()
            .take(limit)
            .map(|entry| String::from_utf8_lossy(&entry.body).into_owned())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use laser_sdk::memory::Lifetime;

    fn store() -> MemoryEngine {
        let owner = MemoryScope::builder()
            .agent("agent".parse().expect("valid agent"))
            .lifetime(Lifetime::Durable)
            .build();
        MemoryEngine::new(owner)
    }

    #[test]
    fn given_the_same_body_when_remembered_twice_with_dedup_then_should_hold_one_item() {
        let mut engine = store();
        let first = engine.remember(MemoryKind::Fact, b"the budget is 5000", true);
        let second = engine.remember(MemoryKind::Fact, b"the budget is 5000", true);
        assert_eq!(first, second, "dedup yields the same id");
        assert_eq!(engine.len(), 1);
    }

    #[test]
    fn given_the_same_body_in_two_conversations_when_remembered_with_dedup_then_should_hold_two_items()
     {
        let mut engine = store();
        let first = ConversationId::derive("first");
        engine.remember_in(MemoryKind::Fact, b"x", true, Some(first));
        engine.remember_in(
            MemoryKind::Fact,
            b"x",
            true,
            Some(ConversationId::derive("second")),
        );
        engine.remember_in(MemoryKind::Fact, b"x", true, Some(first));
        assert_eq!(engine.len(), 2);
    }

    #[test]
    fn given_a_forgotten_item_when_remembered_again_then_should_be_recalled() {
        let mut engine = store();
        let id = engine.remember(MemoryKind::Fact, b"x", true);
        engine.forget(id);
        engine.remember(MemoryKind::Fact, b"x", true);
        assert_eq!(engine.recall(1), vec!["x"]);
    }

    #[test]
    fn given_no_dedup_when_remembered_twice_then_should_hold_two_items() {
        let mut engine = store();
        engine.remember(MemoryKind::Fact, b"x", false);
        engine.remember(MemoryKind::Fact, b"x", false);
        assert_eq!(engine.len(), 2);
    }

    #[test]
    fn given_items_when_recalled_then_should_return_most_recent_first() {
        let mut engine = store();
        engine.remember(MemoryKind::Fact, b"first", false);
        engine.remember(MemoryKind::Fact, b"second", false);
        assert_eq!(engine.recall(2), vec!["second", "first"]);
    }

    #[test]
    fn given_positive_feedback_when_recalled_then_should_promote_the_target() {
        let mut engine = store();
        let cat = engine.remember(MemoryKind::Fact, b"cat", false);
        engine.remember(MemoryKind::Fact, b"dog", false);
        engine.improve(cat, 5.0);
        assert_eq!(engine.recall(2), vec!["cat", "dog"]);
    }

    #[test]
    fn given_a_forgotten_item_when_recalled_then_should_be_excluded() {
        let mut engine = store();
        engine.remember(MemoryKind::Fact, b"keep", false);
        let drop = engine.remember(MemoryKind::Fact, b"drop", false);
        engine.forget(drop);
        assert_eq!(engine.len(), 1);
        assert_eq!(engine.recall(2), vec!["keep"]);
    }
}
