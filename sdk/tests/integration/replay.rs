use crate::harness;
use bytes::Bytes;
use laser_sdk::prelude::full::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_consumer_group_committed_offsets_when_rebuilding_state_then_should_replay_full_history_from_zero()
 {
    let laser = harness::laser().await;
    let conversation = ConversationId::new();
    for i in 1..=5 {
        let provenance = Provenance::builder()
            .conversation_id(conversation)
            .agent("counter".parse().expect("counter is a valid agent id"))
            .idempotency_key(format!("evt-{i}"))
            .build();
        laser
            .send_agent(
                AgentTopic::Commands,
                Bytes::from(format!("{i}")),
                &provenance,
            )
            .await
            .expect("event should be sent");
    }

    // Consume every event through a consumer-group agent. Auto-commit walks the
    // group's server-stored offset past the whole conversation, exactly the
    // state a restarting steady-state consumer would resume from.
    let handled = Arc::new(AtomicUsize::new(0));
    Agent::builder()
        .id("counter".parse().expect("counter is a valid agent id"))
        .listen_on(AgentTopic::Commands)
        .handler(Counter {
            handled: handled.clone(),
        })
        .build()
        .spawn(laser.clone());
    harness::eventually(|| {
        let handled = handled.clone();
        async move { (handled.load(Ordering::SeqCst) >= 5).then_some(()) }
    })
    .await;

    // The rebuild reads from offset 0 by explicit offset, so it folds all five
    // events even though the consumer group has committed offsets past them. And
    // because replay consumes nothing, a second rebuild yields the same fold.
    let sum = ConversationState::load(
        &laser,
        conversation,
        vec![AgentTopic::Commands],
        ReplayBound::Full,
        0,
        sum_events,
    )
    .await
    .expect("replay after commit should succeed");
    assert_eq!(
        sum, 15,
        "replay must fold the whole conversation from offset 0"
    );

    let replayed = ConversationState::load(
        &laser,
        conversation,
        vec![AgentTopic::Commands],
        ReplayBound::Full,
        0,
        sum_events,
    )
    .await
    .expect("second replay should succeed");
    assert_eq!(replayed, 15, "replay is read-only and repeatable");
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_context_assembler_when_topic_actively_consumed_then_should_still_read_from_zero() {
    let laser = harness::laser().await;
    let conversation = ConversationId::new();
    for i in 1..=3 {
        let provenance = Provenance::builder()
            .conversation_id(conversation)
            .agent("ctx".parse().expect("ctx is a valid agent id"))
            .idempotency_key(format!("ctx-{i}"))
            .build();
        laser
            .send_agent(
                AgentTopic::Commands,
                Bytes::from(format!("{i}")),
                &provenance,
            )
            .await
            .expect("event should be sent");
    }

    // A fresh assembler reads the whole conversation from offset 0, regardless of
    // any other reader's position on the same topic.
    let history = harness::eventually(|| {
        let laser = laser.clone();
        async move {
            let history = ContextAssembler::builder()
                .conversation_id(conversation)
                .topics(vec![AgentTopic::Commands])
                .build()
                .assemble(&laser)
                .await
                .expect("assembling the conversation should succeed");
            (history.len() == 3).then_some(history)
        }
    })
    .await;
    assert_eq!(
        history.len(),
        3,
        "context must replay the full conversation"
    );
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_consumer_group_committed_offsets_when_reading_with_a_cursor_then_should_drain_from_zero()
 {
    let laser = harness::laser().await;
    let conversation = ConversationId::new();
    send_events(&laser, conversation, "cur", 1..=5).await;

    // Walk the consumer group past every message, then stop it.
    let handled = Arc::new(AtomicUsize::new(0));
    let agent = Agent::builder()
        .id("counter".parse().expect("counter is a valid agent id"))
        .listen_on(AgentTopic::Commands)
        .handler(Counter {
            handled: handled.clone(),
        })
        .build()
        .spawn(laser.clone());
    harness::eventually(|| {
        let handled = handled.clone();
        async move { (handled.load(Ordering::SeqCst) >= 5).then_some(()) }
    })
    .await;
    agent.shutdown().await.expect("counter shuts down cleanly");

    // A fresh cursor owns its own offsets (start at 0) and polls by explicit
    // offset, so it drains the whole topic regardless of the group's commit.
    let commands = laser.topic(
        AgentTopic::Commands
            .name()
            .expect("commands has a topic name"),
    );
    let mut cursor = commands.replay().expect("reader builds");
    let messages = cursor.poll().await.expect("cursor poll succeeds");
    assert_eq!(
        messages.len(),
        5,
        "cursor must drain the whole topic from offset 0, ignoring the group commit"
    );
    // Polling again is caught up: the cursor advanced its own offsets, it does
    // not rescan from zero.
    assert!(
        cursor
            .poll()
            .await
            .expect("second poll succeeds")
            .is_empty(),
        "a caught-up cursor reads only what is new"
    );
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_one_log_memory_wrote_when_a_fresh_instance_recalls_then_should_fold_from_zero() {
    let laser = harness::laser().await;
    let conversation = ConversationId::new();
    let scope = MemoryScope::builder().conversation(conversation).build();

    // One instance appends three items, advancing only its own in-memory cursor.
    let writer = LogMemory::new(laser.clone());
    for i in 1..=3 {
        writer
            .remember(&scope, format!("item-{i}").into_bytes())
            .await
            .expect("remember should append to the audit log");
    }

    // A fresh instance has no folded projection and no saved offsets, so its first
    // recall rebuilds from offset 0 and sees all three, independent of the
    // writer's cursor.
    let reader = LogMemory::new(laser.clone());
    let items = harness::eventually(|| {
        let reader = &reader;
        let scope = &scope;
        async move {
            let items = reader
                .recall_folded(scope, &MemoryQuery::builder().build())
                .await
                .expect("recall should succeed");
            (items.len() == 3).then_some(items)
        }
    })
    .await;
    assert_eq!(
        items.len(),
        3,
        "a fresh LogMemory folds the whole audit log from zero"
    );
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_consumer_group_committed_when_a_new_member_resumes_then_should_not_replay_history() {
    let laser = harness::laser().await;
    let conversation = ConversationId::new();
    send_events(&laser, conversation, "first", 1..=5).await;

    // First member consumes all five and commits, then stops.
    let first = Arc::new(AtomicUsize::new(0));
    let agent = Agent::builder()
        .id("replay-guard"
            .parse()
            .expect("replay-guard is a valid agent id"))
        .listen_on(AgentTopic::Commands)
        .handler(Counter {
            handled: first.clone(),
        })
        .build()
        .spawn(laser.clone());
    harness::eventually(|| {
        let first = first.clone();
        async move { (first.load(Ordering::SeqCst) >= 5).then_some(()) }
    })
    .await;
    agent
        .shutdown()
        .await
        .expect("first member shuts down cleanly");

    // A new member of the SAME group resumes from the committed offset. It must
    // NOT replay the five already-committed events, the negative the rebuild
    // paths exist to work around.
    let second = Arc::new(AtomicUsize::new(0));
    let mut rejoined = Agent::builder()
        .id("replay-guard"
            .parse()
            .expect("replay-guard is a valid agent id"))
        .listen_on(AgentTopic::Commands)
        .handler(Counter {
            handled: second.clone(),
        })
        .build()
        .spawn(laser.clone());
    rejoined
        .ready()
        .await
        .expect("rejoined member becomes ready");

    // One new event after the rejoin: the new member sees exactly that one,
    // never the five it resumed past.
    send_events(&laser, conversation, "sixth", 6..=6).await;
    harness::eventually(|| {
        let second = second.clone();
        async move { (second.load(Ordering::SeqCst) >= 1).then_some(()) }
    })
    .await;
    rejoined
        .shutdown()
        .await
        .expect("rejoined member shuts down");
    assert_eq!(
        second.load(Ordering::SeqCst),
        1,
        "a consumer group resumes from its commit, it does not replay history"
    );
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_appended_messages_when_streamed_then_should_drain_each_once_and_end_when_caught_up()
{
    use futures::StreamExt;

    let laser = harness::laser().await;
    let conversation = ConversationId::new();
    send_events(&laser, conversation, "stream", 1..=4).await;

    // `stream()` yields one message at a time and ends when caught up, the same
    // shape as `async for` in the Python binding. A fresh cursor starts at offset
    // zero, so it drains the whole topic.
    let commands = laser.topic(
        AgentTopic::Commands
            .name()
            .expect("commands has a topic name"),
    );
    harness::eventually(|| {
        let commands = commands.clone();
        async move {
            let count = commands
                .replay()
                .expect("reader builds")
                .stream()
                .count()
                .await;
            (count == 4).then_some(())
        }
    })
    .await;

    // A second stream from a fresh cursor drains from zero again (streaming is
    // read-only, it commits nothing).
    let again: Vec<_> = commands
        .replay()
        .expect("reader builds")
        .stream()
        .collect()
        .await;
    assert_eq!(
        again.len(),
        4,
        "a fresh stream drains the whole topic again"
    );
    assert!(
        again.into_iter().all(|item| item.is_ok()),
        "every streamed item decodes"
    );
}

async fn send_events(
    laser: &Laser,
    conversation: ConversationId,
    tag: &str,
    range: std::ops::RangeInclusive<i64>,
) {
    for i in range {
        let provenance = Provenance::builder()
            .conversation_id(conversation)
            .agent("counter".parse().expect("counter is a valid agent id"))
            .idempotency_key(format!("{tag}-{i}"))
            .build();
        laser
            .send_agent(
                AgentTopic::Commands,
                Bytes::from(format!("{i}")),
                &provenance,
            )
            .await
            .expect("event should be sent");
    }
}

/// Counts what it consumes and nothing more. Its only purpose is to advance and
/// commit the consumer-group offset, so the replay runs against a topic whose
/// committed offset already sits past every message.
struct Counter {
    handled: Arc<AtomicUsize>,
}

impl AgentHandler for Counter {
    async fn handle(&self, _message: &AgentMessage, _ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        self.handled.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

fn sum_events(acc: i64, message: &ContextMessage) -> i64 {
    acc + String::from_utf8_lossy(&message.payload)
        .parse::<i64>()
        .unwrap_or(0)
}
