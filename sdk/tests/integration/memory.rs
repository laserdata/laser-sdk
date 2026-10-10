use crate::harness;
use laser_sdk::iggy::prelude::{Identifier, StreamClient, TopicClient};
use laser_sdk::prelude::full::*;
use laser_sdk::wire::snapshot::{FoldSnapshot, SnapshotOffset};

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_multi_topic_snapshot_when_resumed_then_should_read_each_topic_after_its_own_offset()
 {
    let laser = harness::laser().await;
    laser
        .bootstrap(
            1,
            laser_sdk::agent::TopicRetention::expire_after(std::time::Duration::from_secs(86_400)),
        )
        .await
        .expect("agent topics exist");
    laser
        .topic("agent.snapshots")
        .ensure(1)
        .await
        .expect("snapshot topic exists");
    let conversation = ConversationId::new();
    let provenance = Provenance::builder()
        .conversation_id(conversation)
        .agent("planner".parse().expect("valid agent id"))
        .build();
    for _ in 0..3 {
        laser
            .send_agent(AgentTopic::Sessions, b"command".to_vec(), &provenance)
            .await
            .expect("command sent");
    }
    laser
        .send_agent(AgentTopic::Streams, b"response".to_vec(), &provenance)
        .await
        .expect("response sent");
    let topics = vec![AgentTopic::Sessions, AgentTopic::Streams];
    harness::eventually(|| {
        let laser = laser.clone();
        let topics = topics.clone();
        async move {
            let history = ContextAssembler::builder()
                .conversation_id(conversation)
                .topics(topics)
                .build()
                .assemble(&laser)
                .await
                .expect("history reads");
            (history.len() == 4).then_some(())
        }
    })
    .await;
    let checkpoint = laser
        .context(conversation)
        .checkpoint(&topics)
        .await
        .expect("checkpoint captured");
    let stream_name = laser.default_stream().expect("default stream");
    let stream_id = Identifier::named(stream_name).expect("stream id");
    let stream = laser
        .client()
        .get_stream(&stream_id)
        .await
        .expect("stream read")
        .expect("stream exists");
    let mut as_of = Vec::new();
    for topic in &topics {
        let name = topic.topic_string();
        let details = laser
            .client()
            .get_topic(&stream_id, &topic.as_identifier())
            .await
            .expect("topic read")
            .expect("topic exists");
        for (&partition, &next) in checkpoint.topic_offsets(&name).expect("topic offsets") {
            if next == 0 {
                continue;
            }
            as_of.push(SnapshotOffset::new(
                details.id,
                details.created_at.as_micros(),
                partition,
                next - 1,
            ));
        }
    }
    as_of.sort_by_key(|entry| {
        (
            entry.topic_id,
            entry.topic_created_at_micros,
            entry.partition_id,
        )
    });
    let snapshot = FoldSnapshot {
        stream: stream_name.to_owned(),
        stream_id: stream.id,
        stream_created_at_micros: stream.created_at.as_micros(),
        conversation: conversation.into(),
        fold: "planner".to_owned(),
        as_of,
        state: b"4".to_vec(),
    };
    let mut stale = snapshot.clone();
    stale.as_of[0].topic_created_at_micros += 1;
    assert!(matches!(
        laser_sdk::agent::checkpoint_from_snapshot(&laser, &stale, &topics).await,
        Err(LaserError::Invalid(_))
    ));
    stale = snapshot.clone();
    stale.stream_created_at_micros += 1;
    assert!(matches!(
        laser_sdk::agent::checkpoint_from_snapshot(&laser, &stale, &topics).await,
        Err(LaserError::Invalid(_))
    ));
    let store = TopicSnapshotStore::new(laser.clone(), "planner");
    store.save(&snapshot).await.expect("snapshot saved");
    laser
        .send_agent(AgentTopic::Sessions, b"command".to_vec(), &provenance)
        .await
        .expect("new command sent");
    laser
        .send_agent(AgentTopic::Streams, b"response".to_vec(), &provenance)
        .await
        .expect("new response sent");
    let count = harness::eventually(|| {
        let laser = laser.clone();
        let topics = topics.clone();
        let store = &store;
        async move {
            let count = ConversationState::load_with(
                &laser,
                store,
                conversation,
                topics,
                0u64,
                |count, _| count + 1,
            )
            .await
            .expect("state resumed");
            (count == 6).then_some(count)
        }
    })
    .await;
    assert_eq!(count, 6);
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_remembered_items_when_recalling_and_forgetting_then_should_reflect_the_changes() {
    let laser = harness::laser().await;
    let memory = LogMemory::new(laser.clone());
    let scope = MemoryScope::builder()
        .conversation(ConversationId::new())
        .agent("notetaker".parse().expect("notetaker is a valid agent id"))
        .build();

    let first = memory
        .remember(&scope, b"dark mode".to_vec())
        .await
        .expect("remembering the first item should succeed");
    memory
        .remember(&scope, b"CET timezone".to_vec())
        .await
        .expect("remembering the second item should succeed");
    memory
        .remember(&scope, b"friday deploys".to_vec())
        .await
        .expect("remembering the third item should succeed");

    let three = harness::eventually(|| async {
        let items = memory
            .recall_folded(&scope, &MemoryQuery::builder().build())
            .await
            .expect("recall should succeed");
        (items.len() == 3).then_some(items)
    })
    .await;
    assert_eq!(three.len(), 3);

    memory
        .forget(&scope, first)
        .await
        .expect("forgetting the first item should succeed");

    let two = harness::eventually(|| async {
        let items = memory
            .recall_folded(&scope, &MemoryQuery::builder().build())
            .await
            .expect("recall should succeed");
        (items.len() == 2).then_some(items)
    })
    .await;
    assert_eq!(two.len(), 2);
    assert!(two.iter().all(|item| item.id != first));
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_configured_memory_topic_when_remembering_then_should_recall_it_back() {
    // The `memory_topic` builder ensures the topic with a partition count and a
    // message-expiry, then the same verbs ride it. More partitions spread scopes.
    // One conversation stays on one partition, so its recall tail is correct.
    let laser = harness::laser().await;
    let memory = laser
        .memory_topic("agent_notes")
        .partitions(4)
        .ttl(std::time::Duration::from_secs(7 * 24 * 60 * 60))
        .build()
        .await
        .expect("building the configured memory topic should succeed");
    let conversation = ConversationId::new();

    memory
        .remember(b"prefers dark mode".to_vec())
        .scope(conversation)
        .send()
        .await
        .expect("remembering the first item should succeed");
    memory
        .remember(b"works in CET".to_vec())
        .scope(conversation)
        .send()
        .await
        .expect("remembering the second item should succeed");

    let items = harness::eventually(|| async {
        let items = memory
            .recall(conversation)
            .limit(10)
            .folded()
            .fetch()
            .await
            .expect("recall should succeed");
        (items.len() == 2).then_some(items)
    })
    .await;
    assert_eq!(items.len(), 2);
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_named_point_state_when_set_then_should_ride_the_stream_and_read_back() {
    // The named-item altitude is memory too: set publishes an event to the memory
    // topic (not a direct key-value write), and a fresh handle folds it back.
    let laser = harness::laser().await;
    let memory = laser.memory("profiles");
    memory
        .set("host:123", br#"{"log_level":"info"}"#.to_vec())
        .await
        .expect("set should publish");
    memory
        .update(
            "host:123",
            br#"{"log_level":"debug","region":"eu"}"#.to_vec(),
        )
        .await
        .expect("update should merge and publish");

    // A fresh handle rebuilds the point state from the topic alone. The topic is
    // eventually consistent across handles, so wait for the merge (the update) to
    // fold in, not just the first set, before asserting.
    let reader = laser.memory("profiles");
    let value = harness::eventually(|| async {
        let payload = reader
            .fetch_folded("host:123")
            .await
            .expect("fetch should succeed")?;
        let json: serde_json::Value = serde_json::from_slice(&payload).ok()?;
        (json["log_level"] == "debug").then_some(json)
    })
    .await;
    assert_eq!(value["log_level"], "debug");
    assert_eq!(value["region"], "eu");

    memory
        .remove("host:123")
        .await
        .expect("remove should publish a tombstone");
    harness::eventually(|| async {
        reader
            .fetch_folded("host:123")
            .await
            .expect("fetch should succeed")
            .is_none()
            .then_some(())
    })
    .await;
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_two_streams_when_recalling_then_should_isolate_at_the_stream_boundary() {
    // User isolation = Iggy stream isolation. Two `Laser`s on separate streams, and what
    // one remembers is invisible from the other: the connection itself is the boundary.
    let acme = harness::laser().await;
    let globex = harness::laser().await;
    let acme_memory = LogMemory::new(acme.clone());
    let globex_memory = LogMemory::new(globex.clone());
    let conversation = ConversationId::new();
    let scope = MemoryScope::builder().conversation(conversation).build();

    acme_memory
        .remember(&scope, b"acme secret".to_vec())
        .await
        .expect("remembering on the acme stream should succeed");
    globex_memory
        .remember(&scope, b"globex secret".to_vec())
        .await
        .expect("remembering on the globex stream should succeed");

    let acme_items = harness::eventually(|| async {
        let items = acme_memory
            .recall_folded(&scope, &MemoryQuery::builder().build())
            .await
            .expect("acme recall should succeed");
        (!items.is_empty()).then_some(items)
    })
    .await;
    assert_eq!(acme_items.len(), 1);
    assert_eq!(acme_items[0].payload.as_slice(), b"acme secret");

    let globex_items = harness::eventually(|| async {
        let items = globex_memory
            .recall_folded(&scope, &MemoryQuery::builder().build())
            .await
            .expect("globex recall should succeed");
        (!items.is_empty()).then_some(items)
    })
    .await;
    assert_eq!(globex_items.len(), 1);
    assert_eq!(globex_items[0].payload.as_slice(), b"globex secret");
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_shared_topic_namespaces_when_feedback_and_forget_run_then_should_keep_their_scope() {
    let laser = harness::laser().await;
    let left = LogMemory::in_namespace(laser.clone(), "left");
    let right = LogMemory::in_namespace(laser.clone(), "right");
    let scope = MemoryScope::builder()
        .conversation(ConversationId::new())
        .agent("notetaker".parse().expect("a valid agent id"))
        .user("reader".to_owned())
        .app("diagnostics".to_owned())
        .build();
    let left_id = left
        .remember(&scope, b"left".to_vec())
        .await
        .expect("remember left");
    let right_id = right
        .remember(&scope, b"right".to_vec())
        .await
        .expect("remember right");
    left.improve(&scope, Feedback::new(left_id, 4.0))
        .await
        .expect("improve left");

    let reader = LogMemory::in_namespace(laser.clone(), "left");
    let improved = harness::eventually(|| async {
        let items = reader
            .recall_folded(&scope, &MemoryQuery::builder().build())
            .await
            .expect("recall left");
        (items.len() == 1 && items[0].score == Some(4.0)).then_some(items)
    })
    .await;
    assert_eq!(improved[0].id, left_id);

    left.forget(&scope, left_id).await.expect("forget left");
    harness::eventually(|| async {
        reader
            .recall_folded(&scope, &MemoryQuery::builder().build())
            .await
            .expect("recall after forget")
            .is_empty()
            .then_some(())
    })
    .await;

    let right_reader = LogMemory::in_namespace(laser, "right");
    let untouched = harness::eventually(|| async {
        let items = right_reader
            .recall_folded(&scope, &MemoryQuery::builder().build())
            .await
            .expect("recall right");
        (items.len() == 1).then_some(items)
    })
    .await;
    assert_eq!(untouched[0].id, right_id);
    assert_eq!(untouched[0].score, None);
}
