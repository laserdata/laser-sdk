use crate::harness;
use laser_sdk::prelude::full::*;

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
        .set("customer:123", br#"{"tier":"pro"}"#.to_vec())
        .await
        .expect("set should publish");
    memory
        .update(
            "customer:123",
            br#"{"tier":"enterprise","region":"eu"}"#.to_vec(),
        )
        .await
        .expect("update should merge and publish");

    // A fresh handle rebuilds the point state from the topic alone. The topic is
    // eventually consistent across handles, so wait for the merge (the update) to
    // fold in, not just the first set, before asserting.
    let reader = laser.memory("profiles");
    let value = harness::eventually(|| async {
        let payload = reader
            .fetch_folded("customer:123")
            .await
            .expect("fetch should succeed")?;
        let json: serde_json::Value = serde_json::from_slice(&payload).ok()?;
        (json["tier"] == "enterprise").then_some(json)
    })
    .await;
    assert_eq!(value["tier"], "enterprise");
    assert_eq!(value["region"], "eu");

    memory
        .remove("customer:123")
        .await
        .expect("remove should publish a tombstone");
    harness::eventually(|| async {
        reader
            .fetch_folded("customer:123")
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
