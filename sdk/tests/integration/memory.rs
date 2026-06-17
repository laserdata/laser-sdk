use crate::harness;
use laser_sdk::prelude::*;

#[tokio::test]
async fn given_remembered_items_when_recalling_and_forgetting_then_should_reflect_the_changes() {
    let laser = harness::laser().await;
    let memory = LogMemory::new(&laser);
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
            .recall(&scope, &MemoryQuery::builder().build())
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
            .recall(&scope, &MemoryQuery::builder().build())
            .await
            .expect("recall should succeed");
        (items.len() == 2).then_some(items)
    })
    .await;
    assert_eq!(two.len(), 2);
    assert!(two.iter().all(|item| item.id != first));
}

#[tokio::test]
async fn given_two_streams_when_recalling_then_should_isolate_at_the_stream_boundary() {
    // User isolation = Iggy stream isolation. Two `Laser`s on separate streams, and what
    // one remembers is invisible from the other - the connection itself is the boundary.
    let acme = harness::laser().await;
    let globex = harness::laser().await;
    let acme_memory = LogMemory::new(&acme);
    let globex_memory = LogMemory::new(&globex);
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
            .recall(&scope, &MemoryQuery::builder().build())
            .await
            .expect("acme recall should succeed");
        (!items.is_empty()).then_some(items)
    })
    .await;
    assert_eq!(acme_items.len(), 1);
    assert_eq!(acme_items[0].payload.as_slice(), b"acme secret");

    let globex_items = harness::eventually(|| async {
        let items = globex_memory
            .recall(&scope, &MemoryQuery::builder().build())
            .await
            .expect("globex recall should succeed");
        (!items.is_empty()).then_some(items)
    })
    .await;
    assert_eq!(globex_items.len(), 1);
    assert_eq!(globex_items[0].payload.as_slice(), b"globex secret");
}
