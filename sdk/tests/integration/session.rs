use crate::harness;
use bytes::Bytes;
use laser_sdk::prelude::full::*;

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_per_user_sessions_when_messaging_then_should_isolate_each_user() {
    let laser = harness::laser().await;
    let policy = SessionPolicy::PerUser;
    let alice = policy.conversation_for("alice");
    let bob = policy.conversation_for("bob");

    for (conversation, text) in [(alice, "alice message"), (bob, "bob message")] {
        let provenance = Provenance::builder().conversation_id(conversation).build();
        laser
            .send_agent(
                AgentTopic::Commands,
                Bytes::copy_from_slice(text.as_bytes()),
                &provenance,
            )
            .await
            .expect("the user message should be sent");
    }

    let alice_context = harness::eventually(|| async {
        let messages = ContextAssembler::builder()
            .conversation_id(alice)
            .topics(vec![AgentTopic::Commands])
            .build()
            .assemble(&laser)
            .await
            .expect("assembling alice's conversation should succeed");
        (!messages.is_empty()).then_some(messages)
    })
    .await;

    assert_eq!(alice_context.len(), 1);
    assert_eq!(alice_context[0].payload.as_slice(), b"alice message");
    // re-deriving the same user yields the same conversation: stable per user
    assert_eq!(policy.conversation_for("alice"), alice);
}
