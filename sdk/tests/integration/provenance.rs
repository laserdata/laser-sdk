use crate::harness;
use bytes::Bytes;
use laser_sdk::prelude::*;

#[tokio::test]
async fn given_a_message_with_provenance_when_read_back_from_iggy_then_should_preserve_every_field()
{
    let laser = harness::laser().await;
    let conversation = ConversationId::new();
    let sent = Provenance::builder()
        .conversation_id(conversation)
        .agent("planner".parse().expect("planner is a valid agent id"))
        .causal_parent(MessageId::new(2, 7))
        .idempotency_key("k1".to_owned())
        .build();

    laser
        .send_agent(AgentTopic::Commands, Bytes::from_static(b"hello"), &sent)
        .await
        .expect("the command should be sent");

    let messages = harness::eventually(|| async {
        let messages = ContextAssembler::builder()
            .conversation_id(conversation)
            .topics(vec![AgentTopic::Commands])
            .build()
            .assemble(&laser)
            .await
            .expect("assembling the conversation should succeed");
        (!messages.is_empty()).then_some(messages)
    })
    .await;

    assert_eq!(messages.len(), 1);
    let back = &messages[0];
    assert_eq!(back.provenance.conversation_id, conversation);
    assert_eq!(
        back.provenance
            .agent
            .as_ref()
            .expect("agent should be set")
            .as_str(),
        "planner"
    );
    assert_eq!(back.provenance.causal_parent, Some(MessageId::new(2, 7)));
    assert_eq!(back.provenance.idempotency_key.as_deref(), Some("k1"));
    assert_eq!(back.payload.as_slice(), b"hello");
}
