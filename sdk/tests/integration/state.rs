use crate::harness;
use bytes::Bytes;
use laser_sdk::prelude::*;

fn sum_events(acc: i64, message: &ContextMessage) -> i64 {
    acc + String::from_utf8_lossy(&message.payload)
        .parse::<i64>()
        .unwrap_or(0)
}

#[tokio::test]
async fn given_appended_events_when_replaying_conversation_state_then_should_fold_deterministically()
 {
    let laser = harness::laser().await;
    let conversation = ConversationId::new();
    for i in 1..=5 {
        let provenance = Provenance::builder()
            .conversation_id(conversation)
            .agent("counter".parse().expect("counter is a valid agent id"))
            .build();
        laser
            .send_agent(
                AgentTopic::Commands,
                Bytes::from(format!("{i}")),
                &provenance,
            )
            .await
            .expect("the event should be sent");
    }

    let sum = harness::eventually(|| async {
        let sum = ConversationState::load(
            &laser,
            conversation,
            vec![AgentTopic::Commands],
            0,
            sum_events,
        )
        .await
        .expect("replaying the conversation should succeed");
        (sum == 15).then_some(sum)
    })
    .await;
    assert_eq!(sum, 15);

    let replayed = ConversationState::load(
        &laser,
        conversation,
        vec![AgentTopic::Commands],
        0,
        sum_events,
    )
    .await
    .expect("replaying again should succeed");
    assert_eq!(replayed, 15);
}
