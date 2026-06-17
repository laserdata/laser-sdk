use crate::harness;
use laser_sdk::prelude::*;
use laser_sdk::wire::agent::{
    ConversationId as WireConversationId, CorrelationId, OPERATION_CHAT, OPERATION_TOOL_ARGS,
};
use serde_json::json;

#[tokio::test]
async fn given_a_snapshot_and_deltas_when_reconstructed_then_should_replay_the_state() {
    let laser = harness::laser().await;
    let conversation = ConversationId::new();
    let source = "ui".parse().expect("ui is a valid agent id");

    // Snapshot the initial shared state, then mutate it with an RFC 6902 delta -
    // both ride the log as `event`s, no SSE.
    laser
        .publish_state_snapshot(
            AgentTopic::Audit,
            source,
            conversation,
            &json!({"count": 0, "items": []}),
        )
        .await
        .expect("the snapshot publishes");
    laser
        .publish_state_delta(
            AgentTopic::Audit,
            "ui".parse().expect("ui is a valid agent id"),
            conversation,
            &json!([
                {"op": "replace", "path": "/count", "value": 1},
                {"op": "add", "path": "/items/-", "value": "a"}
            ]),
        )
        .await
        .expect("the delta publishes");

    // Replaying snapshot + delta reconstructs the current state.
    let state = harness::eventually(|| {
        let laser = laser.clone();
        async move {
            let state = laser
                .reconstruct_state(conversation, AgentTopic::Audit)
                .await
                .expect("reconstruct succeeds");
            state.filter(|state| state["count"] == 1)
        }
    })
    .await;

    assert_eq!(state, json!({"count": 1, "items": ["a"]}));
}

#[tokio::test]
async fn given_a_chat_stream_when_rendered_then_should_produce_agui_text_events() {
    let laser = harness::laser().await;
    let conversation = ConversationId::new();
    let correlation = CorrelationId::from_u128(0x0190_3c1f_aa00_0000_0000_0000_0000_0021);

    let mut stream = laser
        .agdx(
            AgentTopic::LlmIo,
            "model".parse().expect("model is a valid agent id"),
            WireConversationId::from(conversation),
        )
        .stream(correlation, OPERATION_CHAT);
    stream.write(b"Hi".to_vec()).await.expect("chunk writes");
    stream.finish("stop", None).await.expect("terminal writes");

    let events = harness::eventually(|| {
        let laser = laser.clone();
        async move {
            let events = laser
                .agui_events(conversation, AgentTopic::LlmIo)
                .await
                .expect("rendering succeeds");
            (events.len() >= 2).then_some(events)
        }
    })
    .await;

    // The chat chunk stream renders as TEXT_MESSAGE_START -> CONTENT -> END.
    assert!(matches!(
        &events[0],
        AgUiEvent::TextMessageStart { role, .. } if role == "assistant"
    ));
    assert!(events.iter().any(|event| matches!(
        event,
        AgUiEvent::TextMessageContent { delta, .. } if delta == "Hi"
    )));
    assert!(matches!(
        events.last(),
        Some(AgUiEvent::TextMessageEnd { .. })
    ));
}

#[tokio::test]
async fn given_a_tool_args_stream_when_rendered_then_should_produce_agui_tool_call_events() {
    let laser = harness::laser().await;
    let conversation = ConversationId::new();
    let correlation = CorrelationId::from_u128(0x0190_3c1f_aa00_0000_0000_0000_0000_0031);

    let mut stream = laser
        .agdx(
            AgentTopic::ToolCalls,
            "model".parse().expect("model is a valid agent id"),
            WireConversationId::from(conversation),
        )
        .stream(correlation, OPERATION_TOOL_ARGS);
    stream
        .write(br#"{"q":1}"#.to_vec())
        .await
        .expect("args chunk writes");
    stream.finish("stop", None).await.expect("terminal writes");

    let events = harness::eventually(|| {
        let laser = laser.clone();
        async move {
            let events = laser
                .agui_events(conversation, AgentTopic::ToolCalls)
                .await
                .expect("rendering succeeds");
            (events.len() >= 2).then_some(events)
        }
    })
    .await;

    // The tool_args chunk stream renders as TOOL_CALL_START -> ARGS -> END,
    // not as a text message.
    assert!(matches!(&events[0], AgUiEvent::ToolCallStart { .. }));
    assert!(events.iter().any(|event| matches!(
        event,
        AgUiEvent::ToolCallArgs { delta, .. } if delta == r#"{"q":1}"#
    )));
    assert!(matches!(events.last(), Some(AgUiEvent::ToolCallEnd { .. })));
}
