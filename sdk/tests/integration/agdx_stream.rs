use crate::harness;
use laser_sdk::prelude::full::*;
use laser_sdk::wire::agent::{ConversationId as WireConversationId, CorrelationId, OPERATION_CHAT};

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_chunk_stream_when_reassembled_from_the_log_then_should_replay_in_order() {
    let laser = harness::laser().await;
    let conversation = ConversationId::new();
    let correlation = CorrelationId::from_u128(0x0190_3c1f_aa00_0000_0000_0000_0000_0011);

    // Produce a token stream as AGDX chunks (opening chunk declares the purpose).
    let mut stream = laser
        .agdx(
            AgentTopic::LlmIo,
            "model".parse().expect("model is a valid agent id"),
            WireConversationId::from(conversation),
        )
        .stream(correlation, OPERATION_CHAT);
    let channel = stream.channel();
    stream
        .write(b"Hel".to_vec())
        .await
        .expect("first chunk writes");
    stream
        .write(b"lo".to_vec())
        .await
        .expect("second chunk writes");
    stream
        .finish("stop", None)
        .await
        .expect("the terminal writes");

    // Reassemble it from the log after the fact: offset replay, no SSE.
    let events = harness::eventually(|| {
        let laser = laser.clone();
        async move {
            let events = laser
                .reassemble_channel(conversation, AgentTopic::LlmIo, channel)
                .await
                .ok()?;
            events
                .iter()
                .any(|event| {
                    matches!(
                        event,
                        StreamEvent::Finished {
                            synthetic: false,
                            ..
                        }
                    )
                })
                .then_some(events)
        }
    })
    .await;

    let body: Vec<u8> = events
        .iter()
        .flat_map(|event| match event {
            StreamEvent::Body { payload, .. } => payload.clone(),
            _ => Vec::new(),
        })
        .collect();
    assert_eq!(
        body, b"Hello",
        "the chunk bodies reassemble in sequence order"
    );
    assert!(
        matches!(
            events.last(),
            Some(StreamEvent::Finished { synthetic: false, finish_reason: Some(reason), .. }) if reason == "stop"
        ),
        "the real terminal closes the stream, not a synthetic one"
    );
}
