use crate::harness;
use laser_sdk::agent::{Clock, FINISH_REASON_ABANDONED, SystemClock};
use laser_sdk::prelude::full::*;
use laser_sdk::wire::agent::{ConversationId as WireConversationId, CorrelationId, OPERATION_CHAT};
use std::time::Duration;

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_chunk_stream_when_reassembled_from_the_log_then_should_replay_in_order() {
    let laser = harness::laser().await;
    let conversation = ConversationId::new();
    let correlation = CorrelationId::from_u128(0x0190_3c1f_aa00_0000_0000_0000_0000_0011);

    // Produce a token stream as AGDX chunks (opening chunk declares the purpose).
    let mut stream = laser
        .agdx(
            AgentTopic::Sessions,
            "model"
                .parse::<laser_sdk::types::AgentId>()
                .expect("model is a valid agent id"),
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
                .reassemble_channel(conversation, AgentTopic::Sessions, channel)
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

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_an_unfinished_stream_longer_than_fifty_chunks_when_reassembled_past_its_bound_then_should_end_abandoned()
 {
    let laser = harness::laser().await;
    let conversation = ConversationId::new();
    let correlation = CorrelationId::from_u128(0x0190_3c1f_aa00_0000_0000_0000_0000_0012);
    let chunks = 60;

    // The producer dies after its chunks: no terminal, a bound already past.
    let mut stream = laser
        .agdx(
            AgentTopic::Sessions,
            "model"
                .parse::<laser_sdk::types::AgentId>()
                .expect("model is a valid agent id"),
            WireConversationId::from(conversation),
        )
        .stream(correlation, OPERATION_CHAT)
        .with_deadline_at_micros(SystemClock.now_micros())
        .buffered(chunks, Duration::from_secs(60));
    let channel = stream.channel();
    for index in 0..chunks {
        stream
            .write(vec![u8::try_from(index).expect("the index fits a byte")])
            .await
            .expect("the chunk buffers");
    }
    stream
        .flush()
        .await
        .expect("the chunks append as one batch");
    drop(stream);

    let events = harness::eventually(|| {
        let laser = laser.clone();
        async move {
            let events = laser
                .reassemble_channel(conversation, AgentTopic::Sessions, channel)
                .await
                .ok()?;
            (!events.is_empty()).then_some(events)
        }
    })
    .await;

    let sequences: Vec<u64> = events
        .iter()
        .filter_map(|event| match event {
            StreamEvent::Body { sequence, .. } => Some(*sequence),
            _ => None,
        })
        .collect();
    assert_eq!(
        sequences,
        (0..u64::try_from(chunks).expect("the count fits")).collect::<Vec<_>>(),
        "every chunk reassembles from the opening one"
    );
    assert_eq!(
        events.last(),
        Some(&StreamEvent::Finished {
            finish_reason: Some(FINISH_REASON_ABANDONED.to_owned()),
            usage: None,
            synthetic: true,
        }),
        "a stream with no terminal past its opening bound ends abandoned"
    );
}
