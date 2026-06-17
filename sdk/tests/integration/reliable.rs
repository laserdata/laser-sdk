use crate::harness;
use bytes::Bytes;
use iggy::prelude::IggyTimestamp;
use laser_sdk::prelude::*;
use laser_sdk::wire::agent::{AgentDeadLetter, DeadLetterReason};
use laser_sdk::wire::framing::decode_named;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

struct Worker {
    handled: Arc<AtomicUsize>,
}

impl AgentHandler for Worker {
    async fn handle(&self, message: &AgentMessage, _ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        if message.payload.as_slice() == b"poison" {
            return Err(LaserError::Handler("poison message".to_owned()));
        }
        if message.payload.as_slice() == b"reject" {
            return Err(LaserError::rejected("permanent failure"));
        }
        self.handled.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn given_a_duplicate_and_a_poison_message_when_consumed_then_should_dedupe_and_dead_letter() {
    let laser = harness::laser().await;
    let handled = Arc::new(AtomicUsize::new(0));

    Agent::builder()
        .id("worker".parse().expect("worker is a valid agent id"))
        .listen_on(AgentTopic::Commands)
        .handler(Worker {
            handled: handled.clone(),
        })
        .build()
        .spawn(laser.clone());

    let conversation = ConversationId::new();
    let good = Provenance::builder()
        .conversation_id(conversation)
        .idempotency_key("job-1".to_owned())
        .build();
    laser
        .send_agent(AgentTopic::Commands, Bytes::from_static(b"work"), &good)
        .await
        .expect("the first job should be sent");
    laser
        .send_agent(AgentTopic::Commands, Bytes::from_static(b"work"), &good)
        .await
        .expect("the duplicate job should be sent");

    let poison = Provenance::builder()
        .conversation_id(conversation)
        .idempotency_key("job-2".to_owned())
        .build();
    laser
        .send_agent(AgentTopic::Commands, Bytes::from_static(b"poison"), &poison)
        .await
        .expect("the poison message should be sent");

    let dead = harness::eventually(|| {
        let laser = laser.clone();
        async move {
            let dead = ContextAssembler::builder()
                .conversation_id(conversation)
                .topics(vec![AgentTopic::Dlq])
                .build()
                .assemble(&laser)
                .await
                .expect("assembling the dead-letter topic should succeed");
            (!dead.is_empty()).then_some(dead)
        }
    })
    .await;

    // The conversation is ordered, so once the poison is dead-lettered both
    // copies of the good job have been processed: handled exactly once.
    assert_eq!(handled.load(Ordering::SeqCst), 1);
    assert_eq!(dead.len(), 1);
    let capsule = decode_named::<AgentDeadLetter>(&dead[0].payload)
        .expect("the dead-letter payload is an AgentDeadLetter capsule");
    assert_eq!(capsule.reason, DeadLetterReason::RetryExhausted);
    assert_eq!(capsule.attempts, RetryPolicy::default().max_attempts);
    assert!(
        capsule
            .detail
            .unwrap_or_default()
            .contains("poison message")
    );
    assert_eq!(capsule.payload.as_slice(), b"poison");
    // The capsule's log position and the provenance causal parent describe the
    // same poison message, so redrive and the audit trail agree.
    let parent = dead[0]
        .provenance
        .causal_parent
        .expect("the dead-letter carries the source message id as the causal parent");
    assert_eq!(capsule.source.partition_id, parent.partition_id);
    assert_eq!(capsule.source.offset, parent.offset);
}

#[tokio::test]
async fn given_a_rejected_message_when_consumed_then_should_dead_letter_without_retry() {
    let laser = harness::laser().await;
    let handled = Arc::new(AtomicUsize::new(0));

    Agent::builder()
        .id("rejecter".parse().expect("rejecter is a valid agent id"))
        .listen_on(AgentTopic::Commands)
        .handler(Worker {
            handled: handled.clone(),
        })
        .build()
        .spawn(laser.clone());

    let conversation = ConversationId::new();
    let provenance = Provenance::builder()
        .conversation_id(conversation)
        .idempotency_key("rej-1".to_owned())
        .build();
    laser
        .send_agent(
            AgentTopic::Commands,
            Bytes::from_static(b"reject"),
            &provenance,
        )
        .await
        .expect("the rejected message should be sent");

    let dead = harness::eventually(|| {
        let laser = laser.clone();
        async move {
            let dead = ContextAssembler::builder()
                .conversation_id(conversation)
                .topics(vec![AgentTopic::Dlq])
                .build()
                .assemble(&laser)
                .await
                .expect("assembling the dead-letter topic should succeed");
            (!dead.is_empty()).then_some(dead)
        }
    })
    .await;

    assert_eq!(dead.len(), 1);
    let capsule = decode_named::<AgentDeadLetter>(&dead[0].payload)
        .expect("the dead-letter payload is an AgentDeadLetter capsule");
    assert_eq!(capsule.reason, DeadLetterReason::Rejected);
    assert_eq!(capsule.attempts, 1);
    assert!(
        capsule
            .detail
            .unwrap_or_default()
            .contains("rejected: permanent failure")
    );
    assert_eq!(capsule.payload.as_slice(), b"reject");
    // A permanent rejection is never handled (never counted) and never retried.
    assert_eq!(handled.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn given_a_message_past_its_deadline_when_consumed_then_should_dead_letter_before_the_handler()
 {
    let laser = harness::laser().await;
    let handled = Arc::new(AtomicUsize::new(0));

    Agent::builder()
        .id("worker".parse().expect("worker is a valid agent id"))
        .listen_on(AgentTopic::Commands)
        .handler(Worker {
            handled: handled.clone(),
        })
        .build()
        .spawn(laser.clone());

    let conversation = ConversationId::new();
    let provenance = Provenance::builder()
        .conversation_id(conversation)
        // A deadline far in the past, so the message is dropped on arrival.
        .deadline(IggyTimestamp::from(1u64))
        .build();
    laser
        .send_agent(
            AgentTopic::Commands,
            Bytes::from_static(b"work"),
            &provenance,
        )
        .await
        .expect("the expired message should be sent");

    let dead = harness::eventually(|| {
        let laser = laser.clone();
        async move {
            let dead = ContextAssembler::builder()
                .conversation_id(conversation)
                .topics(vec![AgentTopic::Dlq])
                .build()
                .assemble(&laser)
                .await
                .expect("assembling the dead-letter topic should succeed");
            (!dead.is_empty()).then_some(dead)
        }
    })
    .await;

    assert_eq!(dead.len(), 1);
    let capsule = decode_named::<AgentDeadLetter>(&dead[0].payload)
        .expect("the dead-letter payload is an AgentDeadLetter capsule");
    assert_eq!(capsule.reason, DeadLetterReason::DeadlineExceeded);
    assert_eq!(capsule.attempts, 0);
    assert_eq!(capsule.payload.as_slice(), b"work");
    // The deadline is checked before dispatch, so the handler never runs.
    assert_eq!(handled.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn given_a_dead_letter_when_redriven_then_should_reinject_the_original_to_its_source_topic() {
    let laser = harness::laser().await;
    let handled = Arc::new(AtomicUsize::new(0));

    Agent::builder()
        .id("rejecter".parse().expect("rejecter is a valid agent id"))
        .listen_on(AgentTopic::Commands)
        .handler(Worker {
            handled: handled.clone(),
        })
        .build()
        .spawn(laser.clone());

    let conversation = ConversationId::new();
    // No idempotency key, so the redriven copy is not deduped away.
    let provenance = Provenance::builder().conversation_id(conversation).build();
    laser
        .send_agent(
            AgentTopic::Commands,
            Bytes::from_static(b"reject"),
            &provenance,
        )
        .await
        .expect("the rejected message should be sent");

    let dead = harness::eventually(|| {
        let laser = laser.clone();
        async move {
            let dead = ContextAssembler::builder()
                .conversation_id(conversation)
                .topics(vec![AgentTopic::Dlq])
                .build()
                .assemble(&laser)
                .await
                .expect("assembling the dead-letter topic should succeed");
            (!dead.is_empty()).then_some(dead)
        }
    })
    .await;
    let capsule = decode_named::<AgentDeadLetter>(&dead[0].payload)
        .expect("the dead-letter payload is an AgentDeadLetter capsule");

    laser
        .redrive_dead_letter(&capsule)
        .await
        .expect("redrive republishes the original record to its source topic");

    // The redriven copy is rejected again, so a second dead-letter appears for
    // the same payload at a new source position.
    let both = harness::eventually(|| {
        let laser = laser.clone();
        async move {
            let dead = ContextAssembler::builder()
                .conversation_id(conversation)
                .topics(vec![AgentTopic::Dlq])
                .build()
                .assemble(&laser)
                .await
                .expect("assembling the dead-letter topic should succeed");
            (dead.len() >= 2).then_some(dead)
        }
    })
    .await;

    assert_eq!(both.len(), 2);
    let capsules: Vec<_> = both
        .iter()
        .map(|message| {
            decode_named::<AgentDeadLetter>(&message.payload)
                .expect("every dead-letter payload is an AgentDeadLetter capsule")
        })
        .collect();
    assert!(capsules.iter().all(|c| c.payload.as_slice() == b"reject"));
    // The redriven copy lives at a distinct log position from the original.
    assert_ne!(capsules[0].source.offset, capsules[1].source.offset);
}
