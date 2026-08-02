use crate::harness;
use bytes::Bytes;
use iggy::prelude::{Identifier, IggyTimestamp, TopicClient};
use laser_sdk::agent::{ConsumerRef, ConsumptionStatus};
use laser_sdk::prelude::full::*;
use laser_sdk::wire::agent::{AgentDeadLetter, DeadLetterReason};
use laser_sdk::wire::framing::decode_named;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

struct Worker {
    handled: Arc<AtomicUsize>,
}

struct RejectingWorker {
    attempts: Arc<AtomicUsize>,
}

impl AgentHandler for RejectingWorker {
    async fn handle(&self, _message: &AgentMessage, _ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        Err(LaserError::rejected("permanent failure"))
    }
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
#[serial_test::serial(integration)]
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
#[serial_test::serial(integration)]
async fn given_an_agent_restarted_on_its_group_when_a_new_message_arrives_then_should_resume_consuming()
 {
    let laser = harness::laser().await;
    let handled = Arc::new(AtomicUsize::new(0));

    let mut first = Agent::builder()
        .id("resumer".parse().expect("resumer is a valid agent id"))
        .listen_on(AgentTopic::Commands)
        .concurrency(ConcurrencyPolicy::SerialPerPartition { max_partitions: 8 })
        .handler(Worker {
            handled: handled.clone(),
        })
        .build()
        .spawn(laser.clone());
    first
        .ready()
        .await
        .expect("the first consumer becomes ready");

    let conversation = ConversationId::new();
    let before = Provenance::builder()
        .conversation_id(conversation)
        .idempotency_key("resume-1".to_owned())
        .build();
    laser
        .send_agent(AgentTopic::Commands, Bytes::from_static(b"work"), &before)
        .await
        .expect("the pre-restart job should be sent");
    harness::eventually(|| {
        let handled = handled.clone();
        async move { (handled.load(Ordering::SeqCst) == 1).then_some(()) }
    })
    .await;
    first
        .shutdown()
        .await
        .expect("the first consumer drains cleanly");

    // The restarted process opens its own connection while the original
    // process's connection is still alive (a rolling restart, or other clones of
    // the first Laser outliving the drained consumer). The drained member must
    // have left the group, or the newcomer splits partitions with a ghost.
    let restarted = harness::reconnect(&laser).await;
    let mut second = Agent::builder()
        .id("resumer".parse().expect("resumer is a valid agent id"))
        .listen_on(AgentTopic::Commands)
        .concurrency(ConcurrencyPolicy::SerialPerPartition { max_partitions: 8 })
        .handler(Worker {
            handled: handled.clone(),
        })
        .build()
        .spawn(restarted);
    second
        .ready()
        .await
        .expect("the second consumer becomes ready");

    let after = Provenance::builder()
        .conversation_id(conversation)
        .idempotency_key("resume-2".to_owned())
        .build();
    laser
        .send_agent(AgentTopic::Commands, Bytes::from_static(b"work"), &after)
        .await
        .expect("the post-restart job should be sent");
    harness::eventually(|| {
        let handled = handled.clone();
        async move { (handled.load(Ordering::SeqCst) == 2).then_some(()) }
    })
    .await;
    second
        .shutdown()
        .await
        .expect("the second consumer drains cleanly");
}

#[tokio::test]
#[serial_test::serial(integration)]
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
#[serial_test::serial(integration)]
async fn given_a_missing_dlq_topic_when_publish_fails_then_should_redeliver_before_commit() {
    let laser = harness::laser().await;
    let stream = Identifier::named(
        laser
            .default_stream()
            .expect("the test laser names its stream"),
    )
    .expect("the stream name is valid");
    let dlq = Identifier::named(&AgentTopic::Dlq.topic_string()).expect("the DLQ name is valid");
    laser
        .client()
        .delete_topic(&stream, &dlq)
        .await
        .expect("the DLQ topic is removed");

    let attempts = Arc::new(AtomicUsize::new(0));
    let mut first = Agent::builder()
        .id("dlq-outage".parse().expect("the agent id is valid"))
        .listen_on(AgentTopic::Commands)
        .handler(RejectingWorker {
            attempts: attempts.clone(),
        })
        .build()
        .spawn(laser.clone());
    first.ready().await.expect("the first worker becomes ready");

    let conversation = ConversationId::new();
    let provenance = Provenance::builder().conversation_id(conversation).build();
    laser
        .send_agent(
            AgentTopic::Commands,
            Bytes::from_static(b"reject"),
            &provenance,
        )
        .await
        .expect("the rejected message is published");

    let failure = first
        .join()
        .await
        .expect_err("a required DLQ publish failure stops the worker");
    assert!(matches!(failure, LaserError::Iggy(_)));
    assert_eq!(attempts.load(Ordering::SeqCst), 1);

    laser
        .topic(AgentTopic::Dlq.topic_string())
        .ensure(4)
        .await
        .expect("the DLQ topic is restored");
    let restarted = harness::reconnect(&laser).await;
    let mut second = Agent::builder()
        .id("dlq-outage".parse().expect("the agent id is valid"))
        .listen_on(AgentTopic::Commands)
        .handler(RejectingWorker {
            attempts: attempts.clone(),
        })
        .build()
        .spawn(restarted);
    second
        .ready()
        .await
        .expect("the replacement worker becomes ready");

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
            (dead.len() == 1).then_some(dead)
        }
    })
    .await;
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    let capsule = decode_named::<AgentDeadLetter>(&dead[0].payload)
        .expect("the dead-letter payload is an AgentDeadLetter capsule");
    assert_eq!(capsule.payload, b"reject");
    assert!(matches!(
        laser
            .consumed(
                ConsumerRef::Group(
                    "dlq-outage"
                        .parse()
                        .expect("the consumer group name is valid"),
                ),
                capsule.source,
            )
            .await
            .expect("the committed source offset can be read"),
        ConsumptionStatus::Consumed { .. }
    ));
    second
        .shutdown()
        .await
        .expect("the replacement worker drains cleanly");
}

#[tokio::test]
#[serial_test::serial(integration)]
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
#[serial_test::serial(integration)]
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
    // Keyed on purpose: the consumer observed this key when the message first
    // dead-lettered, so the redrive must survive dedup via its re-keyed copy.
    let provenance = Provenance::builder()
        .conversation_id(conversation)
        .idempotency_key("redrive-1".to_owned())
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

struct CapsuleRecorder {
    capsules: Arc<std::sync::Mutex<Vec<AgentDeadLetter>>>,
}

#[async_trait::async_trait]
impl DeadLetterSink for CapsuleRecorder {
    async fn on_dead_letter(
        &self,
        _message: Option<&AgentMessage>,
        capsule: &AgentDeadLetter,
        _publish_result: &Result<(), LaserError>,
    ) {
        self.capsules
            .lock()
            .expect("the lock should not be poisoned")
            .push(capsule.clone());
    }
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_poison_record_inside_a_batch_when_dead_lettered_then_should_stamp_its_own_offset()
{
    use laser_sdk::iggy::prelude::{HeaderKey, HeaderValue};
    use laser_sdk::wire::agent::{
        AgentId as WireAgentId, ConversationId as WireConversationId,
        CorrelationId as WireCorrelationId,
    };
    use laser_sdk::wire::codes::AGENT_OP_VERSION;
    use laser_sdk::wire::headers::AGENT_VERSION;
    use std::str::FromStr;

    let laser = harness::laser().await;
    let conversation = ConversationId::new();
    let wire_conversation = WireConversationId::from(conversation);
    let source = "batcher".parse::<WireAgentId>().expect("the id is valid");
    let garbage = b"not cbor at all, definitely".to_vec();

    // Three records land on one partition before the consumer starts, so one
    // poll returns the batch: valid, undecodable, valid. The capsule must
    // carry the poison record's own offset, not the batch high-water mark.
    laser
        .agdx(AgentTopic::Commands, source.clone(), wire_conversation)
        .command(WireCorrelationId::from_u128(0x0501), b"first".to_vec())
        .send()
        .await
        .expect("the first command should publish");
    let mut headers = std::collections::BTreeMap::new();
    headers.insert(
        HeaderKey::from_str(AGENT_VERSION).expect("the header key is valid"),
        HeaderValue::from(AGENT_OP_VERSION),
    );
    laser
        .topic(AgentTopic::Commands.topic_string())
        .send(garbage.clone(), headers, Some(&conversation.to_string()))
        .await
        .expect("the poison record should publish");
    laser
        .agdx(AgentTopic::Commands, source, wire_conversation)
        .command(WireCorrelationId::from_u128(0x0502), b"second".to_vec())
        .send()
        .await
        .expect("the second command should publish");

    let handled = Arc::new(AtomicUsize::new(0));
    let capsules = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut worker = Agent::builder()
        .id("batcher".parse().expect("the agent id is valid"))
        .listen_on(AgentTopic::Commands)
        .handler(Worker {
            handled: handled.clone(),
        })
        .on_dead_letter(Arc::new(CapsuleRecorder {
            capsules: capsules.clone(),
        }))
        .build()
        .spawn(laser.clone());
    worker.ready().await.expect("the worker should be ready");

    let capsule = harness::eventually(|| {
        let handled = handled.clone();
        let capsules = capsules.clone();
        async move {
            let recorded = capsules
                .lock()
                .expect("the lock should not be poisoned")
                .clone();
            (handled.load(Ordering::SeqCst) == 2 && recorded.len() == 1)
                .then(|| recorded[0].clone())
        }
    })
    .await;
    assert_eq!(capsule.reason, DeadLetterReason::DecodeFailed);
    assert_eq!(capsule.payload, garbage);
    assert_eq!(
        capsule.source.offset, 1,
        "the capsule must stamp the poison record's offset, not the batch high water",
    );

    // Redriving the capsule replays the poison bytes themselves: the copy is
    // undecodable again and dead-letters from a new log position.
    laser
        .redrive_dead_letter(&capsule)
        .await
        .expect("the redrive should republish the poison record");
    let redriven = harness::eventually(|| {
        let capsules = capsules.clone();
        async move {
            let recorded = capsules
                .lock()
                .expect("the lock should not be poisoned")
                .clone();
            (recorded.len() == 2).then(|| recorded[1].clone())
        }
    })
    .await;
    assert_eq!(redriven.payload, garbage);
    assert_ne!(
        (redriven.source.partition_id, redriven.source.offset),
        (capsule.source.partition_id, capsule.source.offset),
    );
    worker.shutdown().await.expect("the worker should drain");
}
