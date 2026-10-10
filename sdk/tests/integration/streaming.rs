use crate::harness;
use laser_sdk::iggy::prelude::{
    Consumer, Identifier, MessageClient, PollingStrategy, StreamClient, TopicClient,
};
use laser_sdk::prelude::{CommitPolicy, ConsumerMessage, ConsumerStart, ProducerMessage, Routing};
use laser_sdk::stream::{HeaderKey, HeaderValue};
use std::str::FromStr;
use std::time::Duration;

const RECEIVE_TIMEOUT: Duration = Duration::from_secs(15);

#[tokio::test]
async fn given_native_offset_history_when_rewound_and_deleted_then_should_follow_the_replay_option()
{
    let laser = harness::laser().await;
    let topic = laser.topic("native-offset-history");
    topic
        .producer()
        .partitions(1)
        .build()
        .await
        .expect("the native producer initializes")
        .send_batch([
            ProducerMessage::new(b"zero".as_slice()),
            ProducerMessage::new(b"one".as_slice()),
            ProducerMessage::new(b"two".as_slice()),
        ])
        .await
        .expect("the native offset records publish");
    for allow_replay in [false, true] {
        let builder = topic
            .consumer(format!("native-offset-history-{allow_replay}"), 0)
            .start_at(ConsumerStart::First)
            .commit_policy(CommitPolicy::Disabled);
        let builder = if allow_replay {
            builder.allow_replay()
        } else {
            builder
        };
        let mut consumer = builder.build().await.expect("the native consumer builds");
        let mut records = Vec::new();
        for _ in 0..3 {
            records.push(
                consumer
                    .next_within(RECEIVE_TIMEOUT)
                    .await
                    .expect("the native record arrives"),
            );
        }
        consumer.commit(&records[2]).await.expect("commit two");
        consumer.commit(&records[1]).await.expect("commit one");
        let expected = if allow_replay { 1 } else { 2 };
        assert_eq!(consumer.last_stored_offset(0), Some(expected));
        consumer.store_offset(2, Some(0)).await.expect("store two");
        consumer.store_offset(2, Some(0)).await.expect("repeat two");
        consumer.store_offset(1, Some(0)).await.expect("store one");
        assert_eq!(
            consumer
                .stored_offset(0)
                .await
                .expect("read the stored offset")
                .expect("an offset exists")
                .stored_offset,
            expected
        );
        consumer
            .delete_offset(Some(0))
            .await
            .expect("delete the offset");
        assert!(
            consumer
                .stored_offset(0)
                .await
                .expect("read after deletion")
                .is_none()
        );
        assert_eq!(consumer.last_stored_offset(0), Some(expected));
        consumer
            .store_offset(1, Some(0))
            .await
            .expect("store after deletion");
        assert_eq!(
            consumer
                .stored_offset(0)
                .await
                .expect("read after storing")
                .map(|offset| offset.stored_offset),
            allow_replay.then_some(1)
        );
        consumer
            .store_offset(0, Some(0))
            .await
            .expect("zero always stores");
        assert_eq!(consumer.last_stored_offset(0), Some(0));
        assert_eq!(
            consumer
                .stored_offset(0)
                .await
                .expect("read zero")
                .expect("zero exists")
                .stored_offset,
            0
        );
        consumer
            .shutdown()
            .await
            .expect("the native consumer closes");
    }
}

#[tokio::test]
async fn given_production_profile_when_streaming_then_should_preserve_delivery_and_offsets() {
    let laser = harness::connected_laser().await;
    let topic = laser.topic("production-streaming");
    let producer = topic
        .producer()
        .batch_length(1000)
        .linger(Duration::from_millis(5))
        .routing(Routing::Balanced)
        .partitions(1)
        .build()
        .await
        .expect("the Laser producer should initialize");

    let type_key = HeaderKey::from_str("type").expect("the type header should be valid");
    let message = ProducerMessage::new(b"production-event".as_slice())
        .header(type_key.clone(), HeaderValue::from(7_u16));
    let first_send = producer
        .send_keyed(message, b"account-42".to_vec())
        .await
        .expect("the keyed message should publish");
    let first_confirmation = first_send
        .confirmations
        .first()
        .expect("the server should confirm the keyed message");
    assert_eq!(first_confirmation.partition_id, 0);
    let sent = producer
        .send_batch_with_routing(
            [ProducerMessage::new(b"production-batch".as_slice())],
            Some(Routing::Partition(0)),
        )
        .await
        .expect("the Laser batch should publish");
    let batch_confirmation = sent
        .confirmations
        .first()
        .expect("the server should confirm the batch");
    assert_eq!(batch_confirmation.partition_id, 0);
    assert!(batch_confirmation.base_offset > first_confirmation.base_offset);

    let mut consumer = topic
        .consumer_group("production-auto-workers")
        .consumer()
        .batch_length(1000)
        .poll_interval(Duration::from_millis(5))
        .commit_policy(CommitPolicy::IntervalOrEach(Duration::from_secs(1)))
        .start_at(ConsumerStart::Next)
        .build()
        .await
        .expect("the Laser consumer should initialize");
    let received: ConsumerMessage = consumer
        .next_within(RECEIVE_TIMEOUT)
        .await
        .expect("the Laser consumer should receive the production event");
    assert_eq!(received.payload.as_ref(), b"production-event");
    assert_eq!(
        received
            .headers
            .get(&type_key)
            .expect("the type header should be present")
            .as_uint16()
            .expect("the type header should remain uint16"),
        7
    );
    let batch = consumer
        .next_within(RECEIVE_TIMEOUT)
        .await
        .expect("the Laser consumer should receive the batch message");
    assert_eq!(batch.payload.as_ref(), b"production-batch");
    assert!(matches!(
        consumer.next_within(Duration::from_millis(50)).await,
        Err(laser_sdk::LaserError::Timeout(_))
    ));
    harness::eventually(|| async {
        (consumer.last_stored_offset(batch.partition_id) == Some(batch.position.offset))
            .then_some(())
    })
    .await;
    consumer
        .shutdown()
        .await
        .expect("the auto-commit consumer should shut down");
    producer
        .send(b"auto-resumed".as_slice())
        .await
        .expect("the resume marker should publish");
    let mut resumed = topic
        .consumer_group("production-auto-workers")
        .consumer()
        .poll_interval(Duration::from_millis(5))
        .commit_policy(CommitPolicy::Disabled)
        .start_at(ConsumerStart::Next)
        .build()
        .await
        .expect("the auto-commit group should rejoin");
    let received = resumed
        .next_within(RECEIVE_TIMEOUT)
        .await
        .expect("the resumed group should receive the auto-resumed marker");
    assert_eq!(received.payload.as_ref(), b"auto-resumed");
    resumed
        .shutdown()
        .await
        .expect("the resumed group should shut down");

    let mut uncommitted = topic
        .consumer_group("production-uncommitted-workers")
        .consumer()
        .poll_interval(Duration::from_millis(5))
        .commit_policy(CommitPolicy::Disabled)
        .start_at(ConsumerStart::Next)
        .build()
        .await
        .expect("the uncommitted group should initialize");
    let first = uncommitted
        .next_within(RECEIVE_TIMEOUT)
        .await
        .expect("the uncommitted group should receive a message");
    uncommitted
        .shutdown()
        .await
        .expect("the uncommitted group should shut down without committing");
    let mut uncommitted = topic
        .consumer_group("production-uncommitted-workers")
        .consumer()
        .poll_interval(Duration::from_millis(5))
        .commit_policy(CommitPolicy::Disabled)
        .start_at(ConsumerStart::Next)
        .build()
        .await
        .expect("the uncommitted group should rejoin");
    let replayed = uncommitted
        .next_within(RECEIVE_TIMEOUT)
        .await
        .expect("the uncommitted group should redeliver the record");
    assert_eq!(replayed.position, first.position);
    uncommitted
        .commit(&replayed)
        .await
        .expect("the redelivered record should commit");
    uncommitted
        .shutdown()
        .await
        .expect("the committed group should shut down");

    let mut manual = topic
        .consumer_group("production-manual-workers")
        .consumer()
        .batch_length(1000)
        .poll_interval(Duration::from_millis(5))
        .commit_policy(CommitPolicy::Disabled)
        .start_at(ConsumerStart::Next)
        .build()
        .await
        .expect("the manual Laser consumer should initialize");
    for _ in 0..3 {
        let received = manual
            .next_within(RECEIVE_TIMEOUT)
            .await
            .expect("the manual Laser consumer should receive a message");
        manual
            .commit(&received)
            .await
            .expect("the handled offset should store");
        assert_eq!(
            manual.last_stored_offset(received.partition_id),
            Some(received.position.offset)
        );
    }
    manual
        .shutdown()
        .await
        .expect("the manual consumer should shut down");
    producer
        .send(b"manual-resumed".as_slice())
        .await
        .expect("the manual resume marker should publish");
    let mut manual = topic
        .consumer_group("production-manual-workers")
        .consumer()
        .poll_interval(Duration::from_millis(5))
        .commit_policy(CommitPolicy::Disabled)
        .start_at(ConsumerStart::Next)
        .build()
        .await
        .expect("the manual group should rejoin");
    let received = manual
        .next_within(RECEIVE_TIMEOUT)
        .await
        .expect("the manual group should receive the manual-resumed marker");
    assert_eq!(received.payload.as_ref(), b"manual-resumed");
    manual
        .shutdown()
        .await
        .expect("the resumed manual group should shut down");

    let mut standalone = topic
        .consumer("production-audit", 0)
        .start_at(ConsumerStart::First)
        .commit_policy(CommitPolicy::Disabled)
        .allow_replay()
        .build()
        .await
        .expect("the standalone Laser consumer should initialize");
    let replayed = standalone
        .next_within(RECEIVE_TIMEOUT)
        .await
        .expect("the standalone consumer should receive the replayed event");
    assert_eq!(replayed.payload.as_ref(), b"production-event");
    standalone
        .shutdown()
        .await
        .expect("the standalone consumer should shut down");
}

#[tokio::test]
async fn given_a_bootstrapped_stream_when_deleted_then_should_remove_it_once() {
    let laser = harness::laser().await;
    let stream = laser
        .default_stream()
        .expect("the test laser names its stream")
        .to_owned();
    assert!(
        laser
            .stream(&stream)
            .delete()
            .await
            .expect("the stream should delete")
    );
    assert!(
        !laser
            .stream(&stream)
            .delete()
            .await
            .expect("a missing stream should report absence")
    );
    let identifier = Identifier::named(&stream).expect("the stream name is a valid identifier");
    assert!(
        laser
            .client()
            .get_stream(&identifier)
            .await
            .expect("the stream lookup should succeed")
            .is_none()
    );
}

#[tokio::test]
async fn given_a_closed_laser_when_used_then_should_refuse_further_requests() {
    let laser = harness::laser().await;
    let clone = laser.clone();
    laser.close().await.expect("the connection should close");
    laser
        .close()
        .await
        .expect("a second close should be a no-op");
    assert!(clone.client().get_streams().await.is_err());
}

#[tokio::test]
async fn given_a_cached_stream_when_deleted_and_recreated_then_should_discard_its_previous_state() {
    let laser = harness::laser().await;
    let stream = laser.default_stream().expect("the test stream is selected");
    let identifier = Identifier::named(stream).expect("the stream name is valid");
    let agent = "retired-agent".parse().expect("the agent id is valid");
    for externally_deleted in [false, true] {
        laser
            .bootstrap(
                1,
                laser_sdk::agent::TopicRetention::expire_after(std::time::Duration::from_secs(
                    86_400,
                )),
            )
            .await
            .expect("bootstrap the stream");
        laser
            .quarantine(
                "operator".parse().expect("the operator id is valid"),
                &agent,
            )
            .await
            .expect("publish a registry fact");
        let mut registry = laser.agent_registry().expect("open the registry");
        registry.refresh(0).await.expect("read the registry");
        assert!(registry.is_quarantined(&agent));
        if externally_deleted {
            laser
                .client()
                .delete_stream(&identifier)
                .await
                .expect("delete outside Laser");
        }
        assert_eq!(
            laser
                .stream(stream)
                .delete()
                .await
                .expect("invalidate the stream"),
            !externally_deleted
        );
        laser
            .bootstrap(
                1,
                laser_sdk::agent::TopicRetention::expire_after(std::time::Duration::from_secs(
                    86_400,
                )),
            )
            .await
            .expect("recreate the stream");
        let mut registry = laser.agent_registry().expect("open a fresh registry");
        assert!(!registry.is_quarantined(&agent));
        laser
            .quarantine(
                "operator".parse().expect("the operator id is valid"),
                &agent,
            )
            .await
            .expect("publish with a fresh producer");
        registry
            .refresh(0)
            .await
            .expect("read from the new stream's start");
        assert!(registry.is_quarantined(&agent));
        laser
            .stream(stream)
            .delete()
            .await
            .expect("remove the recreated stream");
    }
    laser.close().await.expect("close the connection");
}

#[tokio::test]
async fn given_a_fresh_consumer_when_reading_next_then_should_start_at_zero_and_resume_after_a_real_commit()
 {
    let laser = harness::laser().await;
    let topic = laser.topic("default-next");
    let producer = topic
        .producer()
        .partitions(1)
        .build()
        .await
        .expect("producer");
    producer
        .send_batch_with_routing(
            [
                ProducerMessage::new(b"zero".as_slice()),
                ProducerMessage::new(b"one".as_slice()),
            ],
            Some(Routing::Partition(0)),
        )
        .await
        .expect("publish");
    let build = || {
        topic
            .consumer("fresh", 0)
            .batch_length(1)
            .commit_policy(CommitPolicy::Disabled)
            .build()
    };
    let mut first = build().await.expect("first consumer");
    let record = first
        .next_within(RECEIVE_TIMEOUT)
        .await
        .expect("first record");
    assert_eq!(record.position.offset, 0);
    first.shutdown().await.expect("uncommitted shutdown");
    let mut retried = build().await.expect("retry consumer");
    let record = retried
        .next_within(RECEIVE_TIMEOUT)
        .await
        .expect("uncommitted record repeats");
    assert_eq!(record.position.offset, 0);
    retried.commit(&record).await.expect("commit zero");
    retried.shutdown().await.expect("shutdown");
    let mut resumed = build().await.expect("resume consumer");
    assert_eq!(
        resumed
            .next_within(RECEIVE_TIMEOUT)
            .await
            .expect("next after committed zero")
            .position
            .offset,
        1
    );
    resumed.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn given_default_polling_when_shutdown_after_offset_zero_then_should_resume_at_one() {
    let laser = harness::connected_laser().await;
    let topic = laser.topic("default-shutdown-zero");
    let producer = topic
        .producer()
        .partitions(1)
        .build()
        .await
        .expect("producer");
    producer
        .send_batch_with_routing(
            [
                ProducerMessage::new(b"zero".as_slice()),
                ProducerMessage::new(b"one".as_slice()),
                ProducerMessage::new(b"two".as_slice()),
                ProducerMessage::new(b"three".as_slice()),
            ],
            Some(Routing::Partition(0)),
        )
        .await
        .expect("publish");
    let mut first = topic
        .consumer("partial", 0)
        .batch_length(4)
        .build()
        .await
        .expect("consumer");
    assert_eq!(
        first
            .next_within(RECEIVE_TIMEOUT)
            .await
            .expect("first record")
            .position
            .offset,
        0
    );
    first
        .shutdown()
        .await
        .expect("shutdown with a partial batch");
    let mut resumed = topic
        .consumer("partial", 0)
        .batch_length(4)
        .build()
        .await
        .expect("resume");
    assert_eq!(
        resumed
            .next_within(RECEIVE_TIMEOUT)
            .await
            .expect("remaining record")
            .position
            .offset,
        1
    );
    resumed.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn given_each_commit_policy_when_shutdown_after_zero_then_should_preserve_its_resume_contract()
 {
    let server = crate::test_iggy::TestIggy::start_pinned().await;
    let laser = harness::connected_laser_on(&server).await;
    let interval = Duration::from_secs(60);
    let policies = [
        CommitPolicy::Disabled,
        CommitPolicy::Interval(interval),
        CommitPolicy::Polling,
        CommitPolicy::IntervalOrPolling(interval),
        CommitPolicy::All,
        CommitPolicy::IntervalOrAll(interval),
        CommitPolicy::Each,
        CommitPolicy::IntervalOrEach(interval),
        CommitPolicy::Every(10),
        CommitPolicy::IntervalOrEvery(interval, 10),
    ];
    for (index, policy) in policies.into_iter().enumerate() {
        for group in [false, true] {
            let topic = laser.topic(format!("policy-zero-{index}-{group}"));
            let producer = topic
                .producer()
                .partitions(1)
                .build()
                .await
                .expect("producer");
            producer
                .send_batch_with_routing(
                    [
                        ProducerMessage::new(b"zero".as_slice()),
                        ProducerMessage::new(b"one".as_slice()),
                    ],
                    Some(Routing::Partition(0)),
                )
                .await
                .expect("publish");
            let builder = if group {
                topic.consumer_group("worker").consumer()
            } else {
                topic.consumer("worker", 0)
            };
            let mut first = builder
                .batch_length(2)
                .commit_policy(policy)
                .build()
                .await
                .expect("consumer");
            assert_eq!(
                first
                    .next_within(RECEIVE_TIMEOUT)
                    .await
                    .expect("zero")
                    .position
                    .offset,
                0
            );
            first.shutdown().await.expect("shutdown");
            let builder = if group {
                topic.consumer_group("worker").consumer()
            } else {
                topic.consumer("worker", 0)
            };
            let mut resumed = builder
                .commit_policy(CommitPolicy::Disabled)
                .build()
                .await
                .expect("resume");
            let found = resumed
                .next_within(RECEIVE_TIMEOUT)
                .await
                .expect("resumed message")
                .position
                .offset;
            let expected = u64::from(policy != CommitPolicy::Disabled);
            assert_eq!(found, expected, "policy {policy:?}, group {group}");
            resumed.shutdown().await.expect("close");
        }
    }
}

// A purge restarts the partition at offset zero and clears every stored
// offset. A consumer rebuilt after the purge must read the replacement
// history from its first record under every commit policy.
#[tokio::test]
async fn given_a_purged_topic_when_the_consumer_is_rebuilt_then_should_start_at_the_new_offset_zero()
 {
    let laser = harness::connected_laser().await;
    let stream = laser
        .default_stream()
        .expect("the test laser names its stream")
        .to_owned();
    let interval = Duration::from_secs(60);
    let policies = [
        CommitPolicy::Disabled,
        CommitPolicy::Interval(interval),
        CommitPolicy::Polling,
        CommitPolicy::IntervalOrPolling(interval),
        CommitPolicy::All,
        CommitPolicy::IntervalOrAll(interval),
        CommitPolicy::Each,
        CommitPolicy::IntervalOrEach(interval),
        CommitPolicy::Every(10),
        CommitPolicy::IntervalOrEvery(interval, 10),
    ];
    for (index, policy) in policies.into_iter().enumerate() {
        for group in [false, true] {
            let topic_name = format!("policy-purge-{index}-{group}");
            let topic = laser.topic(&topic_name);
            let producer = topic
                .producer()
                .partitions(1)
                .build()
                .await
                .expect("producer");
            producer
                .send_batch_with_routing(
                    [
                        ProducerMessage::new(b"telemetry-0".as_slice()),
                        ProducerMessage::new(b"telemetry-1".as_slice()),
                        ProducerMessage::new(b"telemetry-2".as_slice()),
                    ],
                    Some(Routing::Partition(0)),
                )
                .await
                .expect("publish before purge");
            let builder = if group {
                topic.consumer_group("ground-station").consumer()
            } else {
                topic.consumer("ground-station", 0)
            };
            let mut before = builder
                .batch_length(3)
                .commit_policy(policy)
                .build()
                .await
                .expect("consumer before purge");
            for expected in 0..3 {
                let record = before
                    .next_within(RECEIVE_TIMEOUT)
                    .await
                    .expect("record before purge");
                assert_eq!(record.position.offset, expected);
                if policy == CommitPolicy::Disabled {
                    before.commit(&record).await.expect("manual commit");
                }
            }
            before.shutdown().await.expect("shutdown before purge");

            laser
                .client()
                .purge_topic(
                    &Identifier::named(&stream).expect("stream identifier"),
                    &Identifier::named(&topic_name).expect("topic identifier"),
                )
                .await
                .expect("purge");
            let stream_id = Identifier::named(&stream).expect("stream id");
            let topic_id = Identifier::named(&topic_name).expect("topic id");
            // Metadata acknowledges before the partition owner applies the purge.
            harness::eventually(|| async {
                let visible = laser
                    .client()
                    .poll_messages(
                        &stream_id,
                        &topic_id,
                        Some(0),
                        &Consumer::default(),
                        &PollingStrategy::first(),
                        3,
                        false,
                    )
                    .await
                    .expect("purge visibility poll");
                visible.messages.is_empty().then_some(())
            })
            .await;
            producer
                .send_batch_with_routing(
                    [
                        ProducerMessage::new(b"safe-mode-0".as_slice()),
                        ProducerMessage::new(b"safe-mode-1".as_slice()),
                    ],
                    Some(Routing::Partition(0)),
                )
                .await
                .expect("publish after purge");

            let builder = if group {
                topic.consumer_group("ground-station").consumer()
            } else {
                topic.consumer("ground-station", 0)
            };
            let mut after = builder
                .commit_policy(policy)
                .build()
                .await
                .expect("consumer after purge");
            let record = after
                .next_within(RECEIVE_TIMEOUT)
                .await
                .expect("first record after purge");
            assert_eq!(
                (record.position.offset, record.payload.as_ref()),
                (0, b"safe-mode-0".as_slice()),
                "policy {policy:?}, group {group}"
            );
            after.shutdown().await.expect("shutdown after purge");
            producer.shutdown().await.expect("producer shutdown");
        }
    }
}

#[tokio::test]
async fn given_producers_racing_on_a_new_topic_when_built_then_should_all_initialize() {
    let laser = harness::laser().await;
    let topic = laser.topic("racing-topic-creation");
    let builds = (0..8).map(|_| topic.producer().partitions(1).build());
    for result in futures::future::join_all(builds).await {
        result.expect("a producer that lost the creation race still initializes");
    }
}
