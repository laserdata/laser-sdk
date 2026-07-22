use crate::harness;
use laser_sdk::prelude::{CommitPolicy, ConsumerMessage, ConsumerStart, ProducerMessage, Routing};
use laser_sdk::stream::{HeaderKey, HeaderValue};
use std::str::FromStr;
use std::time::Duration;

const RECEIVE_TIMEOUT: Duration = Duration::from_secs(15);

#[tokio::test]
async fn given_production_profile_when_streaming_then_should_preserve_delivery_and_offsets() {
    let laser = harness::laser().await;
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
    producer
        .send_keyed(message, b"account-42".to_vec())
        .await
        .expect("the keyed message should publish");
    let sent = producer
        .send_batch_with_routing(
            [ProducerMessage::new(b"production-batch".as_slice())],
            Some(Routing::Partition(0)),
        )
        .await
        .expect("the Laser batch should publish");
    assert_eq!(sent, 1);

    let mut consumer = topic
        .consumer_group("production-auto-workers")
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
