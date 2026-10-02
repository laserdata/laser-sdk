// Consumer groups against the fork without a managed plane: no catalog, so no
// group carries a policy. A group consumer reads every record through the
// group engine, and every policy verb answers the typed refusal that names
// what is missing.

use crate::harness;
use laser_sdk::filters::{ConsumerFilter, FilterExpr};
use laser_sdk::prelude::full::{Capabilities, Laser, LaserError};
use laser_sdk::prelude::{CommitPolicy, ConsumerStart, ProducerMessage, Routing};
use laser_sdk::query::CmpOp;
use std::time::Duration;

const TOPIC: &str = "fleet_changes";
const READ_TIMEOUT: Duration = Duration::from_secs(15);

const SAFE_MODE: &str = r#"{"op":"u","table":"satellites","changed":["mode"],"after":{"id":"sat-042","name":"Kestrel-42","mode":"safe","orbit":"leo","battery_pct":61}}"#;
const DECOMMISSION: &str = r#"{"op":"d","table":"satellites","before":{"id":"sat-042"}}"#;
const GROUND_STATION: &str = r#"{"op":"u","table":"ground_stations","changed":["status"],"after":{"id":"svalbard","status":"online"}}"#;

fn safe_mode_filter() -> ConsumerFilter {
    ConsumerFilter::json(FilterExpr::any([
        FilterExpr::pred("op", CmpOp::Eq, "d"),
        FilterExpr::pred("changed", CmpOp::Contains, "mode"),
    ]))
}

async fn publish(laser: &Laser, payloads: &[&str]) {
    let producer = laser
        .topic(TOPIC)
        .producer()
        .partitions(1)
        .build()
        .await
        .expect("the producer initializes");
    producer
        .send_batch_with_routing(
            payloads
                .iter()
                .map(|payload| ProducerMessage::new(payload.as_bytes().to_vec())),
            Some(Routing::Partition(0)),
        )
        .await
        .expect("the fixtures publish");
}

#[tokio::test]
async fn given_no_catalog_when_a_group_is_created_with_a_filter_then_should_refuse_before_creating_it()
 {
    let laser = harness::connected_laser().await;
    assert!(!laser.capabilities().await.filters.catalog);
    laser
        .topic(TOPIC)
        .ensure(1)
        .await
        .expect("the topic exists");
    let group = laser.topic(TOPIC).consumer_group("anomaly-desk");

    let refused = group
        .create()
        .filter(safe_mode_filter())
        .build()
        .await
        .expect_err("a policy needs the catalog");

    assert!(matches!(
        refused,
        LaserError::Unsupported {
            surface: "filters",
            feature: Some("catalog"),
            ..
        }
    ));
    let plain = group
        .create()
        .build()
        .await
        .expect("a plain group needs no catalog");
    assert_eq!(plain.name, "anomaly-desk");
    assert!(plain.filter.is_none());
    assert!(matches!(
        group.filter().get().await,
        Err(LaserError::Unsupported { .. })
    ));
}

#[tokio::test]
async fn given_no_catalog_when_a_group_consumes_then_should_read_every_record_unfiltered() {
    let laser = harness::connected_laser().await;
    assert!(laser.capabilities().await.filters.group_policy_reads);
    publish(&laser, &[SAFE_MODE, GROUND_STATION, DECOMMISSION]).await;
    let group = laser.topic(TOPIC).consumer_group("plain-desk");

    let mut consumer = group
        .consumer()
        .start_at(ConsumerStart::First)
        .commit_policy(CommitPolicy::Disabled)
        .poll_interval(Duration::from_millis(5))
        .build()
        .await
        .expect("a group consumer builds without a catalog");
    let mut offsets = Vec::new();
    for _ in 0..3 {
        let message = consumer
            .next_within(READ_TIMEOUT)
            .await
            .expect("a record arrives");
        offsets.push(message.position.offset);
        consumer.commit(&message).await.expect("the record commits");
    }
    consumer.shutdown().await.expect("the consumer shuts down");

    assert_eq!(
        offsets,
        vec![0, 1, 2],
        "an unbound group receives everything"
    );
    assert_eq!(
        group.info().await.expect("the group exists").filter,
        None,
        "no catalog, no policy"
    );
}

#[tokio::test]
async fn given_each_commit_policy_when_a_group_consumer_shuts_down_after_zero_then_should_resume_after_the_delivered_record()
 {
    let laser = harness::connected_laser().await;
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
        let topic = laser.topic(format!("policy-zero-{index}"));
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
        let group = topic.consumer_group("worker");
        let mut first = group
            .consumer()
            .batch_length(2)
            .commit_policy(policy)
            .build()
            .await
            .expect("consumer");
        assert_eq!(
            first
                .next_within(READ_TIMEOUT)
                .await
                .expect("zero")
                .position
                .offset,
            0
        );
        first.shutdown().await.expect("shutdown");
        let mut resumed = group
            .consumer()
            .commit_policy(CommitPolicy::Disabled)
            .build()
            .await
            .expect("resumed consumer");
        let expected = u64::from(policy != CommitPolicy::Disabled);
        assert_eq!(
            resumed
                .next_within(READ_TIMEOUT)
                .await
                .expect("the next record")
                .position
                .offset,
            expected,
            "{policy:?} resumes after the delivered record, a manual consumer after its last commit"
        );
        resumed.shutdown().await.expect("shutdown");
    }
}

#[tokio::test]
async fn given_a_server_without_consumer_filters_when_building_a_reader_then_should_refuse() {
    let open = Laser::from_client(harness::client().await).with_default_stream("any");
    assert_eq!(open.capabilities().await, Capabilities::OPEN);

    let refused = open
        .topic(TOPIC)
        .consumer_group("any")
        .reader()
        .expect("the builder needs only a stream")
        .build()
        .await;

    assert!(refused.err().is_some_and(|error| error.is_unsupported()));
}
