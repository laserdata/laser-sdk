use crate::harness;
use crate::test_iggy::TestIggyCluster;
use laser_sdk::capabilities::{Capabilities, HelloOutcome};
use laser_sdk::iggy::prelude::{
    Consumer as OffsetOwner, ConsumerGroupClient, ConsumerOffsetClient, Identifier, IggyError,
    StreamClient, TopicClient,
};
use laser_sdk::prelude::full::{Consumer, Laser, LaserError, Producer};
use laser_sdk::prelude::{CommitPolicy, ConsumerMessage, ConsumerStart, ProducerMessage, Routing};
use laser_sdk::stream::{HeaderKey, HeaderValue};
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::Instant;

const RECEIVE_TIMEOUT: Duration = Duration::from_secs(15);
const CLUSTER_PARTITIONS: u32 = 3;
const PULSE: &str = "pulse";
const PULSE_GROUP: &str = "pulse-readers";
const PULSE_POLICY_GROUP: &str = "pulse-policy-readers";
const PULSE_TAIL: &str = "pulse-tail";
const HELD_WAIT: Duration = Duration::from_secs(20);
// A held read woken by the record delivers in moments, one that waited out
// its hold takes `HELD_WAIT`, so half of it tells the two apart.
const PROMPT: Duration = Duration::from_secs(HELD_WAIT.as_secs() / 2);
const RECOVERY: Duration = Duration::from_secs(90);
const FIVE_NODES: usize = 5;
const REPLICATED: &str = "pulse-rf3";
const SINGLE_COPY: &str = "pulse-rf1";
const SERVER_DEFAULT: &str = "pulse-default";
const REFUSED_STREAM: &str = "cluster_refusals";
const REFUSED: &str = "refused";
const REFUSED_READER: &str = "refused-reader";
const REFUSAL_RETRY: Duration = Duration::from_millis(100);
// Six refusals in a row stop a partition after 31 retry intervals, well inside this.
const REFUSAL_BUDGET: Duration = Duration::from_secs(10);
const REBALANCED: &str = "rebalanced";
const REBALANCED_GROUP: &str = "rebalanced-readers";
const REBALANCED_PARTITIONS: u32 = 3;
const ROUND: u64 = 30;

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
        assert_eq!(
            consumer.last_stored_offset(0),
            None,
            "a deletion forgets the local record, so the next store reaches the server"
        );
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
            Some(1)
        );
        consumer.store_offset(0, Some(0)).await.expect("store zero");
        let rewound = if allow_replay { 0 } else { 1 };
        assert_eq!(
            consumer.last_stored_offset(0),
            Some(rewound),
            "zero moves the offset back only with replay allowed"
        );
        assert_eq!(
            consumer
                .stored_offset(0)
                .await
                .expect("read the rewound offset")
                .expect("an offset exists")
                .stored_offset,
            rewound
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

#[tokio::test]
async fn given_producers_racing_on_a_new_topic_when_built_then_should_all_initialize() {
    let laser = harness::laser().await;
    let topic = laser.topic("racing-topic-creation");
    let builds = (0..8).map(|_| topic.producer().partitions(1).build());
    for result in futures::future::join_all(builds).await {
        result.expect("a producer that lost the creation race still initializes");
    }
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_three_node_cluster_when_a_follower_restarts_and_the_leader_fails_over_then_group_and_partition_consumers_should_receive_every_record_through_held_reads()
 {
    let mut cluster = TestIggyCluster::start().await;
    let leader = cluster.leader(0).await;
    cluster.route_endpoint_to(leader);
    let laser = harness::cluster_laser(&cluster, "cluster_native").await;
    assert!(
        laser.capabilities().await.filters.group_policy_reads,
        "the fork reads groups through the policy-aware engine"
    );
    // A server that positively announces no filters reads groups natively.
    let mut refused = Capabilities::OPEN;
    refused.hello = HelloOutcome::Rejected;
    let open = laser.clone().with_capabilities(refused);
    let topic = laser.topic(PULSE);
    topic
        .ensure(CLUSTER_PARTITIONS, None)
        .await
        .expect("the topic exists");
    let primaries = harness::spread_primaries(&laser, PULSE, CLUSTER_PARTITIONS).await;
    assert!(
        primaries.iter().collect::<BTreeSet<_>>().len() > 1,
        "the partitions are led by different nodes: {primaries:?}"
    );
    let follower = (0..3)
        .filter(|node| *node != leader)
        .max_by_key(|node| leads(&primaries, *node))
        .expect("a follower");
    // The partition consumer follows a partition that one of the restarts moves.
    let tail_partition = (0..CLUSTER_PARTITIONS)
        .find(|partition| usize::from(primaries[*partition as usize]) == leader)
        .or_else(|| {
            (0..CLUSTER_PARTITIONS)
                .find(|partition| usize::from(primaries[*partition as usize]) == follower)
        })
        .unwrap_or(0);

    let native = open
        .topic(PULSE)
        .consumer_group(PULSE_GROUP)
        .consumer()
        .start_at(ConsumerStart::First)
        .commit_policy(CommitPolicy::Each)
        .max_wait(HELD_WAIT)
        .build()
        .await
        .expect("the native group consumer builds");
    let policy = topic
        .consumer_group(PULSE_POLICY_GROUP)
        .consumer()
        .start_at(ConsumerStart::First)
        .commit_policy(CommitPolicy::Each)
        .max_wait(HELD_WAIT)
        .build()
        .await
        .expect("the policy-aware group consumer builds");
    let tail = topic
        .consumer(PULSE_TAIL, tail_partition)
        .start_at(ConsumerStart::First)
        .commit_policy(CommitPolicy::Polling)
        .max_wait(HELD_WAIT)
        .build()
        .await
        .expect("the partition consumer builds");
    let stop = Arc::new(AtomicBool::new(false));
    let native = follow(native, Arc::clone(&stop));
    let policy = follow(policy, Arc::clone(&stop));
    let tail = follow(tail, Arc::clone(&stop));
    let producer = topic
        .producer()
        .partitions(CLUSTER_PARTITIONS)
        .build()
        .await
        .expect("the producer initializes");
    let publishing = Arc::new(AtomicBool::new(true));
    let next_sequence = Arc::new(AtomicU64::new(0));
    let confirmed = Arc::new(Mutex::new(BTreeMap::new()));
    let publisher = tokio::spawn(publish_until_stopped(
        producer,
        Arc::clone(&publishing),
        Arc::clone(&next_sequence),
        Arc::clone(&confirmed),
    ));

    let phase = Instant::now();
    wait_for_deliveries(&native.seen, 30, "steady streaming").await;
    let steady = phase.elapsed();
    let phase = Instant::now();
    cluster.restart_node(follower).await;
    let after_follower = delivered_count(&native.seen) + 30;
    wait_for_deliveries(&native.seen, after_follower, "the follower restart").await;
    let follower_restart = phase.elapsed();
    let phase = Instant::now();
    cluster.stop_node(leader);
    let new_leader = cluster.leader(follower).await;
    cluster.route_endpoint_to(new_leader);
    cluster.start_node(leader).await;
    let after_leader = delivered_count(&native.seen) + 30;
    wait_for_deliveries(&native.seen, after_leader, "the leader failover").await;
    let leader_failover = phase.elapsed();
    publishing.store(false, Ordering::Release);
    let producer = publisher.await.expect("the publisher stops");

    let confirmed = confirmed.lock().expect("confirmed lock").clone();
    let phase = Instant::now();
    let mut caught_up = Vec::new();
    for (name, follower, partitions) in [
        ("native group", &native, None),
        ("policy-aware group", &policy, None),
        ("partition", &tail, Some(tail_partition)),
    ] {
        wait_until(
            &format!("every confirmed record reaches the {name} consumer"),
            || {
                let received = sequences(&follower.seen);
                confirmed.iter().all(|(sequence, partition)| {
                    partitions.is_some_and(|only| only != *partition) || received.contains(sequence)
                })
            },
        )
        .await;
        caught_up.push((name, phase.elapsed()));
    }
    eprintln!(
        "cluster consumers: steady {steady:?}, follower restart {follower_restart:?}, leader failover {leader_failover:?}, caught up {caught_up:?}"
    );
    for (name, follower) in [
        ("native group", &native),
        ("policy-aware group", &policy),
        ("partition", &tail),
    ] {
        let deliveries = delivered_count(&follower.seen);
        eprintln!(
            "cluster {name} consumer: {} confirmed, {deliveries} deliveries, {} redelivered, longest gaps {:?}, errors {:?}",
            confirmed.len(),
            deliveries - sequences(&follower.seen).len(),
            longest_gaps(&follower.arrivals),
            follower.errors.lock().expect("errors lock")
        );
    }
    assert!(
        !policy
            .errors
            .lock()
            .expect("errors lock")
            .iter()
            .any(|error| error.contains("not accepted by a backup replica")),
        "the policy-aware reader follows a moved primary instead of reporting the backup's refusal"
    );

    // Once every hold is parked again, a record on any partition ends the
    // hold of its primary at once and arrives exactly once.
    tokio::time::sleep(Duration::from_secs(1)).await;
    let mut prompt = Vec::new();
    for partition in 0..CLUSTER_PARTITIONS {
        let sequence = next_sequence.fetch_add(1, Ordering::AcqRel) + 1;
        let sent = Instant::now();
        producer
            .send_to_partition(
                ProducerMessage::new(sequence.to_le_bytes().to_vec()),
                partition,
            )
            .await
            .expect("the settled cluster takes the record");
        wait_for_sequence(&native.seen, sequence, sent).await;
        wait_for_sequence(&policy.seen, sequence, sent).await;
        if partition == tail_partition {
            wait_for_sequence(&tail.seen, sequence, sent).await;
        }
        prompt.push(sequence);
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    for sequence in &prompt {
        assert_eq!(
            (
                deliveries_of(&native.seen, *sequence),
                deliveries_of(&policy.seen, *sequence)
            ),
            (1, 1),
            "a record after the recovery reaches each group once"
        );
    }
    assert_eq!(
        deliveries_of(&tail.seen, prompt[tail_partition as usize]),
        1,
        "a record after the recovery reaches the partition consumer once"
    );

    let phase = Instant::now();
    stop.store(true, Ordering::Release);
    let mut consumers = Vec::new();
    for follower in [native, policy, tail] {
        consumers.push((
            follower.task.await.expect("the reader stops"),
            follower.seen,
        ));
    }
    let stream = Identifier::named("cluster_native").expect("stream id");
    let topic_id = Identifier::named(PULSE).expect("topic id");
    for (group, seen) in [
        (PULSE_GROUP, &consumers[0].1),
        (PULSE_POLICY_GROUP, &consumers[1].1),
    ] {
        for partition in 0..CLUSTER_PARTITIONS {
            wait_for_stored_offset(
                &laser,
                &OffsetOwner::group(Identifier::named(group).expect("group id")),
                (&stream, &topic_id, partition),
                last_offset(seen, partition),
            )
            .await;
        }
    }
    wait_for_stored_offset(
        &laser,
        &OffsetOwner::new(Identifier::named(PULSE_TAIL).expect("consumer id")),
        (&stream, &topic_id, tail_partition),
        last_offset(&consumers[2].1, tail_partition),
    )
    .await;
    let stored = phase.elapsed();
    for (consumer, _) in &mut consumers {
        consumer.shutdown().await.expect("the consumer shuts down");
    }
    eprintln!(
        "cluster consumers: offsets stored after {stored:?}, shut down after {:?}",
        phase.elapsed()
    );
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_five_node_cluster_with_topics_of_replication_factor_three_and_one_when_a_member_fails_over_then_producers_and_consumers_should_read_only_from_members_and_lose_no_record()
 {
    let mut cluster = TestIggyCluster::start_sized(FIVE_NODES, |_| Vec::new()).await;
    let leader = cluster.leader(0).await;
    cluster.route_endpoint_to(leader);
    let laser = harness::cluster_laser(&cluster, "cluster_replication").await;
    laser
        .stream("cluster_replication")
        .ensure()
        .await
        .expect("the stream exists");
    let replicated = laser.topic(REPLICATED);
    replicated
        .ensure(CLUSTER_PARTITIONS, Some(3))
        .await
        .expect("the factor-3 topic is created");
    let single_copy = laser
        .topic(SINGLE_COPY)
        .producer()
        .partitions(CLUSTER_PARTITIONS)
        .replication_factor(1)
        .build()
        .await
        .expect("the producer creates the factor-1 topic");
    laser
        .topic(SERVER_DEFAULT)
        .ensure(1, None)
        .await
        .expect("the topic without a factor is created");
    let refused = laser.topic("pulse-rf6").ensure(1, Some(6)).await;
    assert!(
        matches!(
            refused,
            Err(LaserError::Iggy(
                IggyError::InvalidReplicationFactor | IggyError::InvalidOptionValue(_)
            ))
        ),
        "a factor above the node count is refused by the server: {refused:?}"
    );
    for (topic, factor) in [
        (REPLICATED, 3),
        (SINGLE_COPY, 1),
        (SERVER_DEFAULT, FIVE_NODES),
    ] {
        let members = harness::partition_members(&laser, topic).await;
        assert!(
            !members.is_empty() && members.values().all(|nodes| nodes.len() == factor),
            "every partition of {topic} is stored on {factor} nodes: {members:?}"
        );
    }
    let members = harness::partition_members(&laser, REPLICATED).await;
    let primaries = harness::spread_primaries(&laser, REPLICATED, CLUSTER_PARTITIONS).await;
    for (partition, primary) in (0..CLUSTER_PARTITIONS).zip(&primaries) {
        assert!(
            members[&partition].contains(primary),
            "partition {partition} is led by a member: {primary} of {members:?}"
        );
    }
    // A member that leads a partition but not the metadata, so the failover
    // moves a partition and not the cluster.
    let victim_partition = (0..CLUSTER_PARTITIONS)
        .find(|partition| usize::from(primaries[*partition as usize]) != leader)
        .expect("a partition led by a node other than the metadata leader");
    let victim = usize::from(primaries[victim_partition as usize]);

    let mut refused_hello = Capabilities::OPEN;
    refused_hello.hello = HelloOutcome::Rejected;
    let open = laser.clone().with_capabilities(refused_hello);
    let native = open
        .topic(REPLICATED)
        .consumer_group(PULSE_GROUP)
        .consumer()
        .start_at(ConsumerStart::First)
        .commit_policy(CommitPolicy::Each)
        .max_wait(HELD_WAIT)
        .build()
        .await
        .expect("the native group consumer builds");
    let policy = replicated
        .consumer_group(PULSE_POLICY_GROUP)
        .consumer()
        .start_at(ConsumerStart::First)
        .commit_policy(CommitPolicy::Each)
        .max_wait(HELD_WAIT)
        .build()
        .await
        .expect("the policy-aware group consumer builds");
    let tail = replicated
        .consumer(PULSE_TAIL, victim_partition)
        .start_at(ConsumerStart::First)
        .commit_policy(CommitPolicy::Polling)
        .max_wait(HELD_WAIT)
        .build()
        .await
        .expect("the partition consumer builds");
    let stop = Arc::new(AtomicBool::new(false));
    let native = follow(native, Arc::clone(&stop));
    let policy = follow(policy, Arc::clone(&stop));
    let tail = follow(tail, Arc::clone(&stop));
    let producer = replicated
        .producer()
        .partitions(CLUSTER_PARTITIONS)
        .replication_factor(3)
        .build()
        .await
        .expect("the producer initializes on the existing topic");
    let publishing = Arc::new(AtomicBool::new(true));
    let next_sequence = Arc::new(AtomicU64::new(0));
    let confirmed = Arc::new(Mutex::new(BTreeMap::new()));
    let publisher = tokio::spawn(publish_until_stopped(
        producer,
        Arc::clone(&publishing),
        Arc::clone(&next_sequence),
        Arc::clone(&confirmed),
    ));

    wait_for_deliveries(&policy.seen, 30, "steady streaming").await;
    let phase = Instant::now();
    cluster.stop_node(victim);
    let moved = harness::eventually(|| async {
        harness::partition_primaries(&laser, REPLICATED, CLUSTER_PARTITIONS)
            .await
            .get(victim_partition as usize)
            .copied()
            .filter(|primary| usize::from(*primary) != victim)
    })
    .await;
    assert!(
        members[&victim_partition].contains(&moved),
        "the partition moved to another member: {moved} of {:?}",
        members[&victim_partition]
    );
    let after_stop = delivered_count(&policy.seen) + 30;
    wait_for_deliveries(&policy.seen, after_stop, "the member stopped").await;
    let member_failover = phase.elapsed();
    cluster.start_node(victim).await;
    let after_return = delivered_count(&policy.seen) + 30;
    wait_for_deliveries(&policy.seen, after_return, "the member returned").await;
    publishing.store(false, Ordering::Release);
    publisher.await.expect("the publisher stops");

    let confirmed = confirmed.lock().expect("confirmed lock").clone();
    for (name, follower, only) in [
        ("native group", &native, None),
        ("policy-aware group", &policy, None),
        ("partition", &tail, Some(victim_partition)),
    ] {
        wait_until(
            &format!("every confirmed record reaches the {name} consumer"),
            || {
                let received = sequences(&follower.seen);
                confirmed.iter().all(|(sequence, partition)| {
                    only.is_some_and(|only| only != *partition) || received.contains(sequence)
                })
            },
        )
        .await;
        let errors = follower.errors.lock().expect("errors lock").clone();
        eprintln!(
            "five-node {name} consumer: {} confirmed, {} deliveries, longest gaps {:?}, errors {errors:?}",
            confirmed.len(),
            delivered_count(&follower.seen),
            longest_gaps(&follower.arrivals)
        );
        assert!(
            !errors
                .iter()
                .any(|error| error.contains("NotPrimary") || error.contains("primary")),
            "the {name} consumer never sees a refusal from a node that does not lead or store its partition: {errors:?}"
        );
    }
    eprintln!("five-node member failover: delivery went on {member_failover:?} after the stop");
    stop.store(true, Ordering::Release);
    for follower in [native, policy, tail] {
        let mut consumer = follower.task.await.expect("the reader stops");
        consumer.shutdown().await.expect("the consumer shuts down");
    }

    // Factor 1: the only member of a partition returns with every confirmed
    // record, and the four nodes that do not store it never serve it.
    let single_members = harness::partition_members(&laser, SINGLE_COPY).await;
    let only_member = usize::from(single_members[&0][0]);
    publish_sequences(&single_copy, 0..5).await;
    let mut reader = laser
        .topic(SINGLE_COPY)
        .consumer("single-copy-reader", 0)
        .start_at(ConsumerStart::First)
        .max_wait(HELD_WAIT)
        .build()
        .await
        .expect("the factor-1 consumer builds");
    let errors = receive_sequences(&mut reader, 0..5, "from the only member").await;
    assert!(errors.is_empty(), "{errors:?}");
    cluster.stop_node(only_member);
    if only_member == leader {
        let new_leader = cluster.leader((leader + 1) % FIVE_NODES).await;
        cluster.route_endpoint_to(new_leader);
    }
    cluster.start_node(only_member).await;
    publish_sequences(&single_copy, 5..8).await;
    let errors = receive_sequences(&mut reader, 5..8, "after the only member returned").await;
    eprintln!("five-node factor-1 consumer errors across the member restart: {errors:?}");
    reader
        .shutdown()
        .await
        .expect("the factor-1 consumer shuts down");
    let mut replay = laser
        .topic(SINGLE_COPY)
        .consumer("single-copy-replay", 0)
        .start_at(ConsumerStart::First)
        .build()
        .await
        .expect("the replay consumer builds");
    receive_sequences(&mut replay, 0..8, "replayed after the member restart").await;
    replay
        .shutdown()
        .await
        .expect("the replay consumer shuts down");
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_native_consumer_whose_partitions_the_reader_stopped_after_lasting_refusals_when_the_cause_clears_then_should_report_the_refusal_and_resume()
 {
    refused_partition_resumes(ConsumerStart::Next).await;
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_native_consumer_started_at_the_first_record_whose_partition_the_reader_stopped_when_the_topic_returns_then_should_read_the_new_history()
 {
    refused_partition_resumes(ConsumerStart::First).await;
}

#[tokio::test]
async fn given_a_native_group_consumer_started_at_the_first_record_when_a_member_joins_and_leaves_then_should_resume_moved_partitions_after_the_stored_offset()
 {
    moved_partitions_resume_after_the_stored_offset(true).await;
}

#[tokio::test]
async fn given_a_policy_aware_group_consumer_started_at_the_first_record_when_a_member_joins_and_leaves_then_should_resume_moved_partitions_after_the_stored_offset()
 {
    moved_partitions_resume_after_the_stored_offset(false).await;
}

#[tokio::test]
async fn given_a_native_group_started_at_the_last_record_when_the_group_stored_an_offset_then_should_resume_after_it_and_skip_nothing()
 {
    last_start_resumes_after_the_stored_offset(true).await;
}

#[tokio::test]
async fn given_a_policy_aware_group_started_at_the_last_record_when_the_group_stored_an_offset_then_should_resume_after_it_and_skip_nothing()
 {
    last_start_resumes_after_the_stored_offset(false).await;
}

#[tokio::test]
async fn given_a_fresh_native_group_when_a_consumer_starts_at_an_offset_or_a_timestamp_then_should_begin_at_the_record_it_names_and_refuse_a_start_past_the_end()
 {
    named_starts_seed_a_fresh_group(true).await;
}

#[tokio::test]
async fn given_a_fresh_policy_aware_group_when_a_consumer_starts_at_an_offset_or_a_timestamp_then_should_begin_at_the_record_it_names_and_refuse_a_start_past_the_end()
 {
    named_starts_seed_a_fresh_group(false).await;
}

#[tokio::test]
async fn given_a_policy_aware_group_started_at_the_last_record_when_it_gains_partitions_the_group_never_stored_then_should_seed_them_before_reading()
 {
    let laser = harness::laser().await;
    let topic = laser.topic("group-gained-start");
    let producer = topic
        .producer()
        .partitions(2)
        .build()
        .await
        .expect("the producer initializes");
    publish_spread_over(&producer, 2, 0..20).await;
    let holder_laser = harness::reconnect(&laser).await;
    let holder_stop = Arc::new(AtomicBool::new(false));
    let holder = follow(
        holder_laser
            .topic("group-gained-start")
            .consumer_group("gained-workers")
            .consumer()
            .commit_policy(CommitPolicy::Disabled)
            .build()
            .await
            .expect("the member that never stores builds"),
        Arc::clone(&holder_stop),
    );
    wait_for_deliveries(&holder.seen, 20, "the holder reads both partitions").await;
    let mut gainer = topic
        .consumer_group("gained-workers")
        .consumer()
        .start_at(ConsumerStart::Last)
        .commit_policy(CommitPolicy::Disabled)
        .build()
        .await
        .expect("the member started at the last record builds");
    let handed = gainer
        .next_within(RECEIVE_TIMEOUT)
        .await
        .expect("a record of the partition the rebalance hands over");
    assert_eq!(
        handed.position.offset, 9,
        "the handed partition begins at its last record"
    );

    holder_stop.store(true, Ordering::Release);
    let mut holder_member = holder.task.await.expect("the holder stops");
    holder_member.shutdown().await.expect("the holder leaves");
    let gained = loop {
        let message = gainer
            .next_within(RECEIVE_TIMEOUT)
            .await
            .expect("a record of the partition gained when the holder left");
        if message.partition_id != handed.partition_id {
            break message;
        }
    };
    assert_eq!(
        gained.position.offset, 9,
        "a partition gained later that the group never stored begins at its last record"
    );
    gainer.shutdown().await.expect("the gaining member stops");
    producer.shutdown().await.expect("the producer shuts down");
}

type Seen = Arc<Mutex<Vec<(u32, u64, u64)>>>;

// The native group consumer of a server without filters, or the policy-aware
// one the fork serves.
fn group_reads(laser: Laser, native: bool) -> Laser {
    if !native {
        return laser;
    }
    let mut refused = Capabilities::OPEN;
    refused.hello = HelloOutcome::Rejected;
    laser.with_capabilities(refused)
}

async fn moved_partitions_resume_after_the_stored_offset(native: bool) {
    let laser = group_reads(harness::laser().await, native);
    let topic = laser.topic(REBALANCED);
    let producer = topic
        .producer()
        .partitions(REBALANCED_PARTITIONS)
        .build()
        .await
        .expect("the producer initializes");
    publish_spread(&producer, 0..ROUND).await;
    let group = OffsetOwner::group(Identifier::named(REBALANCED_GROUP).expect("group id"));
    let stream = Identifier::named(laser.default_stream().expect("the test stream")).expect("id");
    let topic_id = Identifier::named(REBALANCED).expect("topic id");
    let first_stop = Arc::new(AtomicBool::new(false));
    let first = follow(rebalanced_member(&laser).await, Arc::clone(&first_stop));
    wait_until("the first member reads the first round", || {
        sequences(&first.seen).is_superset(&(0..ROUND).collect())
    })
    .await;
    for partition in 0..REBALANCED_PARTITIONS {
        wait_for_stored_offset(&laser, &group, (&stream, &topic_id, partition), 9).await;
    }

    let joined = delivered_count(&first.seen);
    let second_laser = group_reads(harness::reconnect(&laser).await, native);
    let second_stop = Arc::new(AtomicBool::new(false));
    let second = follow(
        rebalanced_member(&second_laser).await,
        Arc::clone(&second_stop),
    );
    publish_spread(&producer, ROUND..2 * ROUND).await;
    wait_until("both members read the second round", || {
        sequences(&first.seen)
            .union(&sequences(&second.seen))
            .copied()
            .collect::<BTreeSet<_>>()
            .is_superset(&(ROUND..2 * ROUND).collect())
    })
    .await;
    let moved = sequences(&second.seen);
    assert!(
        !moved.is_empty(),
        "the second member reads the partition the rebalance gave it"
    );
    let replayed: Vec<u64> = first.seen.lock().expect("seen lock")[joined..]
        .iter()
        .map(|(_, _, sequence)| *sequence)
        .chain(moved.iter().copied())
        .filter(|sequence| *sequence < ROUND)
        .collect();
    assert!(
        replayed.is_empty(),
        "no member replays records the group stored before the join, replayed {replayed:?}"
    );

    second_stop.store(true, Ordering::Release);
    let mut second_member = second.task.await.expect("the second member stops");
    second_member
        .shutdown()
        .await
        .expect("the second member leaves");
    let regained = delivered_count(&first.seen);
    publish_spread(&producer, 2 * ROUND..3 * ROUND).await;
    wait_until("the first member reads the third round alone", || {
        sequences(&first.seen).is_superset(&(2 * ROUND..3 * ROUND).collect())
    })
    .await;
    let replayed: Vec<u64> = first.seen.lock().expect("seen lock")[regained..]
        .iter()
        .map(|(_, _, sequence)| *sequence)
        .filter(|sequence| *sequence < 2 * ROUND)
        .collect();
    assert!(
        replayed.is_empty(),
        "the member that regains a partition resumes after the stored offset, replayed {replayed:?}"
    );
    eprintln!(
        "rebalance: second member read {} records, errors {:?} and {:?}",
        moved.len(),
        first.errors.lock().expect("errors lock"),
        second.errors.lock().expect("errors lock")
    );
    first_stop.store(true, Ordering::Release);
    let mut first_member = first.task.await.expect("the first member stops");
    first_member
        .shutdown()
        .await
        .expect("the first member leaves");
    producer.shutdown().await.expect("the producer shuts down");
}

async fn last_start_resumes_after_the_stored_offset(native: bool) {
    let laser = group_reads(harness::laser().await, native);
    let topic = laser.topic("group-last-start");
    let producer = topic
        .producer()
        .partitions(1)
        .build()
        .await
        .expect("the producer initializes");
    publish_sequences(&producer, 0..10).await;
    let group = topic.consumer_group("tail-workers");
    let mut handled = group
        .consumer()
        .commit_policy(CommitPolicy::Disabled)
        .build()
        .await
        .expect("the first consumer builds");
    let mut last = None;
    for _ in 0..5 {
        last = Some(
            handled
                .next_within(RECEIVE_TIMEOUT)
                .await
                .expect("a handled record"),
        );
    }
    handled
        .commit(&last.expect("five records were handled"))
        .await
        .expect("the handled prefix stores");
    handled.shutdown().await.expect("the first consumer stops");

    let mut resumed = group
        .consumer()
        .start_at(ConsumerStart::Last)
        .batch_length(2)
        .commit_policy(CommitPolicy::Disabled)
        .build()
        .await
        .expect("the consumer started at the last record builds");
    let mut offsets = Vec::new();
    for _ in 5..10 {
        offsets.push(
            resumed
                .next_within(RECEIVE_TIMEOUT)
                .await
                .expect("a record after the stored offset")
                .position
                .offset,
        );
    }
    assert_eq!(offsets, vec![5, 6, 7, 8, 9]);
    resumed
        .shutdown()
        .await
        .expect("the resumed consumer stops");

    let mut fresh = topic
        .consumer_group("tail-fresh")
        .consumer()
        .start_at(ConsumerStart::Last)
        .commit_policy(CommitPolicy::Disabled)
        .build()
        .await
        .expect("a fresh group started at the last record builds");
    assert_eq!(
        fresh
            .next_within(RECEIVE_TIMEOUT)
            .await
            .expect("the last record")
            .position
            .offset,
        9
    );
    assert_eq!(
        fresh
            .stored_offset(0)
            .await
            .expect("the seeded offset reads")
            .map(|stored| stored.stored_offset),
        Some(8),
        "the seed makes the next read start at the last record"
    );
    publish_sequences(&producer, 10..11).await;
    assert_eq!(
        fresh
            .next_within(RECEIVE_TIMEOUT)
            .await
            .expect("the record after the last one")
            .position
            .offset,
        10
    );
    fresh.shutdown().await.expect("the fresh consumer stops");
    producer.shutdown().await.expect("the producer shuts down");
}

async fn named_starts_seed_a_fresh_group(native: bool) {
    let laser = group_reads(harness::laser().await, native);
    let topic = laser.topic("group-named-starts");
    let producer = topic
        .producer()
        .partitions(1)
        .build()
        .await
        .expect("the producer initializes");
    publish_sequences(&producer, 0..10).await;
    let mut probe = topic
        .consumer("timestamp-probe", 0)
        .start_at(ConsumerStart::First)
        .commit_policy(CommitPolicy::Disabled)
        .build()
        .await
        .expect("the probe builds");
    let mut timestamps = Vec::new();
    for _ in 0..10 {
        timestamps.push(
            probe
                .next_within(RECEIVE_TIMEOUT)
                .await
                .expect("a probed record")
                .timestamp_micros,
        );
    }
    probe.shutdown().await.expect("the probe stops");
    let at = timestamps[6];
    let first_at = timestamps
        .iter()
        .position(|timestamp| *timestamp >= at)
        .and_then(|offset| u64::try_from(offset).ok())
        .expect("a record at the timestamp");

    for (group, start, first) in [
        ("offset-workers", ConsumerStart::Offset(3), 3),
        (
            "timestamp-workers",
            ConsumerStart::TimestampMicros(at),
            first_at,
        ),
    ] {
        let mut consumer = topic
            .consumer_group(group)
            .consumer()
            .start_at(start)
            .commit_policy(CommitPolicy::Disabled)
            .build()
            .await
            .expect("the seeded consumer builds");
        assert_eq!(
            consumer
                .next_within(RECEIVE_TIMEOUT)
                .await
                .expect("the record the start names")
                .position
                .offset,
            first,
            "{start:?}"
        );
        assert_eq!(
            consumer
                .stored_offset(0)
                .await
                .expect("the seeded offset reads")
                .map(|stored| stored.stored_offset),
            first.checked_sub(1),
            "{start:?}"
        );
        consumer
            .shutdown()
            .await
            .expect("the seeded consumer stops");
    }

    // The policy-aware consumer seeds a partition installed after its build
    // before the first read, so the refusal can surface there.
    let refused = match topic
        .consumer_group("beyond-workers")
        .consumer()
        .start_at(ConsumerStart::Offset(100))
        .build()
        .await
    {
        Err(error) => error,
        Ok(mut consumer) => {
            let error = consumer
                .next_within(RECEIVE_TIMEOUT)
                .await
                .expect_err("the refused seed surfaces on the first read");
            consumer
                .shutdown()
                .await
                .expect("the refused consumer stops");
            error
        }
    };
    assert!(
        matches!(refused, LaserError::Iggy(IggyError::InvalidOffset(_))),
        "an offset start past the end fails with the server's refusal, got {refused:?}"
    );
    let stream = Identifier::named(laser.default_stream().expect("the test stream")).expect("id");
    let topic_id = Identifier::named("group-named-starts").expect("topic id");
    let group_id = Identifier::named("beyond-workers").expect("group id");
    harness::eventually(|| async {
        laser
            .client()
            .get_consumer_group(&stream, &topic_id, &group_id)
            .await
            .ok()
            .flatten()
            .filter(|details| details.members_count == 0)
    })
    .await;
    producer.shutdown().await.expect("the producer shuts down");
}

// A consumer read until stopped: each delivery as partition, offset and the
// sequence its payload carries, when each partition delivered, and each error
// it reported.
struct Following {
    seen: Seen,
    arrivals: Arc<Mutex<Vec<(u32, Instant)>>>,
    errors: Arc<Mutex<Vec<String>>>,
    task: JoinHandle<Consumer>,
}

fn leads(primaries: &[u8], node: usize) -> usize {
    primaries
        .iter()
        .filter(|primary| usize::from(**primary) == node)
        .count()
}

fn follow(mut consumer: Consumer, stop: Arc<AtomicBool>) -> Following {
    let seen: Seen = Arc::new(Mutex::new(Vec::new()));
    let arrivals = Arc::new(Mutex::new(Vec::new()));
    let errors = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&seen);
    let arrived = Arc::clone(&arrivals);
    let reported = Arc::clone(&errors);
    let task = tokio::spawn(async move {
        while !stop.load(Ordering::Acquire) {
            match consumer.next_within(Duration::from_millis(500)).await {
                Ok(message) => {
                    recorded.lock().expect("seen lock").push((
                        message.partition_id,
                        message.position.offset,
                        sequence_of(&message),
                    ));
                    arrived
                        .lock()
                        .expect("arrivals lock")
                        .push((message.partition_id, Instant::now()));
                }
                Err(LaserError::Timeout(_)) => {}
                Err(error) => reported
                    .lock()
                    .expect("errors lock")
                    .push(error.to_string()),
            }
        }
        consumer
    });
    Following {
        seen,
        arrivals,
        errors,
        task,
    }
}

// The longest time each partition went without a delivery.
fn longest_gaps(arrivals: &Mutex<Vec<(u32, Instant)>>) -> BTreeMap<u32, Duration> {
    let mut last: BTreeMap<u32, Instant> = BTreeMap::new();
    let mut gaps = BTreeMap::new();
    for (partition, at) in arrivals.lock().expect("arrivals lock").iter() {
        if let Some(previous) = last.insert(*partition, *at) {
            let gap: &mut Duration = gaps.entry(*partition).or_default();
            *gap = (*gap).max(at.duration_since(previous));
        }
    }
    gaps
}
// Publish one sequenced record after another, spread over the partitions,
// and keep the partition of each one the cluster confirmed.
async fn publish_until_stopped(
    producer: Producer,
    publishing: Arc<AtomicBool>,
    next_sequence: Arc<AtomicU64>,
    confirmed: Arc<Mutex<BTreeMap<u64, u32>>>,
) -> Producer {
    while publishing.load(Ordering::Acquire) {
        let sequence = next_sequence.fetch_add(1, Ordering::AcqRel) + 1;
        let partition = u32::try_from(sequence % u64::from(CLUSTER_PARTITIONS)).expect("partition");
        match producer
            .send_to_partition(
                ProducerMessage::new(sequence.to_le_bytes().to_vec()),
                partition,
            )
            .await
        {
            Ok(_) => {
                confirmed
                    .lock()
                    .expect("confirmed lock")
                    .insert(sequence, partition);
            }
            Err(error) => eprintln!("cluster publish {sequence} failed: {error}"),
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    producer
}

// Publish each sequence to partition zero, again while a new topic is not routable yet.
async fn refused_partition_resumes(start: ConsumerStart) {
    let cluster = TestIggyCluster::start().await;
    let leader = cluster.leader(0).await;
    cluster.route_endpoint_to(leader);
    let laser = harness::cluster_laser(&cluster, REFUSED_STREAM).await;
    let topic = laser.topic(REFUSED);
    topic.ensure(1, None).await.expect("the topic exists");
    let mut consumer = topic
        .consumer(REFUSED_READER, 0)
        .start_at(start)
        .polling_retry_interval(REFUSAL_RETRY)
        .build()
        .await
        .expect("the partition consumer builds");
    let producer = topic
        .producer()
        .partitions(1)
        .build()
        .await
        .expect("the producer initializes");
    publish_sequences(&producer, 0..2).await;
    producer.shutdown().await.expect("the producer shuts down");
    receive_sequences(&mut consumer, 0..2, "before the topic is deleted").await;

    laser
        .client()
        .delete_topic(
            &Identifier::named(REFUSED_STREAM).expect("stream id"),
            &Identifier::named(REFUSED).expect("topic id"),
        )
        .await
        .expect("the topic is deleted");
    let outage = Instant::now();
    let mut refusals = Vec::new();
    while outage.elapsed() < REFUSAL_BUDGET {
        match consumer.next_within(Duration::from_millis(500)).await {
            Ok(message) => panic!(
                "offset {} of partition {} arrived from a deleted topic",
                message.position.offset, message.partition_id
            ),
            Err(LaserError::Timeout(_)) => {}
            Err(error) => refusals.push(error),
        }
    }
    assert!(
        refusals.iter().any(|error| matches!(
            error,
            LaserError::Iggy(IggyError::TopicIdNotFound(..) | IggyError::TopicNameNotFound(..))
        )),
        "the consumer reports the refusal of the deleted topic, reported {refusals:?}"
    );

    topic
        .ensure(1, None)
        .await
        .expect("the topic is created again");
    let producer = topic
        .producer()
        .partitions(1)
        .build()
        .await
        .expect("the producer initializes on the new topic");
    publish_sequences(&producer, 2..4).await;
    let errors = receive_sequences(&mut consumer, 2..4, "after the topic is created again").await;
    eprintln!(
        "cluster refusals: {} reported while the topic was gone, first {:?}, errors after it returned {errors:?}",
        refusals.len(),
        refusals.first()
    );
    producer.shutdown().await.expect("the producer shuts down");
    consumer.shutdown().await.expect("the consumer shuts down");
}

async fn publish_spread(producer: &Producer, sequences: Range<u64>) {
    publish_spread_over(producer, REBALANCED_PARTITIONS, sequences).await;
}

// Publish each sequence to the partition it names modulo the partition count.
async fn publish_spread_over(producer: &Producer, partitions: u32, sequences: Range<u64>) {
    for sequence in sequences {
        let partition = u32::try_from(sequence % u64::from(partitions)).expect("partition");
        producer
            .send_to_partition(
                ProducerMessage::new(sequence.to_le_bytes().to_vec()),
                partition,
            )
            .await
            .expect("the sequence publishes");
    }
}

async fn rebalanced_member(laser: &Laser) -> Consumer {
    laser
        .topic(REBALANCED)
        .consumer_group(REBALANCED_GROUP)
        .consumer()
        .start_at(ConsumerStart::First)
        .build()
        .await
        .expect("the group member builds")
}

async fn publish_sequences(producer: &Producer, sequences: Range<u64>) {
    for sequence in sequences {
        harness::eventually(|| async move {
            producer
                .send_to_partition(ProducerMessage::new(sequence.to_le_bytes().to_vec()), 0)
                .await
                .ok()
        })
        .await;
    }
}

// Read until every expected sequence arrived and return the errors reported meanwhile.
async fn receive_sequences(
    consumer: &mut Consumer,
    expected: Range<u64>,
    phase: &str,
) -> Vec<String> {
    let deadline = Instant::now() + RECOVERY;
    let mut received = BTreeSet::new();
    let mut errors = Vec::new();
    while !expected
        .clone()
        .all(|sequence| received.contains(&sequence))
    {
        assert!(
            Instant::now() < deadline,
            "records {expected:?} reach the consumer {phase} within {RECOVERY:?}, received {received:?}, errors {errors:?}"
        );
        match consumer.next_within(Duration::from_millis(500)).await {
            Ok(message) => {
                received.insert(sequence_of(&message));
            }
            Err(LaserError::Timeout(_)) => {}
            Err(error) => errors.push(error.to_string()),
        }
    }
    errors
}

fn sequence_of(message: &ConsumerMessage) -> u64 {
    u64::from_le_bytes(
        message
            .payload
            .as_ref()
            .try_into()
            .expect("an eight byte sequence"),
    )
}

fn delivered_count(seen: &Seen) -> usize {
    seen.lock().expect("seen lock").len()
}

fn sequences(seen: &Seen) -> BTreeSet<u64> {
    seen.lock()
        .expect("seen lock")
        .iter()
        .map(|(_, _, sequence)| *sequence)
        .collect()
}

fn deliveries_of(seen: &Seen, sequence: u64) -> usize {
    seen.lock()
        .expect("seen lock")
        .iter()
        .filter(|(_, _, delivered)| *delivered == sequence)
        .count()
}

fn last_offset(seen: &Seen, partition: u32) -> u64 {
    seen.lock()
        .expect("seen lock")
        .iter()
        .filter(|(delivered, _, _)| *delivered == partition)
        .map(|(_, offset, _)| *offset)
        .max()
        .expect("the partition delivered a record")
}

async fn wait_for_deliveries(seen: &Seen, count: usize, phase: &str) {
    wait_until(
        &format!("the group consumer delivers after {phase}"),
        || delivered_count(seen) >= count,
    )
    .await;
}

async fn wait_for_sequence(seen: &Seen, sequence: u64, sent: Instant) {
    while deliveries_of(seen, sequence) == 0 {
        assert!(
            sent.elapsed() < PROMPT,
            "record {sequence} did not arrive within {PROMPT:?}, the held read waited out its hold"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for_stored_offset(
    laser: &Laser,
    owner: &OffsetOwner,
    (stream, topic, partition): (&Identifier, &Identifier, u32),
    expected: u64,
) {
    let deadline = Instant::now() + RECEIVE_TIMEOUT;
    loop {
        let stored = laser
            .client()
            .get_consumer_offset(owner, stream, topic, Some(partition))
            .await
            .ok()
            .flatten()
            .map(|offset| offset.stored_offset);
        if stored == Some(expected) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "partition {partition} stored {stored:?} for {owner}, the last delivered offset is {expected}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_until(condition: &str, check: impl Fn() -> bool) {
    let deadline = Instant::now() + RECOVERY;
    while !check() {
        assert!(Instant::now() < deadline, "{condition} within {RECOVERY:?}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
