// Consumer groups that own their filter policy, against Laser Streams with the
// stand-in catalog of the laser-test-support crate. The stand-in answers the
// backend probe and keeps the group policies the tests configure: a group's
// own filter and revisions, its binding with a policy generation, and the
// unbound answer for a group without one.

use crate::harness::{self, SharedIggy};
use crate::test_iggy::TestIggyCluster;
use laser_sdk::capabilities::{Capabilities, HelloOutcome};
use laser_sdk::filters::{
    ConsumerFilter, ExecutionMode, FaultReason, FilterBinding, FilterErrorReason, FilterExpr,
    FilterGroupIdentity, FilterGroupRef, FilteredReader, FilteredStart, GroupFilterSpec,
    MatchedPage, RecordEvaluation,
};
use laser_sdk::iggy::prelude::{
    Consumer, ConsumerGroupClient, ConsumerOffsetClient, HeaderKey, HeaderValue, Identifier,
    PartitionClient, TopicClient,
};
use laser_sdk::prelude::full::{Consumer as LaserConsumer, ConsumerGroup, Laser, LaserError};
use laser_sdk::prelude::{CommitPolicy, ConsumerStart, ProducerMessage, Routing};
use laser_sdk::query::CmpOp;
use laser_sdk::wire::codes::{AGDX_FILTER_MUTATE_CODE, AGDX_LIST_FILTERS_CODE, FILTER_OP_VERSION};
use laser_sdk::wire::filter::{
    FilterCatalogCommand, FilterCatalogReply, FilterMutation, FilterMutationRequest, FilterOwner,
};
use laser_sdk::wire::framing::{decode_named, encode_named};
use laser_test_support::catalog::{self, CatalogState};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::OnceCell;
use tokio::time::Instant;

const TOPIC: &str = "fleet_changes";
const GROUP: &str = "anomaly-desk";
const POLICY_NAME: &str = "safe-mode";
const READ_TIMEOUT: Duration = Duration::from_secs(15);
const WAIT_MAX: Duration = Duration::from_secs(10);
const PAGES_PER_PARTITION: u32 = 12;
const CLUSTER_PARTITIONS: u32 = 3;
// A held wait answers when a record lands and a wait left alone holds for
// `WAIT_MAX`, so half of it tells a woken reader from one that waited it out.
const PROMPT_WAKE: Duration = Duration::from_secs(WAIT_MAX.as_secs() / 2);
// A primary that has not applied a new member's ownership yet refuses its
// reads, and the reader reports the second refusal in a row, so one pass over
// the partitions goes untimed and reads past that refusal.
const WARM_UP: Duration = Duration::from_secs(60);
const RECOVERY: Duration = Duration::from_secs(90);
const FAILOVER: Duration = Duration::from_secs(10);
const LONG_WAIT: Duration = Duration::from_secs(20);
// The assignment is read again every 2 s, so a gained partition is read
// within a few seconds, and half of the 20 s wait tells that from a wait
// that ran to its end.
const PROMPT_REBALANCE: Duration = Duration::from_secs(LONG_WAIT.as_secs() / 2);

const SAFE_MODE: &str = r#"{"op":"u","table":"satellites","changed":["mode"],"after":{"id":"sat-042","name":"Kestrel-42","mode":"safe","orbit":"leo","battery_pct":61}}"#;
const DECOMMISSION: &str = r#"{"op":"d","table":"satellites","before":{"id":"sat-042"}}"#;
const GROUND_STATION: &str = r#"{"op":"u","table":"ground_stations","changed":["status"],"after":{"id":"svalbard","status":"online"}}"#;

static CATALOG_SERVER: OnceCell<(SharedIggy, tempfile::TempDir, Arc<Mutex<CatalogState>>)> =
    OnceCell::const_new();

fn bound_filter() -> ConsumerFilter {
    ConsumerFilter::json(FilterExpr::any([
        FilterExpr::pred("op", CmpOp::Eq, "d"),
        FilterExpr::pred("changed", CmpOp::Contains, "mode"),
    ]))
}

async fn catalog_server() -> &'static SharedIggy {
    let (server, _socket_dir, _state) = CATALOG_SERVER.get_or_init(start_catalog_server).await;
    server
}

/// A server whose nodes reach one stand-in catalog, each over its own socket.
async fn start_catalog_server() -> (SharedIggy, tempfile::TempDir, Arc<Mutex<CatalogState>>) {
    let socket_dir = tempfile::tempdir().expect("socket directory");
    let state = Arc::new(Mutex::new(CatalogState::default()));
    for node in 0..3 {
        catalog::spawn(
            &node_socket(socket_dir.path(), node),
            Arc::clone(&state),
            node,
        )
        .expect("serve the catalog");
    }
    let sockets = socket_dir.path().to_owned();
    let server = SharedIggy::start(|node| {
        vec![
            ("IGGY_PLANE_ENABLED".to_owned(), "true".to_owned()),
            (
                "IGGY_PLANE_SOCKET_PATH".to_owned(),
                node_socket(&sockets, node).display().to_string(),
            ),
        ]
    })
    .await;
    (server, socket_dir, state)
}

// A three-node cluster whose nodes each reach the one stand-in catalog over a
// socket of their own, so the state records which node forwarded what. Each
// node holds at most `waiting_reads` filtered waits when given.
async fn start_catalog_cluster(
    waiting_reads: Option<u32>,
) -> (TestIggyCluster, tempfile::TempDir, Arc<Mutex<CatalogState>>) {
    let socket_dir = tempfile::tempdir().expect("socket directory");
    let state = Arc::new(Mutex::new(CatalogState::default()));
    for node in 0..3 {
        catalog::spawn(
            &node_socket(socket_dir.path(), node),
            Arc::clone(&state),
            node,
        )
        .expect("serve the catalog");
    }
    let sockets = socket_dir.path().to_owned();
    let cluster = TestIggyCluster::start_with(|node| {
        let mut env = vec![
            ("IGGY_PLANE_ENABLED".to_owned(), "true".to_owned()),
            (
                "IGGY_PLANE_SOCKET_PATH".to_owned(),
                node_socket(&sockets, node).display().to_string(),
            ),
        ];
        if let Some(reads) = waiting_reads {
            env.push((
                "IGGY_PLANE_FILTERS_MAX_WAITING_READS".to_owned(),
                reads.to_string(),
            ));
        }
        env
    })
    .await;
    let leader = cluster.leader(0).await;
    cluster.route_endpoint_to(leader);
    (cluster, socket_dir, state)
}

fn node_socket(directory: &Path, node: usize) -> PathBuf {
    directory.join(format!("backend-{node}.sock"))
}

/// A `Laser` on the catalog cluster with `TOPIC` over every partition.
async fn cluster_catalog_laser(cluster: &TestIggyCluster, stream: &str) -> Laser {
    let laser = harness::cluster_laser(cluster, stream).await;
    laser
        .stream(stream)
        .ensure()
        .await
        .expect("the stream exists");
    laser
        .topic(TOPIC)
        .ensure(CLUSTER_PARTITIONS, None)
        .await
        .expect("the topic exists");
    laser
}

async fn publish(laser: &Laser, topic: &str, payloads: &[&str], partition: u32) {
    let producer = laser
        .topic(topic)
        .producer()
        .partitions(partition + 1)
        .build()
        .await
        .expect("the producer initializes");
    producer
        .send_batch_with_routing(
            payloads
                .iter()
                .map(|payload| ProducerMessage::new(payload.as_bytes().to_vec())),
            Some(Routing::Partition(partition)),
        )
        .await
        .expect("the fixtures publish");
}

/// Publish the three fixtures on partition 0 and return the stream name.
async fn group_source(laser: &Laser) -> String {
    publish(laser, TOPIC, &[SAFE_MODE, GROUND_STATION, DECOMMISSION], 0).await;
    laser.default_stream().expect("stream").to_owned()
}

/// Create `GROUP` with the bound filter as its policy.
async fn bound_group(laser: &Laser) -> ConsumerGroup {
    let group = laser.topic(TOPIC).consumer_group(GROUP);
    let created = group
        .create()
        .filter(POLICY_NAME, bound_filter())
        .build()
        .await
        .expect("the group is created with its filter");
    assert_eq!(
        created.filter.as_ref().map(|binding| binding.revision),
        Some(1)
    );
    group
}

async fn deletion_group(laser: &Laser, collisions: usize) -> (ConsumerGroup, FilterBinding) {
    let topic = laser.topic(TOPIC);
    topic.ensure(1, None).await.expect("the topic exists");
    let stream_id = Identifier::named(laser.default_stream().expect("stream")).expect("stream id");
    let topic_id = Identifier::named(TOPIC).expect("topic id");
    for index in 0..10 {
        let native = laser
            .client()
            .create_consumer_group(&stream_id, &topic_id, &format!("delete-worker-{index}"))
            .await
            .expect("the group is created");
        if native.id == 9 {
            break;
        }
    }
    let group = topic.consumer_group_id(9);
    let binding = group
        .filter()
        .configure(POLICY_NAME, bound_filter())
        .await
        .expect("group 9 receives its own filter");
    let (_, _, state) = CATALOG_SERVER.get().expect("the catalog runs");
    let mut state = state.lock().expect("catalog state");
    for group_id in (90..98).chain(900..1000).take(collisions) {
        state
            .mutate(FilterCatalogCommand {
                limits: None,
                operation_id: u128::from(group_id),
                mutation: FilterMutation::ConfigureGroup {
                    group: FilterGroupRef {
                        group: format!("collision-{group_id}"),
                        ..binding.group.clone()
                    },
                    policy: GroupFilterSpec::Definition {
                        name: POLICY_NAME.to_owned(),
                        filter: bound_filter(),
                    },
                    expected_identity: None,
                },
                identity: Some(FilterGroupIdentity {
                    group_id,
                    ..binding.identity
                }),
                filter_id: None,
                grants: Vec::new(),
            })
            .expect("the catalog contains a prefix collision");
    }
    (group, binding)
}

async fn stored_group_offset(laser: &Laser, stream: &str, partition: u32) -> Option<u64> {
    laser
        .client()
        .get_consumer_offset(
            &Consumer::group(Identifier::named(GROUP).expect("group name")),
            &Identifier::named(stream).expect("stream name"),
            &Identifier::named(TOPIC).expect("topic name"),
            Some(partition),
        )
        .await
        .expect("the group offset reads")
        .map(|offset| offset.stored_offset)
}

fn offsets(page: &laser_sdk::filters::MatchedPage) -> Vec<u64> {
    page.records.iter().map(|record| record.offset).collect()
}

#[tokio::test]
async fn given_a_group_created_with_a_filter_when_consumed_then_should_deliver_only_matches_and_commit_the_scanned_prefix()
 {
    let laser = harness::connected_laser_at(&catalog_server().await.connection_string()).await;
    assert!(laser.capabilities().await.filters.group_policy_reads);
    let stream = group_source(&laser).await;
    let group = bound_group(&laser).await;

    let mut consumer = group
        .consumer()
        .start_at(ConsumerStart::First)
        .commit_policy(CommitPolicy::Disabled)
        .build()
        .await
        .expect("the group consumer builds");
    let mut delivered = Vec::new();
    for _ in 0..2 {
        let message = consumer
            .next_within(READ_TIMEOUT)
            .await
            .expect("a matching record arrives");
        consumer.commit(&message).await.expect("the record commits");
        delivered.push(message.position.offset);
    }
    assert_eq!(delivered, vec![0, 2], "the server ran the group's policy");
    assert_eq!(consumer.last_consumed_offset(0), Some(2));
    assert_eq!(consumer.last_stored_offset(0), Some(2));
    assert!(
        matches!(
            consumer.store_offset(1, Some(0)).await,
            Err(LaserError::Invalid(_))
        ),
        "explicit offsets bypass the acknowledgment contract"
    );
    consumer.shutdown().await.expect("the consumer shuts down");

    assert_eq!(stored_group_offset(&laser, &stream, 0).await, Some(2));
    let info = group.info().await.expect("the group exists");
    assert_eq!(
        info.filter.map(|binding| binding.policy_generation),
        Some(1)
    );
}

#[tokio::test]
async fn given_an_unbound_group_when_consumed_then_should_deliver_every_record_through_the_group_engine()
 {
    let laser = harness::connected_laser_at(&catalog_server().await.connection_string()).await;
    let stream = group_source(&laser).await;
    let group = laser.topic(TOPIC).consumer_group(GROUP);
    let created = group.create().build().await.expect("a plain group");
    assert!(created.filter.is_none());

    let mut consumer = group
        .consumer()
        .start_at(ConsumerStart::First)
        .commit_policy(CommitPolicy::Each)
        .build()
        .await
        .expect("the group consumer builds");
    let mut delivered = Vec::new();
    for _ in 0..3 {
        let message = consumer
            .next_within(READ_TIMEOUT)
            .await
            .expect("a record arrives");
        delivered.push(message.position.offset);
    }
    consumer.shutdown().await.expect("the consumer shuts down");

    assert_eq!(
        delivered,
        vec![0, 1, 2],
        "an unbound group receives everything"
    );
    assert_eq!(stored_group_offset(&laser, &stream, 0).await, Some(2));
}

#[tokio::test]
async fn given_an_unbound_group_when_reading_pages_then_should_return_records_without_evaluating() {
    let laser = harness::connected_laser_at(&catalog_server().await.connection_string()).await;
    let stream = group_source(&laser).await;
    let group = laser.topic(TOPIC).consumer_group(GROUP);
    group.create().build().await.expect("plain group");
    let mut reader = group
        .reader()
        .expect("group source")
        .start(FilteredStart::First)
        .count(3)
        .max_examined(3)
        .build()
        .await
        .expect("an automatic group reader accepts an unbound group");
    let page = tokio::time::timeout(READ_TIMEOUT, reader.next_page())
        .await
        .expect("page deadline")
        .expect("page");
    assert_eq!(offsets(&page), vec![0, 1, 2]);
    assert_eq!(page.policy.mode, ExecutionMode::Unfiltered);
    assert!(
        page.records
            .iter()
            .all(|record| record.evaluation == RecordEvaluation::Unfiltered)
    );
    reader.ack_page(&page).await.expect("acknowledge");
    reader.close().await.expect("close");
    assert_eq!(stored_group_offset(&laser, &stream, 0).await, Some(2));
}

#[tokio::test]
async fn given_an_idle_group_reader_when_a_record_lands_during_its_filtered_wait_then_should_deliver_it_before_the_idle_interval()
 {
    let laser = harness::connected_laser_at(&catalog_server().await.connection_string()).await;
    assert!(
        laser.capabilities().await.filters.max_wait > Duration::ZERO,
        "the test server holds filtered waits"
    );
    group_source(&laser).await;
    let group = laser.topic(TOPIC).consumer_group(GROUP);
    group.create().build().await.expect("plain group");
    let mut reader = group
        .reader()
        .expect("group source")
        .start(FilteredStart::First)
        .idle_interval(Duration::from_secs(30))
        .max_wait(WAIT_MAX)
        .build()
        .await
        .expect("an automatic group reader accepts an unbound group");
    // The first rounds run before the group assignment arrives and would sleep
    // the idle interval, so they are driven here without it.
    let first = tokio::time::timeout(READ_TIMEOUT, async {
        loop {
            if let Some(page) = reader.try_next_page().await.expect("page") {
                return page;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("page deadline");
    assert_eq!(offsets(&first), vec![0, 1, 2]);
    reader.ack_page(&first).await.expect("acknowledge");

    let publisher = laser.clone();
    let late = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(500)).await;
        publish(&publisher, TOPIC, &[SAFE_MODE], 0).await;
    });
    let page = tokio::time::timeout(Duration::from_secs(8), reader.next_page())
        .await
        .expect("the held wait answers when the record lands, long before the 30 s idle interval")
        .expect("page");
    late.await.expect("the late record publishes");
    assert_eq!(offsets(&page), vec![3]);
    reader.ack_page(&page).await.expect("acknowledge");

    // The page confirmed the ready answer, so the next idle round waits again.
    let publisher = laser.clone();
    let later = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(500)).await;
        publish(&publisher, TOPIC, &[SAFE_MODE], 0).await;
    });
    let page = tokio::time::timeout(Duration::from_secs(8), reader.next_page())
        .await
        .expect("the next held wait answers too, long before the 30 s idle interval")
        .expect("page");
    later.await.expect("the later record publishes");
    assert_eq!(offsets(&page), vec![4]);
    reader.ack_page(&page).await.expect("acknowledge");
    reader.close().await.expect("close");
}

#[tokio::test]
async fn given_automatic_commit_policies_when_delivering_a_record_then_should_not_store_it_before_returning()
 {
    for policy in [
        CommitPolicy::Each,
        CommitPolicy::All,
        CommitPolicy::Every(1),
        CommitPolicy::Interval(Duration::from_millis(1)),
        CommitPolicy::IntervalOrEach(Duration::from_millis(1)),
        CommitPolicy::Polling,
    ] {
        let laser = harness::connected_laser_at(&catalog_server().await.connection_string()).await;
        let stream = group_source(&laser).await;
        let group = bound_group(&laser).await;
        let mut consumer = group
            .consumer()
            .start_at(ConsumerStart::First)
            .batch_length(2)
            .commit_policy(policy)
            .build()
            .await
            .expect("group consumer");
        let first = consumer
            .next_within(READ_TIMEOUT)
            .await
            .expect("first delivery");
        assert_eq!(first.position.offset, 0);
        assert_eq!(
            stored_group_offset(&laser, &stream, 0).await,
            None,
            "{policy:?}"
        );
        consumer.shutdown().await.expect("clean stop");
        assert_eq!(
            stored_group_offset(&laser, &stream, 0).await,
            Some(1),
            "{policy:?}"
        );
    }
}

#[tokio::test]
async fn given_an_interval_policy_when_the_partition_is_idle_then_should_store_the_delivered_prefix_before_the_next_page()
 {
    let laser = harness::connected_laser_at(&catalog_server().await.connection_string()).await;
    let stream = group_source(&laser).await;
    let group = bound_group(&laser).await;
    let mut consumer = group
        .consumer()
        .start_at(ConsumerStart::First)
        .commit_policy(CommitPolicy::Interval(Duration::from_millis(400)))
        .build()
        .await
        .expect("group consumer");
    consumer.next_within(READ_TIMEOUT).await.expect("first");
    let last = consumer.next_within(READ_TIMEOUT).await.expect("second");
    assert_eq!(last.position.offset, 2);
    // Nothing new arrives. The interval elapses inside this wait, so the
    // engine must store between its idle rounds instead of inside a blocked
    // page read.
    assert!(matches!(
        consumer.next_within(Duration::from_millis(1500)).await,
        Err(LaserError::Timeout(_))
    ));
    assert_eq!(
        stored_group_offset(&laser, &stream, 0).await,
        Some(2),
        "the interval stores the examined prefix while the partition is idle"
    );
    consumer.shutdown().await.expect("clean stop");
}

#[tokio::test]
async fn given_a_timed_out_next_when_manually_committing_then_should_release_the_in_flight_read_lock()
 {
    let laser = harness::connected_laser_at(&catalog_server().await.connection_string()).await;
    let stream = group_source(&laser).await;
    let group = bound_group(&laser).await;
    let mut consumer = group
        .consumer()
        .start_at(ConsumerStart::First)
        .commit_policy(CommitPolicy::Disabled)
        .build()
        .await
        .expect("consumer");
    consumer.next_within(READ_TIMEOUT).await.expect("first");
    let second = consumer.next_within(READ_TIMEOUT).await.expect("second");
    assert!(matches!(
        consumer.next_within(Duration::from_millis(50)).await,
        Err(LaserError::Timeout(_))
    ));
    tokio::time::timeout(Duration::from_secs(2), consumer.commit(&second))
        .await
        .expect("manual commit must not wait for another read to complete")
        .expect("commit");
    consumer.shutdown().await.expect("close");
    assert_eq!(stored_group_offset(&laser, &stream, 0).await, Some(2));
}

#[tokio::test]
async fn given_a_native_group_by_numeric_id_when_consuming_then_should_resolve_its_name() {
    let laser = harness::connected_laser_at(&catalog_server().await.connection_string()).await;
    group_source(&laser).await;
    let mut capabilities = Capabilities::OPEN;
    capabilities.hello = HelloOutcome::Rejected;
    let native = laser.with_capabilities(capabilities);
    let info = native
        .topic(TOPIC)
        .consumer_group(GROUP)
        .create()
        .build()
        .await
        .expect("native group");
    let mut consumer = native
        .topic(TOPIC)
        .consumer_group_id(u64::from(info.id))
        .consumer()
        .start_at(ConsumerStart::First)
        .commit_policy(CommitPolicy::Disabled)
        .build()
        .await
        .expect("a numeric group can use the native fallback");
    let first = consumer
        .next_within(READ_TIMEOUT)
        .await
        .expect("native delivery");
    assert_eq!(first.position.offset, 0);
    consumer.commit(&first).await.expect("native commit");
    consumer.shutdown().await.expect("native close");
}

#[tokio::test]
async fn given_a_cancelled_binding_delivery_when_returned_then_should_redeliver_without_acknowledging()
 {
    let laser = harness::connected_laser_at(&catalog_server().await.connection_string()).await;
    let stream = group_source(&laser).await;
    let group = bound_group(&laser).await;
    let mut consumer = group
        .consumer()
        .start_at(ConsumerStart::First)
        .commit_policy(CommitPolicy::Each)
        .build()
        .await
        .expect("consumer");
    let original = consumer.next_within(READ_TIMEOUT).await.expect("first");
    consumer
        .return_delivery(original)
        .await
        .expect("cancelled handoff is returned");
    assert_eq!(stored_group_offset(&laser, &stream, 0).await, None);
    let repeated = consumer
        .next_within(READ_TIMEOUT)
        .await
        .expect("returned delivery");
    assert_eq!(repeated.position.offset, 0);
    assert_eq!(stored_group_offset(&laser, &stream, 0).await, None);
    consumer
        .return_delivery(repeated)
        .await
        .expect("return again");
    consumer.shutdown().await.expect("close");
    assert_eq!(
        stored_group_offset(&laser, &stream, 0).await,
        None,
        "shutdown cannot acknowledge an undelivered binding result"
    );
}

#[tokio::test]
async fn given_a_policy_released_while_consuming_then_should_replay_from_the_uncommitted_offset() {
    let laser = harness::connected_laser_at(&catalog_server().await.connection_string()).await;
    group_source(&laser).await;
    let group = bound_group(&laser).await;
    let mut consumer = group
        .consumer()
        .start_at(ConsumerStart::First)
        .commit_policy(CommitPolicy::Each)
        .build()
        .await
        .expect("consumer");
    assert_eq!(
        consumer
            .next_within(READ_TIMEOUT)
            .await
            .expect("first")
            .position
            .offset,
        0
    );
    group
        .filter()
        .release()
        .await
        .expect("release while reading");
    let following = consumer
        .next_within(READ_TIMEOUT)
        .await
        .expect("the consumer goes on without an error");
    assert_eq!(
        following.position.offset, 0,
        "the old page cannot advance the offset after the policy changed"
    );
    consumer.shutdown().await.expect("close");
}

#[tokio::test]
async fn given_a_bound_group_when_reading_pages_then_should_run_the_bound_revision_and_store_group_progress()
 {
    let laser = harness::connected_laser_at(&catalog_server().await.connection_string()).await;
    assert!(laser.capabilities().await.filters.catalog);
    let stream = group_source(&laser).await;
    let group = bound_group(&laser).await;
    let mut reader = group
        .reader()
        .expect("a stream is set")
        .start(FilteredStart::First)
        .build()
        .await
        .expect("the group reader builds");

    // A fresh member owns its partition once the partition primary installed
    // it, so the reader may start with none and pick it up while reading.
    let page = tokio::time::timeout(READ_TIMEOUT, reader.next_page())
        .await
        .expect("a page arrives")
        .expect("the bound read succeeds");
    assert_eq!(reader.partitions(), vec![0]);

    assert_eq!(offsets(&page), vec![0, 2]);
    let binding = group
        .filter()
        .get()
        .await
        .expect("readable")
        .expect("bound");
    assert_eq!(page.policy.filter_id, Some(binding.filter_id));
    assert_eq!(page.policy.revision, Some(1));
    assert_eq!(page.policy.digest, Some(binding.digest));
    assert_eq!(page.policy.mode, ExecutionMode::Filtered);
    assert_eq!(page.policy.policy_generation, 1);
    reader.ack_page(&page).await.expect("the page acknowledges");
    reader.close().await.expect("the reader closes");
    assert_eq!(stored_group_offset(&laser, &stream, 0).await, Some(2));
}

#[tokio::test]
async fn given_a_configured_group_when_created_again_then_should_keep_its_binding_and_refuse_another_policy()
 {
    let laser = harness::connected_laser_at(&catalog_server().await.connection_string()).await;
    group_source(&laser).await;
    let group = bound_group(&laser).await;
    let first = group
        .filter()
        .get()
        .await
        .expect("readable")
        .expect("bound");

    let again = group
        .create()
        .filter(POLICY_NAME, bound_filter())
        .build()
        .await
        .expect("the same policy is idempotent");
    assert_eq!(again.filter, Some(first.clone()));

    let refused = group
        .create()
        .filter(
            "kind-present",
            ConsumerFilter::json(FilterExpr::present("kind")),
        )
        .build()
        .await
        .expect_err("another policy conflicts");
    match refused {
        LaserError::ConsumerGroupSetup {
            group_id,
            name,
            identity,
            source,
        } => {
            assert_eq!(name, GROUP);
            assert_eq!(u64::from(group_id), identity.group_id);
            assert_eq!(identity, first.identity);
            assert_eq!(source.filter_reason(), Some(FilterErrorReason::Conflict));
        }
        other => panic!("a group setup failure, got {other:?}"),
    }
    assert_eq!(
        group.filter().get().await.expect("readable"),
        Some(first),
        "the running policy is preserved"
    );
}

#[tokio::test]
async fn given_lost_membership_when_reading_again_then_should_rejoin_from_committed_progress() {
    let laser = harness::connected_laser_at(&catalog_server().await.connection_string()).await;
    let stream = group_source(&laser).await;
    let group = bound_group(&laser).await;
    let mut reader = group
        .reader()
        .expect("a stream is set")
        .start(FilteredStart::First)
        .build()
        .await
        .expect("reader joins");
    let page = tokio::time::timeout(READ_TIMEOUT, reader.next_page())
        .await
        .expect("page timely")
        .expect("page");
    reader
        .ack_page(&page)
        .await
        .expect("first page acknowledged");
    reader
        .coordinator()
        .leave_consumer_group(
            &Identifier::named(&stream).expect("stream"),
            &Identifier::named(TOPIC).expect("topic"),
            &Identifier::named(GROUP).expect("group"),
        )
        .await
        .expect("membership removed");
    publish(&laser, TOPIC, &[DECOMMISSION], 0).await;
    let next = tokio::time::timeout(READ_TIMEOUT, reader.next_page())
        .await
        .expect("rejoin and read timely")
        .expect("read after rejoin");
    assert_eq!(offsets(&next), vec![3]);
    assert!(
        reader.ack_page(&page).await.is_err(),
        "old membership pages cannot acknowledge the new reader epoch"
    );
    reader.ack_page(&next).await.expect("new page acknowledges");
    reader.close().await.expect("close");
}

#[tokio::test]
async fn given_a_delayed_first_assignment_when_the_partition_arrives_then_should_read_from_the_requested_start()
 {
    let laser = harness::connected_laser_at(&catalog_server().await.connection_string()).await;
    let stream = group_source(&laser).await;
    let group = bound_group(&laser).await;
    let stream_id = Identifier::named(&stream).expect("stream");
    let topic_id = Identifier::named(TOPIC).expect("topic");
    let group_id = Identifier::named(GROUP).expect("group");
    // Another member holds the only partition and has stored progress past
    // the first match, so a reader that resumed after the stored offset would
    // skip offset 0.
    let holder = harness::connected_laser_at(&catalog_server().await.connection_string()).await;
    holder
        .client()
        .join_consumer_group(&stream_id, &topic_id, &group_id)
        .await
        .expect("the holder joins");
    let holder_owns_the_partition = async {
        loop {
            let owned = holder
                .client()
                .get_consumer_group(&stream_id, &topic_id, &group_id)
                .await
                .expect("the group reads")
                .is_some_and(|group| group.members.iter().any(|member| member.partitions == [0]));
            if owned {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    };
    tokio::time::timeout(READ_TIMEOUT, holder_owns_the_partition)
        .await
        .expect("the holder is assigned the partition");
    holder
        .client()
        .store_consumer_offset(
            &Consumer::group(group_id.clone()),
            &stream_id,
            &topic_id,
            Some(0),
            1,
        )
        .await
        .expect("the holder stores progress");
    let mut reader = group
        .reader()
        .expect("a stream is set")
        .start(FilteredStart::First)
        .build()
        .await
        .expect("the group reader builds");
    assert_eq!(
        reader.partitions(),
        Vec::<u32>::new(),
        "the holder keeps the partition, so the first assignment is empty"
    );
    holder
        .client()
        .leave_consumer_group(&stream_id, &topic_id, &group_id)
        .await
        .expect("the holder leaves");
    let page = tokio::time::timeout(READ_TIMEOUT, reader.next_page())
        .await
        .expect("the partition arrives and a page is read")
        .expect("the delayed first read succeeds");
    assert_eq!(reader.partitions(), vec![0]);
    assert_eq!(
        offsets(&page),
        vec![0, 2],
        "the requested start applies to the delayed first assignment"
    );
    reader.close().await.expect("close");
    holder.close().await.expect("the holder closes");
}

#[tokio::test]
async fn given_two_partitions_when_reading_many_pages_then_should_read_and_acknowledge_both() {
    let (server, _socket_dir, _state) = start_catalog_server().await;
    let laser = harness::connected_laser_at(&server.connection_string()).await;
    let stream = group_source(&laser).await;
    let stream_id = Identifier::named(&stream).expect("stream");
    let topic_id = Identifier::named(TOPIC).expect("topic");
    laser
        .client()
        .create_partitions(&stream_id, &topic_id, 1)
        .await
        .expect("second partition");
    for partition in 0..2 {
        publish(
            &laser,
            TOPIC,
            &vec![SAFE_MODE; PAGES_PER_PARTITION as usize],
            partition,
        )
        .await;
    }
    let group = bound_group(&laser).await;
    let mut reader = group
        .reader()
        .expect("a stream is set")
        .start(FilteredStart::First)
        .count(1)
        .build()
        .await
        .expect("the group reader builds");
    let mut partitions_read = BTreeSet::new();
    for _ in 0..PAGES_PER_PARTITION * 2 {
        let page = tokio::time::timeout(READ_TIMEOUT, reader.next_page())
            .await
            .expect("a page arrives")
            .expect("the bound read succeeds");
        partitions_read.insert(page.partition_id);
        reader.ack_page(&page).await.expect("the page acknowledges");
    }
    assert_eq!(partitions_read.len(), 2, "both partitions were read");
    reader.close().await.expect("the reader closes");
}

#[tokio::test]
async fn given_a_revoked_partition_when_draining_then_should_store_its_acknowledgment() {
    let server = catalog_server().await;
    let laser = harness::connected_laser_at(&server.connection_string()).await;
    let stream = group_source(&laser).await;
    let stream_id = Identifier::named(&stream).expect("stream");
    let topic_id = Identifier::named(TOPIC).expect("topic");
    laser
        .client()
        .create_partitions(&stream_id, &topic_id, 1)
        .await
        .expect("second partition");
    publish(&laser, TOPIC, &[SAFE_MODE], 1).await;
    let group = bound_group(&laser).await;
    let mut reader = group
        .reader()
        .expect("a stream is set")
        .start(FilteredStart::First)
        .build()
        .await
        .expect("first member");
    let first = tokio::time::timeout(READ_TIMEOUT, reader.next_page())
        .await
        .expect("first timely")
        .expect("first page");
    let second = tokio::time::timeout(READ_TIMEOUT, reader.next_page())
        .await
        .expect("second timely")
        .expect("second page");
    let peer = Laser::connect(&server.connection_string())
        .await
        .expect("peer connects");
    peer.client()
        .join_consumer_group(
            &stream_id,
            &topic_id,
            &Identifier::named(GROUP).expect("group"),
        )
        .await
        .expect("peer joins");
    let revoked = tokio::time::timeout(READ_TIMEOUT, async {
        loop {
            reader.try_next_page().await.expect("refreshes membership");
            if let Some(partition) = [0, 1]
                .into_iter()
                .find(|partition| !reader.partitions().contains(partition))
            {
                break partition;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("a partition is revoked");
    let page = if first.partition_id == revoked {
        &first
    } else {
        &second
    };
    reader
        .ack_page(page)
        .await
        .expect("a revoked partition drains through the fenced offset store");
    assert_eq!(
        stored_group_offset(&laser, &stream, revoked).await,
        page.safe_ack_offset
    );
    reader.close().await.expect("reader closes");
    peer.close().await.expect("peer closes");
}

#[tokio::test]
async fn given_a_recreated_group_when_acknowledging_an_old_page_then_should_not_advance_the_new_group()
 {
    let laser = harness::connected_laser_at(&catalog_server().await.connection_string()).await;
    let stream = group_source(&laser).await;
    let stream_id = Identifier::named(&stream).expect("stream");
    let topic_id = Identifier::named(TOPIC).expect("topic");
    let group_id = Identifier::named(GROUP).expect("group");
    let group = bound_group(&laser).await;
    let mut reader = group
        .reader()
        .expect("a stream is set")
        .start(FilteredStart::First)
        .build()
        .await
        .expect("reader");
    let page = tokio::time::timeout(READ_TIMEOUT, reader.next_page())
        .await
        .expect("page timely")
        .expect("page");
    laser
        .client()
        .delete_consumer_group(&stream_id, &topic_id, &group_id)
        .await
        .expect("delete old group");
    laser
        .client()
        .create_consumer_group(&stream_id, &topic_id, GROUP)
        .await
        .expect("new group");
    laser
        .client()
        .join_consumer_group(&stream_id, &topic_id, &group_id)
        .await
        .expect("join new incarnation");
    let result = reader.ack_page(&page).await;
    assert!(
        matches!(result, Err(ref error) if error.filter_reason() == Some(FilterErrorReason::SourceChanged)),
        "old page cannot acknowledge a new group: {result:?}"
    );
    assert!(
        stored_group_offset(&laser, &stream, page.partition_id)
            .await
            .is_none(),
        "new group has no acknowledged records"
    );
    assert_eq!(
        group.filter().get().await.expect("readable"),
        None,
        "a recreated group inherits no policy"
    );
    let _ = reader.close().await;
}

#[tokio::test]
async fn given_a_recreated_group_when_configuring_with_its_old_identity_then_should_leave_the_replacement_unbound()
 {
    let laser = harness::connected_laser_at(&catalog_server().await.connection_string()).await;
    let stream = group_source(&laser).await;
    let group = bound_group(&laser).await;
    let original = group.info().await.expect("original group");
    laser
        .client()
        .delete_consumer_group(
            &Identifier::named(&stream).expect("stream"),
            &Identifier::named(TOPIC).expect("topic"),
            &Identifier::named(GROUP).expect("group"),
        )
        .await
        .expect("delete original group");
    let replacement = group.create().build().await.expect("replacement group");
    assert_ne!(replacement.identity, original.identity);
    let mutation = FilterMutationRequest {
        v: FILTER_OP_VERSION,
        operation_id: u128::from(ulid::Ulid::generate()),
        mutation: FilterMutation::ConfigureGroup {
            group: FilterGroupRef {
                stream,
                topic: TOPIC.to_owned(),
                group: GROUP.to_owned(),
            },
            policy: GroupFilterSpec::Definition {
                name: POLICY_NAME.to_owned(),
                filter: bound_filter(),
            },
            expected_identity: Some(original.identity),
        },
    };
    let reply = laser
        .client()
        .send_binary_request(
            AGDX_FILTER_MUTATE_CODE,
            encode_named(&mutation).expect("mutation encodes").into(),
        )
        .await
        .expect("typed refusal");
    let reply: FilterCatalogReply = decode_named(&reply).expect("mutation reply");
    assert!(
        matches!(reply, FilterCatalogReply::Err(error) if error.reason == FilterErrorReason::SourceChanged)
    );
    assert!(
        group
            .filter()
            .get()
            .await
            .expect("replacement policy reads")
            .is_none()
    );
}

#[tokio::test]
async fn given_a_numeric_group_handle_when_reading_then_should_pin_the_topic_incarnation() {
    let laser = harness::connected_laser_at(&catalog_server().await.connection_string()).await;
    let stream = group_source(&laser).await;
    let stream_id = Identifier::named(&stream).expect("stream");
    let topic_id = Identifier::named(TOPIC).expect("topic");
    let named = Identifier::named(GROUP).expect("group");
    let created = bound_group(&laser)
        .await
        .info()
        .await
        .expect("the group exists");
    let by_id = laser.topic(TOPIC).consumer_group_id(u64::from(created.id));
    assert_eq!(
        by_id.filter().get().await.expect("readable"),
        created.filter,
        "a numeric handle reads the same policy"
    );
    let mut reader = by_id
        .reader()
        .expect("a stream is set")
        .start(FilteredStart::First)
        .build()
        .await
        .expect("numeric group");
    let page = tokio::time::timeout(READ_TIMEOUT, reader.next_page())
        .await
        .expect("deadline")
        .expect("page");
    assert_eq!(page.policy.group_id, Some(u64::from(created.id)));
    reader.ack_page(&page).await.expect("ack");
    reader.close().await.expect("close");
    laser
        .client()
        .delete_consumer_group(&stream_id, &topic_id, &named)
        .await
        .expect("delete group");
    laser
        .client()
        .create_consumer_group(&stream_id, &topic_id, GROUP)
        .await
        .expect("recreate name");
    assert!(
        by_id
            .reader()
            .expect("a stream is set")
            .build()
            .await
            .is_err(),
        "old id must not join the replacement group"
    );
}

#[tokio::test]
async fn given_out_of_order_acks_when_acknowledging_then_should_store_only_the_completed_prefix() {
    let laser = harness::connected_laser_at(&catalog_server().await.connection_string()).await;
    let stream = group_source(&laser).await;
    let group = bound_group(&laser).await;
    let mut reader = group
        .reader()
        .expect("a stream is set")
        .start(FilteredStart::First)
        .count(1)
        .build()
        .await
        .expect("the reader builds");
    let first = tokio::time::timeout(READ_TIMEOUT, reader.next_record())
        .await
        .expect("a record arrives")
        .expect("the read succeeds");
    let second = tokio::time::timeout(READ_TIMEOUT, reader.next_record())
        .await
        .expect("a record arrives")
        .expect("the read succeeds");
    assert_eq!((first.offset, second.offset), (0, 2));

    reader
        .ack(&second)
        .await
        .expect("the later record acknowledges");
    assert_eq!(
        stored_group_offset(&laser, &stream, 0).await,
        None,
        "the earlier record is still open"
    );
    reader
        .ack(&first)
        .await
        .expect("the earlier record acknowledges");

    assert_eq!(stored_group_offset(&laser, &stream, 0).await, Some(2));
    reader.close().await.expect("the reader closes");
}

/// Publish sixteen records on partition 0, matches at the even offsets.
async fn alternating_source(laser: &Laser) -> String {
    let payloads: Vec<&str> = (0..16)
        .map(|index| {
            if index % 2 == 0 {
                SAFE_MODE
            } else {
                GROUND_STATION
            }
        })
        .collect();
    publish(laser, TOPIC, &payloads, 0).await;
    laser.default_stream().expect("stream").to_owned()
}

#[tokio::test]
async fn given_a_later_record_acknowledged_through_when_an_earlier_one_is_acknowledged_after_then_should_keep_the_higher_stored_offset()
 {
    let laser = harness::connected_laser_at(&catalog_server().await.connection_string()).await;
    let stream = alternating_source(&laser).await;
    let group = bound_group(&laser).await;
    let mut reader = group
        .reader()
        .expect("a stream is set")
        .start(FilteredStart::First)
        .count(1)
        .build()
        .await
        .expect("the reader builds");
    let mut records = Vec::new();
    for _ in 0..3 {
        records.push(
            tokio::time::timeout(READ_TIMEOUT, reader.next_record())
                .await
                .expect("a record arrives")
                .expect("the read succeeds"),
        );
    }
    assert_eq!(
        records
            .iter()
            .map(|record| record.offset)
            .collect::<Vec<_>>(),
        vec![0, 2, 4]
    );

    reader
        .ack_through(&records[2])
        .await
        .expect("the highest record acknowledges everything before it");
    let highest = stored_group_offset(&laser, &stream, 0).await;
    assert!(highest.is_some_and(|offset| offset >= 4), "{highest:?}");

    reader
        .ack(&records[1])
        .await
        .expect("an earlier record completed after is still accepted");
    assert_eq!(
        stored_group_offset(&laser, &stream, 0).await,
        highest,
        "progress never moves back below the acknowledged offset"
    );
    let next = tokio::time::timeout(READ_TIMEOUT, reader.next_record())
        .await
        .expect("a record arrives")
        .expect("the read succeeds");
    assert_eq!(
        next.offset, 6,
        "local reading continues where this reader left off"
    );
    reader.close().await.expect("the reader closes");
}

#[tokio::test]
async fn given_another_member_that_stored_higher_progress_when_the_displaced_reader_acknowledges_its_page_then_should_refuse_it_and_keep_the_higher_offset()
 {
    let laser = harness::connected_laser_at(&catalog_server().await.connection_string()).await;
    let stream = alternating_source(&laser).await;
    let group = bound_group(&laser).await;
    let stream_id = Identifier::named(&stream).expect("stream");
    let topic_id = Identifier::named(TOPIC).expect("topic");
    let group_id = Identifier::named(GROUP).expect("group");
    let mut reader = group
        .reader()
        .expect("a stream is set")
        .start(FilteredStart::First)
        .count(1)
        .build()
        .await
        .expect("the reader builds");
    let first = tokio::time::timeout(READ_TIMEOUT, reader.next_record())
        .await
        .expect("a record arrives")
        .expect("the read succeeds");
    assert_eq!(first.offset, 0);
    // The reader leaves the group, and another member takes the partition
    // and stores progress past every record the reader holds.
    reader
        .coordinator()
        .leave_consumer_group(&stream_id, &topic_id, &group_id)
        .await
        .expect("the reader's membership is removed");
    let other = harness::connected_laser_at(&catalog_server().await.connection_string()).await;
    other
        .client()
        .join_consumer_group(&stream_id, &topic_id, &group_id)
        .await
        .expect("the other member joins");
    harness::eventually(|| async {
        other
            .client()
            .get_consumer_group(&stream_id, &topic_id, &group_id)
            .await
            .expect("the group reads")
            .is_some_and(|group| group.members.iter().any(|member| member.partitions == [0]))
            .then_some(())
    })
    .await;
    other
        .client()
        .store_consumer_offset(
            &Consumer::group(group_id.clone()),
            &stream_id,
            &topic_id,
            Some(0),
            15,
        )
        .await
        .expect("the other member stores its progress");

    assert!(
        reader.ack(&first).await.is_err(),
        "a displaced reader never claims the other member's receipt for its own page"
    );
    assert_eq!(
        stored_group_offset(&laser, &stream, 0).await,
        Some(15),
        "the other member's greater progress stands"
    );
    // The reader left its group above, so its close has no membership to end.
    let _ = reader.close().await;
}

#[tokio::test]
async fn given_a_recreated_topic_when_a_page_of_the_old_generation_is_acknowledged_then_should_refuse_the_mismatched_generation()
 {
    let laser = harness::connected_laser_at(&catalog_server().await.connection_string()).await;
    let stream = alternating_source(&laser).await;
    let group = bound_group(&laser).await;
    let stream_id = Identifier::named(&stream).expect("stream");
    let topic_id = Identifier::named(TOPIC).expect("topic");
    let mut reader = group
        .reader()
        .expect("a stream is set")
        .start(FilteredStart::First)
        .count(1)
        .build()
        .await
        .expect("the reader builds");
    let page = tokio::time::timeout(READ_TIMEOUT, reader.next_page())
        .await
        .expect("a page arrives")
        .expect("the read succeeds");
    laser
        .client()
        .delete_topic(&stream_id, &topic_id)
        .await
        .expect("the topic is deleted");
    laser
        .topic(TOPIC)
        .ensure(CLUSTER_PARTITIONS, None)
        .await
        .expect("the topic is created again");

    let refused = reader
        .ack_page(&page)
        .await
        .expect_err("a page of the old generation never stores into the new one");
    assert!(
        !refused.is_retryable() || refused.filter_reason().is_some(),
        "the mismatch is a definite refusal: {refused:?}"
    );
    assert_eq!(
        laser
            .client()
            .get_consumer_offset(
                &Consumer::group(Identifier::named(GROUP).expect("group")),
                &stream_id,
                &topic_id,
                Some(0)
            )
            .await
            .ok()
            .flatten()
            .map(|offset| offset.stored_offset),
        None,
        "the new generation holds no progress from the old page"
    );
    // The group of the old topic generation is gone with it.
    let _ = reader.close().await;
}

#[tokio::test]
async fn given_a_malformed_record_under_stop_when_reading_then_should_deliver_earlier_matches_then_the_fault()
 {
    let (server, _socket_dir, _state) = start_catalog_server().await;
    let laser = harness::connected_laser_at(&server.connection_string()).await;
    publish(&laser, TOPIC, &[SAFE_MODE, "not json", DECOMMISSION], 0).await;
    let stream = laser.default_stream().expect("stream").to_owned();
    let group = bound_group(&laser).await;
    let mut reader = group
        .reader()
        .expect("a stream is set")
        .start(FilteredStart::First)
        .build()
        .await
        .expect("the reader builds");

    let page = tokio::time::timeout(READ_TIMEOUT, reader.next_page())
        .await
        .expect("a page arrives")
        .expect("the read succeeds");
    assert_eq!(offsets(&page), vec![0]);
    reader.ack_page(&page).await.expect("the page acknowledges");
    let fault = reader
        .next_page()
        .await
        .expect_err("the fault stops the reader");

    assert!(matches!(
        fault,
        LaserError::FilterFault {
            partition_id: 0,
            offset: 1,
            reason: FaultReason::Malformed,
        }
    ));
    assert_eq!(
        stored_group_offset(&laser, &stream, 0).await,
        Some(0),
        "the fault stays unacknowledged"
    );
    reader.close().await.expect("the reader closes");
}

#[tokio::test]
async fn given_a_malformed_record_under_stop_when_skipped_then_should_store_its_offset_and_read_on_after_it()
 {
    let laser = harness::connected_laser_at(&catalog_server().await.connection_string()).await;
    publish(&laser, TOPIC, &[SAFE_MODE, "not json", DECOMMISSION], 0).await;
    let stream = laser.default_stream().expect("stream").to_owned();
    let group = bound_group(&laser).await;
    let mut reader = group
        .reader()
        .expect("a stream is set")
        .start(FilteredStart::First)
        .build()
        .await
        .expect("the reader builds");
    let page = tokio::time::timeout(READ_TIMEOUT, reader.next_page())
        .await
        .expect("a page arrives")
        .expect("the read succeeds");
    reader.ack_page(&page).await.expect("the page acknowledges");
    let fault = reader
        .next_page()
        .await
        .expect_err("the fault stops the reader");
    assert!(matches!(
        fault,
        LaserError::FilterFault {
            partition_id: 0,
            offset: 1,
            ..
        }
    ));

    assert!(
        matches!(reader.skip(0, 2).await, Err(LaserError::Invalid(_))),
        "the partition is stopped at offset 1"
    );
    assert_eq!(stored_group_offset(&laser, &stream, 0).await, Some(0));
    reader.skip(0, 1).await.expect("the fault is skipped");
    assert_eq!(
        stored_group_offset(&laser, &stream, 0).await,
        Some(1),
        "the skip stores the fault's offset"
    );
    let page = tokio::time::timeout(READ_TIMEOUT, reader.next_page())
        .await
        .expect("a page arrives")
        .expect("the read after the fault succeeds");
    assert_eq!(offsets(&page), vec![2]);
    assert!(
        matches!(reader.skip(0, 1).await, Err(LaserError::Invalid(_))),
        "the block is gone"
    );
    reader.close().await.expect("the reader closes");
}

#[tokio::test]
async fn given_a_malformed_record_under_stop_when_the_group_consumer_skips_it_then_should_deliver_the_next_match()
 {
    let laser = harness::connected_laser_at(&catalog_server().await.connection_string()).await;
    publish(&laser, TOPIC, &[SAFE_MODE, "not json", DECOMMISSION], 0).await;
    let stream = laser.default_stream().expect("stream").to_owned();
    let group = bound_group(&laser).await;
    let mut consumer = group
        .consumer()
        .start_at(ConsumerStart::First)
        .commit_policy(CommitPolicy::Disabled)
        .build()
        .await
        .expect("the group consumer builds");
    let first = consumer
        .next_within(READ_TIMEOUT)
        .await
        .expect("the first match arrives");
    consumer.commit(&first).await.expect("the record commits");
    let fault = consumer
        .next_within(READ_TIMEOUT)
        .await
        .expect_err("the fault stops the consumer");
    assert!(matches!(
        fault,
        LaserError::FilterFault {
            partition_id: 0,
            offset: 1,
            ..
        }
    ));

    assert!(matches!(
        consumer.skip(0, 0).await,
        Err(LaserError::Invalid(_))
    ));
    consumer.skip(0, 1).await.expect("the fault is skipped");
    let next = consumer
        .next_within(READ_TIMEOUT)
        .await
        .expect("the match after the fault arrives");
    assert_eq!(next.position.offset, 2);
    consumer.commit(&next).await.expect("the record commits");
    consumer.shutdown().await.expect("the consumer shuts down");
    assert_eq!(stored_group_offset(&laser, &stream, 0).await, Some(2));
}

#[tokio::test]
async fn given_a_local_guard_when_reading_then_should_agree_with_the_server() {
    let laser = harness::connected_laser_at(&catalog_server().await.connection_string()).await;
    group_source(&laser).await;
    let group = bound_group(&laser).await;
    let mut reader = group
        .reader()
        .expect("a stream is set")
        .start(FilteredStart::First)
        .local_guard(true)
        .build()
        .await
        .expect("the reader builds");

    let page = tokio::time::timeout(READ_TIMEOUT, reader.next_page())
        .await
        .expect("a page arrives")
        .expect("the local evaluation agrees");

    assert_eq!(offsets(&page), vec![0, 2]);
    reader.close().await.expect("the reader closes");
}

#[tokio::test]
async fn given_a_paused_revision_when_reading_then_should_refuse_new_reads_until_resumed() {
    let laser = harness::connected_laser_at(&catalog_server().await.connection_string()).await;
    group_source(&laser).await;
    let group = bound_group(&laser).await;
    let mut reader = group
        .reader()
        .expect("a stream is set")
        .start(FilteredStart::First)
        .count(1)
        .build()
        .await
        .expect("the reader builds");
    let first = tokio::time::timeout(READ_TIMEOUT, reader.next_record())
        .await
        .expect("a record arrives")
        .expect("the read succeeds");
    group
        .filter()
        .set_revision_enabled(1, false)
        .await
        .expect("the revision pauses");
    reader
        .ack(&first)
        .await
        .expect("delivered work still acknowledges");
    let refused = reader.try_next_page().await.expect_err("new reads stop");
    assert_eq!(
        refused.filter_reason(),
        Some(FilterErrorReason::RevisionDisabled)
    );
    group
        .filter()
        .set_revision_enabled(1, true)
        .await
        .expect("the revision resumes");
    let second = tokio::time::timeout(READ_TIMEOUT, reader.next_record())
        .await
        .expect("a record arrives")
        .expect("reads resume");
    assert_eq!(second.offset, 2);
    let revisions = group
        .filter()
        .revisions(1, 10)
        .await
        .expect("revisions list");
    assert_eq!(revisions.total_results, 1);
    reader.close().await.expect("close");
}

#[tokio::test]
async fn given_a_group_with_a_filter_when_deleted_then_should_release_it_and_remove_its_filter() {
    let laser = harness::connected_laser_at(&catalog_server().await.connection_string()).await;
    let stream = group_source(&laser).await;
    let group = bound_group(&laser).await;
    let before = group
        .filter()
        .get()
        .await
        .expect("readable")
        .expect("bound");
    assert!(group.filter().delete().await.expect("the filter deletes"));
    assert_eq!(group.filter().get().await.expect("readable"), None);
    assert!(
        !group.filter().delete().await.expect("nothing left"),
        "a second delete finds no filter"
    );
    let again = group
        .filter()
        .configure(POLICY_NAME, bound_filter())
        .await
        .expect("the same group configures again");
    assert_ne!(again.filter_id, before.filter_id, "the id is not reused");
    assert_eq!(again.digest, before.digest);
    assert_eq!(stored_group_offset(&laser, &stream, 0).await, None);
    group.filter().delete().await.expect("cleanup");
}

#[tokio::test]
async fn given_groups_sharing_a_policy_name_when_one_is_deleted_then_should_delete_only_the_filter_it_owns()
 {
    let laser = harness::connected_laser_at(&catalog_server().await.connection_string()).await;
    let (group, before) = deletion_group(&laser, 24).await;
    assert!(
        group
            .filter()
            .delete()
            .await
            .expect("the owned filter deletes")
    );
    assert_eq!(group.filter().get().await.expect("readable"), None);
    assert!(!group.filter().delete().await.expect("nothing left"));
    let (_, _, state) = CATALOG_SERVER.get().expect("the catalog runs");
    let state = state.lock().expect("catalog state");
    assert!(!state.filters.contains_key(&before.filter_id));
    for group_id in (90..98).chain(900..916) {
        let collision = FilterGroupIdentity {
            group_id,
            ..before.identity
        };
        let filter_id = state
            .owners
            .get(&collision)
            .expect("another group keeps its filter");
        assert_eq!(
            state.owned.get(filter_id).map(|owned| owned.name.as_str()),
            Some(POLICY_NAME),
            "names are not unique, owners are"
        );
    }
    assert!(
        state
            .owner_lookups
            .iter()
            .filter(|owner| **owner == before.identity)
            .count()
            >= 1,
        "the deletion looked the filter up by its owner"
    );
}

#[tokio::test]
async fn given_a_failed_owner_lookup_when_deletion_is_retried_then_should_report_the_error_and_converge()
 {
    let laser = harness::connected_laser_at(&catalog_server().await.connection_string()).await;
    let (group, before) = deletion_group(&laser, 8).await;
    let (_, _, state) = CATALOG_SERVER.get().expect("the catalog runs");
    state
        .lock()
        .expect("catalog state")
        .owner_lookup_failures
        .insert(before.identity);
    let error = group
        .filter()
        .delete()
        .await
        .expect_err("the owner lookup fails");
    assert_eq!(
        error.filter_reason(),
        Some(FilterErrorReason::CatalogUnavailable)
    );
    assert_eq!(group.filter().get().await.expect("readable"), Some(before));
    assert!(group.filter().delete().await.expect("the retry deletes"));
    assert!(!group.filter().delete().await.expect("nothing left"));
}

#[tokio::test]
async fn given_a_catalog_answering_with_another_owner_when_deleted_then_should_delete_nothing() {
    let laser = harness::connected_laser_at(&catalog_server().await.connection_string()).await;
    let (group, before) = deletion_group(&laser, 8).await;
    let (_, _, state) = CATALOG_SERVER.get().expect("the catalog runs");
    state
        .lock()
        .expect("catalog state")
        .owner_lookup_mismatches
        .insert(before.identity);
    assert_eq!(
        group.filter().summary().await.expect("the lookup answers"),
        None,
        "a row owned by another group is not this group's filter"
    );
    assert!(
        !group.filter().delete().await.expect("the lookup answers"),
        "nothing owned by this group was found"
    );
    {
        let mut state = state.lock().expect("catalog state");
        assert!(state.filters.contains_key(&before.filter_id));
        for group_id in 90..98 {
            assert!(state.owners.contains_key(&FilterGroupIdentity {
                group_id,
                ..before.identity
            }));
        }
        state.owner_lookup_mismatches.remove(&before.identity);
    }
    assert_eq!(group.filter().get().await.expect("readable"), Some(before));
}

#[tokio::test]
async fn given_a_named_configure_when_summarized_then_should_return_the_caller_name_and_the_owner()
{
    let laser = harness::connected_laser_at(&catalog_server().await.connection_string()).await;
    group_source(&laser).await;
    let group = laser.topic(TOPIC).consumer_group("named-desk");
    group.create().build().await.expect("the group is created");
    assert_eq!(
        group.filter().summary().await.expect("the lookup answers"),
        None,
        "an unbound group owns no filter"
    );
    let binding = group
        .filter()
        .configure("decommissions", bound_filter())
        .await
        .expect("the group is configured");
    let summary = group
        .filter()
        .summary()
        .await
        .expect("the lookup answers")
        .expect("the group owns its filter");
    assert_eq!(summary.id, binding.filter_id);
    assert_eq!(summary.name, "decommissions");
    assert_eq!(summary.bindings, 1);
    assert_eq!(
        summary.owner,
        Some(FilterOwner {
            identity: binding.identity,
            group: binding.group.clone(),
        })
    );
    assert_eq!(binding.group.group, "named-desk");
}

#[tokio::test]
async fn given_the_same_digest_with_a_new_name_when_configured_then_should_rename_the_group_filter()
{
    let laser = harness::connected_laser_at(&catalog_server().await.connection_string()).await;
    group_source(&laser).await;
    let group = laser.topic(TOPIC).consumer_group("renamed-desk");
    let first = group
        .create()
        .filter("first-name", bound_filter())
        .build()
        .await
        .expect("the group is created with its filter")
        .filter
        .expect("bound");
    let renamed = group
        .filter()
        .configure("second-name", bound_filter())
        .await
        .expect("the same digest keeps the binding");
    assert_eq!(
        renamed, first,
        "a rename keeps the binding and its generation"
    );
    let summary = group
        .filter()
        .summary()
        .await
        .expect("the lookup answers")
        .expect("the group owns its filter");
    assert_eq!(summary.id, first.filter_id);
    assert_eq!(summary.name, "second-name");
}

#[tokio::test]
async fn given_a_released_group_when_deleted_then_should_remove_the_filter_it_still_owns() {
    let laser = harness::connected_laser_at(&catalog_server().await.connection_string()).await;
    group_source(&laser).await;
    let group = bound_group(&laser).await;
    let released = group.filter().release().await.expect("the policy releases");
    let summary = group
        .filter()
        .summary()
        .await
        .expect("the lookup answers")
        .expect("a released group still owns its filter");
    assert_eq!(summary.id, released.filter_id);
    assert_eq!(summary.name, POLICY_NAME);
    assert_eq!(summary.bindings, 0);
    assert!(group.filter().delete().await.expect("the filter deletes"));
    assert_eq!(group.filter().summary().await.expect("readable"), None);
    assert_eq!(group.filter().get().await.expect("readable"), None);
    let (_, _, state) = CATALOG_SERVER.get().expect("the catalog runs");
    assert!(
        !state
            .lock()
            .expect("catalog state")
            .filters
            .contains_key(&released.filter_id)
    );
}

#[tokio::test]
async fn given_a_released_group_when_consumed_then_should_deliver_every_record_and_keep_its_digest()
{
    let laser = harness::connected_laser_at(&catalog_server().await.connection_string()).await;
    let stream = group_source(&laser).await;
    let group = bound_group(&laser).await;
    let released = group.filter().release().await.expect("the policy releases");
    assert_eq!(released.policy_generation, 1);
    assert_eq!(group.filter().get().await.expect("readable"), None);

    let mut consumer = group
        .consumer()
        .start_at(ConsumerStart::First)
        .commit_policy(CommitPolicy::All)
        .build()
        .await
        .expect("the group consumer builds");
    let mut delivered = Vec::new();
    for _ in 0..3 {
        delivered.push(
            consumer
                .next_within(READ_TIMEOUT)
                .await
                .expect("a record arrives")
                .position
                .offset,
        );
    }
    consumer.shutdown().await.expect("shutdown");
    assert_eq!(
        delivered,
        vec![0, 1, 2],
        "a released group receives everything"
    );
    assert_eq!(stored_group_offset(&laser, &stream, 0).await, Some(2));

    let again = group
        .filter()
        .configure(POLICY_NAME, bound_filter())
        .await
        .expect("the digest it ran binds again");
    assert_eq!(again.policy_generation, 3);
    let refused = group
        .filter()
        .configure(
            "kind-present",
            ConsumerFilter::json(FilterExpr::present("kind")),
        )
        .await
        .expect_err("another digest conflicts");
    assert_eq!(refused.filter_reason(), Some(FilterErrorReason::Conflict));
}

#[tokio::test]
async fn given_typed_custom_headers_when_filtered_then_should_preserve_types_and_payload() {
    let laser = harness::connected_laser_at(&catalog_server().await.connection_string()).await;
    let producer = laser
        .topic(TOPIC)
        .producer()
        .partitions(1)
        .build()
        .await
        .expect("producer");
    for priority in [
        HeaderValue::from(1_u8),
        HeaderValue::from(2_u8),
        HeaderValue::try_from("2").expect("text"),
    ] {
        let message = ProducerMessage::new(vec![0xff, 0x00])
            .header(
                HeaderKey::try_from("routing.priority").expect("key"),
                priority,
            )
            .header(
                HeaderKey::try_from("armed").expect("key"),
                HeaderValue::from(true),
            )
            .header(
                HeaderKey::try_from("temperature").expect("key"),
                HeaderValue::from(-1.5_f32),
            )
            .header(
                HeaderKey::try_from("sequence").expect("key"),
                HeaderValue::from(u64::MAX),
            );
        producer
            .send_to_partition(message, 0)
            .await
            .expect("publish");
    }
    let group = laser.topic(TOPIC).consumer_group("typed-headers");
    group
        .create()
        .filter(
            "typed-headers",
            ConsumerFilter::headers_only(FilterExpr::all([
                FilterExpr::header("routing.priority", CmpOp::Eq, 2_i32),
                FilterExpr::header("armed", CmpOp::Eq, true),
                FilterExpr::header("temperature", CmpOp::Lt, 0_i32),
                FilterExpr::header("sequence", CmpOp::Gt, i64::MAX),
            ])),
        )
        .build()
        .await
        .expect("the group is created with its filter");
    let mut reader = group
        .reader()
        .expect("a stream is set")
        .start(FilteredStart::First)
        .local_guard(true)
        .build()
        .await
        .expect("reader");
    let record = tokio::time::timeout(READ_TIMEOUT, reader.next_record())
        .await
        .expect("read deadline")
        .expect("record");
    assert_eq!(record.offset, 1);
    assert_eq!(record.message.payload.as_ref(), &[0xff, 0x00]);
    reader.ack(&record).await.expect("acknowledge");
    assert!(
        reader
            .try_next_page()
            .await
            .expect("remaining records")
            .is_none()
    );
    reader.close().await.expect("close");
}

struct AddresseeRecorder(Arc<Mutex<Vec<String>>>);

impl laser_sdk::agent::AgentHandler for AddresseeRecorder {
    async fn handle(
        &self,
        message: &laser_sdk::agent::AgentMessage,
        _ctx: &laser_sdk::agent::AgentCtx<'_>,
    ) -> Result<(), LaserError> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(String::from_utf8_lossy(message.body()).into_owned());
        Ok(())
    }
}

#[tokio::test]
async fn given_a_filtering_server_when_an_agent_spawns_on_the_session_topic_then_should_bind_its_group_to_the_addressee_filter()
 {
    use laser_sdk::types::MintUlid;
    let laser = harness::connected_laser_at(&catalog_server().await.connection_string()).await;
    let me: laser_sdk::wire::agent::AgentId = "triage".parse().expect("valid agent id");
    let seen = Arc::new(Mutex::new(Vec::new()));
    let mut agent = laser_sdk::agent::Agent::builder()
        .id("triage".parse().expect("valid agent id"))
        .listen_on(laser_sdk::provenance::AgentTopic::Sessions)
        .handler(AddresseeRecorder(Arc::clone(&seen)))
        .build()
        .spawn(laser.clone());
    agent.ready().await.expect("the agent is ready");

    let info = laser
        .topic(laser_sdk::wire::topics::AGENT_SESSIONS)
        .consumer_group("triage")
        .info()
        .await
        .expect("the role group exists");
    assert_eq!(
        info.filter.map(|binding| binding.digest),
        Some(laser_sdk::wire::dispatch::addressee_filter(&me).digest()),
        "the role group runs the addressee filter"
    );
    let own = laser
        .topic(laser_sdk::wire::topics::AGENT_SESSIONS)
        .consumer_group("triage")
        .filter()
        .summary()
        .await
        .expect("the lookup answers")
        .expect("the role group owns its filter");
    assert_eq!(own.name, laser_sdk::wire::dispatch::ADDRESSEE_FILTER_NAME);

    let conversation = laser_sdk::wire::agent::ConversationId::mint();
    let client: laser_sdk::wire::agent::AgentId = "client".parse().expect("valid agent id");
    for (target, body) in [
        (Some("triage"), "mine"),
        (Some("critic"), "theirs"),
        (None, "everyone"),
    ] {
        let producer = laser.agdx(
            laser_sdk::provenance::AgentTopic::Sessions,
            client.clone(),
            conversation,
        );
        let command = producer.command(
            laser_sdk::wire::agent::CorrelationId::mint(),
            body.as_bytes().to_vec(),
        );
        let command = match target {
            Some(target) => command.with_target(
                target
                    .parse::<laser_sdk::types::AgentId>()
                    .expect("valid agent id"),
            ),
            None => command,
        };
        command.send().await.expect("the command is sent");
    }
    harness::eventually(|| async {
        let seen = seen
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        (seen.len() >= 2).then_some(())
    })
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let mut seen = seen
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    seen.sort();
    assert_eq!(seen, vec!["everyone".to_owned(), "mine".to_owned()]);
    agent.shutdown().await.expect("the agent stops");
}

#[tokio::test]
async fn given_a_role_group_bound_to_another_filter_when_an_agent_spawns_then_should_refuse_to_run()
{
    let laser = harness::connected_laser_at(&catalog_server().await.connection_string()).await;
    laser
        .topic(laser_sdk::wire::topics::AGENT_SESSIONS)
        .consumer_group("auditor")
        .create()
        .filter("broadcast", laser_sdk::wire::dispatch::broadcast_filter())
        .build()
        .await
        .expect("the group is bound to the broadcast filter");
    let agent = laser_sdk::agent::Agent::builder()
        .id("auditor".parse().expect("valid agent id"))
        .listen_on(laser_sdk::provenance::AgentTopic::Sessions)
        .handler(AddresseeRecorder(Arc::default()))
        .build()
        .spawn(laser.clone());
    let error = agent
        .join()
        .await
        .expect_err("a group bound to another digest is refused");
    assert!(
        matches!(error, LaserError::ConsumerGroupSetup { .. }),
        "{error:?}"
    );
}

#[tokio::test]
async fn given_a_released_policy_when_acknowledging_its_page_then_should_refuse_without_storing() {
    let laser = harness::connected_laser_at(&catalog_server().await.connection_string()).await;
    let stream = group_source(&laser).await;
    let group = bound_group(&laser).await;
    let mut reader = group
        .reader()
        .expect("reader")
        .start(FilteredStart::First)
        .build()
        .await
        .expect("build");
    let page = tokio::time::timeout(READ_TIMEOUT, reader.next_page())
        .await
        .expect("deadline")
        .expect("page");
    group.filter().release().await.expect("release");
    let error = reader
        .ack_page(&page)
        .await
        .expect_err("old policy must be refused");
    assert_eq!(error.filter_reason(), Some(FilterErrorReason::Conflict));
    assert_eq!(stored_group_offset(&laser, &stream, 0).await, None);
    let current = tokio::time::timeout(READ_TIMEOUT, reader.next_page())
        .await
        .expect("deadline")
        .expect("current page");
    reader
        .ack_page(&current)
        .await
        .expect("current policy can acknowledge");
    assert_eq!(
        stored_group_offset(&laser, &stream, 0).await,
        current.safe_ack_offset
    );
    reader.close().await.expect("close");
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_three_node_cluster_when_an_idle_group_reader_waits_then_should_hold_one_filtered_wait_per_serving_node_and_wake_on_any_partition()
 {
    // Each node takes one filtered wait here, so a second wait on a node is
    // refused and the reader would sleep its 30 s idle interval.
    let (_cluster, _sockets, _state) = start_catalog_cluster(Some(1)).await;
    let laser = cluster_catalog_laser(&_cluster, "cluster_waits").await;
    let capabilities = laser.capabilities().await;
    assert!(capabilities.filters.group_policy_reads);
    assert!(
        capabilities.filters.max_wait > Duration::ZERO,
        "the cluster holds filtered waits"
    );
    let primaries =
        harness::settled_primaries(&laser, TOPIC, CLUSTER_PARTITIONS, _cluster.size()).await;
    assert!(
        primaries.iter().collect::<BTreeSet<_>>().len() > 1,
        "the partitions are led by different nodes: {primaries:?}"
    );
    let group = bound_group(&laser).await;
    let mut reader = group
        .reader()
        .expect("group source")
        .start(FilteredStart::First)
        .idle_interval(Duration::from_secs(30))
        .max_wait(WAIT_MAX)
        .build()
        .await
        .expect("the group reader builds");
    assigned(&mut reader).await;
    for partition in 0..CLUSTER_PARTITIONS {
        let page = wake_on(&laser, &mut reader, partition, WARM_UP).await;
        reader.ack_page(&page).await.expect("acknowledge");
    }
    for round in 0..2 {
        for partition in 0..CLUSTER_PARTITIONS {
            let page = wake_on(&laser, &mut reader, partition, PROMPT_WAKE).await;
            assert_eq!(
                (page.partition_id, page.records.len()),
                (partition, 1),
                "round {round}: the wait of partition {partition}'s primary ended with its one match"
            );
            reader.ack_page(&page).await.expect("acknowledge");
        }
    }
    reader.close().await.expect("close");
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_policy_aware_group_consumer_on_a_cluster_when_a_partition_primary_fails_over_then_should_keep_delivering_matches_and_store_its_offsets()
 {
    let (mut cluster, _sockets, _state) = start_catalog_cluster(None).await;
    let stream = "cluster_failover";
    let laser = cluster_catalog_laser(&cluster, stream).await;
    let primaries = harness::partition_primaries(&laser, TOPIC, CLUSTER_PARTITIONS).await;
    let group = bound_group(&laser).await;
    let mut consumer = group
        .consumer()
        .start_at(ConsumerStart::First)
        .commit_policy(CommitPolicy::Each)
        .max_wait(WAIT_MAX)
        .build()
        .await
        .expect("the group consumer builds");
    let mut delivered = BTreeMap::new();
    for partition in 0..CLUSTER_PARTITIONS {
        deliver_one(
            &laser,
            &mut consumer,
            &mut delivered,
            partition,
            PROMPT_WAKE,
        )
        .await;
    }

    let leader = cluster.leader(0).await;
    let stopped = usize::from(primaries[0]);
    cluster.stop_node(stopped);
    if stopped == leader {
        let new_leader = cluster.leader((stopped + 1) % 3).await;
        cluster.route_endpoint_to(new_leader);
    }
    let failover = Instant::now();
    // The successor leads within milliseconds of a graceful stop, so a read
    // still held by the stopped node must move on long before the client's
    // 30 s operation deadline.
    for partition in 0..CLUSTER_PARTITIONS {
        deliver_one(&laser, &mut consumer, &mut delivered, partition, FAILOVER).await;
    }
    eprintln!(
        "cluster policy-aware consumer: delivered every partition {:?} after node {stopped} stopped",
        failover.elapsed()
    );
    cluster.start_node(stopped).await;
    for partition in 0..CLUSTER_PARTITIONS {
        deliver_one(&laser, &mut consumer, &mut delivered, partition, RECOVERY).await;
    }
    for partition in 0..CLUSTER_PARTITIONS {
        deliver_one(
            &laser,
            &mut consumer,
            &mut delivered,
            partition,
            PROMPT_WAKE,
        )
        .await;
    }
    consumer.shutdown().await.expect("the consumer shuts down");
    for (partition, offset) in &delivered {
        wait_for_group_offset(&laser, stream, *partition, Some(*offset)).await;
    }
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_page_read_before_its_primary_crashes_on_a_cluster_when_acknowledged_then_should_store_it_through_the_new_primary()
 {
    let (mut cluster, _sockets, _state) = start_catalog_cluster(None).await;
    let stream = "cluster_ack";
    let laser = cluster_catalog_laser(&cluster, stream).await;
    let primaries = harness::spread_primaries(&laser, TOPIC, CLUSTER_PARTITIONS).await;
    let leader = cluster.leader(0).await;
    let partition = (0..CLUSTER_PARTITIONS)
        .find(|partition| usize::from(primaries[*partition as usize]) != leader)
        .expect("a partition led by a follower");
    let crashed = usize::from(primaries[partition as usize]);
    publish(
        &laser,
        TOPIC,
        &[SAFE_MODE, GROUND_STATION, DECOMMISSION],
        partition,
    )
    .await;
    let group = bound_group(&laser).await;
    let mut reader = group
        .reader()
        .expect("group source")
        .start(FilteredStart::First)
        .build()
        .await
        .expect("the group reader builds");
    let page = tokio::time::timeout(READ_TIMEOUT, reader.next_page())
        .await
        .expect("page deadline")
        .expect("page");
    assert_eq!((page.partition_id, offsets(&page)), (partition, vec![0, 2]));

    cluster.kill_node(crashed);
    let started = Instant::now();
    reader
        .ack_page(&page)
        .await
        .expect("the acknowledgment follows the partition to its new primary");
    let stored = started.elapsed();
    eprintln!("cluster acknowledgment: stored {stored:?} after the primary crashed");
    assert!(
        stored < FAILOVER,
        "the acknowledgment took {stored:?} after the primary crashed"
    );
    wait_for_group_offset(&laser, stream, partition, page.safe_ack_offset).await;
    reader.close().await.expect("close");
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_group_reader_holding_its_filtered_waits_on_a_cluster_when_another_member_leaves_then_should_read_a_partition_it_gains_before_the_waits_end()
 {
    let (_cluster, _sockets, _state) = start_catalog_cluster(None).await;
    let laser = cluster_catalog_laser(&_cluster, "cluster_rebalance").await;
    let group = bound_group(&laser).await;
    let build = || {
        group
            .reader()
            .expect("group source")
            .start(FilteredStart::First)
            .max_wait(LONG_WAIT)
            .build()
    };
    let mut first = build().await.expect("the first member builds");
    assigned(&mut first).await;
    let mut second = build().await.expect("the second member builds");
    tokio::time::timeout(READ_TIMEOUT, async {
        loop {
            assert!(
                first
                    .try_next_page()
                    .await
                    .expect("an idle round")
                    .is_none()
            );
            assert!(
                second
                    .try_next_page()
                    .await
                    .expect("an idle round")
                    .is_none()
            );
            if first.partitions().len() < CLUSTER_PARTITIONS as usize
                && !second.partitions().is_empty()
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the two members split the partitions");
    let gained = second.partitions()[0];

    // The first member holds its waits on its own partitions when the second
    // one leaves, and the record lands on a partition it gains.
    let waiting = tokio::spawn(async move {
        let page = tokio::time::timeout(PROMPT_REBALANCE, first.next_page()).await;
        (first, page)
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    second.close().await.expect("the second member leaves");
    publish(&laser, TOPIC, &[SAFE_MODE], gained).await;
    let (first, page) = waiting.await.expect("the first member reads");
    let page = page
        .expect("the gained partition is read long before the 20 s waits end")
        .expect("page");
    assert_eq!((page.partition_id, offsets(&page)), (gained, vec![0]));
    first.close().await.expect("close");
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_named_group_policy_configured_through_one_node_when_read_and_deleted_through_another_then_should_find_it_by_its_owner()
 {
    let (_cluster, _sockets, state) = start_catalog_cluster(None).await;
    let stream = "cluster_names";
    let configurer = cluster_catalog_laser(&_cluster, stream).await;
    let group = configurer.topic(TOPIC).consumer_group(GROUP);
    let created = group
        .create()
        .filter(POLICY_NAME, bound_filter())
        .build()
        .await
        .expect("the group is created with its named filter");
    let binding = created.filter.expect("the group is bound");
    let configured_on = forwarded_through(&state, 0, AGDX_FILTER_MUTATE_CODE)
        .expect("a node forwarded the configuration");

    // Each client keeps to a node picked at random, so new clients are
    // opened until one reaches the catalog through another node.
    let mut found = None;
    for _ in 0..30 {
        let other = harness::cluster_laser(&_cluster, stream).await;
        let mark = state.lock().expect("catalog state").forwarded.len();
        let summary = other
            .topic(TOPIC)
            .consumer_group(GROUP)
            .filter()
            .summary()
            .await
            .expect("the summary reads");
        let node = forwarded_through(&state, mark, AGDX_LIST_FILTERS_CODE)
            .expect("a node forwarded the owner lookup");
        if node != configured_on {
            found = Some((other, summary, node));
            break;
        }
    }
    let (other, summary, read_on) =
        found.expect("a client reached the catalog through another node");
    let summary = summary.expect("the group's own filter is found by its owner");
    assert_eq!(summary.id, binding.filter_id);
    assert_eq!(
        summary.name, POLICY_NAME,
        "the caller's name survives the node"
    );
    let owner = summary.owner.expect("the summary names its owner");
    assert_eq!(
        owner.identity, binding.identity,
        "every node stamps one identity"
    );
    assert_eq!(owner.group.group, GROUP);

    let mark = state.lock().expect("catalog state").forwarded.len();
    assert!(
        other
            .topic(TOPIC)
            .consumer_group(GROUP)
            .filter()
            .delete()
            .await
            .expect("the filter deletes through the other node")
    );
    assert_eq!(
        forwarded_through(&state, mark, AGDX_FILTER_MUTATE_CODE),
        Some(read_on),
        "the deletion went through the node that read the summary"
    );
    assert_eq!(
        group.filter().summary().await.expect("the summary reads"),
        None,
        "the client of the first node sees the filter gone"
    );
    assert_eq!(group.filter().get().await.expect("readable"), None);
}

// Read rounds without the idle wait until the reader holds every partition,
// past a refusal reported while a primary still applies the member's ownership.
async fn assigned(reader: &mut FilteredReader) {
    tokio::time::timeout(READ_TIMEOUT, async {
        loop {
            match reader.try_next_page().await {
                Ok(page) => assert!(page.is_none(), "nothing is published yet"),
                Err(error) if error.filter_reason() == Some(FilterErrorReason::MembershipStale) => {
                }
                Err(error) => panic!("an idle round: {error}"),
            }
            if reader.partitions().len() == CLUSTER_PARTITIONS as usize {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the reader is assigned every partition");
}

// Publish one other and one matching record on `partition` while the reader
// holds its waits, and take the page they end within `bound`. A refusal the
// reader reports while a primary still applies the member's ownership is read
// past.
async fn wake_on(
    laser: &Laser,
    reader: &mut FilteredReader,
    partition: u32,
    bound: Duration,
) -> MatchedPage {
    let publisher = laser.clone();
    let late = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        publish(&publisher, TOPIC, &[GROUND_STATION, SAFE_MODE], partition).await;
    });
    let page = tokio::time::timeout(bound, async {
        loop {
            match reader.next_page().await {
                Ok(page) => break page,
                Err(error)
                    if error.filter_reason() == Some(FilterErrorReason::MembershipStale) =>
                {
                    eprintln!("partition {partition} is read again after a refusal: {error}");
                }
                Err(error) => panic!("page: {error}"),
            }
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!("the held wait answers when a record lands on partition {partition}, long before the 30 s idle interval")
    });
    late.await.expect("the late record publishes");
    page
}

// Publish one other and one matching record on `partition`, retried while a
// failover leaves the partition without a primary, and take the match within
// `bound`. A record delivered again after the failover is read past.
async fn deliver_one(
    laser: &Laser,
    consumer: &mut LaserConsumer,
    delivered: &mut BTreeMap<u32, u64>,
    partition: u32,
    bound: Duration,
) {
    let started = Instant::now();
    loop {
        let producer = laser.topic(TOPIC).producer().build().await;
        let sent = match producer {
            Ok(producer) => producer
                .send_batch_with_routing(
                    [GROUND_STATION, SAFE_MODE]
                        .iter()
                        .map(|payload| ProducerMessage::new(payload.as_bytes().to_vec())),
                    Some(Routing::Partition(partition)),
                )
                .await
                .map(|_| ()),
            Err(error) => Err(error),
        };
        match sent {
            Ok(()) => break,
            Err(error) => {
                assert!(
                    started.elapsed() < bound,
                    "partition {partition} took no record within {bound:?}: {error}"
                );
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
    loop {
        let remaining = bound.saturating_sub(started.elapsed());
        let message = consumer
            .next_within(remaining)
            .await
            .unwrap_or_else(|error| {
                panic!("the match on partition {partition} arrives within {bound:?}: {error}")
            });
        if delivered
            .get(&message.partition_id)
            .is_some_and(|last| *last >= message.position.offset)
        {
            continue;
        }
        assert_eq!(
            (message.partition_id, message.payload.as_ref()),
            (partition, SAFE_MODE.as_bytes()),
            "only the match of the published partition arrives"
        );
        delivered.insert(partition, message.position.offset);
        return;
    }
}

// Read the group's stored offset of `partition` until it is `expected`,
// while a failover can leave its primary unknown for a moment.
async fn wait_for_group_offset(laser: &Laser, stream: &str, partition: u32, expected: Option<u64>) {
    let deadline = Instant::now() + READ_TIMEOUT;
    loop {
        let stored = laser
            .client()
            .get_consumer_offset(
                &Consumer::group(Identifier::named(GROUP).expect("group name")),
                &Identifier::named(stream).expect("stream name"),
                &Identifier::named(TOPIC).expect("topic name"),
                Some(partition),
            )
            .await
            .ok()
            .flatten()
            .map(|offset| offset.stored_offset);
        if stored == expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "partition {partition} stored {stored:?}, expected {expected:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

// The node that forwarded the first command `code` after `mark`.
fn forwarded_through(state: &Arc<Mutex<CatalogState>>, mark: usize, code: u32) -> Option<usize> {
    state
        .lock()
        .expect("catalog state")
        .forwarded
        .iter()
        .skip(mark)
        .find(|(_, forwarded)| *forwarded == code)
        .map(|(node, _)| *node)
}
