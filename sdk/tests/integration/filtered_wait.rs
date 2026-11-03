use crate::harness;
use crate::test_iggy::TestIggyCluster;
use bytes::Bytes;
use laser_sdk::prelude::full::*;
use laser_sdk::wire::codes::{
    AGDX_FILTERED_WAIT_CANCEL_CODE, AGDX_FILTERED_WAIT_CODE, FILTER_OP_VERSION,
};
use laser_sdk::wire::filter::{
    FilterConsumer, FilterOutcome, FilterReply, FilterSource, FilteredStart, FilteredWaitCancel,
    FilteredWaitCancelled, FilteredWaitPartition, FilteredWaitRequest, FilteredWaitResult,
};
use laser_sdk::wire::framing::{decode_named, encode_named};
use std::collections::BTreeSet;
use std::time::Duration;
use tokio::time::Instant;

const WAIT_TOPIC: &str = "waits";
const WAIT_GROUP: &str = "wait-readers";
const WAIT_PARTITIONS: u32 = 6;
const LONG_WAIT: Duration = Duration::from_secs(20);
// Longer than every bound below, so a round that slept instead of holding a
// wait fails the test.
const IDLE_INTERVAL: Duration = Duration::from_secs(30);
// A cancelled wait frees its slot in a round trip, a wait left alone holds
// for `LONG_WAIT`, so half of it tells the two apart however loaded the host.
const CANCEL_BOUND: Duration = Duration::from_secs(LONG_WAIT.as_secs() / 2);
const SETTLE: Duration = Duration::from_secs(15);
const FIVE_NODES: usize = 5;
const MEMBER_FAILOVER: Duration = Duration::from_secs(10);

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_group_reader_holding_a_filtered_wait_on_each_node_of_a_cluster_when_one_node_answers_ready_then_should_cancel_the_waits_on_the_other_nodes_at_once()
 {
    let http: Vec<String> = (0..3).map(|_| harness::free_http_address()).collect();
    let cluster =
        TestIggyCluster::start_with(|node| harness::cluster_http_settings(&http, node)).await;
    let leader = cluster.leader(0).await;
    cluster.route_endpoint_to(leader);
    let stream = "cluster_wait_cancel";
    let laser = harness::cluster_laser(&cluster, stream).await;
    laser
        .stream(stream)
        .ensure()
        .await
        .expect("the stream exists");
    laser
        .topic(WAIT_TOPIC)
        .ensure(WAIT_PARTITIONS, None)
        .await
        .expect("the topic exists");
    let primaries =
        harness::settled_primaries(&laser, WAIT_TOPIC, WAIT_PARTITIONS, cluster.size()).await;
    let serving: BTreeSet<usize> = primaries
        .iter()
        .map(|primary| usize::from(*primary))
        .collect();
    assert!(
        serving.len() > 1,
        "the partitions are led by different nodes: {primaries:?}"
    );

    // The server ends a wait its connection cancels by id, at once and empty.
    let probe_partition = 0;
    let probe_node = primaries[probe_partition as usize];
    let wait = FilteredWaitRequest {
        v: FILTER_OP_VERSION,
        source: FilterSource {
            stream: stream.to_owned(),
            topic: WAIT_TOPIC.to_owned(),
        },
        consumer: FilterConsumer::Consumer("wait-probe".to_owned()),
        partitions: vec![FilteredWaitPartition {
            partition_id: probe_partition,
            offset: 0,
        }],
        max_wait_ms: u32::try_from(LONG_WAIT.as_millis()).expect("wait in milliseconds"),
        wait_id: 7,
    };
    let client = laser.client();
    let held = {
        let client = std::sync::Arc::clone(&client);
        let payload = Bytes::from(encode_named(&wait).expect("encode the wait"));
        tokio::spawn(async move {
            let sent = Instant::now();
            let reply = client
                .send_binary_request_to_replica(AGDX_FILTERED_WAIT_CODE, payload, probe_node)
                .await;
            (reply, sent.elapsed())
        })
    };
    let (parked, _) =
        harness::held_requests_until(&http[usize::from(probe_node)], SETTLE, |held| held >= 1)
            .await;
    assert_eq!(parked, Some(1), "the probe wait parks on its node");
    let cancel = FilteredWaitCancel {
        v: FILTER_OP_VERSION,
        wait_ids: vec![7],
    };
    let cancelled = client
        .send_binary_request_to_replica(
            AGDX_FILTERED_WAIT_CANCEL_CODE,
            Bytes::from(encode_named(&cancel).expect("encode the cancel")),
            probe_node,
        )
        .await
        .expect("the cancel is answered");
    assert_eq!(
        decode_named::<FilterReply>(&cancelled).expect("a filter reply"),
        FilterReply::Ok(FilterOutcome::Cancelled(FilteredWaitCancelled {
            cancelled: vec![7]
        }))
    );
    let (reply, held_for) = held.await.expect("the probe wait ends");
    assert_eq!(
        decode_named::<FilterReply>(&reply.expect("the wait is answered")).expect("a filter reply"),
        FilterReply::Ok(FilterOutcome::Ready(FilteredWaitResult {
            ready: Vec::new()
        }))
    );
    assert!(
        held_for < CANCEL_BOUND,
        "the cancelled wait answered after {held_for:?}"
    );
    let (released, _) =
        harness::held_requests_until(&http[usize::from(probe_node)], CANCEL_BOUND, |held| {
            held == 0
        })
        .await;
    assert_eq!(released, Some(0), "the cancelled wait holds no slot");

    // A reader holding one wait per node cancels the others when one answers.
    let group = laser.topic(WAIT_TOPIC).consumer_group(WAIT_GROUP);
    group.create().build().await.expect("the group exists");
    let mut reader = group
        .reader()
        .expect("group source")
        .start(FilteredStart::Next)
        .max_wait(LONG_WAIT)
        .build()
        .await
        .expect("the reader builds");
    tokio::time::timeout(SETTLE, async {
        while reader.partitions().len() < WAIT_PARTITIONS as usize {
            assert!(
                reader
                    .try_next_page()
                    .await
                    .expect("an idle round")
                    .is_none()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the reader owns every partition");
    // A node that has not applied the assignment yet refuses a wait once,
    // and the reader paces and waits again there.
    let waiting = tokio::spawn(async move {
        let page = tokio::time::timeout(LONG_WAIT, async {
            loop {
                match reader.next_page().await {
                    Ok(page) => return page,
                    Err(error) if error.is_retryable() => {}
                    Err(error) => panic!("the reader failed: {error}"),
                }
            }
        })
        .await;
        (reader, page)
    });
    for node in &serving {
        let (parked, _) =
            harness::held_requests_until(&http[*node], SETTLE, |held| held >= 1).await;
        assert_eq!(parked, Some(1), "node {node} holds the reader's wait");
    }
    let woken_partition = 0;
    let woken_node = usize::from(primaries[woken_partition as usize]);
    let producer = laser
        .topic(WAIT_TOPIC)
        .producer()
        .partitions(WAIT_PARTITIONS)
        .build()
        .await
        .expect("the producer initializes");
    producer
        .send_to_partition(ProducerMessage::new(b"wake".to_vec()), woken_partition)
        .await
        .expect("the record is published");
    let (reader, page) = waiting.await.expect("the reader reads");
    let page = page.expect("the wait answers ready");
    assert_eq!(page.partition_id, woken_partition);
    let answered = Instant::now();
    for node in serving.iter().filter(|node| **node != woken_node) {
        let (held, released_after) =
            harness::held_requests_until(&http[*node], LONG_WAIT, |held| held == 0).await;
        eprintln!(
            "filtered wait cancel: node {node} released its wait {released_after:?} after the ready answer"
        );
        assert_eq!(held, Some(0), "node {node} holds no wait of the reader");
        assert!(
            answered.elapsed() < CANCEL_BOUND,
            "node {node} released its wait {:?} after another node answered, it held to its deadline",
            answered.elapsed()
        );
    }
    reader.close().await.expect("the reader closes");
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_group_reader_over_partitions_stored_on_three_of_five_nodes_of_a_cluster_when_a_member_primary_stops_then_should_read_wait_and_acknowledge_through_members_only()
 {
    let mut cluster = TestIggyCluster::start_sized(FIVE_NODES, |_| Vec::new()).await;
    let leader = cluster.leader(0).await;
    cluster.route_endpoint_to(leader);
    let stream = "cluster_members";
    let laser = harness::cluster_laser(&cluster, stream).await;
    laser
        .stream(stream)
        .ensure()
        .await
        .expect("the stream exists");
    laser
        .topic(WAIT_TOPIC)
        .ensure(3, Some(3))
        .await
        .expect("the factor-3 topic exists");
    let members = harness::partition_members(&laser, WAIT_TOPIC).await;
    let primaries = harness::partition_primaries(&laser, WAIT_TOPIC, 3).await;
    for (partition, primary) in (0..3).zip(&primaries) {
        assert_eq!(members[&partition].len(), 3, "{members:?}");
        assert!(members[&partition].contains(primary), "{members:?}");
    }
    let group = laser.topic(WAIT_TOPIC).consumer_group(WAIT_GROUP);
    group.create().build().await.expect("the group exists");
    let mut reader = group
        .reader()
        .expect("group source")
        .start(FilteredStart::First)
        .max_wait(Duration::from_secs(5))
        .idle_interval(IDLE_INTERVAL)
        .build()
        .await
        .expect("the reader builds");
    let producer = laser
        .topic(WAIT_TOPIC)
        .producer()
        .partitions(3)
        .build()
        .await
        .expect("the producer initializes");
    for partition in 0..3 {
        producer
            .send_to_partition(
                ProducerMessage::new(format!("before-{partition}").into_bytes()),
                partition,
            )
            .await
            .expect("the record is published");
    }
    let mut read = BTreeSet::new();
    tokio::time::timeout(SETTLE, async {
        while read.len() < 3 {
            let page = reader.next_page().await.expect("a page from a member");
            read.insert(page.partition_id);
            reader
                .ack_page(&page)
                .await
                .expect("stored through a member");
        }
    })
    .await
    .expect("every partition delivers through its members");

    let victim_partition = (0..3)
        .find(|partition| usize::from(primaries[*partition as usize]) != leader)
        .expect("a partition led by a node other than the metadata leader");
    let victim = usize::from(primaries[victim_partition as usize]);
    cluster.stop_node(victim);
    let sent = Instant::now();
    harness::eventually(|| async {
        producer
            .send_to_partition(ProducerMessage::new(b"after".to_vec()), victim_partition)
            .await
            .ok()
    })
    .await;
    let page = tokio::time::timeout(MEMBER_FAILOVER, async {
        loop {
            match reader.next_page().await {
                Ok(page) if page.partition_id == victim_partition => return page,
                Ok(page) => reader.ack_page(&page).await.expect("stored"),
                Err(error) => panic!("the reader surfaced a refusal: {error}"),
            }
        }
    })
    .await
    .expect("the moved partition delivers through another member");
    eprintln!(
        "member failover: partition {victim_partition} delivered {:?} after its primary stopped",
        sent.elapsed()
    );
    reader
        .ack_page(&page)
        .await
        .expect("the acknowledgment stores through the new primary");
    let moved =
        harness::partition_primaries(&laser, WAIT_TOPIC, 3).await[victim_partition as usize];
    assert!(
        usize::from(moved) != victim && members[&victim_partition].contains(&moved),
        "the partition moved to another member: {moved} of {:?}",
        members[&victim_partition]
    );
    cluster.start_node(victim).await;
    reader.close().await.expect("the reader closes");
}
