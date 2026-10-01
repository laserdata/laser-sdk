// Consumer-group filtered reads against the fork with a stand-in catalog. The
// stand-in answers the backend probe and resolves every group binding to one
// saved revision, which is all a bound read asks the catalog. It runs on its
// own thread and runtime because the server outlives each test's runtime.

use crate::harness;
use crate::test_iggy::TestIggy;
use laser_sdk::filters::{
    ConsumerFilter, FilterErrorReason, FilterExpr, FilterState, FilteredStart,
};
use laser_sdk::iggy::prelude::{
    Consumer, ConsumerGroupClient, ConsumerOffsetClient, Identifier, PartitionClient,
};
use laser_sdk::prelude::full::Laser;
use laser_sdk::prelude::{ProducerMessage, Routing};
use laser_sdk::query::CmpOp;
use laser_sdk::wire::codes::{
    AGDX_BACKEND_HELLO_CODE, AGDX_RESOLVE_FILTER_POLICY_CODE, CONTROL_OP_VERSION,
    FILTER_OP_VERSION, FORK_OP_VERSION, KV_OP_VERSION, QUERY_OP_VERSION,
};
use laser_sdk::wire::filter::{FilterCatalogOutcome, FilterCatalogReply, ResolvedFilterPolicy};
use laser_sdk::wire::framing::{decode_named, encode_named};
use laser_sdk::wire::hello::{BackendAnnounce, OpVersions};
use serde::Deserialize;
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::OnceCell;

const TOPIC: &str = "fleet_changes";
const GROUP: &str = "anomaly-desk";
const READ_TIMEOUT: Duration = Duration::from_secs(15);
const PAGES_PER_PARTITION: u32 = 12;

const SAFE_MODE: &str = r#"{"op":"u","table":"satellites","changed":["mode"],"after":{"id":"sat-042","name":"Kestrel-42","mode":"safe","orbit":"leo","battery_pct":61}}"#;
const DECOMMISSION: &str = r#"{"op":"d","table":"satellites","before":{"id":"sat-042"}}"#;
const GROUND_STATION: &str = r#"{"op":"u","table":"ground_stations","changed":["status"],"after":{"id":"svalbard","status":"online"}}"#;

static CATALOG_SERVER: OnceCell<(TestIggy, tempfile::TempDir)> = OnceCell::const_new();

// The fields of the server's forwarded frame this stand-in reads.
#[derive(Deserialize)]
struct ForwardedCommand {
    command_code: u32,
}

fn bound_filter() -> ConsumerFilter {
    ConsumerFilter::json(FilterExpr::any([
        FilterExpr::pred("op", CmpOp::Eq, "d"),
        FilterExpr::pred("changed", CmpOp::Contains, "mode"),
    ]))
}

async fn catalog_server() -> &'static TestIggy {
    let (server, _socket_dir) = CATALOG_SERVER
        .get_or_init(|| async {
            let socket_dir = tempfile::tempdir().expect("socket directory");
            let socket = socket_dir.path().join("plane.sock");
            spawn_catalog(socket.clone());
            let server = TestIggy::start_with(vec![
                ("IGGY_PLANE_ENABLED".to_owned(), "true".to_owned()),
                (
                    "IGGY_PLANE_SOCKET_PATH".to_owned(),
                    socket.display().to_string(),
                ),
            ])
            .await;
            (server, socket_dir)
        })
        .await;
    server
}

fn spawn_catalog(socket: PathBuf) {
    let listener = std::os::unix::net::UnixListener::bind(&socket).expect("bind the catalog");
    listener
        .set_nonblocking(true)
        .expect("nonblocking catalog socket");
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("catalog runtime");
        runtime.block_on(async move {
            let listener = UnixListener::from_std(listener).expect("adopt the catalog socket");
            while let Ok((socket, _)) = listener.accept().await {
                tokio::spawn(serve(socket));
            }
        });
    });
}

async fn serve(mut socket: UnixStream) {
    let policy = ResolvedFilterPolicy {
        filter_id: 1,
        revision: 1,
        digest: bound_filter().digest(),
        state: FilterState::Active,
        filter: bound_filter(),
    };
    loop {
        let mut length = [0u8; 4];
        if socket.read_exact(&mut length).await.is_err() {
            return;
        }
        let mut frame = vec![0u8; u32::from_le_bytes(length) as usize];
        if socket.read_exact(&mut frame).await.is_err() {
            return;
        }
        let Ok(forwarded) = decode_named::<ForwardedCommand>(&frame) else {
            return;
        };
        let reply = match forwarded.command_code {
            AGDX_BACKEND_HELLO_CODE => encode_named(&BackendAnnounce::new(
                OpVersions::new(
                    QUERY_OP_VERSION,
                    CONTROL_OP_VERSION,
                    KV_OP_VERSION,
                    FORK_OP_VERSION,
                )
                .with_filter(FILTER_OP_VERSION),
            )),
            AGDX_RESOLVE_FILTER_POLICY_CODE => encode_named(&FilterCatalogReply::Ok(Box::new(
                FilterCatalogOutcome::Policy(policy.clone()),
            ))),
            _ => return,
        }
        .expect("the reply encodes");
        let written = socket
            .write_all(
                &u32::try_from(reply.len())
                    .expect("small reply")
                    .to_le_bytes(),
            )
            .await;
        if written.is_err() || socket.write_all(&reply).await.is_err() {
            return;
        }
    }
}

async fn group_source(laser: &Laser) -> String {
    let stream = laser.default_stream().expect("stream").to_owned();
    let producer = laser
        .topic(TOPIC)
        .producer()
        .partitions(1)
        .build()
        .await
        .expect("the producer initializes");
    producer
        .send_batch_with_routing(
            [SAFE_MODE, GROUND_STATION, DECOMMISSION]
                .iter()
                .map(|payload| ProducerMessage::new(payload.as_bytes())),
            Some(Routing::Partition(0)),
        )
        .await
        .expect("the fixtures publish");
    laser
        .client()
        .create_consumer_group(
            &Identifier::named(&stream).expect("stream name"),
            &Identifier::named(TOPIC).expect("topic name"),
            GROUP,
        )
        .await
        .expect("the consumer group is created");
    stream
}

#[tokio::test]
async fn given_a_bound_group_when_reading_then_should_run_the_bound_revision_and_store_group_progress()
 {
    let laser = harness::connected_laser_on(catalog_server().await).await;
    assert!(laser.capabilities().await.filters.catalog);
    let stream = group_source(&laser).await;
    let mut reader = laser
        .filters()
        .reader(&stream, TOPIC)
        .group(GROUP)
        .start(FilteredStart::First)
        .build()
        .await
        .expect("the group reader builds");
    assert_eq!(reader.partitions(), vec![0]);

    let page = tokio::time::timeout(READ_TIMEOUT, reader.next_page())
        .await
        .expect("a page arrives")
        .expect("the bound read succeeds");

    let offsets: Vec<u64> = page.records.iter().map(|record| record.offset).collect();
    assert_eq!(offsets, vec![0, 2]);
    assert_eq!(page.policy.filter_id, Some(1));
    assert_eq!(page.policy.revision, Some(1));
    reader.ack_page(&page).await.expect("the page acknowledges");
    reader.close().await.expect("the reader closes");
    let stored = laser
        .client()
        .get_consumer_offset(
            &Consumer::group(Identifier::named(GROUP).expect("group name")),
            &Identifier::named(&stream).expect("stream name"),
            &Identifier::named(TOPIC).expect("topic name"),
            Some(0),
        )
        .await
        .expect("the group offset reads")
        .map(|offset| offset.stored_offset);
    assert_eq!(stored, Some(2));
}

#[tokio::test]
async fn given_a_bound_group_when_a_reader_brings_another_filter_then_should_conflict() {
    let laser = harness::connected_laser_on(catalog_server().await).await;
    let stream = group_source(&laser).await;
    let mut reader = laser
        .filters()
        .reader(&stream, TOPIC)
        .group(GROUP)
        .inline(ConsumerFilter::json(FilterExpr::present("kind")))
        .start(FilteredStart::First)
        .build()
        .await
        .expect("the group reader builds");

    let refused = tokio::time::timeout(READ_TIMEOUT, reader.next_page())
        .await
        .expect("the read answers");

    assert!(matches!(
        refused,
        Err(ref error) if error.filter_reason() == Some(FilterErrorReason::Conflict)
    ));
    reader.close().await.expect("the reader closes");
}

#[tokio::test]
async fn given_lost_membership_when_reading_again_then_should_rejoin_from_committed_progress() {
    let laser = harness::connected_laser_on(catalog_server().await).await;
    let stream = group_source(&laser).await;
    let mut reader = laser
        .filters()
        .reader(&stream, TOPIC)
        .group(GROUP)
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
    let producer = laser
        .topic(TOPIC)
        .producer()
        .partitions(1)
        .build()
        .await
        .expect("producer");
    producer
        .send_batch_with_routing(
            [ProducerMessage::new(DECOMMISSION.as_bytes())],
            Some(Routing::Partition(0)),
        )
        .await
        .expect("new record");
    let next = tokio::time::timeout(READ_TIMEOUT, reader.next_page())
        .await
        .expect("rejoin and read timely")
        .expect("read after rejoin");
    assert_eq!(
        next.records
            .iter()
            .map(|record| record.offset)
            .collect::<Vec<_>>(),
        vec![3]
    );
    assert!(
        reader.ack_page(&page).await.is_err(),
        "old membership pages cannot acknowledge the new reader epoch"
    );
    reader.ack_page(&next).await.expect("new page acknowledges");
    reader.close().await.expect("close");
}

#[tokio::test]
async fn given_two_partitions_on_one_node_when_reading_many_pages_then_should_keep_one_data_connection()
 {
    let laser = harness::connected_laser_on(catalog_server().await).await;
    let stream = group_source(&laser).await;
    let stream_id = Identifier::named(&stream).expect("stream");
    let topic_id = Identifier::named(TOPIC).expect("topic");
    laser
        .client()
        .create_partitions(&stream_id, &topic_id, 1)
        .await
        .expect("second partition");
    let producer = laser
        .topic(TOPIC)
        .producer()
        .partitions(2)
        .build()
        .await
        .expect("producer");
    for partition in 0..2 {
        producer
            .send_batch_with_routing(
                (0..PAGES_PER_PARTITION).map(|_| ProducerMessage::new(SAFE_MODE.as_bytes())),
                Some(Routing::Partition(partition)),
            )
            .await
            .expect("the partition fixtures publish");
    }
    let mut reader = laser
        .filters()
        .reader(&stream, TOPIC)
        .group(GROUP)
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
    assert_eq!(
        reader.data_connections_opened(),
        1,
        "every page and acknowledgment used one connection"
    );
    reader.close().await.expect("the reader closes");
}

#[tokio::test]
async fn given_a_revoked_partition_when_draining_then_should_route_acknowledgments_separately() {
    let server = catalog_server().await;
    let laser = harness::connected_laser_on(server).await;
    let stream = group_source(&laser).await;
    let stream_id = Identifier::named(&stream).expect("stream");
    let topic_id = Identifier::named(TOPIC).expect("topic");
    laser
        .client()
        .create_partitions(&stream_id, &topic_id, 1)
        .await
        .expect("second partition");
    let producer = laser
        .topic(TOPIC)
        .producer()
        .partitions(2)
        .build()
        .await
        .expect("producer");
    producer
        .send_batch_with_routing(
            [ProducerMessage::new(SAFE_MODE.as_bytes())],
            Some(Routing::Partition(1)),
        )
        .await
        .expect("partition one fixture");
    let mut reader = laser
        .filters()
        .reader(&stream, TOPIC)
        .group(GROUP)
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
        .expect("revoked partition can drain through offset routing");
    let stored = peer
        .client()
        .get_consumer_offset(
            &Consumer::group(Identifier::named(GROUP).expect("group")),
            &stream_id,
            &topic_id,
            Some(revoked),
        )
        .await
        .expect("offset reads")
        .expect("offset stored");
    assert_eq!(Some(stored.stored_offset), page.safe_ack_offset);
    reader.close().await.expect("reader closes");
    peer.close().await.expect("peer closes");
}

#[tokio::test]
async fn given_a_recreated_group_when_acknowledging_an_old_page_then_should_not_advance_the_new_group()
 {
    let laser = harness::connected_laser_on(catalog_server().await).await;
    let stream = group_source(&laser).await;
    let stream_id = Identifier::named(&stream).expect("stream");
    let topic_id = Identifier::named(TOPIC).expect("topic");
    let group_id = Identifier::named(GROUP).expect("group");
    let mut reader = laser
        .filters()
        .reader(&stream, TOPIC)
        .group(GROUP)
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
    let stored = laser
        .client()
        .get_consumer_offset(
            &Consumer::group(group_id),
            &stream_id,
            &topic_id,
            Some(page.partition_id),
        )
        .await
        .expect("read new group offset");
    assert!(stored.is_none(), "new group has no acknowledged records");
    let _ = reader.close().await;
}

#[tokio::test]
async fn given_a_numeric_group_when_selecting_a_revision_then_should_keep_the_binding_and_identity()
{
    let laser = harness::connected_laser_on(catalog_server().await).await;
    let stream = group_source(&laser).await;
    let stream_id = Identifier::named(&stream).expect("stream");
    let topic_id = Identifier::named(TOPIC).expect("topic");
    let named = Identifier::named(GROUP).expect("group");
    let group = laser
        .client()
        .get_consumer_group(&stream_id, &topic_id, &named)
        .await
        .expect("group lookup")
        .expect("group");
    let mut reader = laser
        .filters()
        .reader(&stream, TOPIC)
        .group_id(u64::from(group.id))
        .revision(1, 1)
        .start(FilteredStart::First)
        .build()
        .await
        .expect("numeric group");
    let page = tokio::time::timeout(READ_TIMEOUT, reader.next_page())
        .await
        .expect("deadline")
        .expect("page");
    assert_eq!(page.policy.group_id, Some(u64::from(group.id)));
    reader.ack_page(&page).await.expect("ack");
    reader.close().await.expect("close");
    let mut mismatch = laser
        .filters()
        .reader(&stream, TOPIC)
        .group_id(u64::from(group.id))
        .revision(1, 2)
        .build()
        .await
        .expect("reader");
    let error = tokio::time::timeout(READ_TIMEOUT, mismatch.next_page())
        .await
        .expect("deadline")
        .expect_err("another revision is refused");
    assert_eq!(error.filter_reason(), Some(FilterErrorReason::Conflict));
    mismatch.close().await.expect("close");
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
        laser
            .filters()
            .reader(&stream, TOPIC)
            .group_id(u64::from(group.id))
            .build()
            .await
            .is_err(),
        "old id must not join the replacement group"
    );
}
