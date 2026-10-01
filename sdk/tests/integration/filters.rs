// Consumer filters against the fork with its catalog off: inline filtered
// reads through the partition primary, fenced acknowledgments, fault stops,
// previews, sample tests, and the typed refusals of what is not served.

use crate::harness;
use laser_sdk::filters::{
    ConsumerFilter, FaultReason, FilterExpr, FilterRef, FilteredStart, ReadMode, Verdict,
};
use laser_sdk::iggy::prelude::{
    Consumer, ConsumerOffsetClient, HeaderKey, HeaderValue, Identifier, MessageClient,
    PollingStrategy,
};
use laser_sdk::prelude::full::{Capabilities, Laser, LaserError};
use laser_sdk::prelude::{ProducerMessage, Routing};
use laser_sdk::query::CmpOp;
use std::time::Duration;

const TOPIC: &str = "fleet_changes";
const READ_TIMEOUT: Duration = Duration::from_secs(15);

const SAFE_MODE: &str = r#"{"op":"u","table":"satellites","changed":["mode"],"after":{"id":"sat-042","name":"Kestrel-42","mode":"safe","orbit":"leo","battery_pct":61}}"#;
const SAFE_MODE_VALUES: &str = r#"{"op":"u","table":"satellites","after":{"id":"sat-042","name":"Kestrel-42","mode":"safe","orbit":"leo","battery_pct":61}}"#;
const DECOMMISSION: &str = r#"{"op":"d","table":"satellites","before":{"id":"sat-042"}}"#;
const TELEMETRY: &str =
    r#"{"event":"satellite.telemetry_changed","satellite_id":"sat-042","fields":{"mode":"safe"}}"#;
const GROUND_STATION: &str = r#"{"op":"u","table":"ground_stations","changed":["status"],"after":{"id":"svalbard","status":"online"}}"#;

fn safe_mode_filter() -> ConsumerFilter {
    let satellites = || FilterExpr::pred("table", CmpOp::Eq, "satellites");
    ConsumerFilter::json(FilterExpr::any([
        FilterExpr::all([
            satellites(),
            FilterExpr::pred("op", CmpOp::Eq, "u"),
            FilterExpr::pred("changed", CmpOp::Contains, "mode"),
            FilterExpr::pred("after.mode", CmpOp::Eq, "safe"),
        ]),
        FilterExpr::all([satellites(), FilterExpr::pred("op", CmpOp::Eq, "d")]),
        FilterExpr::all([
            FilterExpr::pred("event", CmpOp::Eq, "satellite.telemetry_changed"),
            FilterExpr::present("fields.mode"),
        ]),
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
    let stream = Identifier::named(laser.default_stream().expect("stream")).expect("stream id");
    let topic = Identifier::named(TOPIC).expect("topic id");
    harness::eventually(|| async {
        let visible = laser
            .client()
            .poll_messages(
                &stream,
                &topic,
                Some(0),
                &Consumer::default(),
                &PollingStrategy::first(),
                u32::try_from(payloads.len()).expect("fixture count"),
                false,
            )
            .await
            .expect("fixture visibility poll");
        (visible.messages.len() == payloads.len()).then_some(())
    })
    .await;
}

async fn stored_offset(laser: &Laser, consumer: &str) -> Option<u64> {
    let stream = laser
        .default_stream()
        .expect("the test laser names its stream");
    laser
        .client()
        .get_consumer_offset(
            &Consumer::new(Identifier::named(consumer).expect("consumer name")),
            &Identifier::named(stream).expect("stream name"),
            &Identifier::named(TOPIC).expect("topic name"),
            Some(0),
        )
        .await
        .expect("the offset reads")
        .map(|offset| offset.stored_offset)
}

fn offsets(page: &laser_sdk::filters::MatchedPage) -> Vec<u64> {
    page.records.iter().map(|record| record.offset).collect()
}

#[tokio::test]
async fn given_edge_records_when_reading_an_inline_filter_then_should_return_only_matches_at_their_offsets()
 {
    let laser = harness::connected_laser().await;
    publish(
        &laser,
        &[
            SAFE_MODE,
            SAFE_MODE_VALUES,
            DECOMMISSION,
            TELEMETRY,
            GROUND_STATION,
        ],
    )
    .await;
    let stream = laser.default_stream().expect("stream").to_owned();
    let mut reader = laser
        .filters()
        .reader(&stream, TOPIC)
        .consumer("safe-mode-backfill")
        .inline(safe_mode_filter())
        .start(FilteredStart::First)
        .build()
        .await
        .expect("the reader builds");

    let page = tokio::time::timeout(READ_TIMEOUT, reader.next_page())
        .await
        .expect("a page arrives")
        .expect("the read succeeds");

    assert_eq!(offsets(&page), vec![0, 2, 3]);
    assert_eq!(
        page.records[1].message.payload.as_ref(),
        DECOMMISSION.as_bytes()
    );
    assert_eq!(page.policy.digest, safe_mode_filter().digest());
    reader.ack_page(&page).await.expect("the page acknowledges");
    reader.close().await.expect("the reader closes");
    assert_eq!(
        stored_offset(&laser, "safe-mode-backfill").await,
        Some(4),
        "the safe offset covers the trailing non-match"
    );
}

#[tokio::test]
async fn given_stored_progress_when_a_new_reader_starts_next_then_should_read_only_new_matches() {
    let laser = harness::connected_laser().await;
    publish(&laser, &[SAFE_MODE, GROUND_STATION]).await;
    let stream = laser.default_stream().expect("stream").to_owned();
    let reader = || {
        laser
            .filters()
            .reader(&stream, TOPIC)
            .consumer("safe-mode-resume")
            .inline(safe_mode_filter())
    };
    let mut first = reader()
        .start(FilteredStart::First)
        .build()
        .await
        .expect("the first reader builds");
    let page = tokio::time::timeout(READ_TIMEOUT, first.next_page())
        .await
        .expect("a page arrives")
        .expect("the read succeeds");
    first.ack_page(&page).await.expect("the page acknowledges");
    first.close().await.expect("the first reader closes");

    publish(&laser, &[GROUND_STATION, DECOMMISSION]).await;
    let mut second = reader().build().await.expect("the second reader builds");
    let page = tokio::time::timeout(READ_TIMEOUT, second.next_page())
        .await
        .expect("a page arrives")
        .expect("the read succeeds");

    assert_eq!(offsets(&page), vec![3]);
    second.close().await.expect("the second reader closes");
}

#[tokio::test]
async fn given_only_non_matching_records_when_reading_then_should_store_the_scanned_range() {
    let laser = harness::connected_laser().await;
    publish(&laser, &[GROUND_STATION, GROUND_STATION, GROUND_STATION]).await;
    let stream = laser.default_stream().expect("stream").to_owned();
    let mut reader = laser
        .filters()
        .reader(&stream, TOPIC)
        .consumer("safe-mode-sparse")
        .inline(safe_mode_filter())
        .start(FilteredStart::First)
        .build()
        .await
        .expect("the reader builds");

    let page = reader.try_next_page().await.expect("the round succeeds");

    assert!(page.is_none(), "nothing matched");
    assert_eq!(
        stored_offset(&laser, "safe-mode-sparse").await,
        Some(2),
        "an empty page still stores the range it examined"
    );
    reader.close().await.expect("the reader closes");
}

#[tokio::test]
async fn given_out_of_order_acks_when_acknowledging_then_should_store_only_the_completed_prefix() {
    let laser = harness::connected_laser().await;
    publish(&laser, &[SAFE_MODE, DECOMMISSION, TELEMETRY]).await;
    let stream = laser.default_stream().expect("stream").to_owned();
    let mut reader = laser
        .filters()
        .reader(&stream, TOPIC)
        .consumer("safe-mode-ordered")
        .inline(safe_mode_filter())
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

    reader
        .ack(&second)
        .await
        .expect("the later record acknowledges");
    assert_eq!(
        stored_offset(&laser, "safe-mode-ordered").await,
        None,
        "the earlier record is still open"
    );
    reader
        .ack(&first)
        .await
        .expect("the earlier record acknowledges");

    assert_eq!(stored_offset(&laser, "safe-mode-ordered").await, Some(1));
    reader.close().await.expect("the reader closes");
}

#[tokio::test]
async fn given_a_malformed_record_under_stop_when_reading_then_should_deliver_earlier_matches_then_the_fault()
 {
    let laser = harness::connected_laser().await;
    publish(&laser, &[SAFE_MODE, "not json", DECOMMISSION]).await;
    let stream = laser.default_stream().expect("stream").to_owned();
    let mut reader = laser
        .filters()
        .reader(&stream, TOPIC)
        .consumer("safe-mode-fault")
        .inline(safe_mode_filter())
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
        stored_offset(&laser, "safe-mode-fault").await,
        Some(0),
        "the fault stays unacknowledged"
    );
    reader.close().await.expect("the reader closes");
}

#[tokio::test]
async fn given_a_local_guard_when_reading_then_should_agree_with_the_server() {
    let laser = harness::connected_laser().await;
    publish(&laser, &[GROUND_STATION, TELEMETRY]).await;
    let stream = laser.default_stream().expect("stream").to_owned();
    let mut reader = laser
        .filters()
        .reader(&stream, TOPIC)
        .consumer("safe-mode-guarded")
        .inline(safe_mode_filter())
        .start(FilteredStart::First)
        .local_guard(true)
        .build()
        .await
        .expect("the reader builds");

    let page = tokio::time::timeout(READ_TIMEOUT, reader.next_page())
        .await
        .expect("a page arrives")
        .expect("the local evaluation agrees");

    assert_eq!(offsets(&page), vec![1]);
    reader.close().await.expect("the reader closes");
}

#[tokio::test]
async fn given_a_local_reader_when_acknowledging_then_should_refuse() {
    let laser = harness::connected_laser().await;
    publish(&laser, &[DECOMMISSION]).await;
    let stream = laser.default_stream().expect("stream").to_owned();
    let mut reader = laser
        .filters()
        .reader(&stream, TOPIC)
        .consumer("safe-mode-local")
        .inline(safe_mode_filter())
        .start(FilteredStart::First)
        .read_mode(ReadMode::Local)
        .build()
        .await
        .expect("the reader builds");
    let page = tokio::time::timeout(READ_TIMEOUT, reader.next_page())
        .await
        .expect("a page arrives")
        .expect("a local read succeeds");

    let refused = reader.ack_page(&page).await;

    assert!(matches!(refused, Err(LaserError::Invalid(_))));
    assert_eq!(stored_offset(&laser, "safe-mode-local").await, None);
}

#[tokio::test]
async fn given_stored_records_when_previewed_then_should_judge_them_without_storing_progress() {
    let laser = harness::connected_laser().await;
    publish(&laser, &[SAFE_MODE, SAFE_MODE_VALUES, DECOMMISSION]).await;
    let stream = laser.default_stream().expect("stream").to_owned();

    let preview = laser
        .filters()
        .preview(&stream, TOPIC, 0, FilterRef::Inline(safe_mode_filter()))
        .max_records(10)
        .explain(true)
        .send()
        .await
        .expect("the preview runs");

    assert_eq!(preview.examined, 3);
    assert_eq!(preview.matched, 2);
    let verdicts: Vec<(u64, Verdict)> = preview
        .records
        .iter()
        .map(|record| (record.offset, record.verdict))
        .collect();
    assert!(verdicts.contains(&(1, Verdict::Rejected)));
    assert!(verdicts.contains(&(2, Verdict::Selected)));
}

#[tokio::test]
async fn given_a_sample_when_tested_and_validated_then_should_explain_and_digest() {
    let laser = harness::connected_laser().await;

    let tested = laser
        .filters()
        .test(
            FilterRef::Inline(safe_mode_filter()),
            SAFE_MODE_VALUES,
            Vec::new(),
        )
        .await
        .expect("the sample test runs");
    let validation = laser
        .filters()
        .validate(&safe_mode_filter())
        .await
        .expect("the filter validates");

    assert_eq!(tested.explanation.verdict, Verdict::Rejected);
    assert_eq!(validation.digest, safe_mode_filter().digest());
    assert!(validation.reads_payload);
}

#[tokio::test]
async fn given_no_catalog_when_listing_saved_filters_then_should_refuse_as_unsupported() {
    let laser = harness::connected_laser().await;
    assert!(laser.capabilities().await.filters.native);

    let refused = laser.filters().list().send().await;

    assert!(matches!(
        refused,
        Err(LaserError::Unsupported {
            surface: "filters",
            feature: Some("catalog"),
            ..
        })
    ));
}

#[tokio::test]
async fn given_a_server_without_consumer_filters_when_building_a_reader_then_should_refuse() {
    let open = Laser::from_client(harness::client().await);
    assert_eq!(open.capabilities().await, Capabilities::OPEN);

    let refused = open
        .filters()
        .reader("any", TOPIC)
        .consumer("any")
        .inline(safe_mode_filter())
        .build()
        .await;

    assert!(refused.err().is_some_and(|error| error.is_unsupported()));
}

#[tokio::test]
async fn given_two_readers_when_acknowledging_another_readers_page_then_should_reject_without_progress()
 {
    let laser = harness::connected_laser().await;
    publish(&laser, &[SAFE_MODE]).await;
    let stream = laser.default_stream().expect("stream");
    let mut first = laser
        .filters()
        .reader(stream, TOPIC)
        .consumer("owner-one")
        .inline(safe_mode_filter())
        .start(FilteredStart::First)
        .build()
        .await
        .expect("first reader");
    let mut second = laser
        .filters()
        .reader(stream, TOPIC)
        .consumer("owner-two")
        .inline(safe_mode_filter())
        .start(FilteredStart::First)
        .build()
        .await
        .expect("second reader");
    let first_page = tokio::time::timeout(READ_TIMEOUT, first.next_page())
        .await
        .expect("first page timely")
        .expect("first page");
    let second_page = tokio::time::timeout(READ_TIMEOUT, second.next_page())
        .await
        .expect("second page timely")
        .expect("second page");
    assert!(matches!(
        second.ack_page(&first_page).await,
        Err(LaserError::Invalid(_))
    ));
    assert!(matches!(
        second.ack(&first_page.records[0]).await,
        Err(LaserError::Invalid(_))
    ));
    assert_eq!(stored_offset(&laser, "owner-two").await, None);
    second
        .ack_page(&second_page)
        .await
        .expect("own page acknowledges");
    assert_eq!(stored_offset(&laser, "owner-two").await, Some(0));
    first.close().await.expect("first closes");
    second.close().await.expect("second closes");
}

#[tokio::test]
async fn given_typed_custom_headers_when_filtered_then_should_preserve_types_and_payload() {
    let laser = harness::connected_laser().await;
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
    let filter = ConsumerFilter::headers_only(FilterExpr::all([
        FilterExpr::header("routing.priority", CmpOp::Eq, 2_i32),
        FilterExpr::header("armed", CmpOp::Eq, true),
        FilterExpr::header("temperature", CmpOp::Lt, 0_i32),
        FilterExpr::header("sequence", CmpOp::Gt, i64::MAX),
    ]));
    let mut reader = laser
        .filters()
        .reader(laser.default_stream().expect("stream"), TOPIC)
        .consumer("typed-headers")
        .inline(filter)
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

#[tokio::test]
async fn given_a_fresh_filtered_reader_when_reading_next_then_should_start_at_zero() {
    let laser = harness::connected_laser().await;
    publish(&laser, &[SAFE_MODE, DECOMMISSION]).await;
    let stream = laser.default_stream().expect("stream").to_owned();
    let mut first = laser
        .filters()
        .reader(&stream, TOPIC)
        .consumer("fresh-next")
        .inline(safe_mode_filter())
        .count(1)
        .build()
        .await
        .expect("reader");
    let record = tokio::time::timeout(READ_TIMEOUT, first.next_record())
        .await
        .expect("deadline")
        .expect("first record");
    assert_eq!(record.offset, 0);
    first.ack(&record).await.expect("ack zero");
    first.close().await.expect("close");
    let mut resumed = laser
        .filters()
        .reader(&stream, TOPIC)
        .consumer("fresh-next")
        .inline(safe_mode_filter())
        .count(1)
        .build()
        .await
        .expect("reader");
    assert_eq!(
        tokio::time::timeout(READ_TIMEOUT, resumed.next_record())
            .await
            .expect("deadline")
            .expect("next after zero")
            .offset,
        1
    );
    resumed.close().await.expect("close");
}

#[tokio::test]
async fn given_pass_through_decode_faults_when_guarded_then_should_verify_the_server_bounds() {
    let laser = harness::connected_laser().await;
    publish(&laser, &["broken JSON", SAFE_MODE]).await;
    let stream = laser.default_stream().expect("stream").to_owned();
    let mut reader = laser
        .filters()
        .reader(&stream, TOPIC)
        .consumer("guarded-pass")
        .inline(safe_mode_filter().with_fault_policy(laser_sdk::filters::FaultPolicy::Pass))
        .start(FilteredStart::First)
        .local_guard(true)
        .count(2)
        .build()
        .await
        .expect("reader");
    let page = tokio::time::timeout(READ_TIMEOUT, reader.next_page())
        .await
        .expect("deadline")
        .expect("verified page");
    assert_eq!(offsets(&page), vec![0, 1]);
    assert!(!page.records[0].evaluated);
    assert!(page.records[1].evaluated);
    reader
        .ack_page(&page)
        .await
        .expect("acknowledge processed records");
    reader.close().await.expect("close");
}

#[tokio::test]
async fn given_more_than_64_outstanding_pages_when_configured_then_should_read_and_ack_the_whole_prefix()
 {
    let laser = harness::connected_laser().await;
    publish(&laser, &[SAFE_MODE; 80]).await;
    let mut reader = laser
        .filters()
        .reader(laser.default_stream().expect("stream"), TOPIC)
        .consumer("large-window")
        .inline(safe_mode_filter())
        .count(1)
        .max_unacked_pages(80)
        .build()
        .await
        .expect("reader");
    let mut pages = Vec::new();
    for offset in 0..80 {
        let page = tokio::time::timeout(READ_TIMEOUT, reader.next_page())
            .await
            .expect("read deadline")
            .expect("page");
        assert_eq!(
            page.records
                .iter()
                .map(|record| record.offset)
                .collect::<Vec<_>>(),
            vec![offset]
        );
        pages.push(page);
    }
    reader
        .ack_through(&pages.last().expect("page").records[0])
        .await
        .expect("prefix acknowledgment");
    reader.close().await.expect("close");
    laser.close().await.expect("close laser");
}
