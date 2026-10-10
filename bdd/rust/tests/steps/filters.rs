use crate::common::test_iggy::fresh_connected_laser;
use crate::common::world::LaserWorld;
use cucumber::{given, then, when};
use laser_bdd::filter_fixtures::{feed, filter, record};
use laser_sdk::filters::{FaultPolicy, FilterHeader, FilteredStart, HeaderScalar, Verdict};
use laser_sdk::iggy::prelude::{Consumer, ConsumerOffsetClient, Identifier};
use laser_sdk::prelude::{CommitPolicy, ConsumerStart, ProducerMessage, Routing};
use laser_sdk::types::MintUlid;
use laser_sdk::wire::filter::ConsumerFilter;
use laser_sdk::wire::filter::eval::{CompiledFilter, DecodeLimits, FilterRecord, HeaderRef};
use laser_sdk::wire::filter::{ExecutionMode, GroupFilterSpec};
use std::time::Duration;

const TOPIC: &str = "fleet_changes";
const READ_TIMEOUT: Duration = Duration::from_secs(15);

fn evaluate(world: &mut LaserWorld, payload: &[u8], headers: &[FilterHeader]) {
    let compiled = CompiledFilter::compile(world.filter.as_ref().expect("a filter was chosen"))
        .expect("the filter compiles");
    let headers: Vec<HeaderRef<'_>> = headers.iter().map(HeaderRef::from).collect();
    world.verdict = Some(compiled.evaluate(
        &FilterRecord {
            payload,
            headers: &headers,
        },
        &DecodeLimits::default(),
    ));
}

#[given(regex = r#"^the "([^"]+)" filter$"#)]
async fn given_filter(world: &mut LaserWorld, name: String) {
    world.filter = Some(filter(&name));
}

#[when(regex = r#"^it evaluates the "([^"]+)" record$"#)]
async fn evaluate_record(world: &mut LaserWorld, name: String) {
    evaluate(world, record(&name), &[]);
}

#[when(regex = r#"^it evaluates the "([^"]+)" record with header "([^"]+)" set to "([^"]+)"$"#)]
async fn evaluate_record_with_header(
    world: &mut LaserWorld,
    name: String,
    key: String,
    value: String,
) {
    let headers = [FilterHeader {
        key,
        value: HeaderScalar::String(value),
    }];
    evaluate(world, record(&name), &headers);
}

#[then(regex = r"^the record is (selected|rejected|a fault)$")]
async fn then_verdict(world: &mut LaserWorld, verdict: String) {
    let expected = match verdict.as_str() {
        "selected" => Verdict::Selected,
        "rejected" => Verdict::Rejected,
        _ => Verdict::Fault,
    };
    assert_eq!(world.verdict, Some(expected));
}

#[then("its digest survives a round trip through its wire form")]
async fn digest_round_trip(world: &mut LaserWorld) {
    let filter = world.filter.as_ref().expect("a filter was chosen");
    let json = serde_json::to_string(filter).expect("the filter encodes");
    let back: ConsumerFilter = serde_json::from_str(&json).expect("the filter decodes");
    assert_eq!(back.digest(), filter.digest());
}

#[then("a different fault policy gives a different digest")]
async fn fault_policy_digest(world: &mut LaserWorld) {
    let filter = world.filter.clone().expect("a filter was chosen");
    let dropping = filter.clone().with_fault_policy(FaultPolicy::Drop);
    assert_ne!(dropping.digest(), filter.digest());
}

#[given("a fresh fleet change feed")]
async fn fresh_feed(world: &mut LaserWorld) {
    let fresh = fresh_connected_laser().await;
    let producer = fresh
        .laser
        .topic(TOPIC)
        .producer()
        .partitions(1)
        .build()
        .await
        .expect("the producer initializes");
    producer
        .send_batch_with_routing(
            feed()
                .iter()
                .map(|name| ProducerMessage::new(record(name).to_vec())),
            Some(Routing::Partition(0)),
        )
        .await
        .expect("the feed publishes");
    world.platform = fresh.iggy;
    world.laser = Some(fresh.laser);
}

#[when(regex = r#"^the anomaly desk reads the feed with the "([^"]+)" filter$"#)]
async fn read_feed(world: &mut LaserWorld, name: String) {
    let laser = world.laser().clone();
    let group = laser.topic(TOPIC).consumer_group("anomaly-desk");
    group
        .create()
        .filter(filter(&name))
        .build()
        .await
        .expect("group policy configured");
    let mut reader = group
        .reader()
        .expect("group source")
        .start(FilteredStart::First)
        .build()
        .await
        .expect("reader builds");
    let page = tokio::time::timeout(READ_TIMEOUT, reader.next_page())
        .await
        .expect("a page arrives")
        .expect("the read succeeds");
    world.filtered_payloads = page
        .records
        .iter()
        .map(|matched| matched.message.payload.to_vec())
        .collect();
    reader.ack_page(&page).await.expect("the page acknowledges");
    reader.close().await.expect("the reader closes");
}

#[then(regex = r#"^it receives the "([^"]+)", "([^"]+)", and "([^"]+)" records$"#)]
async fn receives(world: &mut LaserWorld, first: String, second: String, third: String) {
    let expected: Vec<Vec<u8>> = [first, second, third]
        .iter()
        .map(|name| record(name).to_vec())
        .collect();
    assert_eq!(world.filtered_payloads, expected);
}

#[when("the anomaly desk lists its saved filters")]
async fn list_filters(world: &mut LaserWorld) {
    let result = world
        .laser()
        .topic(TOPIC)
        .consumer_group("anomaly-desk")
        .filter()
        .revisions(0, 10)
        .await;
    world.last_result = Some(result.map(|_| ()).map_err(|error| {
        if error.is_unsupported() {
            "unsupported".to_owned()
        } else {
            format!("{error:?}")
        }
    }));
}

#[then("the catalog is refused as unsupported")]
async fn catalog_unsupported(world: &mut LaserWorld) {
    assert_eq!(world.last_result, Some(Err("unsupported".to_owned())));
}

#[when("the anomaly desk manages a saved policy by numeric group id")]
async fn managed_group(world: &mut LaserWorld) {
    let laser = world.laser().clone();
    let named = laser.topic(TOPIC).consumer_group("managed-desk");
    let operation_id = laser_sdk::wire::agent::RecordId::mint().as_u128();
    let first = named
        .create()
        .filter(filter("safe mode"))
        .operation_id(operation_id)
        .build()
        .await
        .expect("configure group");
    let retry = named
        .create()
        .filter(filter("safe mode"))
        .operation_id(operation_id)
        .build()
        .await
        .expect("retry original configuration");
    assert_eq!(retry, first);
    let active = first.filter.expect("group is bound");
    let group = laser.topic(TOPIC).consumer_group_id(u64::from(first.id));
    let same = group
        .filter()
        .configure_as(
            Some(operation_id),
            GroupFilterSpec::Definition(filter("safe mode")),
        )
        .await
        .expect("repeat saved operation");
    assert_eq!(same, active);
    let mut reader = group
        .reader()
        .expect("group source")
        .build()
        .await
        .expect("numeric group");
    let page = tokio::time::timeout(READ_TIMEOUT, reader.next_page())
        .await
        .expect("read timeout")
        .expect("read");
    group
        .filter()
        .set_revision_enabled(active.revision, false)
        .await
        .expect("pause");
    reader
        .ack_page(&page)
        .await
        .expect("acknowledge already delivered work while paused");
    let refusal = reader
        .try_next_page()
        .await
        .expect_err("paused revision refuses new work");
    assert!(refusal.to_string().contains("revision_disabled"));
    group
        .filter()
        .set_revision_enabled(active.revision, true)
        .await
        .expect("resume");
    reader.close().await.expect("close");
    world.filtered_payloads = page
        .records
        .iter()
        .map(|record| record.message.payload.to_vec())
        .collect();
    group.filter().release().await.expect("release binding");
}

#[when("the anomaly desk reads every record through an unbound consumer group")]
async fn unbound_consumer(world: &mut LaserWorld) {
    let group = world.laser().topic(TOPIC).consumer_group("unbound-desk");
    let mut consumer = group
        .consumer()
        .start_at(ConsumerStart::First)
        .commit_policy(CommitPolicy::Disabled)
        .build()
        .await
        .expect("unbound consumer");
    world.filtered_payloads.clear();
    for _ in feed() {
        let message = consumer
            .next_within(READ_TIMEOUT)
            .await
            .expect("original record");
        world.filtered_payloads.push(message.payload.to_vec());
        consumer
            .commit(&message)
            .await
            .expect("acknowledge original record");
    }
    consumer.shutdown().await.expect("close");
}

#[when("the anomaly desk reads every record through an unbound group reader")]
async fn unbound_reader(world: &mut LaserWorld) {
    let group = world.laser().topic(TOPIC).consumer_group("unbound-desk");
    group.create().build().await.expect("plain group");
    let reader = group
        .reader()
        .expect("group source")
        .start(FilteredStart::First)
        .count(100)
        .max_examined(100)
        .build()
        .await;
    if !world
        .laser()
        .capabilities()
        .await
        .filters
        .group_policy_reads
    {
        let error = reader.err().expect("group-aware reads must be refused");
        assert!(matches!(
            error,
            laser_sdk::LaserError::Unsupported {
                surface: "filters",
                feature: Some("group_policy_reads"),
                ..
            }
        ));
        world.last_result = Some(Err("unsupported".to_owned()));
        return;
    }
    let mut reader = reader.expect("unbound reader");
    world.filtered_payloads.clear();
    while world.filtered_payloads.len() < feed().len() {
        let page = tokio::time::timeout(READ_TIMEOUT, reader.next_page())
            .await
            .expect("page deadline")
            .expect("unbound page");
        assert_eq!(page.policy.mode, ExecutionMode::Unfiltered);
        assert!(page.records.iter().all(|record| !record.evaluated));
        world.filtered_payloads.extend(
            page.records
                .iter()
                .map(|record| record.message.payload.to_vec()),
        );
        reader
            .ack_page(&page)
            .await
            .expect("acknowledge original records");
    }
    reader.close().await.expect("close");
}

#[then("the advanced reader follows the group-aware read capability")]
async fn advanced_reader_capability(world: &mut LaserWorld) {
    if world
        .laser()
        .capabilities()
        .await
        .filters
        .group_policy_reads
    {
        assert_eq!(
            world.filtered_payloads,
            feed()
                .iter()
                .map(|name| record(name).to_vec())
                .collect::<Vec<_>>()
        );
    } else {
        assert_eq!(world.last_result, Some(Err("unsupported".to_owned())));
    }
}

#[then("it receives every original feed record")]
async fn every_original_record(world: &mut LaserWorld) {
    let expected: Vec<_> = feed().iter().map(|name| record(name).to_vec()).collect();
    assert_eq!(world.filtered_payloads, expected);
}

#[when("the anomaly desk scans a hundred non-matches before its first match")]
async fn hundred_non_matches(world: &mut LaserWorld) {
    let topic = world.laser().topic("sparse_changes");
    let producer = topic
        .producer()
        .partitions(1)
        .build()
        .await
        .expect("producer");
    let messages = (0..100)
        .map(|_| ProducerMessage::new(record("ground station update").to_vec()))
        .chain([ProducerMessage::new(record("mode update").to_vec())]);
    producer
        .send_batch_with_routing(messages, Some(Routing::Partition(0)))
        .await
        .expect("sparse feed publishes");
    let group = topic.consumer_group("sparse-desk");
    group
        .create()
        .filter(filter("safe mode"))
        .build()
        .await
        .expect("configured group");
    let mut reader = group
        .reader()
        .expect("group source")
        .start(FilteredStart::First)
        .count(1)
        .max_examined(100)
        .build()
        .await
        .expect("bounded reader");
    let (empty, more) = reader.read_round().await.expect("first bounded scan");
    assert!(empty.is_none() && more);
    assert_eq!(reader.examined_in_round(), 100);
    let page = tokio::time::timeout(READ_TIMEOUT, reader.next_page())
        .await
        .expect("next slice deadline")
        .expect("matching page");
    assert_eq!(page.examined, 1);
    assert_eq!(page.records[0].offset, 100);
    world.filtered_payloads = page
        .records
        .iter()
        .map(|record| record.message.payload.to_vec())
        .collect();
    reader
        .ack_page(&page)
        .await
        .expect("scanned prefix acknowledges");
    reader.close().await.expect("close");
    world.last_batch_count = Some(100);
}

#[then("its first match follows an empty scan of one hundred records")]
async fn match_after_empty_scan(world: &mut LaserWorld) {
    assert_eq!(world.last_batch_count, Some(100));
    assert_eq!(
        world.filtered_payloads,
        vec![record("mode update").to_vec()]
    );
}

#[when("the anomaly desk consumes a hundred non-matches before its first match")]
async fn hundred_non_matches_consumer(world: &mut LaserWorld) {
    let laser = world.laser().clone();
    let topic = laser.topic("sparse_changes");
    let producer = topic
        .producer()
        .partitions(1)
        .build()
        .await
        .expect("producer");
    let messages = (0..100)
        .map(|_| ProducerMessage::new(record("ground station update").to_vec()))
        .chain([ProducerMessage::new(record("mode update").to_vec())]);
    producer
        .send_batch_with_routing(messages, Some(Routing::Partition(0)))
        .await
        .expect("sparse feed publishes");
    let group = topic.consumer_group("normal-sparse-desk");
    group
        .create()
        .filter(filter("safe mode"))
        .build()
        .await
        .expect("configured group");
    let mut consumer = group
        .consumer()
        .start_at(ConsumerStart::First)
        .batch_length(100)
        .commit_policy(CommitPolicy::Disabled)
        .build()
        .await
        .expect("normal group consumer");
    let message = consumer
        .next_within(READ_TIMEOUT)
        .await
        .expect("first matching record");
    assert_eq!(message.position.offset, 100);
    let stored = laser
        .client()
        .get_consumer_offset(
            &Consumer::group(Identifier::named("normal-sparse-desk").expect("group")),
            &Identifier::named(laser.default_stream().expect("stream")).expect("stream id"),
            &Identifier::named("sparse_changes").expect("topic"),
            Some(0),
        )
        .await
        .expect("group offset")
        .expect("empty slice made safe progress");
    assert_eq!(stored.stored_offset, 99);
    world.filtered_payloads = vec![message.payload.to_vec()];
    world.last_batch_count = Some(100);
    consumer
        .commit(&message)
        .await
        .expect("acknowledge matching work");
    consumer.shutdown().await.expect("close");
}
