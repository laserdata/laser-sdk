use crate::common::test_iggy::fresh_connected_laser;
use crate::common::world::LaserWorld;
use cucumber::{given, then, when};
use laser_bdd::filter_fixtures::{feed, filter, record};
use laser_sdk::filters::{FaultPolicy, FilterHeader, FilteredStart, HeaderScalar, Verdict};
use laser_sdk::prelude::{ProducerMessage, Routing};
use laser_sdk::types::MintUlid;
use laser_sdk::wire::filter::ConsumerFilter;
use laser_sdk::wire::filter::eval::{CompiledFilter, DecodeLimits, FilterRecord, HeaderRef};
use laser_sdk::wire::filter::{FilterGroupRef, FilterMutation, FilterMutationResult};
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
    let stream = laser
        .default_stream()
        .expect("the feed has a stream")
        .to_owned();
    let mut reader = laser
        .filters()
        .reader(&stream, TOPIC)
        .consumer("anomaly-desk")
        .inline(filter(&name))
        .start(FilteredStart::First)
        .build()
        .await
        .expect("the reader builds");
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
    let result = world.laser().filters().list().send().await;
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
    let stream = laser.default_stream().expect("stream").to_owned();
    let filters = laser.filters();
    let operation_id = laser_sdk::wire::agent::RecordId::mint().as_u128();
    let mutation = FilterMutation::Register {
        name: format!("bdd-{stream}"),
        description: String::new(),
        filter: filter("safe mode"),
    };
    let first = filters
        .apply_as(operation_id, mutation.clone())
        .await
        .expect("register");
    assert_eq!(
        filters
            .apply_as(operation_id, mutation)
            .await
            .expect("retry"),
        first
    );
    let FilterMutationResult::Registered(saved) = first else {
        panic!("registration result");
    };
    let binding = filters
        .create_consumer_group(
            FilterGroupRef {
                stream: stream.clone(),
                topic: TOPIC.to_owned(),
                group: "managed-desk".to_owned(),
            },
            saved.filter_id,
            saved.revision,
        )
        .await
        .expect("create and bind");
    let mut reader = filters
        .reader(&stream, TOPIC)
        .group_id(binding.identity.group_id)
        .build()
        .await
        .expect("numeric group");
    let page = tokio::time::timeout(READ_TIMEOUT, reader.next_page())
        .await
        .expect("read timeout")
        .expect("read");
    filters
        .set_revision_enabled(saved.filter_id, saved.revision, false)
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
    filters
        .set_revision_enabled(saved.filter_id, saved.revision, true)
        .await
        .expect("resume");
    reader.close().await.expect("close");
    world.filtered_payloads = page
        .records
        .iter()
        .map(|record| record.message.payload.to_vec())
        .collect();
    filters
        .unbind_binding(binding)
        .await
        .expect("release binding");
    filters.archive(saved.filter_id).await.expect("archive");
    filters.delete(saved.filter_id).await.expect("drop");
}
