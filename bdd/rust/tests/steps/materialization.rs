use crate::common::world::LaserWorld;
use cucumber::{given, then, when};
use laser_sdk::materialize::{MaterializationReadiness, MaterializationWait};
use laser_sdk::wire::browse::{BindingStatus, FailureClass, PartitionProgress, RealizationState};
use laser_sdk::wire::control::{OversizePolicy, Projection, ProjectionKey, SourceSelector};
use laser_sdk::wire::schema::ORIGINAL_PAYLOAD_FIELD_NAME;
use std::time::Duration;

#[given(regex = r"^a realized binding with captured head (\d+) and applied offset (\d+)$")]
async fn captured(world: &mut LaserWorld, head: u64, applied: u64) {
    let status = BindingStatus {
        history: Default::default(),
        source: SourceSelector::new("shop", "orders"),
        index: "orders".to_owned(),
        realization: RealizationState::Realized,
        realization_reason: None,
        failure: None,
        stall: None,
        source_incarnation: Some(7),
        partitions: vec![PartitionProgress {
            partition_id: 0,
            head_offset: Some(head),
            applied_offset: Some(applied),
            first_offset: Some(0),
        }],
        holds: Vec::new(),
        accepted_gaps: Vec::new(),
        skipped_records: 0,
    };
    world.materialization_target = Some(status.clone());
    world.materialization_status = Some(status);
}
#[when(regex = r"^the binding advances to offset (\d+) while its source head grows to (\d+)$")]
async fn advances(world: &mut LaserWorld, applied: u64, head: u64) {
    let status = world.materialization_status.as_mut().expect("status");
    status.partitions[0].applied_offset = Some(applied);
    status.partitions[0].head_offset = Some(head);
}
#[then("the binding has reached the captured head")]
async fn reached(world: &mut LaserWorld) {
    assert!(
        world
            .materialization_status
            .as_ref()
            .expect("status")
            .caught_up_to(world.materialization_target.as_ref().expect("target"))
    );
}
#[when("the binding has no known first offset")]
async fn unknown_start(world: &mut LaserWorld) {
    world
        .materialization_status
        .as_mut()
        .expect("status")
        .partitions[0]
        .first_offset = None;
}
#[then("the binding cannot claim complete history")]
async fn incomplete(world: &mut LaserWorld) {
    assert!(
        !world
            .materialization_status
            .as_ref()
            .expect("status")
            .history_complete(OversizePolicy::Block)
    );
}
#[when("another partition lost its records before the binding read them")]
async fn expired_unread(world: &mut LaserWorld) {
    let status = world.materialization_status.as_mut().expect("status");
    assert!(status.history_complete(OversizePolicy::Block));
    status.partitions.push(PartitionProgress {
        partition_id: 1,
        head_offset: None,
        applied_offset: None,
        first_offset: Some(6),
    });
}
#[given(regex = r#"^a pending binding whose table creation failed with class "(\w+)"$"#)]
async fn failed_binding(world: &mut LaserWorld, failure: String) {
    world.materialization_status = Some(BindingStatus {
        history: Default::default(),
        source: SourceSelector::new("shop", "orders"),
        index: "orders".to_owned(),
        realization: RealizationState::Pending,
        realization_reason: None,
        failure: Some(failure_class(&failure)),
        stall: None,
        source_incarnation: None,
        partitions: Vec::new(),
        holds: Vec::new(),
        accepted_gaps: Vec::new(),
        skipped_records: 0,
    });
}
#[when(regex = r#"^its failure class becomes "(\w+)"$"#)]
async fn failure_becomes(world: &mut LaserWorld, failure: String) {
    world
        .materialization_status
        .as_mut()
        .expect("status")
        .failure = Some(failure_class(&failure));
}
#[then("the binding has failed for good")]
async fn failed_for_good(world: &mut LaserWorld) {
    assert!(
        world
            .materialization_status
            .as_ref()
            .expect("status")
            .is_failed()
    );
}
#[then("the binding has not failed for good")]
async fn not_failed_for_good(world: &mut LaserWorld) {
    assert!(
        !world
            .materialization_status
            .as_ref()
            .expect("status")
            .is_failed()
    );
}

fn failure_class(word: &str) -> FailureClass {
    serde_json::from_value(serde_json::Value::String(word.to_owned())).expect("a failure class")
}
#[when("I materialize whole records twice under one operation identity")]
async fn materialize(world: &mut LaserWorld) {
    let topic = world.laser().topic("whole_records");
    topic.ensure(1, None).await.expect("topic");
    let request = topic
        .materialize()
        .operation_id(laser_sdk::prelude::ConversationId::new().as_u128());
    request.clone().apply().await.expect("first setup");
    world.materialization = Some(request.apply().await.expect("repeated setup"));
}
#[when("I publish two arbitrary records to the materialized topic")]
async fn publish(world: &mut LaserWorld) {
    for body in [b"not-json".to_vec(), vec![0, 255, 1, 2]] {
        world
            .laser()
            .topic("whole_records")
            .publish()
            .payload(body)
            .send()
            .await
            .expect("publish");
    }
}
#[then("materialization reaches the captured source head")]
async fn ready(world: &mut LaserWorld) {
    let result = world
        .materialization
        .as_ref()
        .expect("materialization")
        .wait(MaterializationReadiness::CaughtUp, Duration::from_secs(20))
        .await
        .expect("wait");
    assert!(
        matches!(result, MaterializationWait::Ready { .. }),
        "{result:?}"
    );
}
#[then("the materialized query returns both original bodies exactly once")]
async fn bodies(world: &mut LaserWorld) {
    let result = world
        .laser()
        .query("whole_records")
        .with_payload()
        .fetch()
        .await
        .expect("query");
    assert_eq!(result.rows.len(), 2);
    let mut bodies = result
        .rows
        .iter()
        .map(|row| match result.value(row, ORIGINAL_PAYLOAD_FIELD_NAME) {
            Some(laser_sdk::wire::schema::TypedValue::Binary(value)) => value.0.clone(),
            other => panic!("original bytes missing: {other:?}"),
        })
        .collect::<Vec<_>>();
    bodies.sort();
    let mut expected = vec![b"not-json".to_vec(), vec![0, 255, 1, 2]];
    expected.sort();
    assert_eq!(bodies, expected);
}

#[when("I materialize keyed records")]
async fn keyed(world: &mut LaserWorld) {
    materialize_keyed(world, Some("delete")).await;
}

#[when("I materialize keyed records without a delete header")]
async fn keyed_without_delete(world: &mut LaserWorld) {
    materialize_keyed(world, None).await;
}

async fn materialize_keyed(world: &mut LaserWorld, delete_header: Option<&str>) {
    let topic = world.laser().topic("keyed_records");
    topic.ensure(1, None).await.expect("topic");
    let projection = Projection::builder("keyed_records")
        .record()
        .keyed(
            ProjectionKey::Header {
                name: "key".to_owned(),
            },
            delete_header,
        )
        .build();
    world.materialization = Some(
        topic
            .materialize()
            .typed(projection)
            .apply()
            .await
            .expect("keyed setup"),
    );
}

#[when(regex = r#"^I publish keyed value "([^"]+)"$"#)]
async fn keyed_value(world: &mut LaserWorld, value: String) {
    world
        .laser()
        .topic("keyed_records")
        .publish()
        .header("key", "one")
        .payload(value.into_bytes())
        .send()
        .await
        .expect("keyed publish");
}

#[when(regex = r#"^I publish keyed value "([^"]+)" with a "([^"]+)" header$"#)]
async fn keyed_value_with_header(world: &mut LaserWorld, value: String, header: String) {
    world
        .laser()
        .topic("keyed_records")
        .publish()
        .header("key", "one")
        .header(header, "true")
        .payload(value.into_bytes())
        .send()
        .await
        .expect("keyed publish");
}

#[when("I delete the materialized key")]
async fn delete_key(world: &mut LaserWorld) {
    world
        .laser()
        .topic("keyed_records")
        .publish()
        .header("key", "one")
        .header("delete", "true")
        .payload(vec![0])
        .send()
        .await
        .expect("delete record");
}

#[then(regex = r#"^the keyed view contains only "([^"]+)"$"#)]
async fn keyed_contains(world: &mut LaserWorld, value: String) {
    ready(world).await;
    let result = world
        .laser()
        .query("keyed_records")
        .with_payload()
        .fetch()
        .await
        .expect("keyed read");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.value(&result.rows[0], ORIGINAL_PAYLOAD_FIELD_NAME),
        Some(&laser_sdk::wire::schema::TypedValue::Binary(
            laser_sdk::wire::schema::BinaryValue(value.into_bytes())
        ))
    );
}

#[then("the keyed view is empty")]
async fn keyed_empty(world: &mut LaserWorld) {
    ready(world).await;
    assert!(
        world
            .laser()
            .query("keyed_records")
            .fetch()
            .await
            .expect("keyed read")
            .rows
            .is_empty()
    );
}

#[then("operational SQL counts the two materialized records")]
async fn sql_count(world: &mut LaserWorld) {
    let result = world
        .laser()
        .query("whole_records")
        .raw_sql(
            laser_sdk::wire::query::SqlDialect::Sqlite,
            "SELECT count(*) AS n FROM whole_records",
        )
        .fetch()
        .await
        .expect("authorized SQL");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result
            .value(&result.rows[0], "n")
            .expect("count")
            .diagnostic_text(),
        "2"
    );
}

#[then("operational SQL refuses an internal relation and an unserved dialect")]
async fn sql_refusals(world: &mut LaserWorld) {
    for (dialect, statement) in [
        (
            laser_sdk::wire::query::SqlDialect::Sqlite,
            "SELECT * FROM _ld_watermarks",
        ),
        (
            laser_sdk::wire::query::SqlDialect::MySql,
            "SELECT * FROM whole_records",
        ),
    ] {
        assert!(
            world
                .laser()
                .query("whole_records")
                .raw_sql(dialect, statement)
                .fetch()
                .await
                .is_err()
        );
    }
}

#[given("a declared SQL backend")]
async fn declared_sql_backend(world: &mut LaserWorld) {
    let capabilities = world.laser().capabilities().await;
    let descriptor = capabilities
        .backends
        .iter()
        .find(|descriptor| {
            matches!(
                descriptor.implementation.kind.as_str(),
                "postgres" | "velodb"
            ) && descriptor.mode == laser_sdk::wire::hello::BackendMode::Operational
        })
        .expect("the stack declares a Postgres or Doris backend");
    let dialect = if descriptor.implementation.kind == "postgres" {
        laser_sdk::wire::query::SqlDialect::Postgres
    } else {
        laser_sdk::wire::query::SqlDialect::MySql
    };
    world.sql_backend = Some((
        laser_sdk::wire::destination::BackendBinding {
            resource_id: descriptor.resource_id,
            generation: descriptor.observed_backend_generation,
        },
        dialect,
    ));
}

#[when("I materialize whole records on the declared SQL backend")]
async fn materialize_remote(world: &mut LaserWorld) {
    let (binding, _) = world.sql_backend.clone().expect("declared SQL backend");
    let topic = world.laser().topic(REMOTE_TOPIC);
    topic.ensure(1, None).await.expect("topic");
    world.materialization = Some(
        topic
            .materialize()
            .backend(binding)
            .apply()
            .await
            .expect("remote setup"),
    );
}

#[when("I publish two arbitrary records to the remote materialized topic")]
async fn publish_remote(world: &mut LaserWorld) {
    for body in [b"not-json".to_vec(), vec![0, 255, 1, 2]] {
        world
            .laser()
            .topic(REMOTE_TOPIC)
            .publish()
            .payload(body)
            .send()
            .await
            .expect("publish");
    }
}

#[then("the remote materialized query returns both original bodies exactly once")]
async fn remote_bodies(world: &mut LaserWorld) {
    let result = world
        .laser()
        .query(REMOTE_TOPIC)
        .with_payload()
        .fetch()
        .await
        .expect("remote query");
    assert_eq!(result.rows.len(), 2);
    let mut bodies = result
        .rows
        .iter()
        .map(|row| match result.value(row, ORIGINAL_PAYLOAD_FIELD_NAME) {
            Some(laser_sdk::wire::schema::TypedValue::Binary(value)) => value.0.clone(),
            other => panic!("original bytes missing: {other:?}"),
        })
        .collect::<Vec<_>>();
    bodies.sort();
    let mut expected = vec![b"not-json".to_vec(), vec![0, 255, 1, 2]];
    expected.sort();
    assert_eq!(bodies, expected);
}

#[then("operational SQL in the backend dialect counts the two remote records")]
async fn remote_sql_count(world: &mut LaserWorld) {
    let (_, dialect) = world.sql_backend.clone().expect("declared SQL backend");
    let result = world
        .laser()
        .query(REMOTE_TOPIC)
        .raw_sql(dialect, format!("SELECT count(*) AS n FROM {REMOTE_TOPIC}"))
        .fetch()
        .await
        .expect("authorized remote SQL");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result
            .value(&result.rows[0], "n")
            .expect("count")
            .diagnostic_text(),
        "2"
    );
}

#[then("operational SQL refuses internal and catalog relations of the declared backend")]
async fn remote_sql_refusals(world: &mut LaserWorld) {
    let (_, dialect) = world.sql_backend.clone().expect("declared SQL backend");
    for statement in [
        "SELECT * FROM _ld_watermarks",
        "SELECT * FROM ld_watermarks",
        "SELECT * FROM information_schema.tables",
        "SELECT * FROM whole_records",
    ] {
        assert!(
            world
                .laser()
                .query(REMOTE_TOPIC)
                .raw_sql(dialect, statement)
                .fetch()
                .await
                .is_err(),
            "{statement}"
        );
    }
    assert!(
        world
            .laser()
            .query(REMOTE_TOPIC)
            .raw_sql(
                laser_sdk::wire::query::SqlDialect::Sqlite,
                format!("SELECT count(*) AS n FROM {REMOTE_TOPIC}")
            )
            .fetch()
            .await
            .is_err(),
        "a dialect the declared backend does not speak"
    );
}

const REMOTE_TOPIC: &str = "remote_records";
