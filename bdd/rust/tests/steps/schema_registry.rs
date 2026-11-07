use crate::common::world::LaserWorld;
use cucumber::{then, when};
use laser_sdk::query::SchemaSource;

#[when(regex = r#"^the Avro writer schema "([^"]+)" is registered in the stream$"#)]
async fn register_in_the_stream(world: &mut LaserWorld, name: String) {
    let stream = world
        .laser()
        .default_stream()
        .expect("a default stream")
        .to_owned();
    register(world, stream, name).await;
}

#[when(regex = r#"^the Avro writer schema "([^"]+)" is registered in a second stream$"#)]
async fn register_in_a_second_stream(world: &mut LaserWorld, name: String) {
    let stream = format!(
        "{}-second",
        world.laser().default_stream().expect("a default stream")
    );
    world
        .laser()
        .stream(&stream)
        .ensure()
        .await
        .expect("the second stream exists");
    register(world, stream, name).await;
}

#[then("both registrations answer schema id 1")]
fn both_registrations_answer_one(world: &mut LaserWorld) {
    let ids: Vec<u32> = world
        .schema_registrations
        .iter()
        .map(|(_, _, id)| *id)
        .collect();
    assert_eq!(ids, [1, 1]);
}

#[then("each stream reads schema 1 as its own writer schema")]
async fn each_stream_reads_its_own(world: &mut LaserWorld) {
    for (stream, name, _) in &world.schema_registrations {
        let info = world
            .laser()
            .with_default_stream(stream)
            .schemas()
            .get(1)
            .await
            .expect("the schema read answers");
        assert_eq!(
            info.and_then(|info| info.schema.name).as_deref(),
            Some(name.as_str()),
            "{stream}"
        );
    }
}

#[then("each stream lists only its own writer schema")]
async fn each_stream_lists_its_own(world: &mut LaserWorld) {
    for (stream, name, _) in &world.schema_registrations {
        let page = world
            .laser()
            .with_default_stream(stream)
            .schemas()
            .list()
            .fetch()
            .await
            .expect("the schema listing answers");
        let names: Vec<Option<String>> = page
            .items
            .into_iter()
            .map(|info| info.schema.name)
            .collect();
        assert_eq!(names, [Some(name.clone())], "{stream}");
    }
}

async fn register(world: &mut LaserWorld, stream: String, name: String) {
    let source = SchemaSource::Avro {
        schema: format!(
            r#"{{"type":"record","name":"{name}","fields":[{{"name":"id","type":"long"}}]}}"#
        ),
    };
    let id = world
        .laser()
        .with_default_stream(&stream)
        .schemas()
        .register(source)
        .name(name.as_str())
        .send()
        .await
        .expect("the schema registers");
    world.schema_registrations.push((stream, name, id));
}
