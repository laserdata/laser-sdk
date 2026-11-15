use crate::common::test_iggy::laser_without_stream;
use crate::common::world::LaserWorld;
use cucumber::{given, then, when};
use laser_sdk::agent::TopicRetention;
use laser_sdk::prelude::LaserError;
use std::time::Duration;

#[when("I bootstrap the stream with the defaults")]
async fn bootstrap_defaults(world: &mut LaserWorld) {
    world
        .laser()
        .bootstrap(1, TopicRetention::default())
        .await
        .expect("bootstrap with the defaults");
}

#[then(regex = r#"^topic "([^"]+)" has (\d+) partition and expires records after (\d+) seconds$"#)]
async fn topic_defaults(world: &mut LaserWorld, name: String, partitions: u32, seconds: u64) {
    let laser = world.laser();
    let info = laser
        .stream(laser.default_stream().expect("a default stream"))
        .topic(name)
        .get()
        .await
        .expect("the topic reads")
        .expect("the topic exists");
    assert_eq!(info.partitions_count, partitions);
    assert_eq!(info.message_expiry, Some(Duration::from_secs(seconds)));
}

#[given("a client without a default stream")]
async fn client_without_default_stream(world: &mut LaserWorld) {
    let fresh = laser_without_stream().await;
    world.platform = fresh.iggy;
    world.laser = Some(fresh.laser);
}

#[when(regex = r#"^I publish "([^"]+)" to topic "([^"]+)" of the default stream$"#)]
async fn publish_to_default_stream(world: &mut LaserWorld, payload: String, topic: String) {
    publish(world, &payload, &topic).await;
}

#[then("the error names the call that sets a default stream")]
async fn names_default_stream_call(world: &mut LaserWorld) {
    let error = failed(world);
    assert!(error.contains("with_default_stream"), "{error}");
}

async fn publish(world: &mut LaserWorld, payload: &str, topic: &str) {
    let result = world
        .laser()
        .topic(topic)
        .send(payload.as_bytes().to_vec(), Default::default(), None)
        .await
        .map(|_| ())
        .map_err(|error: LaserError| error.to_string());
    world.last_result = Some(result);
}

fn failed(world: &LaserWorld) -> &str {
    match world.last_result.as_ref().expect("a publish ran") {
        Ok(()) => panic!("the publish fails"),
        Err(error) => error,
    }
}
