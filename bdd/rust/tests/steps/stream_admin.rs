use crate::common::world::LaserWorld;
use cucumber::{then, when};
use laser_sdk::prelude::Topic;

fn unique_stream(world: &LaserWorld) -> String {
    format!(
        "{}-admin",
        world
            .laser()
            .default_stream()
            .expect("the scenario connects with a stream")
    )
}

fn topic(world: &LaserWorld, name: &str) -> Topic {
    world.laser().stream(unique_stream(world)).topic(name)
}

#[when(regex = r#"^I create the unique stream with topic "([^"]+)" of (\d+) partitions$"#)]
async fn create_unique_stream(world: &mut LaserWorld, name: String, partitions: u32) {
    topic(world, &name)
        .ensure(partitions, None)
        .await
        .expect("the stream and topic are created");
}

#[then("the stream listing includes the unique stream")]
async fn listing_includes(world: &mut LaserWorld) {
    let name = unique_stream(world);
    let streams = world.laser().streams().await.expect("streams are listed");
    assert!(streams.iter().any(|stream| stream.name == name));
}

#[then("the stream listing omits the unique stream")]
async fn listing_omits(world: &mut LaserWorld) {
    let name = unique_stream(world);
    let streams = world.laser().streams().await.expect("streams are listed");
    assert!(streams.iter().all(|stream| stream.name != name));
}

#[then(regex = r"^the unique stream exists with (\d+) topics?$")]
async fn stream_exists(world: &mut LaserWorld, topics: u32) {
    let stream = world.laser().stream(unique_stream(world));
    assert!(stream.exists().await.expect("the stream reads"));
    let info = stream
        .get()
        .await
        .expect("the stream reads")
        .expect("the stream exists");
    assert_eq!(info.topics_count, topics);
}

#[then(regex = r#"^the unique stream lists topic "([^"]+)" with (\d+) partitions$"#)]
async fn stream_lists_topic(world: &mut LaserWorld, name: String, partitions: u32) {
    let topics = world
        .laser()
        .stream(unique_stream(world))
        .topics()
        .await
        .expect("topics are listed");
    let info = topics
        .iter()
        .find(|info| info.name == name)
        .expect("the topic is listed");
    assert_eq!(info.partitions_count, partitions);
}

#[when("I delete the unique stream")]
async fn delete_stream(world: &mut LaserWorld) {
    let deleted = world
        .laser()
        .stream(unique_stream(world))
        .delete()
        .await
        .expect("the stream deletes");
    assert!(deleted);
}

#[then("the unique stream no longer exists")]
async fn stream_gone(world: &mut LaserWorld) {
    let stream = world.laser().stream(unique_stream(world));
    assert!(!stream.exists().await.expect("the stream reads"));
    assert!(stream.get().await.expect("the stream reads").is_none());
}

#[when(regex = r#"^I add (\d+) partitions to topic "([^"]+)"$"#)]
async fn add_partitions(world: &mut LaserWorld, count: u32, name: String) {
    topic(world, &name)
        .add_partitions(count)
        .await
        .expect("partitions are added");
}

#[then(regex = r#"^topic "([^"]+)" has (\d+) partitions$"#)]
async fn topic_partitions(world: &mut LaserWorld, name: String, count: usize) {
    let topic = topic(world, &name);
    let info = topic
        .get()
        .await
        .expect("the topic reads")
        .expect("the topic exists");
    assert_eq!(info.partitions_count as usize, count);
    let partitions = topic.partitions().await.expect("partitions are listed");
    assert_eq!(partitions.len(), count);
}

#[when(regex = r#"^I delete topic "([^"]+)"$"#)]
async fn delete_topic(world: &mut LaserWorld, name: String) {
    assert!(
        topic(world, &name)
            .delete()
            .await
            .expect("the topic deletes")
    );
}

#[then(regex = r#"^topic "([^"]+)" no longer exists$"#)]
async fn topic_gone(world: &mut LaserWorld, name: String) {
    let topic = topic(world, &name);
    assert!(!topic.exists().await.expect("the topic reads"));
    assert!(topic.get().await.expect("the topic reads").is_none());
}

#[then(regex = r#"^a second delete of topic "([^"]+)" reports it missing$"#)]
async fn second_delete(world: &mut LaserWorld, name: String) {
    assert!(
        !topic(world, &name)
            .delete()
            .await
            .expect("a missing topic deletes")
    );
}

#[when(regex = r#"^I create consumer group "([^"]+)" on topic "([^"]+)"$"#)]
async fn create_group(world: &mut LaserWorld, group: String, name: String) {
    topic(world, &name)
        .ensure_consumer_group(&group)
        .await
        .expect("the group is created");
}

#[then(regex = r#"^topic "([^"]+)" lists consumer group "([^"]+)"$"#)]
async fn lists_group(world: &mut LaserWorld, name: String, group: String) {
    let groups = topic(world, &name)
        .consumer_groups()
        .await
        .expect("groups are listed");
    assert!(groups.iter().any(|summary| summary.name == group));
}

#[when(regex = r#"^I delete consumer group "([^"]+)" on topic "([^"]+)"$"#)]
async fn delete_group(world: &mut LaserWorld, group: String, name: String) {
    assert!(
        topic(world, &name)
            .consumer_group(group)
            .delete()
            .await
            .expect("the group deletes")
    );
}

#[then(regex = r#"^topic "([^"]+)" lists no consumer groups$"#)]
async fn lists_no_groups(world: &mut LaserWorld, name: String) {
    let groups = topic(world, &name)
        .consumer_groups()
        .await
        .expect("groups are listed");
    assert!(groups.is_empty());
}
