use crate::harness;
use laser_sdk::prelude::full::Laser;

fn unique_stream(laser: &Laser, suffix: &str) -> String {
    format!(
        "{}_{suffix}",
        laser
            .default_stream()
            .expect("the test laser names its stream")
    )
}

#[tokio::test]
async fn given_a_created_stream_when_listed_and_deleted_then_should_report_it_until_gone() {
    let laser = harness::connected_laser().await;
    let name = unique_stream(&laser, "admin");
    let stream = laser.stream(&name);
    assert!(
        stream
            .get()
            .await
            .expect("a missing stream reads")
            .is_none()
    );
    assert!(!stream.exists().await.expect("a missing stream reads"));
    stream.ensure().await.expect("the stream is created");
    stream
        .topic("metrics")
        .ensure(2, None)
        .await
        .expect("the topic is created");

    let listed = laser.streams().await.expect("streams are listed");
    let info = listed
        .iter()
        .find(|info| info.name == name)
        .expect("the new stream is listed");
    assert_eq!(info.topics_count, 1);
    assert!(info.created_at_micros > 0);
    assert_eq!(
        stream.get().await.expect("the stream reads").as_ref(),
        Some(info)
    );
    assert!(stream.exists().await.expect("the stream reads"));

    let topics = stream.topics().await.expect("topics are listed");
    assert_eq!(topics.len(), 1);
    assert_eq!(topics[0].name, "metrics");
    assert_eq!(topics[0].partitions_count, 2);
    assert_eq!(topics[0].message_expiry, None);

    assert!(stream.delete().await.expect("the stream deletes"));
    assert!(!stream.exists().await.expect("a deleted stream reads"));
    assert!(
        laser
            .streams()
            .await
            .expect("streams are listed")
            .iter()
            .all(|info| info.name != name)
    );
}

#[tokio::test]
async fn given_a_topic_when_partitions_changed_and_deleted_then_should_report_each_state() {
    let laser = harness::connected_laser().await;
    let name = unique_stream(&laser, "partitions");
    let topic = laser.stream(&name).topic("hosts");
    assert!(topic.get().await.expect("a missing topic reads").is_none());
    assert!(!topic.delete().await.expect("a missing topic deletes"));
    topic.ensure(1, None).await.expect("the topic is created");
    topic.add_partitions(2).await.expect("partitions are added");
    let info = topic
        .get()
        .await
        .expect("the topic reads")
        .expect("the topic exists");
    assert_eq!(info.name, "hosts");
    assert_eq!(info.partitions_count, 3);
    assert_eq!(
        topic
            .partitions()
            .await
            .expect("partitions are listed")
            .len(),
        3
    );
    topic
        .delete_partitions(1)
        .await
        .expect("a partition is deleted");
    assert_eq!(
        topic
            .partitions()
            .await
            .expect("partitions are listed")
            .len(),
        2
    );
    assert!(topic.delete().await.expect("the topic deletes"));
    assert!(!topic.exists().await.expect("a deleted topic reads"));
    let missing = topic
        .partitions()
        .await
        .expect_err("a deleted topic has no partitions");
    assert!(missing.is_stream_or_topic_not_found());
    laser
        .stream(&name)
        .delete()
        .await
        .expect("the stream is removed");
}

#[tokio::test]
async fn given_a_missing_stream_when_topics_or_groups_listed_then_should_fail_as_not_found() {
    let laser = harness::connected_laser().await;
    let stream = laser.stream(unique_stream(&laser, "missing"));
    let missing = stream
        .topics()
        .await
        .expect_err("a missing stream has no topics");
    assert!(missing.is_stream_or_topic_not_found());
    let missing = stream
        .topic("jobs")
        .consumer_groups()
        .await
        .expect_err("a missing topic has no consumer groups");
    assert!(missing.is_stream_or_topic_not_found());
    assert!(!stream.delete().await.expect("a missing stream deletes"));
}

#[tokio::test]
async fn given_a_consumer_group_when_listed_and_deleted_then_should_report_it_until_gone() {
    let laser = harness::connected_laser().await;
    let name = unique_stream(&laser, "groups");
    let topic = laser.stream(&name).topic("jobs");
    topic.ensure(1, None).await.expect("the topic is created");
    topic
        .ensure_consumer_group("workers")
        .await
        .expect("the group is created");
    let groups = topic.consumer_groups().await.expect("groups are listed");
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].name, "workers");
    assert_eq!(groups[0].members_count, 0);
    let group = topic.consumer_group("workers");
    assert!(group.delete().await.expect("the group deletes"));
    assert!(!group.delete().await.expect("a missing group deletes"));
    assert!(
        topic
            .consumer_groups()
            .await
            .expect("groups are listed")
            .is_empty()
    );
    laser
        .stream(&name)
        .delete()
        .await
        .expect("the stream is removed");
}
