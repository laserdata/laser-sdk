use crate::common::container::fresh_laser;
use crate::common::world::LaserWorld;
use cucumber::{given, then, when};
use laser_sdk::prelude::ConversationId;

#[given("a running data platform")]
async fn running_platform(_world: &mut LaserWorld) {
    // Precondition only: the shared Apache Iggy container starts lazily on the
    // first connection in `a fresh stream`.
}

#[given("a fresh stream")]
async fn fresh_stream(world: &mut LaserWorld) {
    world.laser = Some(fresh_laser().await);
}

#[given(regex = r"^a fresh stream bootstrapped with (\d+) partitions$")]
async fn fresh_stream_bootstrapped(world: &mut LaserWorld, partitions: u32) {
    let laser = fresh_laser().await;
    laser
        .bootstrap(partitions)
        .await
        .expect("bootstrap the stream");
    world.laser = Some(laser);
}

#[given("a new conversation")]
async fn new_conversation(world: &mut LaserWorld) {
    world.conversation = Some(ConversationId::new());
}

#[when("I start another conversation")]
async fn start_another_conversation(world: &mut LaserWorld) {
    world.conversation = Some(ConversationId::new());
}

#[when(regex = r"^I bootstrap the stream with (\d+) partitions$")]
async fn bootstrap(world: &mut LaserWorld, partitions: u32) {
    let result = world.laser().bootstrap(partitions).await;
    world.last_result = Some(result.map(|_| ()).map_err(|error| format!("{error:?}")));
}

#[then("the stream is ready")]
async fn stream_ready(world: &mut LaserWorld) {
    assert!(world.laser().stream().is_some(), "the stream should be set");
    if let Some(result) = &world.last_result {
        assert!(result.is_ok(), "bootstrap failed: {result:?}");
    }
}
