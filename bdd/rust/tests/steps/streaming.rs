use crate::common::world::LaserWorld;
use cucumber::{then, when};
use laser_sdk::prelude::LaserError;
use serde_json::json;

#[when(regex = r#"^I publish a JSON event to topic "([^"]+)"$"#)]
async fn publish_event(world: &mut LaserWorld, topic: String) {
    let value = json!({ "endpoint": "/v1/items", "status": 200, "latency_ms": 42 });
    let result: Result<(), LaserError> = async {
        let handle = world.laser().topic(&topic);
        handle.publish().json(&value)?.send().await?;
        Ok(())
    }
    .await;
    world.last_result = Some(result.map_err(|error| format!("{error:?}")));
}

#[then("the publish succeeds")]
async fn publish_succeeds(world: &mut LaserWorld) {
    let result = world.last_result.as_ref().expect("a publish was attempted");
    assert!(result.is_ok(), "publish failed: {result:?}");
}

#[when(regex = r#"^I publish a batch of (\d+) JSON events to topic "([^"]+)"$"#)]
async fn publish_batch(world: &mut LaserWorld, count: u32, topic: String) {
    let handle = world.laser().topic(&topic);
    let mut batch = handle.publish_batch();
    for index in 0..count {
        let value = json!({ "endpoint": "/v1/items", "status": 200, "latency_ms": index });
        match batch.add_json(&value) {
            Ok(next) => batch = next,
            Err(error) => {
                world.last_result = Some(Err(format!("{error:?}")));
                return;
            }
        }
    }
    match batch.send().await {
        Ok(published) => {
            world.last_batch_count = Some(published);
            world.last_result = Some(Ok(()));
        }
        Err(error) => world.last_result = Some(Err(format!("{error:?}"))),
    }
}

#[then(regex = r"^all (\d+) events are published$")]
async fn all_published(world: &mut LaserWorld, count: u32) {
    let result = world.last_result.as_ref().expect("a batch was attempted");
    assert!(result.is_ok(), "batch publish failed: {result:?}");
    assert_eq!(world.last_batch_count, Some(count as usize));
}
