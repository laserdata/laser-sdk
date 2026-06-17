use crate::common::world::LaserWorld;
use cucumber::{then, when};
use laser_sdk::prelude::LaserError;
use serde_json::json;

#[when(regex = r#"^I publish a JSON event to topic "([^"]+)"$"#)]
async fn publish_event(world: &mut LaserWorld, topic: String) {
    let value = json!({ "endpoint": "/v1/items", "status": 200, "latency_ms": 42 });
    let result: Result<(), LaserError> = async {
        world.laser().publish(&topic).json(&value)?.send().await?;
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
    let mut result: Result<(), String> = Ok(());
    for index in 0..count {
        let value = json!({ "endpoint": "/v1/items", "status": 200, "latency_ms": index });
        let one: Result<(), LaserError> = async {
            world.laser().publish(&topic).json(&value)?.send().await?;
            Ok(())
        }
        .await;
        if let Err(error) = one {
            result = Err(format!("{error:?}"));
            break;
        }
    }
    world.last_result = Some(result);
}

#[then(regex = r"^all (\d+) events are published$")]
async fn all_published(world: &mut LaserWorld, _count: u32) {
    let result = world.last_result.as_ref().expect("a batch was attempted");
    assert!(result.is_ok(), "batch publish failed: {result:?}");
}
