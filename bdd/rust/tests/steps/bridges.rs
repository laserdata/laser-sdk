use crate::common::world::LaserWorld;
use cucumber::{then, when};
use laser_sdk::a2a::{A2aBridge, TaskState, enter_bridge};
use laser_sdk::agui::AgUiEvent;
use laser_sdk::prelude::full::*;
use laser_sdk::wire::agent::{
    ConversationId as WireConversationId, CorrelationId, OPERATION_CHAT,
};
use serde_json::json;
use tokio::time::{Duration, sleep};

#[when(regex = r#"^bridge "([^"]+)" enters after hops "([^"]+)"$"#)]
async fn bridge_enters_after(world: &mut LaserWorld, bridge: String, hops: String) {
    let previous = hops.split(',').map(str::to_owned).collect::<Vec<_>>();
    world.bridge_hops = enter_bridge(&bridge, &previous).expect("the bridge path is new");
}

#[then(regex = r#"^the bridge hops are "([^"]+)"$"#)]
async fn bridge_hops_are(world: &mut LaserWorld, hops: String) {
    assert_eq!(world.bridge_hops, hops.split(',').collect::<Vec<_>>());
}

#[when(regex = r#"^bridge "([^"]+)" enters the same route$"#)]
async fn bridge_enters_same_route(world: &mut LaserWorld, bridge: String) {
    world.bridge_loop_rejected = enter_bridge(&bridge, &world.bridge_hops).is_err();
}

#[then("the bridge route is rejected as a loop")]
async fn bridge_route_rejected(world: &mut LaserWorld) {
    assert!(world.bridge_loop_rejected);
}

#[when("I submit and cancel an A2A task")]
async fn submit_and_cancel_a2a(world: &mut LaserWorld) {
    let bridge = A2aBridge::new(
        world.laser().clone(),
        "a2a-gateway".parse().expect("a2a-gateway is valid"),
        AgentTopic::Commands,
        AgentTopic::Responses,
    );
    let task = bridge
        .submit(br#"{"message":{"role":"user","text":"cancel me"}}"#.to_vec())
        .await
        .expect("submit succeeds");
    bridge.cancel(&task.id).await.expect("cancel succeeds");
    for _ in 0..80 {
        let replayed = bridge.task(&task.id).await.expect("task replay succeeds");
        if replayed.status.state == TaskState::Canceled {
            world.bridge_task_state = Some("Canceled".to_owned());
            return;
        }
        sleep(Duration::from_millis(25)).await;
    }
    panic!("canceled task did not replay");
}

#[then(regex = r#"^the replayed A2A task state is "([^"]+)"$"#)]
async fn replayed_state(world: &mut LaserWorld, state: String) {
    assert_eq!(world.bridge_task_state.as_deref(), Some(state.as_str()));
}

#[when("I publish an AG-UI count snapshot of 1 and replace it with 2")]
async fn publish_state(world: &mut LaserWorld) {
    let source = "agui-gateway".parse().expect("agui-gateway is valid");
    world
        .laser()
        .publish_state_snapshot(AgentTopic::Audit, source, world.conversation(), &json!({"count": 1}))
        .await
        .expect("snapshot publishes");
    world
        .laser()
        .publish_state_delta(
            AgentTopic::Audit,
            "agui-gateway".parse().expect("agui-gateway is valid"),
            world.conversation(),
            &json!([{"op": "replace", "path": "/count", "value": 2}]),
        )
        .await
        .expect("delta publishes");
    for _ in 0..80 {
        let state = world
            .laser()
            .reconstruct_state(world.conversation(), AgentTopic::Audit)
            .await
            .expect("state reconstructs");
        if state.as_ref().is_some_and(|value| value["count"] == 2) {
            world.reconstructed_state = state;
            return;
        }
        sleep(Duration::from_millis(25)).await;
    }
    panic!("state delta did not become visible");
}

#[then(regex = r"^the reconstructed AG-UI count is (\d+)$")]
async fn reconstructed_count(world: &mut LaserWorld, count: u64) {
    assert_eq!(world.reconstructed_state.as_ref(), Some(&json!({"count": count})));
}

#[when(regex = r#"^I stream chat chunks "([^"]+)" and "([^"]+)"$"#)]
async fn stream_chat(world: &mut LaserWorld, first: String, second: String) {
    let conversation = world.conversation();
    let mut stream = world
        .laser()
        .agdx(
            AgentTopic::LlmIo,
            "assistant".parse().expect("assistant is valid"),
            WireConversationId::from(conversation),
        )
        .stream(CorrelationId::from_u128(conversation.as_u128()), OPERATION_CHAT);
    stream.write(first.into_bytes()).await.expect("first chunk writes");
    stream.write(second.into_bytes()).await.expect("second chunk writes");
    stream.finish("stop", None).await.expect("terminal writes");
    for _ in 0..80 {
        let events = world
            .laser()
            .agui_events(conversation, AgentTopic::LlmIo)
            .await
            .expect("AG-UI events render");
        if events.len() >= 4 {
            world.agui_event_types = events
                .iter()
                .map(|event| match event {
                    AgUiEvent::TextMessageStart { .. } => "TEXT_MESSAGE_START",
                    AgUiEvent::TextMessageContent { .. } => "TEXT_MESSAGE_CONTENT",
                    AgUiEvent::TextMessageEnd { .. } => "TEXT_MESSAGE_END",
                    _ => "OTHER",
                })
                .map(str::to_owned)
                .collect();
            return;
        }
        sleep(Duration::from_millis(25)).await;
    }
    panic!("chat events did not become visible");
}

#[then("AG-UI renders the chat lifecycle in order")]
async fn chat_lifecycle(world: &mut LaserWorld) {
    assert_eq!(
        world.agui_event_types,
        [
            "TEXT_MESSAGE_START",
            "TEXT_MESSAGE_CONTENT",
            "TEXT_MESSAGE_CONTENT",
            "TEXT_MESSAGE_END",
        ]
    );
}
