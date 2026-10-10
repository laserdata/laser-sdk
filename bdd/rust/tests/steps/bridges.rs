use crate::common::world::LaserWorld;
use cucumber::{given, then, when};
use laser_sdk::a2a::{A2aBridge, TaskState, enter_bridge};
use laser_sdk::agui::AgUiEvent;
use laser_sdk::mcp::McpBridge;
use laser_sdk::prelude::full::*;
use laser_sdk::wire::agent::{ConversationId as WireConversationId, CorrelationId, OPERATION_CHAT};
use serde_json::json;
use std::sync::{Arc, Mutex};
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
        "a2a-gateway"
            .parse::<laser_sdk::types::AgentId>()
            .expect("a2a-gateway is valid"),
        AgentTopic::Sessions,
        AgentTopic::Sessions,
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
    let source = "agui-gateway"
        .parse::<laser_sdk::types::AgentId>()
        .expect("agui-gateway is valid");
    world
        .laser()
        .publish_state_snapshot(source, world.conversation(), &json!({"count": 1}))
        .await
        .expect("snapshot publishes");
    world
        .laser()
        .publish_state_delta(
            "agui-gateway"
                .parse::<laser_sdk::types::AgentId>()
                .expect("agui-gateway is valid"),
            world.conversation(),
            &json!([{"op": "replace", "path": "/count", "value": 2}]),
        )
        .await
        .expect("delta publishes");
    for _ in 0..80 {
        let state = world
            .laser()
            .reconstruct_state(world.conversation())
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
    assert_eq!(
        world.reconstructed_state.as_ref(),
        Some(&json!({"count": count}))
    );
}

#[when(regex = r#"^I stream chat chunks "([^"]+)" and "([^"]+)"$"#)]
async fn stream_chat(world: &mut LaserWorld, first: String, second: String) {
    let conversation = world.conversation();
    let mut stream = world
        .laser()
        .agdx(
            AgentTopic::Sessions,
            "assistant"
                .parse::<laser_sdk::types::AgentId>()
                .expect("assistant is valid"),
            WireConversationId::from(conversation),
        )
        .stream(
            CorrelationId::from_u128(conversation.as_u128()),
            OPERATION_CHAT,
        );
    stream
        .write(first.into_bytes())
        .await
        .expect("first chunk writes");
    stream
        .write(second.into_bytes())
        .await
        .expect("second chunk writes");
    stream.finish("stop", None).await.expect("terminal writes");
    for _ in 0..80 {
        let events = world
            .laser()
            .agui_events(conversation, AgentTopic::Sessions)
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

// Answers every command with its own name and records what each command was:
// a bridged A2A task, a bridged MCP tool call, or an input request.
struct Responder {
    name: String,
    answered: Arc<Mutex<Vec<String>>>,
}

impl AgentHandler for Responder {
    async fn handle(&self, message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        let Some(envelope) = message.envelope.as_ref() else {
            return Ok(());
        };
        if envelope.kind != laser_sdk::wire::agent::AgentKind::Command {
            return Ok(());
        }
        let label = if envelope.tool.is_some() {
            "tool"
        } else if envelope.operation.as_deref() == Some(OPERATION_CHAT) {
            "task"
        } else {
            "input"
        };
        self.answered
            .lock()
            .expect("the answered list is not poisoned")
            .push(label.to_owned());
        ctx.respond_input(AgentTopic::Sessions, self.name.clone().into_bytes())
            .await
    }
}

fn agent_id(name: &str) -> laser_sdk::wire::agent::AgentId {
    name.parse().expect("a valid agent id")
}

#[given(regex = r#"^responders "([^"]+)" and "([^"]+)" answer every command with their own name$"#)]
async fn responders(world: &mut LaserWorld, first: String, second: String) {
    for name in [first, second] {
        let answered = Arc::new(Mutex::new(Vec::new()));
        let mut handle = Agent::builder()
            .id(name.parse().expect("a valid agent id"))
            .listen_on(AgentTopic::Sessions)
            .handler(Responder {
                name: name.clone(),
                answered: Arc::clone(&answered),
            })
            .build()
            .spawn(world.laser().clone());
        handle.ready().await.expect("the responder is ready");
        world.responders.push(handle);
        world.answered.insert(name, answered);
    }
}

#[when(regex = r#"^I submit an A2A task to "([^"]+)"$"#)]
async fn submit_to(world: &mut LaserWorld, target: String) {
    let bridge = A2aBridge::new(
        world.laser().clone(),
        agent_id("a2a-gateway"),
        AgentTopic::Sessions,
        AgentTopic::Sessions,
    );
    let task = bridge
        .submit_to(
            agent_id(&target),
            br#"{"message":{"role":"user","text":"addressed"}}"#.to_vec(),
        )
        .await
        .expect("the addressed submit succeeds");
    world.bridge_task = Some(task.id);
}

#[when(regex = r#"^I call the MCP tool "([^"]+)" on "([^"]+)"$"#)]
async fn call_tool_on(world: &mut LaserWorld, tool: String, target: String) {
    let bridge = McpBridge::new(
        world.laser().clone(),
        agent_id("mcp-gateway"),
        AgentTopic::Sessions,
        AgentTopic::Sessions,
        "tools",
    )
    .with_timeout(Duration::from_secs(15));
    let result = bridge
        .call_tool_to(agent_id(&target), &tool, br#"{"q":"addressed"}"#.to_vec())
        .await
        .expect("the addressed tool call completes");
    world.tool_result = result.content.first().map(|content| content.text.clone());
}

#[when(regex = r#"^I request input from "([^"]+)"$"#)]
async fn request_input_from(world: &mut LaserWorld, target: String) {
    let decision = world
        .laser()
        .agdx(
            AgentTopic::Sessions,
            agent_id("orchestrator"),
            WireConversationId::from(ConversationId::new()),
        )
        .request_input_from(
            agent_id(&target),
            AgentTopic::Sessions,
            b"approve?".to_vec(),
            Duration::from_secs(15),
        )
        .await
        .expect("the addressed input request is answered");
    world.input_decision = Some(String::from_utf8_lossy(&decision).into_owned());
}

#[then(regex = r#"^the A2A task completes with "([^"]+)"$"#)]
async fn task_completes_with(world: &mut LaserWorld, text: String) {
    let bridge = A2aBridge::new(
        world.laser().clone(),
        agent_id("a2a-gateway"),
        AgentTopic::Sessions,
        AgentTopic::Sessions,
    );
    let id = world.bridge_task.clone().expect("a task was submitted");
    for _ in 0..600 {
        let task = bridge.task(&id).await.expect("task lookup succeeds");
        if task.status.state == TaskState::Completed {
            assert_eq!(task.artifacts[0].text, text);
            return;
        }
        sleep(Duration::from_millis(25)).await;
    }
    panic!("the addressed A2A task did not complete");
}

#[then(regex = r#"^the MCP tool result is "([^"]+)"$"#)]
async fn tool_result_is(world: &mut LaserWorld, text: String) {
    assert_eq!(world.tool_result.as_deref(), Some(text.as_str()));
}

#[then(regex = r#"^the input decision is "([^"]+)"$"#)]
async fn input_decision_is(world: &mut LaserWorld, text: String) {
    assert_eq!(world.input_decision.as_deref(), Some(text.as_str()));
}

#[then(regex = r#"^responder "([^"]+)" answered exactly (".+")$"#)]
async fn answered_exactly(world: &mut LaserWorld, name: String, list: String) {
    let expected: Vec<String> = list
        .split(", ")
        .map(|label| label.trim_matches('"').to_owned())
        .collect();
    let answered = world.answered.get(&name).expect("a responder").clone();
    let seen = answered
        .lock()
        .expect("the answered list is not poisoned")
        .clone();
    assert_eq!(seen, expected, "responder {name}");
}
