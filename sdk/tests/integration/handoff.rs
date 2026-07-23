use crate::harness;
use bytes::Bytes;
use laser_sdk::prelude::full::*;

struct Planner;

impl AgentHandler for Planner {
    async fn handle(&self, message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        let mut handoff = Provenance::builder()
            .conversation_id(message.provenance.conversation_id)
            .causal_parent(message.id)
            .agent("planner".parse()?)
            .build();
        Router::to("executor".parse()?).apply(&mut handoff);
        ctx.send(AgentTopic::ToolCalls, message.payload.clone(), &handoff)
            .await
    }
}

struct Executor;

impl AgentHandler for Executor {
    async fn handle(&self, message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        let done = Bytes::from(format!(
            "executed: {}",
            String::from_utf8_lossy(&message.payload)
        ));
        ctx.respond(done).await
    }
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_planner_and_executor_when_a_command_arrives_then_should_hand_off_across_topics() {
    let laser = harness::laser().await;
    Agent::builder()
        .id("planner".parse().expect("planner is a valid agent id"))
        .listen_on(AgentTopic::Commands)
        .handler(Planner)
        .build()
        .spawn(laser.clone());
    Agent::builder()
        .id("executor".parse().expect("executor is a valid agent id"))
        .listen_on(AgentTopic::ToolCalls)
        .respond_on(AgentTopic::Responses)
        .handler(Executor)
        .build()
        .spawn(laser.clone());

    let conversation = ConversationId::new();
    let command = Provenance::builder().conversation_id(conversation).build();
    laser
        .send_agent(
            AgentTopic::Commands,
            Bytes::from_static(b"ship it"),
            &command,
        )
        .await
        .expect("the command should be sent");

    let responses = harness::eventually(|| {
        let laser = laser.clone();
        async move {
            let responses = ContextAssembler::builder()
                .conversation_id(conversation)
                .topics(vec![AgentTopic::Responses])
                .build()
                .assemble(&laser)
                .await
                .expect("assembling the responses should succeed");
            (!responses.is_empty()).then_some(responses)
        }
    })
    .await;

    assert_eq!(responses.len(), 1);
    assert_eq!(
        responses[0]
            .provenance
            .agent
            .as_ref()
            .expect("agent should be set")
            .as_str(),
        "executor"
    );
    assert_eq!(responses[0].payload.as_slice(), b"executed: ship it");
    assert!(responses[0].provenance.causal_parent.is_some());
}
