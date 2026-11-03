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
        ctx.send(AgentTopic::Sessions, message.payload.clone(), &handoff)
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
async fn given_a_planner_and_executor_when_a_command_arrives_then_should_hand_off_on_the_shared_topic()
 {
    let laser = harness::laser().await;
    let _agent_lifetime_1 = Agent::builder()
        .id("planner".parse().expect("planner is a valid agent id"))
        .listen_on(AgentTopic::Sessions)
        .handler(Planner)
        .build()
        .spawn(laser.clone());
    let _agent_lifetime_2 = Agent::builder()
        .id("executor".parse().expect("executor is a valid agent id"))
        .listen_on(AgentTopic::Sessions)
        .respond_on(AgentTopic::Sessions)
        .handler(Executor)
        .build()
        .spawn(laser.clone());

    let conversation = ConversationId::new();
    // Both agents read the shared session topic, so the client addresses the
    // planner. An untargeted command would be work for every role.
    let mut command = Provenance::builder().conversation_id(conversation).build();
    Router::to("planner".parse().expect("planner is a valid agent id")).apply(&mut command);
    laser
        .send_agent(
            AgentTopic::Sessions,
            Bytes::from_static(b"ship it"),
            &command,
        )
        .await
        .expect("the command should be sent");

    let responses = harness::eventually(|| {
        let laser = laser.clone();
        async move {
            let responses: Vec<_> = ContextAssembler::builder()
                .conversation_id(conversation)
                .topics(vec![AgentTopic::Sessions])
                .build()
                .assemble(&laser)
                .await
                .expect("assembling the responses should succeed")
                .into_iter()
                .filter(|message| {
                    message
                        .provenance
                        .agent
                        .as_ref()
                        .is_some_and(|agent| agent.as_str() == "executor")
                })
                .collect();
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
