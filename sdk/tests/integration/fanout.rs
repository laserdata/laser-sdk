use crate::harness;
use bytes::Bytes;
use laser_sdk::prelude::*;

struct Worker;

impl AgentHandler for Worker {
    async fn handle(&self, message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        let output = Bytes::from(format!(
            "done: {}",
            String::from_utf8_lossy(&message.payload)
        ));
        ctx.respond(output).await
    }
}

#[tokio::test]
async fn given_fanned_out_subconversations_when_aggregating_then_should_collect_every_result_at_the_root()
 {
    let laser = harness::laser().await;
    Agent::builder()
        .id("worker".parse().expect("worker is a valid agent id"))
        .listen_on(AgentTopic::Commands)
        .respond_on(AgentTopic::Responses)
        .handler(Worker)
        .build()
        .spawn(laser.clone());

    let root = Provenance::builder()
        .conversation_id(ConversationId::new())
        .build();
    let root_id = root.conversation_id;
    for i in 1..=3 {
        let subtask = laser.spawn_subconversation(&root);
        laser
            .send_agent(
                AgentTopic::Commands,
                Bytes::from(format!("sub{i}")),
                &subtask,
            )
            .await
            .expect("the subtask should be sent");
    }

    let results = harness::eventually(|| {
        let laser = laser.clone();
        async move {
            let results = ContextAssembler::builder()
                .conversation_id(root_id)
                .across_subconversations(true)
                .topics(vec![AgentTopic::Responses])
                .build()
                .assemble(&laser)
                .await
                .expect("aggregating across subconversations should succeed");
            (results.len() == 3).then_some(results)
        }
    })
    .await;

    assert_eq!(results.len(), 3);
    assert!(results.iter().all(|m| {
        m.provenance
            .agent
            .as_ref()
            .expect("agent should be set")
            .as_str()
            == "worker"
    }));
}
