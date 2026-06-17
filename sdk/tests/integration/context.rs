use crate::harness;
use bytes::Bytes;
use laser_sdk::prelude::*;
use std::collections::HashSet;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn given_interleaved_turns_when_assembling_context_then_should_order_chronologically_and_filter_by_role()
 {
    let laser = harness::laser().await;
    let conversation = ConversationId::new();
    let turns = [
        (AgentTopic::Commands, "planner", "draft"),
        (AgentTopic::Responses, "writer", "first draft"),
        (AgentTopic::Commands, "planner", "tighten"),
        (AgentTopic::Responses, "writer", "tightened"),
    ];
    for (topic, agent, text) in turns {
        let provenance = Provenance::builder()
            .conversation_id(conversation)
            .agent(agent.parse().expect("agent id is valid"))
            .build();
        laser
            .send_agent(topic, Bytes::copy_from_slice(text.as_bytes()), &provenance)
            .await
            .expect("the turn should be sent");
        // keep Iggy timestamps distinct so the ordering is deterministic
        sleep(Duration::from_millis(2)).await;
    }

    let all = harness::eventually(|| async {
        let messages = ContextAssembler::builder()
            .conversation_id(conversation)
            .build()
            .assemble(&laser)
            .await
            .expect("assembling the conversation should succeed");
        (messages.len() == 4).then_some(messages)
    })
    .await;

    let texts: Vec<String> = all
        .iter()
        .map(|m| String::from_utf8_lossy(&m.payload).into_owned())
        .collect();
    assert_eq!(texts, ["draft", "first draft", "tighten", "tightened"]);

    let last_two: Vec<String> = LastN(2)
        .select(&all)
        .iter()
        .map(|m| String::from_utf8_lossy(&m.payload).into_owned())
        .collect();
    assert_eq!(last_two, ["tighten", "tightened"]);

    let writer: HashSet<AgentId> =
        HashSet::from(["writer".parse().expect("writer is a valid agent id")]);
    let writer_messages = RoleFilter(writer).select(&all);
    assert_eq!(writer_messages.len(), 2);
    assert!(writer_messages.iter().all(|m| {
        m.provenance
            .agent
            .as_ref()
            .expect("agent should be set")
            .as_str()
            == "writer"
    }));
}
