use crate::harness;
use bytes::Bytes;
use laser_sdk::prelude::full::*;
use std::collections::HashSet;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
#[serial_test::serial(integration)]
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
            .ok()?;
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

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_context_scope_when_appending_and_recalling_then_should_bind_one_conversation() {
    let laser = harness::laser().await;
    let conversation = ConversationId::new();
    let session = laser.context(conversation);

    // The scope's append reads back through the scope's own fetch, and its
    // scoped memory recalls the same items the unscoped handle would under the
    // conversation, without repeating the id: the session is one scope.
    session
        .append(AgentTopic::Audit, Bytes::from_static(b"incident opened"))
        .await
        .expect("append into the scope");
    let trail = harness::eventually(|| {
        let session = session.clone();
        async move {
            let trail = session
                .fetch(vec![AgentTopic::Audit], 8)
                .await
                .expect("fetch the scope");
            (!trail.is_empty()).then_some(trail)
        }
    })
    .await;
    assert_eq!(
        String::from_utf8_lossy(&trail[0].payload),
        "incident opened"
    );

    let scoped = session.memory("ctx-scope-it");
    scoped
        .remember(Bytes::from_static(b"checkout is slow"))
        .send()
        .await
        .expect("remember in the scoped memory");
    let via_scope = scoped
        .recall()
        .folded()
        .limit(5)
        .fetch()
        .await
        .expect("scoped recall");
    let via_handle = laser
        .memory("ctx-scope-it")
        .recall(conversation)
        .folded()
        .limit(5)
        .fetch()
        .await
        .expect("unscoped recall under the conversation");
    assert_eq!(
        via_scope.len(),
        via_handle.len(),
        "the scoped recall matches the unscoped recall under the same conversation"
    );
    assert!(
        via_scope
            .iter()
            .any(|item| item.payload == b"checkout is slow"),
        "the remembered fact recalls within the scope"
    );
}
