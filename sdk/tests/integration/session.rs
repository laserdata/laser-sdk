use crate::harness;
use bytes::Bytes;
use laser_sdk::prelude::full::*;

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_per_user_sessions_when_messaging_then_should_isolate_each_user() {
    let laser = harness::laser().await;
    let policy = SessionPolicy::PerUser;
    let alice = policy.conversation_for("alice");
    let bob = policy.conversation_for("bob");

    for (conversation, text) in [(alice, "alice message"), (bob, "bob message")] {
        let provenance = Provenance::builder().conversation_id(conversation).build();
        laser
            .send_agent(
                AgentTopic::Commands,
                Bytes::copy_from_slice(text.as_bytes()),
                &provenance,
            )
            .await
            .expect("the user message should be sent");
    }

    let alice_context = harness::eventually(|| async {
        let messages = ContextAssembler::builder()
            .conversation_id(alice)
            .topics(vec![AgentTopic::Commands])
            .build()
            .assemble(&laser)
            .await
            .expect("assembling alice's conversation should succeed");
        (!messages.is_empty()).then_some(messages)
    })
    .await;

    assert_eq!(alice_context.len(), 1);
    assert_eq!(alice_context[0].payload.as_slice(), b"alice message");
    // re-deriving the same user yields the same conversation: stable per user
    assert_eq!(policy.conversation_for("alice"), alice);
}

fn collect_payloads(mut acc: Vec<String>, message: &ContextMessage) -> Vec<String> {
    acc.push(String::from_utf8_lossy(&message.payload).into_owned());
    acc
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_session_when_appending_typed_turns_then_context_should_read_them_back() {
    let laser = harness::laser().await;
    let sessions = laser.sessions();
    let session = sessions.create("agent-42");

    session
        .append(
            SessionEventKind::Instruction,
            b"summarize the ticket".to_vec(),
        )
        .await
        .expect("appending the instruction should succeed");
    session
        .append(SessionEventKind::ToolCall, b"search(ticket=42)".to_vec())
        .await
        .expect("appending the tool call should succeed");
    session
        .append(SessionEventKind::ToolResult, b"3 comments found".to_vec())
        .await
        .expect("appending the tool result should succeed");
    session
        .append(
            SessionEventKind::ModelResponse,
            b"it's a login bug".to_vec(),
        )
        .await
        .expect("appending the model response should succeed");

    let turns = harness::eventually(|| async {
        let messages = session
            .context()
            .await
            .expect("assembling the session's context should succeed");
        (messages.len() == 4).then_some(messages)
    })
    .await;
    assert_eq!(turns.len(), 4);

    // `create` derives deterministically: the same id always reaches the same
    // conversation, so re-"creating" it is really a resume.
    assert_eq!(sessions.resume("agent-42").id(), session.id());
    assert_ne!(sessions.start().id(), sessions.start().id());
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_checkpoint_when_more_turns_are_appended_then_state_at_and_replay_should_split_history_there()
 {
    let laser = harness::laser().await;
    let session = laser.sessions().start();

    session
        .append(SessionEventKind::Instruction, b"first".to_vec())
        .await
        .expect("appending the first turn should succeed");
    session
        .append(SessionEventKind::ModelResponse, b"second".to_vec())
        .await
        .expect("appending the second turn should succeed");

    // A checkpoint is a plain tail read, so it is itself subject to the same
    // ingest-visibility lag as any other read here: retry until it reports
    // exactly the two turns already appended, not zero, one, or a stale mix.
    let checkpoint = harness::eventually(|| async {
        let checkpoint = session
            .checkpoint()
            .await
            .expect("reading a checkpoint should succeed");
        let seen = session
            .state_at(checkpoint.clone(), 0usize, |count, _| count + 1)
            .await
            .expect("folding up to the checkpoint should succeed");
        (seen == 2).then_some(checkpoint)
    })
    .await;

    session
        .append(SessionEventKind::ToolResult, b"third".to_vec())
        .await
        .expect("appending the third turn should succeed");

    let before = session
        .state_at(checkpoint.clone(), Vec::new(), collect_payloads)
        .await
        .expect("state_at should fold only up to the checkpoint");
    assert_eq!(before, vec!["first".to_owned(), "second".to_owned()]);

    let after = harness::eventually(|| async {
        let turns = session
            .replay(checkpoint.clone(), Vec::new(), collect_payloads)
            .await
            .expect("replay should fold forward from the checkpoint");
        (turns.len() == 1).then_some(turns)
    })
    .await;
    assert_eq!(after, vec!["third".to_owned()]);
}
