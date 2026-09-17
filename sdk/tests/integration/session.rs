use crate::harness;
use iggy::prelude::Identifier;
use laser_sdk::prelude::full::*;
use std::sync::LazyLock;

static SUPPORT_TURNS: LazyLock<Identifier> =
    LazyLock::new(|| Identifier::named("support.turns").expect("a valid identifier"));
static SUPPORT_REPLIES: LazyLock<Identifier> =
    LazyLock::new(|| Identifier::named("support.replies").expect("a valid identifier"));

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_per_user_sessions_when_messaging_then_should_isolate_each_user() {
    let laser = harness::laser().await;
    let sessions = laser.sessions();
    let alice = sessions.create("alice");
    let bob = sessions.create("bob");

    for (session, text) in [(&alice, "alice message"), (&bob, "bob message")] {
        session
            .append(SessionTurnKind::Instruction, text.as_bytes().to_vec())
            .await
            .expect("the user turn should be appended");
    }

    let turns = harness::eventually(|| async {
        let turns = alice
            .context()
            .await
            .expect("assembling alice's context should succeed");
        (!turns.is_empty()).then_some(turns)
    })
    .await;

    assert_eq!(turns.len(), 1);
    assert_eq!(turns[0].text(), "alice message");
    assert_eq!(
        sessions.create("alice").conversation(),
        alice.conversation()
    );
    assert_ne!(alice.conversation(), bob.conversation());
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_session_when_appending_typed_turns_then_context_should_read_them_back_with_kinds()
{
    let laser = harness::laser().await;
    let sessions = laser.sessions();
    let session = sessions.create("agent-42");
    let script = [
        (SessionTurnKind::Instruction, "summarize the ticket"),
        (SessionTurnKind::ToolCall, "search(ticket=42)"),
        (SessionTurnKind::ToolResult, "3 comments found"),
        (SessionTurnKind::ModelResponse, "it looks like a login bug"),
        (SessionTurnKind::HumanInput, "approved"),
        (SessionTurnKind::Response, "it is a login bug"),
    ];

    for (kind, text) in script {
        session
            .append(kind, text.as_bytes().to_vec())
            .await
            .expect("appending the turn should succeed");
    }

    let turns = harness::eventually(|| async {
        let turns = session
            .context()
            .await
            .expect("assembling the session's context should succeed");
        (turns.len() == script.len()).then_some(turns)
    })
    .await;
    let read_back: Vec<(SessionTurnKind, String)> =
        turns.iter().map(|turn| (turn.kind, turn.text())).collect();
    let expected: Vec<(SessionTurnKind, String)> = script
        .iter()
        .map(|(kind, text)| (*kind, (*text).to_owned()))
        .collect();
    assert_eq!(read_back, expected);

    assert_eq!(
        sessions.open(session.conversation()).conversation(),
        session.conversation()
    );
    assert_ne!(
        sessions.start().conversation(),
        sessions.start().conversation()
    );
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_checkpoint_when_more_turns_are_appended_then_state_at_and_replay_should_split_history_there()
 {
    let laser = harness::laser().await;
    let session = laser.sessions().start();

    session
        .append(SessionTurnKind::Instruction, b"first".to_vec())
        .await
        .expect("appending the first turn should succeed");
    session
        .append(SessionTurnKind::ModelResponse, b"second".to_vec())
        .await
        .expect("appending the second turn should succeed");

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
        .append(SessionTurnKind::ToolResult, b"third".to_vec())
        .await
        .expect("appending the third turn should succeed");
    let after = harness::eventually(|| async {
        let turns = session
            .turns_since(checkpoint.clone())
            .await
            .expect("replay should read forward from the checkpoint");
        (turns.len() == 1).then_some(turns)
    })
    .await;
    assert_eq!(after[0].kind, SessionTurnKind::ToolResult);
    assert_eq!(after[0].text(), "third");

    let before: Vec<String> = session
        .turns_at(checkpoint.clone())
        .await
        .expect("turns up to the checkpoint should read")
        .iter()
        .map(SessionTurn::text)
        .collect();
    assert_eq!(before, vec!["first".to_owned(), "second".to_owned()]);

    let persisted = serde_json::to_string(&checkpoint).expect("a checkpoint serializes");
    let restored: Checkpoint = serde_json::from_str(&persisted).expect("a checkpoint deserializes");
    let replayed = session
        .replay(restored, Vec::new(), |mut acc: Vec<String>, turn| {
            acc.push(turn.text());
            acc
        })
        .await
        .expect("replay from a restored checkpoint should succeed");
    assert_eq!(replayed, vec!["third".to_owned()]);
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_custom_layout_when_turns_ride_their_own_stream_and_topic_then_should_read_and_checkpoint_there()
 {
    let laser = harness::laser().await;
    let stream = format!(
        "{}-support",
        laser.default_stream().expect("a default stream")
    );
    laser
        .stream(&stream)
        .ensure()
        .await
        .expect("the support stream should be created");
    for topic in ["support.turns", "support.replies"] {
        laser
            .stream(&stream)
            .topic(topic)
            .ensure(2)
            .await
            .expect("the support topic should be created");
    }
    let config = SessionConfig::new()
        .stream(stream.clone())
        .topic(
            SessionTurnKind::Instruction,
            AgentTopic::Custom(&SUPPORT_TURNS),
        )
        .topic(
            SessionTurnKind::Response,
            AgentTopic::Custom(&SUPPORT_REPLIES),
        )
        .memory_namespace("support.sessions")
        .context_turns(10);
    let session = laser
        .sessions_with(config)
        .expect("a layout with one topic per kind is valid")
        .create("ticket-7");

    session
        .append(SessionTurnKind::Instruction, b"where is my order".to_vec())
        .await
        .expect("appending on the custom topic should succeed");
    let turns = harness::eventually(|| async {
        let turns = session
            .context()
            .await
            .expect("assembling the custom-layout context should succeed");
        (turns.len() == 1).then_some(turns)
    })
    .await;
    assert_eq!(turns[0].kind, SessionTurnKind::Instruction);
    assert_eq!(turns[0].message.topic, "support.turns");

    let untouched = laser
        .sessions()
        .create("ticket-7")
        .context()
        .await
        .expect("the default layout should still read");
    assert!(untouched.is_empty());

    let checkpoint = harness::eventually(|| async {
        let checkpoint = session
            .checkpoint()
            .await
            .expect("a checkpoint over a custom topic should read");
        (checkpoint.topic_offsets("support.turns").is_some()
            && session
                .turns_at(checkpoint.clone())
                .await
                .expect("turns up to the checkpoint should read")
                .len()
                == 1)
            .then_some(checkpoint)
    })
    .await;
    session
        .append(SessionTurnKind::Response, b"shipped yesterday".to_vec())
        .await
        .expect("appending the response should succeed");
    let since = harness::eventually(|| async {
        let turns = session
            .turns_since(checkpoint.clone())
            .await
            .expect("turns after the checkpoint should read");
        (turns.len() == 1).then_some(turns)
    })
    .await;
    assert_eq!(since[0].kind, SessionTurnKind::Response);
}
