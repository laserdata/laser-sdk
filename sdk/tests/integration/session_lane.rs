use crate::harness;
use async_trait::async_trait;
use iggy::prelude::{HeaderKey, HeaderValue, IggyMessage};
use laser_sdk::agent::{ModelRequest, ModelResponse};
use laser_sdk::govern::{ActionDecision, ActionGovernor, GovernedAction, GovernorMode};
use laser_sdk::prelude::full::*;
use laser_sdk::types::MintUlid;
use laser_sdk::wire::agent::{
    AgentEnvelope, AgentId, AgentKind, Budget, CorrelationId, OPERATION_STATE_DELTA,
    OPERATION_STATE_SNAPSHOT, RecordId, StateDelta, StateSnapshot, TaskState, TokenUsage,
};
use laser_sdk::wire::dispatch::OPERATION_INVOKE_AGENT;
use laser_sdk::wire::framing::{decode_named, encode_named};
use laser_sdk::wire::limits::MAX_MANIFEST_FRAGMENTS;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_state_write_that_fails_when_the_next_write_runs_then_should_reseed_and_apply() {
    let armed = Arc::new(AtomicBool::new(false));
    let laser = harness::laser().await.with_governor(
        Arc::new(BlockArmed {
            armed: armed.clone(),
            operation: "state_delta",
        }),
        GovernorMode::Enforce,
    );
    let (session, lease) = laser
        .sessions()
        .start()
        .agent(agent("writer"))
        .begin()
        .await
        .expect("the session starts");
    let state = session.state();
    state
        .set("a", serde_json::json!(1))
        .await
        .expect("the first write lands");
    armed.store(true, Ordering::SeqCst);
    state
        .set("b", serde_json::json!(2))
        .await
        .expect_err("the governor blocks the second write");
    armed.store(false, Ordering::SeqCst);
    state
        .set("c", serde_json::json!(3))
        .await
        .expect("the third write lands");
    let view = harness::eventually(|| async {
        let view = laser
            .sessions()
            .open(session.conversation())
            .state()
            .get()
            .await
            .ok()?;
        (view.revision >= 2).then_some(view)
    })
    .await;
    assert_eq!(view.document, serde_json::json!({"a": 1, "c": 3}));
    assert_eq!(view.revision, 2);
    drop(lease);
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_more_than_a_window_of_neighbour_records_when_folding_a_session_then_should_read_its_whole_lane()
 {
    let laser = harness::laser().await;
    let (session, lease) = laser
        .sessions()
        .start()
        .agent(agent("folder"))
        .budget(Budget {
            tokens: Some(10),
            cost_micros: None,
        })
        .begin()
        .await
        .expect("the session starts");
    session
        .state()
        .set("k", serde_json::json!(1))
        .await
        .expect("the state write lands");
    session
        .record_model_call(
            ModelRequest::new("m", b"q".to_vec()),
            ModelResponse {
                body: b"a".to_vec(),
                usage: Some(TokenUsage {
                    input_tokens: 20,
                    output_tokens: 0,
                    ..TokenUsage::default()
                }),
                ..ModelResponse::default()
            },
            None,
        )
        .await
        .expect("the model call is recorded");
    laser
        .sessions()
        .control(
            laser.default_stream().expect("a stream"),
            session.conversation(),
        )
        .as_operator(agent("operator"))
        .cancel()
        .await
        .expect("the cancel lands");
    let key = session.conversation().to_string();
    for topic in [AgentTopic::Sessions, AgentTopic::Control] {
        for _ in 0..11 {
            let filler: Vec<IggyMessage> = (0..1_000)
                .map(|_| {
                    IggyMessage::builder()
                        .payload(bytes::Bytes::from_static(b"neighbour"))
                        .build()
                        .expect("a message builds")
                })
                .collect();
            laser
                .topic(topic.topic_string())
                .batch(filler, Some(&key))
                .await
                .expect("the filler lands on the session's partition");
        }
    }
    let lens = laser.sessions().open(session.conversation());
    let view = lens.state().get().await.expect("the lane folds");
    assert_eq!(view.document, serde_json::json!({"k": 1}));
    assert_eq!(view.revision, 1);
    assert!(view.complete);
    assert!(
        lens.over_budget().await.expect("the lane folds"),
        "the start record and the usage are still read"
    );
    assert!(
        lens.cancel_requested()
            .await
            .expect("the control route folds"),
        "the cancel is still read"
    );
    drop(lease);
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_recorded_calls_on_an_agents_own_lane_when_its_runtime_runs_then_should_hand_it_only_real_work()
 {
    let laser = harness::laser().await;
    let handled = Arc::new(Mutex::new(Vec::new()));
    let mut handle = Agent::builder()
        .id("planner".parse().expect("a valid agent id"))
        .listen_on(AgentTopic::Sessions)
        .handler(Recording {
            handled: handled.clone(),
        })
        .build()
        .spawn(laser.clone());
    handle.ready().await.expect("the runtime starts");
    let (session, lease) = laser
        .sessions()
        .start()
        .agent(agent("planner"))
        .begin()
        .await
        .expect("the session starts");
    session
        .model(ModelRequest::new("m", b"prompt".to_vec()), None)
        .await
        .expect("the model request is recorded");
    session
        .tool("lookup", serde_json::json!({"id": 7}))
        .await
        .expect("the tool call is recorded");
    laser
        .agdx(
            AgentTopic::Sessions,
            agent("client"),
            session.conversation().into(),
        )
        .command(CorrelationId::mint(), b"real".to_vec())
        .with_operation(OPERATION_INVOKE_AGENT)
        .with_target(agent("planner"))
        .send()
        .await
        .expect("the work lands");
    harness::eventually(|| {
        let handled = handled.clone();
        async move {
            handled
                .lock()
                .expect("handled lock")
                .contains(&b"real".to_vec())
                .then_some(())
        }
    })
    .await;
    handle.shutdown().await.expect("the runtime drains");
    assert_eq!(
        *handled.lock().expect("handled lock"),
        vec![b"real".to_vec()],
        "the recorded model and tool calls never reach the handler"
    );
    drop(lease);
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_an_ended_labeled_session_when_begun_again_then_should_refuse_it_and_resume_should_reopen_it()
 {
    let laser = harness::laser().await;
    let sessions = laser.sessions();
    let (session, lease) = sessions
        .create("ticket-7")
        .agent(agent("desk"))
        .begin()
        .await
        .expect("the session starts");
    session.end().await.expect("the session ends");
    drop(lease);
    let refused = sessions
        .create("ticket-7")
        .agent(agent("desk"))
        .begin()
        .await
        .map(|_| ())
        .expect_err("an ended session does not begin again");
    assert!(
        matches!(&refused, LaserError::SessionEnded { status, .. } if status == "completed"),
        "{refused:?}"
    );
    let (resumed, lease) = sessions
        .create("ticket-7")
        .agent(agent("desk"))
        .resume()
        .await
        .expect("a completed session resumes");
    assert_eq!(resumed.conversation(), session.conversation());
    resumed.cancel().await.expect("the session is canceled");
    drop(lease);
    let canceled = sessions
        .create("ticket-7")
        .agent(agent("desk"))
        .resume()
        .await
        .map(|_| ())
        .expect_err("a canceled session stays ended");
    assert!(
        matches!(&canceled, LaserError::SessionEnded { status, .. } if status == "canceled"),
        "{canceled:?}"
    );
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_submission_whose_command_fails_when_sent_then_should_end_its_session_failed() {
    let armed = Arc::new(AtomicBool::new(true));
    let laser = harness::laser().await.with_governor(
        Arc::new(BlockArmed {
            armed,
            operation: OPERATION_INVOKE_AGENT,
        }),
        GovernorMode::Enforce,
    );
    let builder = laser
        .sessions()
        .submit(agent("worker"), b"ticket".to_vec())
        .from(agent("intake"));
    let id = builder.id().expect("the stream is set");
    assert_eq!(builder.id().expect("the stream is set"), id);
    builder
        .send()
        .await
        .expect_err("the governor blocks the command");
    let states = harness::eventually(|| async {
        let turns = laser
            .sessions()
            .open(id)
            .context_with(Box::new(LastN(10)))
            .await
            .ok()?;
        let states: Vec<TaskState> = turns
            .iter()
            .filter_map(|turn| {
                let envelope = turn.message.envelope.as_ref()?;
                (envelope.kind == AgentKind::Status).then_some(envelope.task_state?)
            })
            .collect();
        (states.len() >= 2).then_some(states)
    })
    .await;
    assert_eq!(states, [TaskState::Submitted, TaskState::Failed]);
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_cancel_addressed_to_another_agent_when_asked_then_should_not_report_a_cancel() {
    let laser = harness::laser().await;
    let (session, lease) = laser
        .sessions()
        .start()
        .agent(agent("planner"))
        .begin()
        .await
        .expect("the session starts");
    laser
        .agdx(
            AgentTopic::Control,
            agent("operator"),
            session.conversation().into(),
        )
        .command(CorrelationId::mint(), b"{}".to_vec())
        .with_operation(laser_sdk::wire::dispatch::OPERATION_SESSION_CANCEL)
        .with_target(agent("critic"))
        .send()
        .await
        .expect("the cancel lands");
    assert!(
        !session
            .cancel_requested()
            .await
            .expect("the control route folds"),
        "a cancel for the critic does not stop the planner"
    );
    assert!(
        laser
            .sessions()
            .open(session.conversation())
            .as_agent(agent("critic"))
            .cancel_requested()
            .await
            .expect("the control route folds")
    );
    drop(lease);
}

// Blocks the actions of one operation while armed.
#[tokio::test]
#[serial_test::serial(integration)]
async fn given_an_undecodable_state_record_when_the_state_is_read_then_should_skip_it_and_keep_writing()
 {
    let laser = harness::laser().await;
    let (session, lease) = laser
        .sessions()
        .start()
        .agent(agent("writer"))
        .begin()
        .await
        .expect("the session starts");
    let state = session.state();
    state
        .set("a", serde_json::json!(1))
        .await
        .expect("the first write lands");
    let mut undecodable = AgentEnvelope::event(
        RecordId::mint(),
        session.conversation().into(),
        agent("writer"),
        vec![0xff, 0x00],
    );
    undecodable.operation = Some(OPERATION_STATE_DELTA.to_owned());
    session
        .append(undecodable)
        .await
        .expect("the undecodable record appends");
    let reader = laser
        .sessions()
        .open(session.conversation())
        .as_agent(agent("writer"));
    reader
        .state()
        .set("b", serde_json::json!(2))
        .await
        .expect("a write after the undecodable record seeds and lands");
    await_state_deltas(&laser, session.conversation(), 3).await;
    let view = laser
        .sessions()
        .open(session.conversation())
        .state()
        .get()
        .await
        .expect("the state read skips the undecodable record");
    assert_eq!(view.document, serde_json::json!({"a": 1, "b": 2}));
    assert_eq!(view.revision, 2);
    drop(lease);
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_snapshot_at_the_reached_revision_with_another_document_when_the_state_is_read_then_should_keep_the_folded_document()
 {
    let laser = harness::laser().await;
    let (session, lease) = laser
        .sessions()
        .start()
        .agent(agent("writer"))
        .begin()
        .await
        .expect("the session starts");
    session
        .state()
        .set("progress", serde_json::json!(1.0))
        .await
        .expect("the first write lands");
    // The same document from a client that writes whole numbers as integers.
    session
        .append(state_snapshot(
            session.conversation(),
            1,
            serde_json::json!({"progress": 1}),
        ))
        .await
        .expect("the equal snapshot appends");
    // A writer whose own delta never applied names the reached revision.
    session
        .append(state_snapshot(
            session.conversation(),
            1,
            serde_json::json!({"progress": 9}),
        ))
        .await
        .expect("the stale snapshot appends");
    let reader = laser
        .sessions()
        .open(session.conversation())
        .as_agent(agent("writer"));
    reader
        .state()
        .set("b", serde_json::json!(2))
        .await
        .expect("a write after the snapshots seeds and lands");
    await_state_deltas(&laser, session.conversation(), 2).await;
    let view = reader.state().get().await.expect("the state folds");
    assert_eq!(view.document, serde_json::json!({"progress": 1, "b": 2}));
    assert_eq!(view.revision, 2);
    drop(lease);
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_handle_whose_delta_was_stale_when_it_writes_again_then_should_leave_the_winners_document()
 {
    let laser = harness::laser().await;
    let (session, lease) = laser
        .sessions()
        .start()
        .agent(agent("planner"))
        .begin()
        .await
        .expect("the session starts");
    let conversation = session.conversation();
    let second = laser
        .sessions()
        .open(conversation)
        .as_agent(agent("reviewer"));
    second
        .state()
        .set("note", serde_json::json!("joined"))
        .await
        .expect("the second handle's first write lands");
    await_state_deltas(&laser, conversation, 1).await;
    session
        .state()
        .replace(serde_json::json!({"status": "escalated"}))
        .await
        .expect("the first handle's replace lands");
    await_state_deltas(&laser, conversation, 2).await;
    second
        .state()
        .replace(serde_json::json!({"status": "closed"}))
        .await
        .expect("the stale replace appends");
    second
        .state()
        .set("owner", serde_json::json!("reviewer"))
        .await
        .expect("the delta after the stale one appends");
    let deltas = await_state_deltas(&laser, conversation, 4).await;
    let view = session.state().get().await.expect("the state folds");
    assert_eq!(view.document, serde_json::json!({"status": "escalated"}));
    assert_eq!(view.revision, 2);
    assert_eq!(view.head_op_id.as_deref(), Some(deltas[1].op_id.as_str()));
    assert_eq!(
        deltas[1].base_op_id.as_deref(),
        Some(deltas[0].op_id.as_str()),
        "a seeded handle names the delta that produced the revision it read"
    );
    assert_eq!(
        deltas[3].base_op_id.as_deref(),
        Some(deltas[2].op_id.as_str()),
        "a handle names its own last delta, applied or not"
    );
    drop(lease);
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_whole_float_checkpointed_as_an_integer_when_the_writer_snapshots_then_should_write_it()
 {
    let laser = harness::laser().await;
    let (session, lease) = laser
        .sessions()
        .start()
        .agent(agent("writer"))
        .begin()
        .await
        .expect("the session starts");
    let state = session.state();
    state
        .set("progress", serde_json::json!(1.0))
        .await
        .expect("the first write lands");
    session
        .append(state_snapshot(
            session.conversation(),
            1,
            serde_json::json!({"progress": 1}),
        ))
        .await
        .expect("the checkpoint appends");
    harness::eventually(|| async {
        let view = state.get().await.ok()?;
        (view.document == serde_json::json!({"progress": 1})).then_some(())
    })
    .await;
    state
        .snapshot()
        .await
        .expect("the writer still holds the current document");
    drop(lease);
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_concurrent_state_writes_when_sent_then_should_publish_one_at_a_time_in_revision_order()
 {
    let laser = harness::laser().await;
    let (session, lease) = laser
        .sessions()
        .start()
        .agent(agent("writer"))
        .begin()
        .await
        .expect("the session starts");
    let first = session.state();
    let second = session.clone().state();
    let (a, b, c) = tokio::join!(
        first.set("a", serde_json::json!(1)),
        second.set("b", serde_json::json!(2)),
        first.set("c", serde_json::json!(3)),
    );
    for written in [a, b, c] {
        written.expect("each concurrent write lands");
    }
    await_state_deltas(&laser, session.conversation(), 3).await;
    let view = laser
        .sessions()
        .open(session.conversation())
        .state()
        .get()
        .await
        .expect("the state folds");
    assert_eq!(
        view.document,
        serde_json::json!({"a": 1, "b": 2, "c": 3}),
        "every concurrent write applies at its own revision"
    );
    assert_eq!(view.revision, 3);
    drop(lease);
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_more_conversation_records_than_the_manifest_cap_when_assembled_then_should_refuse_it()
 {
    let laser = harness::laser().await;
    let (session, lease) = laser
        .sessions()
        .start()
        .agent(agent("planner"))
        .begin()
        .await
        .expect("the session starts");
    let provenance = Provenance::builder()
        .conversation_id(session.conversation())
        .build();
    let headers = BTreeMap::<HeaderKey, HeaderValue>::try_from(&provenance)
        .expect("the provenance encodes as headers");
    let key = session.conversation().to_string();
    let mut written = 0;
    while written <= MAX_MANIFEST_FRAGMENTS {
        let count = (MAX_MANIFEST_FRAGMENTS + 1 - written).min(1_000);
        let turns: Vec<IggyMessage> = (0..count)
            .map(|_| {
                IggyMessage::builder()
                    .payload(bytes::Bytes::from_static(b"turn"))
                    .user_headers(headers.clone())
                    .build()
                    .expect("a message builds")
            })
            .collect();
        laser
            .topic(AgentTopic::Sessions.topic_string())
            .batch(turns, Some(&key))
            .await
            .expect("the turns land on the session's partition");
        written += count;
    }
    harness::eventually(|| async {
        let turns = session.context_with(Box::new(LastN(2_000))).await.ok()?;
        (turns.len() > MAX_MANIFEST_FRAGMENTS).then_some(())
    })
    .await;
    let refused = session
        .assemble(Box::new(LastN(2_000)))
        .await
        .map(|_| ())
        .expect_err("a selection past the manifest cap is refused");
    assert!(matches!(refused, LaserError::Invalid(_)), "{refused:?}");
    drop(lease);
}

struct BlockArmed {
    armed: Arc<AtomicBool>,
    operation: &'static str,
}

#[async_trait]
impl ActionGovernor for BlockArmed {
    async fn decide(&self, action: &GovernedAction<'_>) -> Result<ActionDecision, LaserError> {
        if self.armed.load(Ordering::SeqCst) && action.operation == Some(self.operation) {
            return Ok(ActionDecision::block("blocked for the test"));
        }
        Ok(ActionDecision::allow())
    }
}

// Records the body of every record the handler receives.
struct Recording {
    handled: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl AgentHandler for Recording {
    async fn handle(&self, message: &AgentMessage, _ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        self.handled
            .lock()
            .expect("handled lock")
            .push(message.body().to_vec());
        Ok(())
    }
}

// Wait until `count` state delta records of `conversation` are readable and
// return the ones that decode, in lane order.
async fn await_state_deltas(
    laser: &Laser,
    conversation: ConversationId,
    count: usize,
) -> Vec<StateDelta> {
    harness::eventually(|| async {
        let records = laser
            .context(conversation)
            .fetch_with(vec![AgentTopic::Sessions], Box::new(LastN(100)))
            .await
            .ok()?;
        let deltas: Vec<_> = records
            .iter()
            .filter_map(|record| record.envelope.as_ref())
            .filter(|envelope| envelope.operation.as_deref() == Some(OPERATION_STATE_DELTA))
            .collect();
        (deltas.len() >= count).then(|| {
            deltas
                .into_iter()
                .filter_map(|envelope| decode_named(&envelope.body).ok())
                .collect()
        })
    })
    .await
}

// A state snapshot record as another writer on the session lane appends it.
fn state_snapshot(
    conversation: ConversationId,
    base_revision: u64,
    document: serde_json::Value,
) -> AgentEnvelope {
    let mut envelope = AgentEnvelope::event(
        RecordId::mint(),
        conversation.into(),
        agent("writer"),
        encode_named(&StateSnapshot {
            base_revision,
            document,
            base_op_id: None,
        })
        .expect("the snapshot encodes"),
    );
    envelope.operation = Some(OPERATION_STATE_SNAPSHOT.to_owned());
    envelope
}

fn agent(name: &str) -> AgentId {
    name.parse().expect("a valid agent id")
}
