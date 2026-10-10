use crate::harness;
use iggy::prelude::{Consumer, Identifier, MessageClient, PollingStrategy, TopicClient};
use laser_sdk::agent::{PendingControl, SessionConfig, derive_session_id};
use laser_sdk::prelude::full::*;
use laser_sdk::wire::agent::{
    AgentEnvelope, AgentId, AgentKind, CorrelationId, RecordId, SessionEnd, SessionStart, TaskState,
};
use laser_sdk::wire::dispatch::DisplayType;
use laser_sdk::wire::framing::decode_named;
use laser_sdk::wire::session::SessionHeartbeat;
use std::time::Duration;

fn agent() -> AgentId {
    "planner".parse().expect("planner is a valid agent id")
}

async fn lifecycle(laser: &Laser, session: ConversationId) -> Vec<(TaskState, Vec<u8>)> {
    harness::eventually(|| async {
        let records = laser
            .sessions()
            .open(session)
            .context()
            .await
            .expect("reading the session lane should succeed");
        let statuses: Vec<_> = records
            .into_iter()
            .filter_map(|turn| {
                let envelope = turn.message.envelope?;
                (envelope.kind == AgentKind::Status).then(|| {
                    (
                        envelope.task_state.expect("session status has a state"),
                        envelope.body,
                    )
                })
            })
            .collect();
        (statuses.len() >= 2).then_some(statuses)
    })
    .await
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_started_session_when_ended_then_should_write_start_and_completed_on_the_lane() {
    let laser = harness::laser().await;
    let (session, lease) = laser
        .sessions()
        .create("incident")
        .agent(agent())
        .namespace("ops")
        .tag("vip")
        .begin()
        .await
        .expect("the session starts");
    assert_eq!(
        session.conversation(),
        derive_session_id(laser.default_stream().expect("stream"), "ops", "incident")
    );
    session.end().await.expect("the session ends");
    session
        .end()
        .await
        .expect("a repeated end retries the same record");
    assert!(matches!(
        session.cancel().await,
        Err(LaserError::Invalid(_))
    ));
    drop(lease);

    let statuses = lifecycle(&laser, session.conversation()).await;
    assert_eq!(statuses[0].0, TaskState::Working);
    let start: SessionStart = decode_named(&statuses[0].1).expect("the start body decodes");
    assert_eq!(start.label.as_deref(), Some("incident"));
    assert_eq!(start.sdk.language, "rust");
    assert_eq!(start.tags, vec!["vip".to_owned()]);
    assert_eq!(start.idle_timeout_micros, Some(300_000_000));
    assert_eq!(statuses[1].0, TaskState::Completed);
    let turns = laser
        .sessions()
        .open(session.conversation())
        .context()
        .await
        .expect("reading the lane succeeds");
    assert_eq!(turns[0].display, DisplayType::SessionStarted);
    assert_eq!(turns[1].display, DisplayType::SessionCompleted);
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_session_run_when_the_work_panics_then_should_write_failed_and_resume_the_panic() {
    let laser = harness::laser().await;
    let (session, lease) = laser
        .sessions()
        .start()
        .agent(agent())
        .begin()
        .await
        .expect("the session starts");
    let id = session.conversation();
    let outcome = tokio::spawn(async move {
        session
            .run(lease, |_session| async move {
                if id != ConversationId::new() {
                    panic!("model provider exploded");
                }
                Ok::<(), LaserError>(())
            })
            .await
    })
    .await;
    assert!(outcome.expect_err("the panic resumes").is_panic());
    let statuses = lifecycle(&laser, id).await;
    assert_eq!(statuses[1].0, TaskState::Failed);
    let end: SessionEnd = decode_named(&statuses[1].1).expect("the end body decodes");
    let error = end.error.expect("the failure carries an error");
    assert_eq!(error.message.as_deref(), Some("model provider exploded"));
    assert!(
        error
            .detail
            .is_some_and(|detail| detail.contains_key("panic"))
    );
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_session_run_when_the_work_fails_then_should_return_the_work_error_and_fail_the_session()
 {
    let laser = harness::laser().await;
    let (session, lease) = laser
        .sessions()
        .start()
        .agent(agent())
        .begin()
        .await
        .expect("the session starts");
    let id = session.conversation();
    let result: Result<(), LaserError> = session
        .run(lease, |_session| async {
            Err(LaserError::Handler("tool timed out".to_owned()))
        })
        .await;
    assert!(matches!(result, Err(LaserError::Handler(message)) if message == "tool timed out"));
    let statuses = lifecycle(&laser, id).await;
    assert_eq!(statuses[1].0, TaskState::Failed);
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_held_lease_when_the_interval_passes_then_should_list_the_session_in_a_heartbeat() {
    let laser = harness::laser().await;
    let sessions = laser.sessions_with(SessionConfig::new().heartbeat(Duration::from_millis(50)));
    let (session, lease) = sessions
        .start()
        .agent(agent())
        .begin()
        .await
        .expect("the session starts");
    let heartbeat = harness::eventually(|| async {
        let stream = Identifier::named(laser.default_stream().expect("stream")).expect("id");
        let topic = Identifier::named("agent.heartbeats").expect("id");
        let reader = Consumer::new(Identifier::named("heartbeat-probe").expect("id"));
        for partition in 0..4 {
            let polled = laser
                .client()
                .poll_messages(
                    &stream,
                    &topic,
                    Some(partition),
                    &reader,
                    &PollingStrategy::offset(0),
                    100,
                    false,
                )
                .await
                .ok()?;
            for message in polled.messages {
                let envelope: AgentEnvelope = decode_named(&message.payload).ok()?;
                if let Ok(beat) = decode_named::<SessionHeartbeat>(&envelope.body) {
                    return Some(beat);
                }
            }
        }
        None
    })
    .await;
    assert!(heartbeat.sessions.contains(&session.conversation().into()));
    assert_eq!(heartbeat.stream, laser.default_stream().expect("stream"));
    drop(lease);
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_an_envelope_of_another_session_when_appended_then_should_be_refused() {
    let laser = harness::laser().await;
    let session = laser.sessions().open(ConversationId::new());
    let envelope = AgentEnvelope::command(
        RecordId::from_u128(1),
        ConversationId::new().into(),
        agent(),
        CorrelationId::from_u128(2),
        b"{}".to_vec(),
    );
    assert!(matches!(
        session.append(envelope).await,
        Err(LaserError::Invalid(_))
    ));
    let mut ok = AgentEnvelope::command(
        RecordId::from_u128(3),
        session.conversation().into(),
        agent(),
        CorrelationId::from_u128(4),
        b"{}".to_vec(),
    );
    ok.operation = Some("summarize".to_owned());
    session.append(ok).await.expect("an own envelope appends");
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_model_and_tool_calls_when_recorded_then_should_write_correlated_records_with_manifest()
 {
    use laser_sdk::agent::{ModelRequest, ModelResponse};
    use laser_sdk::wire::agent::TokenUsage;
    let laser = harness::laser().await;
    let (session, lease) = laser
        .sessions()
        .start()
        .agent(agent())
        .begin()
        .await
        .expect("the session starts");
    let assembled = session
        .assemble(Box::new(LastN(10)))
        .await
        .expect("assembling reads the lane");
    assert_eq!(assembled.manifest.fragments.len(), 1);
    let call = session
        .model(
            ModelRequest::new("gpt-test", br#"{"prompt":"hi","api_key":"k"}"#.to_vec()),
            Some(&assembled),
        )
        .await
        .expect("the model request is recorded");
    call.complete(ModelResponse {
        body: b"hello".to_vec(),
        model: Some("gpt-test-1".to_owned()),
        finish_reason: Some("stop".to_owned()),
        usage: Some(TokenUsage {
            input_tokens: 3,
            output_tokens: 1,
            cost_micros: Some(20),
            ..TokenUsage::default()
        }),
        duration: None,
    })
    .await
    .expect("the model response is recorded");
    let tool = session
        .tool("lookup", serde_json::json!({"id": 7, "token": "secret"}))
        .await
        .expect("the tool call is recorded");
    tool.complete(b"found".to_vec())
        .await
        .expect("the tool result is recorded");
    drop(lease);
    let turns = harness::eventually(|| async {
        let turns = laser
            .sessions()
            .open(session.conversation())
            .context_with(Box::new(LastN(20)))
            .await
            .ok()?;
        (turns.len() >= 6).then_some(turns)
    })
    .await;
    let displays: Vec<DisplayType> = turns.iter().map(|turn| turn.display).collect();
    assert_eq!(
        displays,
        [
            DisplayType::SessionStarted,
            DisplayType::ModelRequest,
            DisplayType::ContextAssembled,
            DisplayType::ModelResponse,
            DisplayType::ToolCall,
            DisplayType::ToolResult,
        ]
    );
    let request = turns[1].message.envelope.as_ref().expect("an envelope");
    assert!(!String::from_utf8_lossy(&request.body).contains("\"k\""));
    let tool_call = turns[4].message.envelope.as_ref().expect("an envelope");
    assert!(String::from_utf8_lossy(&tool_call.body).contains("[redacted]"));
    let response = turns[3].message.envelope.as_ref().expect("an envelope");
    assert_eq!(response.correlation, request.correlation);
    assert_eq!(response.usage.and_then(|usage| usage.cost_micros), Some(20));
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_state_writes_when_folded_then_should_rebuild_the_document_and_snapshot_on_end() {
    let laser = harness::laser().await;
    let (session, lease) = laser
        .sessions()
        .start()
        .agent(agent())
        .begin()
        .await
        .expect("the session starts");
    let state = session.state();
    state
        .set("tasks", serde_json::json!(["triage"]))
        .await
        .expect("set writes a delta");
    state
        .set("step", serde_json::json!(2))
        .await
        .expect("set writes a delta");
    let view = harness::eventually(|| async {
        let view = state.get().await.ok()?;
        (view.revision == 2).then_some(view)
    })
    .await;
    assert!(view.complete);
    assert_eq!(
        view.document,
        serde_json::json!({"tasks": ["triage"], "step": 2})
    );
    session.end().await.expect("the session ends");
    drop(lease);
    let turns = harness::eventually(|| async {
        let turns = laser
            .sessions()
            .open(session.conversation())
            .context()
            .await
            .ok()?;
        (turns.len() >= 5).then_some(turns)
    })
    .await;
    assert_eq!(turns[3].display, DisplayType::StateUpdated);
    assert_eq!(turns[4].display, DisplayType::SessionCompleted);
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_submitted_session_when_its_agent_picks_it_up_then_should_move_from_submitted_to_working()
 {
    struct Ender;
    impl AgentHandler for Ender {
        async fn handle(
            &self,
            _message: &AgentMessage,
            ctx: &AgentCtx<'_>,
        ) -> Result<(), LaserError> {
            ctx.session().end().await
        }
    }
    let laser = harness::laser().await;
    let mut worker = Agent::builder()
        .id("worker".parse().expect("worker is a valid agent id"))
        .listen_on(AgentTopic::Sessions)
        .handler(Ender)
        .build()
        .spawn(laser.clone());
    worker.ready().await.expect("the worker is ready");
    let submitted = laser
        .sessions()
        .submit(
            "worker"
                .parse::<laser_sdk::types::AgentId>()
                .expect("valid agent id"),
            b"{}".to_vec(),
        )
        .from(
            "client"
                .parse::<laser_sdk::types::AgentId>()
                .expect("valid agent id"),
        )
        .label("ticket-7")
        .send()
        .await
        .expect("the session is submitted");
    let turns = harness::eventually(|| async {
        let turns = laser
            .sessions()
            .open(submitted.session)
            .context()
            .await
            .ok()?;
        turns
            .iter()
            .any(|turn| turn.display == DisplayType::SessionCompleted)
            .then_some(turns)
    })
    .await;
    let displays: Vec<DisplayType> = turns.iter().map(|turn| turn.display).collect();
    assert_eq!(displays[0], DisplayType::SessionSubmitted);
    assert!(displays.contains(&DisplayType::SessionResumed));
    worker.shutdown().await.expect("the worker stops");
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_operator_control_when_sent_then_should_ride_the_control_topic() {
    let laser = harness::laser().await;
    laser
        .topic(AgentTopic::Control.topic_string())
        .ensure(4)
        .await
        .expect("provisioning creates the control topic");
    let session = ConversationId::new();
    let control = laser
        .sessions()
        .control(laser.default_stream().expect("stream"), session)
        .as_operator(
            "operator"
                .parse::<laser_sdk::types::AgentId>()
                .expect("valid agent id"),
        );
    control.cancel().await.expect("cancel is sent");
    control.force_cancel().await.expect("force cancel is sent");
    let records = harness::eventually(|| async {
        let records = laser
            .context(session)
            .fetch_with(vec![AgentTopic::Control], Box::new(LastN(10)))
            .await
            .ok()?;
        (records.len() == 2).then_some(records)
    })
    .await;
    let first = records[0].envelope.as_ref().expect("an envelope");
    assert_eq!(first.operation.as_deref(), Some("session_cancel"));
    let second = records[1].envelope.as_ref().expect("an envelope");
    assert_eq!(second.task_state, Some(TaskState::Canceled));
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_declared_partition_layout_when_sending_then_should_route_commands_to_the_addressee_and_replies_to_the_requester()
 {
    use laser_sdk::agent::{SessionLayout, TopicRetention};
    use std::collections::BTreeMap;
    let laser = harness::laser().await;
    let worker: AgentId = "worker".parse().expect("worker is a valid agent id");
    let sessions = laser.sessions_with(SessionConfig::new().layout(
        SessionLayout::PerAgentPartition(BTreeMap::from([(agent(), 0), (worker.clone(), 2)])),
    ));
    sessions
        .bootstrap(3, TopicRetention::expire_after(Duration::from_secs(3600)))
        .await
        .expect("bootstrap creates the agent topics");
    let session = ConversationId::new();
    let correlation = CorrelationId::from_u128(9);
    let command = laser
        .agdx(AgentTopic::Sessions, agent(), session.into())
        .command(correlation, b"{}".to_vec())
        .with_target(worker.clone())
        .send_receipt()
        .await
        .expect("the command is sent");
    let reply = laser
        .agdx(AgentTopic::Sessions, worker, session.into())
        .respond(correlation, b"{}".to_vec())
        .with_target(agent())
        .send_receipt()
        .await
        .expect("the reply is sent");
    assert_eq!(command.partition_id, Some(2));
    assert_eq!(reply.partition_id, Some(0));
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_declared_topic_layout_when_sending_then_should_route_work_to_the_agent_topics() {
    use laser_sdk::agent::{SessionLayout, TopicRetention};
    use std::collections::BTreeMap;
    let laser = harness::laser().await;
    let worker: AgentId = "worker".parse().expect("worker is a valid agent id");
    let sessions = laser.sessions_with(SessionConfig::new().layout(SessionLayout::PerAgentTopic(
        BTreeMap::from([
            (agent(), "planner.inbox".to_owned()),
            (worker.clone(), "worker.inbox".to_owned()),
        ]),
    )));
    sessions
        .bootstrap(2, TopicRetention::expire_after(Duration::from_secs(3600)))
        .await
        .expect("bootstrap creates the agent topics");
    let stream = Identifier::named(laser.default_stream().expect("stream")).expect("stream id");
    for name in ["planner.inbox", "worker.inbox"] {
        let topic = laser
            .client()
            .get_topic(&stream, &Identifier::named(name).expect("topic id"))
            .await
            .expect("read the topic")
            .expect("bootstrap created the declared topic");
        assert_eq!(topic.partitions_count, 2);
    }
    let (session, _lease) = sessions
        .start()
        .agent(laser_sdk::types::AgentId::new("planner").expect("valid agent id"))
        .begin()
        .await
        .expect("the session starts");
    let conversation = session.conversation();
    let correlation = CorrelationId::from_u128(11);
    laser
        .agdx(AgentTopic::Sessions, agent(), conversation.into())
        .command(correlation, b"work".to_vec())
        .with_target(worker.clone())
        .send()
        .await
        .expect("the command is sent");
    laser
        .agdx(AgentTopic::Sessions, worker, conversation.into())
        .respond(correlation, b"done".to_vec())
        .with_target(agent())
        .send()
        .await
        .expect("the reply is sent");
    let read = |name: &'static str| {
        let laser = laser.clone();
        async move {
            let topic: &'static Identifier =
                Box::leak(Box::new(Identifier::named(name).expect("topic id")));
            laser
                .context(conversation)
                .fetch_with(vec![AgentTopic::Custom(topic)], Box::new(LastN(10)))
                .await
                .expect("read the topic")
                .into_iter()
                .filter_map(|message| message.envelope.map(|envelope| envelope.kind))
                .collect::<Vec<_>>()
        }
    };
    let on_worker = harness::eventually(|| async {
        let kinds = read("worker.inbox").await;
        (!kinds.is_empty()).then_some(kinds)
    })
    .await;
    assert_eq!(on_worker, [AgentKind::Command]);
    let on_planner = harness::eventually(|| async {
        let kinds = read("planner.inbox").await;
        (!kinds.is_empty()).then_some(kinds)
    })
    .await;
    assert_eq!(on_planner, [AgentKind::Response]);
    let lane: Vec<AgentKind> = laser
        .context(conversation)
        .fetch_with(vec![AgentTopic::Sessions], Box::new(LastN(10)))
        .await
        .expect("read the lane")
        .into_iter()
        .filter_map(|message| message.envelope.map(|envelope| envelope.kind))
        .collect();
    assert!(
        lane.iter().all(|kind| *kind == AgentKind::Status),
        "{lane:?}"
    );
    session.end().await.expect("the session ends");
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_declared_topic_layout_when_contracting_then_should_complete_through_the_agent_topics()
 {
    use laser_sdk::agent::{SessionLayout, TopicRetention};
    use std::collections::BTreeMap;
    struct Echo;
    impl AgentHandler for Echo {
        async fn handle(
            &self,
            message: &AgentMessage,
            ctx: &AgentCtx<'_>,
        ) -> Result<(), LaserError> {
            ctx.respond(message.body().to_vec()).await
        }
    }
    let laser = harness::laser().await;
    let config = SessionConfig::new().layout(SessionLayout::PerAgentTopic(BTreeMap::from([
        (agent(), "planner.inbox".to_owned()),
        (
            "worker".parse().expect("valid agent id"),
            "worker.inbox".to_owned(),
        ),
    ])));
    laser
        .sessions_with(config.clone())
        .bootstrap(1, TopicRetention::expire_after(Duration::from_secs(3600)))
        .await
        .expect("bootstrap creates the agent topics");
    let mut worker = Agent::builder()
        .id("worker".parse().expect("worker is a valid agent id"))
        .listen_on(AgentTopic::Sessions)
        .respond_on(AgentTopic::Sessions)
        .sessions(config)
        .handler(Echo)
        .build()
        .spawn(laser.clone());
    worker.ready().await.expect("the worker is ready");
    let outcome = laser
        .contract(Router::to("worker".parse().expect("valid agent id")))
        .from("planner".parse().expect("valid agent id"))
        .payload(b"ping".to_vec())
        .inbox_route(InboxRoute::Fixed(AgentTopic::Sessions))
        .deadline(Duration::from_secs(20))
        .send()
        .await
        .expect("the contract runs");
    assert!(matches!(outcome, Contract::Completed(_)), "{outcome:?}");
    worker.shutdown().await.expect("the worker stops");
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_retrieval_and_a_compaction_when_recorded_then_should_show_on_the_lane_with_source_ids()
 {
    use laser_sdk::wire::agent::ContextCompaction;
    use laser_sdk::wire::graph::{ProducerInfo, SourceRef};
    let laser = harness::laser().await;
    let (session, lease) = laser
        .sessions()
        .start()
        .agent(agent())
        .begin()
        .await
        .expect("the session starts");
    session
        .memory()
        .remember(b"the deploy key rotates on fridays".to_vec())
        .send()
        .await
        .expect("the fact is remembered");
    let items = harness::eventually(|| async {
        let items = session
            .memory()
            .recall()
            .keyword("deploy")
            .folded()
            .fetch()
            .await
            .ok()?;
        (!items.is_empty()).then_some(items)
    })
    .await;
    assert_eq!(items.len(), 1);
    session
        .record_retrieval(Some("deploy".to_owned()), &items)
        .await
        .expect("the retrieval is recorded");
    session
        .record_compaction(ContextCompaction {
            summary_at: SourceRef::Message {
                stream: 1,
                topic: 1,
                partition: 0,
                offset: 0,
                generation: None,
                conversation: Some(session.conversation().to_string()),
            },
            covered: vec![(1, 0, 0, 3)],
            summarizer: ProducerInfo {
                name: "summarizer".to_owned(),
                version: "1".to_owned(),
            },
        })
        .await
        .expect("the compaction is recorded");
    drop(lease);
    let turns = harness::eventually(|| async {
        let turns = laser
            .sessions()
            .open(session.conversation())
            .context_with(Box::new(LastN(20)))
            .await
            .ok()?;
        turns
            .iter()
            .any(|turn| turn.display == DisplayType::ContextCompacted)
            .then_some(turns)
    })
    .await;
    let displays: Vec<DisplayType> = turns.iter().map(|turn| turn.display).collect();
    assert!(displays.contains(&DisplayType::ContextRetrieved));
    let first = &turns[0].message;
    assert!(first.timestamp_micros > 0);
    assert!(
        turns
            .iter()
            .all(|turn| turn.message.topic_id == first.topic_id)
    );
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_open_iggy_when_reading_the_session_index_then_should_be_unsupported() {
    let laser = harness::laser().await;
    let sessions = laser.sessions();
    let id = ConversationId::new();
    let error = sessions.get(id).await.expect_err("get is managed");
    assert!(error.is_unsupported(), "{error:?}");
    let error = sessions.list().fetch().await.expect_err("list is managed");
    assert!(error.is_unsupported(), "{error:?}");
    let error = sessions
        .changes(0, 0)
        .await
        .expect_err("changes are managed");
    assert!(error.is_unsupported(), "{error:?}");
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_message_reference_when_read_at_then_should_return_that_record_while_the_topic_lives()
 {
    use laser_sdk::wire::graph::SourceRef;
    let laser = harness::laser().await;
    let (session, lease) = laser
        .sessions()
        .start()
        .agent(agent())
        .begin()
        .await
        .expect("the session starts");
    drop(lease);
    let turns = harness::eventually(|| async {
        let turns = session.context().await.ok()?;
        (!turns.is_empty()).then_some(turns)
    })
    .await;
    let first = &turns[0].message;
    let topic = laser
        .client()
        .get_topic(
            &Identifier::numeric(first.stream_id).expect("stream id"),
            &Identifier::numeric(first.topic_id).expect("topic id"),
        )
        .await
        .expect("topic read")
        .expect("topic exists");
    let at = SourceRef::Message {
        stream: first.stream_id,
        topic: first.topic_id,
        partition: first.id.partition_id,
        offset: first.id.offset,
        generation: Some(topic.created_at.as_micros()),
        conversation: None,
    };
    let record = laser
        .read_at(&at)
        .await
        .expect("read_at succeeds")
        .expect("the record is there");
    assert_eq!(record.id, first.id);
    assert_eq!(record.envelope, first.envelope);
    let stale = SourceRef::Message {
        stream: first.stream_id,
        topic: first.topic_id,
        partition: first.id.partition_id,
        offset: first.id.offset,
        generation: Some(1),
        conversation: None,
    };
    assert!(
        laser
            .read_at(&stale)
            .await
            .expect("read_at succeeds")
            .is_none()
    );
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_fail_on_dead_letter_when_a_record_of_a_session_dead_letters_then_should_fail_that_session()
 {
    struct Rejecter;
    impl AgentHandler for Rejecter {
        async fn handle(
            &self,
            _message: &AgentMessage,
            _ctx: &AgentCtx<'_>,
        ) -> Result<(), LaserError> {
            Err(LaserError::rejected("permanent failure"))
        }
    }
    let laser = harness::laser().await;
    let mut worker = Agent::builder()
        .id("strict".parse().expect("strict is a valid agent id"))
        .listen_on(AgentTopic::Sessions)
        .sessions(SessionConfig::new().fail_on_dead_letter(true))
        .handler(Rejecter)
        .build()
        .spawn(laser.clone());
    worker.ready().await.expect("the worker is ready");
    let session = ConversationId::new();
    laser
        .agdx(AgentTopic::Sessions, agent(), session.into())
        .command(
            <CorrelationId as laser_sdk::types::MintUlid>::mint(),
            b"{}".to_vec(),
        )
        .with_target(
            "strict"
                .parse::<laser_sdk::types::AgentId>()
                .expect("valid agent id"),
        )
        .send()
        .await
        .expect("the command is sent");
    let turns = harness::eventually(|| async {
        let turns = laser.sessions().open(session).context().await.ok()?;
        turns
            .iter()
            .any(|turn| turn.display == DisplayType::SessionFailed)
            .then_some(turns)
    })
    .await;
    let failed = turns
        .iter()
        .find(|turn| turn.display == DisplayType::SessionFailed)
        .and_then(|turn| turn.message.envelope.as_ref())
        .expect("the failure is an envelope");
    assert_eq!(failed.source.as_str(), "strict");
    let end: SessionEnd = decode_named(&failed.body).expect("the failure carries an end body");
    assert!(
        end.error
            .and_then(|error| error.message)
            .is_some_and(|message| message.contains("dead-lettered"))
    );
    worker.shutdown().await.expect("the worker stops");
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_operator_control_when_a_handler_reads_its_session_then_should_see_earlier_and_live_requests()
 {
    struct Watcher(tokio::sync::mpsc::UnboundedSender<(ConversationId, PendingControl)>);
    impl AgentHandler for Watcher {
        async fn handle(
            &self,
            message: &AgentMessage,
            ctx: &AgentCtx<'_>,
        ) -> Result<(), LaserError> {
            let session = ctx.session();
            let initial = session.pending_control().await?;
            let _ = self.0.send((message.provenance.conversation_id, initial));
            if initial != PendingControl::default() {
                return Ok(());
            }
            // Control never interrupts the handler: it reads the requests and
            // decides itself.
            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            while tokio::time::Instant::now() < deadline {
                let flags = session.pending_control().await?;
                if flags != initial {
                    let _ = self.0.send((message.provenance.conversation_id, flags));
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Ok(())
        }
    }
    let laser = harness::laser().await;
    laser
        .topic(AgentTopic::Control.topic_string())
        .ensure(4)
        .await
        .expect("provisioning creates the control topic");
    let stream = laser.default_stream().expect("stream").to_owned();
    let operator: AgentId = "operator".parse().expect("valid agent id");
    let worker_id: AgentId = "watcher".parse().expect("valid agent id");

    // A request sent before the worker exists is folded from the log.
    let canceled = ConversationId::new();
    laser
        .sessions()
        .control(&stream, canceled)
        .as_operator(operator.clone())
        .cancel()
        .await
        .expect("cancel is sent");

    let (observed, mut flags) = tokio::sync::mpsc::unbounded_channel();
    let mut worker = Agent::builder()
        .id(worker_id.as_str().parse().expect("valid agent id"))
        .listen_on(AgentTopic::Sessions)
        .concurrency(ConcurrencyPolicy::SerialPerPartition { max_partitions: 4 })
        .handler(Watcher(observed))
        .build()
        .spawn(laser.clone());
    worker.ready().await.expect("the worker is ready");

    let send = |session: ConversationId| {
        let laser = laser.clone();
        let worker_id = worker_id.clone();
        async move {
            laser
                .agdx(AgentTopic::Sessions, agent(), session.into())
                .command(
                    <CorrelationId as laser_sdk::types::MintUlid>::mint(),
                    b"{}".to_vec(),
                )
                .with_target(worker_id)
                .send()
                .await
                .expect("the command is sent");
        }
    };
    send(canceled).await;
    assert_eq!(
        next_flags(&mut flags).await,
        (
            canceled,
            PendingControl {
                pause_requested: false,
                cancel_requested: true
            }
        )
    );

    // A request sent while the handler runs reaches it through the control
    // subscription.
    let paused = ConversationId::new();
    send(paused).await;
    assert_eq!(
        next_flags(&mut flags).await,
        (paused, PendingControl::default())
    );
    laser
        .sessions()
        .control(&stream, paused)
        .as_operator(operator)
        .pause()
        .await
        .expect("pause is sent");
    assert_eq!(
        next_flags(&mut flags).await,
        (
            paused,
            PendingControl {
                pause_requested: true,
                cancel_requested: false
            }
        )
    );
    worker.shutdown().await.expect("the worker stops");
}

async fn next_flags(
    flags: &mut tokio::sync::mpsc::UnboundedReceiver<(ConversationId, PendingControl)>,
) -> (ConversationId, PendingControl) {
    tokio::time::timeout(Duration::from_secs(15), flags.recv())
        .await
        .expect("the handler reports in time")
        .expect("the handler is alive")
}
