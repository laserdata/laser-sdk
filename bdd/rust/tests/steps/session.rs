use crate::common::test_iggy::fresh_laser;
use crate::common::world::LaserWorld;
use cucumber::{given, then, when};
use laser_sdk::agent::{
    AgentHandle, AssembledContext, ModelRequest, ModelResponse, SessionConfig, SessionLayout,
    SessionLease, Sessions, TopicRetention, derive_session_id,
};
use laser_sdk::govern::{ActionDecision, ActionGovernor, ActionKind, GovernedAction, GovernorMode};
use laser_sdk::prelude::full::*;
use laser_sdk::types::MintUlid;
use laser_sdk::wire::agent::{
    AgentEnvelope, AgentId, ContextManifest, CorrelationId, Fragment, OPERATION_SESSION, RecordId,
    SessionEnd,
};
use laser_sdk::wire::framing::decode_named;
use laser_sdk::wire::graph::SourceRef;
use laser_sdk::wire::session::SessionHeartbeat;
use laser_sdk::wire::topics::AGENT_HEARTBEATS;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

async fn eventually<T>(mut read: impl AsyncFnMut() -> Option<T>) -> T {
    for _ in 0..200 {
        if let Some(value) = read().await {
            return value;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("the session read did not converge within 10s");
}

fn agent(name: &str) -> AgentId {
    name.parse().expect("a valid agent id")
}

fn names(list: &str) -> Vec<String> {
    list.split(", ")
        .map(|name| name.trim_matches('"').to_owned())
        .collect()
}

// The records of `session` on its lane, read until `done` holds.
async fn lane(
    world: &LaserWorld,
    session: ConversationId,
    done: impl Fn(&[SessionTurn]) -> bool,
) -> Vec<SessionTurn> {
    let laser = world.laser().clone();
    eventually(async || {
        let turns = laser
            .sessions()
            .open(session)
            .context_with(Box::new(LastN(200)))
            .await
            .ok()?;
        done(&turns).then_some(turns)
    })
    .await
}

fn displays(turns: &[SessionTurn]) -> Vec<String> {
    turns
        .iter()
        .map(|turn| turn.display.as_str().to_owned())
        .collect()
}

struct EndsItsSession;

impl AgentHandler for EndsItsSession {
    async fn handle(&self, _message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        ctx.session().end().await
    }
}

#[when(regex = r#"^agent "([^"]+)" starts the session "([^"]+)"$"#)]
async fn start_session(world: &mut LaserWorld, owner: String, label: String) {
    if world.managed.active() {
        return crate::steps::managed_sessions::start_managed(world, owner, label).await;
    }
    let (session, lease) = sessions(world)
        .create(label)
        .agent(agent(&owner))
        .begin()
        .await
        .expect("the session starts");
    world.session = Some(session);
    world.session_lease = Some(lease);
}

#[when("the session ends")]
async fn end_session(world: &mut LaserWorld) {
    world.session().end().await.expect("the session ends");
}

#[when("I cancel the session")]
async fn cancel_session(world: &mut LaserWorld) {
    world.last_result = Some(
        world
            .session()
            .cancel()
            .await
            .map_err(|error| format!("{error:?}")),
    );
}

#[when(
    regex = r#"^agent "([^"]+)" runs the session "([^"]+)" with work that fails with "([^"]+)"$"#
)]
async fn run_failing(world: &mut LaserWorld, owner: String, label: String, message: String) {
    let (session, lease) = world
        .laser()
        .sessions()
        .create(label)
        .agent(agent(&owner))
        .begin()
        .await
        .expect("the session starts");
    world.session = Some(session.clone());
    let failure = message.clone();
    let result: Result<(), LaserError> = session
        .run(lease, |_session| async move {
            Err(LaserError::Handler(failure))
        })
        .await;
    assert!(result.is_err(), "the work error is returned");
}

#[then(regex = r#"^the session lane shows (".+")$"#)]
async fn lane_shows(world: &mut LaserWorld, list: String) {
    let expected = names(&list);
    let session = world.session().conversation();
    let turns = lane(world, session, |turns| turns.len() >= expected.len()).await;
    assert_eq!(displays(&turns), expected);
}

#[then(regex = r#"^the session failure message is "([^"]+)"$"#)]
async fn failure_message(world: &mut LaserWorld, message: String) {
    let session = world.session().conversation();
    let turns = lane(world, session, |turns| {
        turns
            .iter()
            .any(|turn| turn.display.as_str() == "session.failed")
    })
    .await;
    let failed = turns
        .iter()
        .find(|turn| turn.display.as_str() == "session.failed")
        .and_then(|turn| turn.message.envelope.clone())
        .expect("a failed record");
    let end: SessionEnd = decode_named(&failed.body).expect("the end body decodes");
    let recorded = end
        .error
        .and_then(|error| error.message)
        .expect("the failure carries a message");
    assert!(recorded.contains(&message), "got {recorded}");
}

#[then(regex = r#"^the session "([^"]+)" has the same id when created twice$"#)]
async fn same_id(world: &mut LaserWorld, label: String) {
    let sessions = world.laser().sessions();
    let first = sessions.create(label.clone()).id().expect("an id");
    let second = sessions.create(label.clone()).id().expect("an id");
    assert_eq!(first, second);
    let stream = sessions.stream().expect("a stream");
    assert_eq!(first, derive_session_id(stream, "", &label));
}

#[then(regex = r#"^the sessions "([^"]+)" and "([^"]+)" have different ids$"#)]
async fn different_ids(world: &mut LaserWorld, first: String, second: String) {
    let sessions = world.laser().sessions();
    assert_ne!(
        sessions.create(first).id().expect("an id"),
        sessions.create(second).id().expect("an id")
    );
}

#[when(regex = r#"^the session records the message "([^"]+)"$"#)]
async fn record_message(world: &mut LaserWorld, text: String) {
    let session = world.session().clone();
    let source = session.agent().cloned().expect("the session has an agent");
    let envelope = AgentEnvelope::event(
        <RecordId as laser_sdk::types::MintUlid>::mint(),
        session.conversation().into(),
        source,
        text.into_bytes(),
    )
    .with_operation("note");
    session
        .append(envelope)
        .await
        .expect("the message is recorded");
}

#[when(regex = r"^I take a session checkpoint after (\d+) records?$")]
async fn take_checkpoint(world: &mut LaserWorld, records: usize) {
    let session = world.session().clone();
    let checkpoint = eventually(async || {
        let checkpoint = session.checkpoint().await.ok()?;
        let seen = session.turns_at(checkpoint.clone()).await.ok()?;
        (seen.len() == records).then_some(checkpoint)
    })
    .await;
    world.checkpoint = Some(checkpoint);
}

#[then(regex = r#"^the records since the checkpoint are "([^"]+)"$"#)]
async fn records_since(world: &mut LaserWorld, text: String) {
    let session = world.session().clone();
    let checkpoint = world.checkpoint.clone().expect("a checkpoint was taken");
    let turns = eventually(async || {
        let turns = session.turns_since(checkpoint.clone()).await.ok()?;
        (turns.len() == 1).then_some(turns)
    })
    .await;
    assert_eq!(turns[0].text(), text);
}

#[then(regex = r#"^the last record at the checkpoint is "([^"]+)"$"#)]
async fn last_at_checkpoint(world: &mut LaserWorld, text: String) {
    let checkpoint = world.checkpoint.clone().expect("a checkpoint was taken");
    let turns = world
        .session()
        .turns_at(checkpoint)
        .await
        .expect("records at the checkpoint read");
    assert_eq!(turns.last().map(SessionTurn::text), Some(text));
}

#[when(
    regex = r#"^the session records a model call to "([^"]+)" whose request carries an api key$"#
)]
async fn record_model(world: &mut LaserWorld, model: String) {
    world
        .session()
        .record_model_call(
            ModelRequest::new(model, br#"{"prompt":"hi","api_key":"k-secret"}"#.to_vec()),
            ModelResponse {
                body: b"hello".to_vec(),
                ..ModelResponse::default()
            },
            None,
        )
        .await
        .expect("the model call is recorded");
}

#[when(regex = r#"^the session records a call of tool "([^"]+)" whose arguments carry a token$"#)]
async fn record_tool(world: &mut LaserWorld, tool: String) {
    let call = world
        .session()
        .tool(tool, serde_json::json!({"id": 7, "token": "t-secret"}))
        .await
        .expect("the tool call is recorded");
    call.complete(b"found".to_vec())
        .await
        .expect("the tool result is recorded");
}

#[then("no recorded call carries a secret value")]
async fn no_secret(world: &mut LaserWorld) {
    let session = world.session().conversation();
    let turns = lane(world, session, |turns| turns.len() >= 5).await;
    for turn in turns {
        let text = turn.text();
        assert!(
            !text.contains("k-secret") && !text.contains("t-secret"),
            "{text}"
        );
    }
}

#[when(regex = r#"^the session state sets "([^"]+)" to (.+)$"#)]
async fn state_set(world: &mut LaserWorld, key: String, value: String) {
    let value: serde_json::Value = serde_json::from_str(&value).expect("a JSON value");
    world
        .session()
        .state()
        .set(&key, value)
        .await
        .expect("the state delta is written");
}

#[then(
    regex = r#"^the folded session state has "([^"]+)" (\S+) and "([^"]+)" (\S+) at revision (\d+)$"#
)]
async fn folded_state(
    world: &mut LaserWorld,
    first: String,
    first_value: String,
    second: String,
    second_value: String,
    revision: u64,
) {
    let state = world.session().state();
    let view = eventually(async || {
        let view = state.get().await.ok()?;
        (view.revision == revision).then_some(view)
    })
    .await;
    let first_value: serde_json::Value = serde_json::from_str(&first_value).expect("JSON");
    let second_value: serde_json::Value = serde_json::from_str(&second_value).expect("JSON");
    assert_eq!(view.document[&first], first_value);
    assert_eq!(view.document[&second], second_value);
    assert!(view.complete);
}

#[then(regex = r#"^the session lane shows (\d+) "([^"]+)" records$"#)]
async fn lane_counts(world: &mut LaserWorld, count: usize, display: String) {
    let session = world.session().conversation();
    let turns = lane(world, session, |turns| {
        turns
            .iter()
            .filter(|turn| turn.display.as_str() == display)
            .count()
            >= count
    })
    .await;
    let seen = turns
        .iter()
        .filter(|turn| turn.display.as_str() == display)
        .count();
    assert_eq!(seen, count);
}

#[given(regex = r#"^agent "([^"]+)" ends every session it handles$"#)]
async fn ending_worker(world: &mut LaserWorld, name: String) {
    let mut worker = Agent::builder()
        .id(name.parse().expect("a valid agent id"))
        .listen_on(AgentTopic::Sessions)
        .handler(EndsItsSession)
        .maybe_sessions(world.native.config.clone())
        .build()
        .spawn(world.laser().clone());
    worker.ready().await.expect("the worker is ready");
    world.worker = Some(worker);
}

#[when(regex = r#"^"([^"]+)" submits a session to agent "([^"]+)"$"#)]
async fn submit_session(world: &mut LaserWorld, submitter: String, target: String) {
    let submitted = world
        .laser()
        .sessions()
        .submit(agent(&target), b"{}".to_vec())
        .from(agent(&submitter))
        .send()
        .await
        .expect("the session is submitted");
    world.other_session = Some(submitted.session);
}

#[then(regex = r#"^the submitted session starts as "([^"]+)" and reaches "([^"]+)"$"#)]
async fn submitted_reaches(world: &mut LaserWorld, first: String, last: String) {
    let session = world.other_session.expect("a session was submitted");
    let turns = lane(world, session, |turns| {
        turns.iter().any(|turn| turn.display.as_str() == last)
    })
    .await;
    assert_eq!(turns[0].display.as_str(), first);
    if let Some(worker) = world.worker.take() {
        worker.shutdown().await.expect("the worker stops");
    }
}

#[when(regex = r#"^operator "([^"]+)" asks to cancel the session "([^"]+)"$"#)]
async fn operator_cancel(world: &mut LaserWorld, operator: String, label: String) {
    let laser = world.laser().clone();
    laser
        .topic(AgentTopic::Control.topic_string())
        .ensure(1)
        .await
        .expect("provisioning creates the control topic");
    let stream = laser.default_stream().expect("a stream").to_owned();
    let session = derive_session_id(&stream, "", &label);
    laser
        .sessions()
        .control(&stream, session)
        .as_operator(agent(&operator))
        .cancel()
        .await
        .expect("the cancel request is sent");
    world.other_session = Some(session);
}

#[then(regex = r#"^the control topic holds a "([^"]+)" request for the session$"#)]
async fn control_holds(world: &mut LaserWorld, operation: String) {
    let session = world.other_session.expect("a session was controlled");
    let laser = world.laser().clone();
    let records = eventually(async || {
        let records = laser
            .context(session)
            .fetch_with(vec![AgentTopic::Control], Box::new(LastN(10)))
            .await
            .ok()?;
        (!records.is_empty()).then_some(records)
    })
    .await;
    let envelope = records[0].envelope.as_ref().expect("an envelope");
    assert_eq!(envelope.operation.as_deref(), Some(operation.as_str()));
}

#[given(
    regex = r#"^the agents use a declared partition layout with "([^"]+)" on (\d+) and "([^"]+)" on (\d+)$"#
)]
async fn declared_layout(
    world: &mut LaserWorld,
    first: String,
    first_partition: u32,
    second: String,
    second_partition: u32,
) {
    world.laser().sessions_with(
        SessionConfig::new().layout(SessionLayout::PerAgentPartition(BTreeMap::from([
            (agent(&first), first_partition),
            (agent(&second), second_partition),
        ]))),
    );
}

#[when(
    regex = r#"^"([^"]+)" sends a command to "([^"]+)" in a new session and "([^"]+)" replies$"#
)]
async fn routed_exchange(
    world: &mut LaserWorld,
    requester: String,
    target: String,
    _responder: String,
) {
    let laser = world.laser().clone();
    let session = ConversationId::new();
    let correlation = laser_sdk::wire::agent::CorrelationId::from_u128(7);
    let command = laser
        .agdx(AgentTopic::Sessions, agent(&requester), session.into())
        .command(correlation, b"{}".to_vec())
        .with_target(agent(&target))
        .send_receipt()
        .await
        .expect("the command is sent");
    let reply = laser
        .agdx(AgentTopic::Sessions, agent(&target), session.into())
        .respond(correlation, b"{}".to_vec())
        .with_target(agent(&requester))
        .send_receipt()
        .await
        .expect("the reply is sent");
    world.routed = Some((command.partition_id, reply.partition_id));
    world.routed_session = Some(session);
}

#[given(
    regex = r#"^the agents use a declared topic layout with "([^"]+)" on "([^"]+)" and "([^"]+)" on "([^"]+)"$"#
)]
async fn declared_topic_layout(
    world: &mut LaserWorld,
    first: String,
    first_topic: String,
    second: String,
    second_topic: String,
) {
    world
        .laser()
        .sessions_with(
            SessionConfig::new().layout(SessionLayout::PerAgentTopic(BTreeMap::from([
                (agent(&first), first_topic),
                (agent(&second), second_topic),
            ]))),
        )
        .bootstrap(
            1,
            laser_sdk::agent::TopicRetention::expire_after(Duration::from_secs(86_400)),
        )
        .await
        .expect("bootstrap creates the declared topics");
}

// The envelope kinds of the routed session's records on `topic`.
async fn routed_kinds(world: &LaserWorld, topic: &str) -> Vec<laser_sdk::wire::agent::AgentKind> {
    let session = world.routed_session.expect("a routed exchange ran");
    let topic: &'static laser_sdk::iggy::prelude::Identifier = Box::leak(Box::new(
        laser_sdk::iggy::prelude::Identifier::named(topic).expect("a valid topic name"),
    ));
    world
        .laser()
        .context(session)
        .fetch_with(vec![AgentTopic::Custom(topic)], Box::new(LastN(10)))
        .await
        .map(|messages| {
            messages
                .into_iter()
                .filter_map(|message| message.envelope.map(|envelope| envelope.kind))
                .collect()
        })
        .unwrap_or_default()
}

#[then(
    regex = r#"^the topic "([^"]+)" holds the command and the topic "([^"]+)" holds the reply$"#
)]
async fn routed_topics(world: &mut LaserWorld, work: String, reply: String) {
    use laser_sdk::wire::agent::AgentKind;
    let commands = eventually(async || {
        let kinds = routed_kinds(world, &work).await;
        (!kinds.is_empty()).then_some(kinds)
    })
    .await;
    assert_eq!(commands, [AgentKind::Command]);
    let replies = eventually(async || {
        let kinds = routed_kinds(world, &reply).await;
        (!kinds.is_empty()).then_some(kinds)
    })
    .await;
    assert_eq!(replies, [AgentKind::Response]);
}

#[then("the session lane holds neither")]
async fn lane_holds_neither(world: &mut LaserWorld) {
    let lane = routed_kinds(world, laser_sdk::wire::topics::AGENT_SESSIONS).await;
    assert!(lane.is_empty(), "{lane:?}");
}

#[then(regex = r"^the command landed on partition (\d+) and the reply on partition (\d+)$")]
async fn routed_partitions(world: &mut LaserWorld, command: u32, reply: u32) {
    assert_eq!(world.routed, Some((Some(command), Some(reply))));
}

#[when(regex = r#"^the session remembers "([^"]+)" through its linked memory$"#)]
async fn remember_linked(world: &mut LaserWorld, text: String) {
    world
        .session()
        .linked_memory()
        .remember(text.into_bytes())
        .send()
        .await
        .expect("the item is remembered");
}

#[then(regex = r#"^the remembered item names producer "([^"]+)"$"#)]
async fn remembered_producer(world: &mut LaserWorld, producer: String) {
    let memory = world.session().memory();
    let items = eventually(async || {
        let items = memory.recall().recent().folded().fetch().await.ok()?;
        (!items.is_empty()).then_some(items)
    })
    .await;
    let named = items[0]
        .producer
        .as_ref()
        .map(|producer| producer.name.clone());
    assert_eq!(named, Some(producer));
}

#[when(regex = r#"^I publish (\d+) unrelated records to topic "([^"]+)"$"#)]
async fn publish_unrelated(world: &mut LaserWorld, count: usize, topic: String) {
    let topic = world.laser().topic(topic);
    let mut batch = topic.publish_batch();
    for index in 0..count {
        batch = batch.add_payload(format!("unrelated-{index}").into_bytes());
    }
    batch
        .send()
        .await
        .expect("the unrelated records should be published");
}

#[then(regex = r#"^the session context holds only the message "([^"]+)"$"#)]
async fn context_only(world: &mut LaserWorld, text: String) {
    let session = world.session().clone();
    let texts = eventually(async || {
        let turns = session.context().await.ok()?;
        let texts: Vec<String> = turns.iter().map(SessionTurn::text).collect();
        (texts == [text.clone()]).then_some(texts)
    })
    .await;
    assert_eq!(texts, vec![text]);
}

/// The state of the native session scenarios that the shared world fields do
/// not cover.
#[derive(Default)]
pub struct Native {
    config: Option<SessionConfig>,
    refusals: Option<Arc<AtomicUsize>>,
    snapshot_refusals: Option<Arc<AtomicUsize>>,
    outcomes: Vec<(&'static str, Result<(), String>)>,
    winner: Option<&'static str>,
    agents: Vec<AgentHandle>,
    handled: BTreeMap<String, Arc<Mutex<Vec<String>>>>,
    partitions: Vec<Option<u32>>,
    assembled: Option<AssembledContext>,
    second_stream: Option<String>,
    leases: BTreeMap<String, (String, ConversationId, SessionLease)>,
    labels: BTreeMap<String, (String, ConversationId)>,
}

// The session factory of the scenario: the configured one, or the default.
fn sessions(world: &LaserWorld) -> Sessions {
    match &world.native.config {
        Some(config) => world.laser().sessions_with(config.clone()),
        None => world.laser().sessions(),
    }
}

fn retention() -> TopicRetention {
    TopicRetention::expire_after(Duration::from_secs(86_400))
}

const TERMINAL: [&str; 3] = ["session.completed", "session.failed", "session.canceled"];

fn terminal_records(turns: &[SessionTurn]) -> Vec<&SessionTurn> {
    turns
        .iter()
        .filter(|turn| TERMINAL.contains(&turn.display.as_str()))
        .collect()
}

// Blocks the next `count` session status publishes, then allows everything.
struct RefuseSessionStatus {
    remaining: Arc<AtomicUsize>,
    snapshots: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl ActionGovernor for RefuseSessionStatus {
    async fn decide(&self, action: &GovernedAction<'_>) -> Result<ActionDecision, LaserError> {
        let session_status =
            action.kind == ActionKind::Status && action.operation == Some(OPERATION_SESSION);
        if session_status
            && self
                .remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                    left.checked_sub(1)
                })
                .is_ok()
        {
            return Ok(ActionDecision::block("injected publish refusal"));
        }
        if action.operation == Some(laser_sdk::wire::agent::OPERATION_STATE_SNAPSHOT)
            && self
                .snapshots
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                    left.checked_sub(1)
                })
                .is_ok()
        {
            return Ok(ActionDecision::block("injected publish refusal"));
        }
        Ok(ActionDecision::allow())
    }
}

#[given("session publishes pass through a policy that can refuse them")]
async fn refusing_policy(world: &mut LaserWorld) {
    let remaining = Arc::new(AtomicUsize::new(0));
    let snapshots = Arc::new(AtomicUsize::new(0));
    let governed = world.laser().with_governor(
        Arc::new(RefuseSessionStatus {
            remaining: Arc::clone(&remaining),
            snapshots: Arc::clone(&snapshots),
        }),
        GovernorMode::Enforce,
    );
    world.laser = Some(governed);
    world.native.refusals = Some(remaining);
    world.native.snapshot_refusals = Some(snapshots);
}

#[when("the policy refuses the next session status publish")]
async fn refuse_next(world: &mut LaserWorld) {
    world
        .native
        .refusals
        .as_ref()
        .expect("a refusing policy is installed")
        .store(1, Ordering::SeqCst);
}

#[when("I end the session")]
async fn end_captured(world: &mut LaserWorld) {
    world.last_result = Some(
        world
            .session()
            .end()
            .await
            .map_err(|error| format!("{error:?}")),
    );
}

#[then("every terminal record on the lane carries one record id")]
async fn one_terminal_record(world: &mut LaserWorld) {
    let session = world.session().conversation();
    let turns = lane(world, session, |turns| !terminal_records(turns).is_empty()).await;
    let records: BTreeSet<String> = terminal_records(&turns)
        .iter()
        .map(|turn| {
            let envelope = turn.message.envelope.as_ref().expect("an envelope");
            envelope.record.expect("a terminal record id").to_string()
        })
        .collect();
    assert_eq!(records.len(), 1, "terminal record ids: {records:?}");
}

#[when(regex = r"^(\d+) clones of the session end it while (\d+) other clones cancel it at once$")]
async fn racing_clones(world: &mut LaserWorld, enders: usize, cancelers: usize) {
    let session = world.session().clone();
    let total = enders + cancelers;
    let start = Arc::new(tokio::sync::Barrier::new(total));
    let mut calls = tokio::task::JoinSet::new();
    for index in 0..total {
        let clone = session.clone();
        let start = Arc::clone(&start);
        // Alternate the verbs so neither side gets a head start.
        let ends = index % 2 == 0 && index / 2 < enders || index / 2 >= cancelers;
        calls.spawn(async move {
            start.wait().await;
            let (verb, result) = if ends {
                ("end", clone.end().await)
            } else {
                ("cancel", clone.cancel().await)
            };
            (verb, result.map_err(|error| format!("{error:?}")))
        });
    }
    world.native.outcomes = calls.join_all().await;
}

#[then("the calls of one verb all succeed and the calls of the other all fail as invalid")]
async fn one_verb_wins(world: &mut LaserWorld) {
    let outcomes = &world.native.outcomes;
    let winners: BTreeSet<&str> = outcomes
        .iter()
        .filter(|(_, result)| result.is_ok())
        .map(|(verb, _)| *verb)
        .collect();
    assert_eq!(winners.len(), 1, "outcomes: {outcomes:?}");
    let winner = *winners.first().expect("a winning verb");
    for (verb, result) in outcomes {
        match result {
            Ok(()) => assert_eq!(*verb, winner),
            Err(error) => {
                assert_ne!(*verb, winner);
                assert!(error.contains("Invalid"), "expected Invalid, got {error}");
            }
        }
    }
    world.native.winner = Some(winner);
}

#[then(regex = r"^the lane holds (\d+) terminal records of the winning verb under one record id$")]
async fn winner_on_lane(world: &mut LaserWorld, count: usize) {
    let display = match world.native.winner.expect("a winning verb") {
        "end" => "session.completed",
        _ => "session.canceled",
    };
    let session = world.session().conversation();
    let turns = lane(world, session, |turns| {
        terminal_records(turns).len() >= count
    })
    .await;
    let terminal = terminal_records(&turns);
    assert_eq!(terminal.len(), count);
    assert!(
        terminal.iter().all(|turn| turn.display.as_str() == display),
        "terminal records: {:?}",
        displays(&turns)
    );
    let records: BTreeSet<String> = terminal
        .iter()
        .map(|turn| {
            let envelope = turn.message.envelope.as_ref().expect("an envelope");
            envelope.record.expect("a terminal record id").to_string()
        })
        .collect();
    assert_eq!(records.len(), 1, "terminal record ids: {records:?}");
}

#[given("a fresh stream bootstrapped for sessions in the single-partition layout")]
async fn single_partition_stream(world: &mut LaserWorld) {
    let fresh = fresh_laser().await;
    fresh
        .laser
        .sessions_with(
            SessionConfig::new()
                .layout(SessionLayout::SinglePartition)
                .register_source(false),
        )
        .bootstrap(4, retention())
        .await
        .expect("bootstrap the single-partition stream");
    world.platform = fresh.iggy;
    world.laser = Some(fresh.laser);
}

// Records the body of every command it handles.
struct RecordsHandled {
    seen: Arc<Mutex<Vec<String>>>,
}

impl AgentHandler for RecordsHandled {
    async fn handle(&self, message: &AgentMessage, _ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        let body = match &message.envelope {
            Some(envelope) => envelope.body.clone(),
            None => message.payload.clone(),
        };
        self.seen
            .lock()
            .expect("handled records")
            .push(String::from_utf8_lossy(&body).into_owned());
        Ok(())
    }
}

#[given(regex = r#"^agents "([^"]+)" and "([^"]+)" each record the commands they handle$"#)]
async fn recording_agents(world: &mut LaserWorld, first: String, second: String) {
    for name in [first, second] {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let mut handle = Agent::builder()
            .id(name.parse().expect("a valid agent id"))
            .listen_on(AgentTopic::Sessions)
            .handler(RecordsHandled {
                seen: Arc::clone(&seen),
            })
            .build()
            .spawn(world.laser().clone());
        handle.ready().await.expect("the agent is ready");
        world.native.agents.push(handle);
        world.native.handled.insert(name, seen);
    }
}

#[when(regex = r#"^"([^"]+)" sends (.+) in separate sessions$"#)]
async fn send_separately(world: &mut LaserWorld, sender: String, list: String) {
    let laser = world.laser().clone();
    for pair in list.split(", ") {
        let (body, target) = pair
            .split_once(" to ")
            .expect("a pair reads \"<body>\" to \"<agent>\"");
        let receipt = laser
            .agdx(
                AgentTopic::Sessions,
                agent(&sender),
                ConversationId::new().into(),
            )
            .command(
                CorrelationId::mint(),
                body.trim_matches('"').as_bytes().to_vec(),
            )
            .with_target(agent(target.trim_matches('"')))
            .send_receipt()
            .await
            .expect("the command is sent");
        world.native.partitions.push(receipt.partition_id);
    }
}

#[then("every command landed on the same partition")]
async fn same_partition(world: &mut LaserWorld) {
    let partitions: BTreeSet<Option<u32>> = world.native.partitions.iter().copied().collect();
    assert_eq!(partitions.len(), 1, "partitions: {partitions:?}");
    assert!(partitions.first().is_some_and(Option::is_some));
}

#[then(regex = r#"^agent "([^"]+)" handled exactly (".+")$"#)]
async fn handled_exactly(world: &mut LaserWorld, name: String, list: String) {
    let expected = names(&list);
    let seen = Arc::clone(world.native.handled.get(&name).expect("a recording agent"));
    let read = || seen.lock().expect("handled records").clone();
    eventually(async || (read() == expected).then_some(())).await;
    // The other agent's records share the partition, so give a wrongly
    // handled one time to show up before the final check.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(read(), expected);
}

#[when(
    regex = r#"^"([^"]+)" writes the control requests (".+") for agent "([^"]+)" on the session lane$"#
)]
async fn misplaced_control(world: &mut LaserWorld, sender: String, list: String, target: String) {
    let session = world.session().conversation();
    let laser = world.laser().clone();
    for operation in names(&list) {
        laser
            .agdx(AgentTopic::Sessions, agent(&sender), session.into())
            .command(CorrelationId::mint(), b"{}".to_vec())
            .with_operation(operation)
            .with_target(agent(&target))
            .send()
            .await
            .expect("the misplaced control request is written");
    }
}

#[when(regex = r#"^"([^"]+)" sends work to agent "([^"]+)" in the session$"#)]
async fn send_work(world: &mut LaserWorld, sender: String, target: String) {
    let session = world.session().conversation();
    world
        .laser()
        .agdx(AgentTopic::Sessions, agent(&sender), session.into())
        .command(CorrelationId::mint(), b"work".to_vec())
        .with_target(agent(&target))
        .send()
        .await
        .expect("the work is sent");
}

#[then(regex = r#"^agent "([^"]+)" sees no pending pause or cancel for the session$"#)]
async fn no_pending_control(world: &mut LaserWorld, name: String) {
    let lens = sessions(world)
        .open(world.session().conversation())
        .as_agent(agent(&name));
    let pending = lens
        .pending_control()
        .await
        .expect("the control state reads");
    assert!(!pending.pause_requested, "a pause was applied");
    assert!(!pending.cancel_requested, "a cancel was applied");
    assert!(
        !lens
            .cancel_requested()
            .await
            .expect("the cancel flag reads"),
        "a cancel was applied"
    );
    if let Some(worker) = world.worker.take() {
        worker.shutdown().await.expect("the worker stops");
    }
}

#[when(
    regex = r#"^the session records a model call to "([^"]+)" with the context assembled from its (\d+) records$"#
)]
async fn record_model_assembled(world: &mut LaserWorld, model: String, records: usize) {
    let session = world.session().clone();
    let assembled = eventually(async || {
        let assembled = session.assemble(Box::new(LastN(50))).await.ok()?;
        (assembled.fragments.len() == records).then_some(assembled)
    })
    .await;
    session
        .record_model_call(
            ModelRequest::new(model, br#"{"prompt":"hi"}"#.to_vec()),
            ModelResponse {
                body: b"hello".to_vec(),
                ..ModelResponse::default()
            },
            Some(&assembled),
        )
        .await
        .expect("the model call is recorded");
    world.native.assembled = Some(assembled);
}

#[when(regex = r#"^the session records a model call to "([^"]+)" without an assembled context$"#)]
async fn record_model_bare(world: &mut LaserWorld, model: String) {
    world
        .session()
        .record_model_call(
            ModelRequest::new(model, br#"{"prompt":"hi"}"#.to_vec()),
            ModelResponse {
                body: b"hello".to_vec(),
                ..ModelResponse::default()
            },
            None,
        )
        .await
        .expect("the model call is recorded");
}

#[then("the context manifest lists the address of every assembled fragment")]
async fn manifest_addresses(world: &mut LaserWorld) {
    let session = world.session().conversation();
    let turns = lane(world, session, |turns| {
        turns
            .iter()
            .any(|turn| turn.display.as_str() == "context.assembled")
    })
    .await;
    let position = |turn: &SessionTurn| (turn.message.id.partition_id, turn.message.id.offset);
    let manifest_turn = turns
        .iter()
        .find(|turn| turn.display.as_str() == "context.assembled")
        .expect("a manifest record");
    let request = turns
        .iter()
        .find(|turn| turn.display.as_str() == "model.request")
        .and_then(|turn| turn.message.envelope.as_ref())
        .expect("a model request");
    let envelope = manifest_turn
        .message
        .envelope
        .as_ref()
        .expect("an envelope");
    let manifest: ContextManifest = decode_named(&envelope.body).expect("the manifest decodes");
    assert_eq!(manifest.correlation, request.correlation);
    let listed: Vec<(u32, u64)> = manifest
        .fragments
        .iter()
        .map(|fragment| match fragment {
            Fragment::Message {
                at:
                    SourceRef::Message {
                        partition,
                        offset,
                        generation,
                        conversation,
                        ..
                    },
                ..
            } => {
                assert!(generation.is_some(), "a fragment without its generation");
                assert_eq!(conversation.as_deref(), Some(session.to_string().as_str()));
                (*partition, *offset)
            }
            other => panic!("a fragment that is not a log message: {other:?}"),
        })
        .collect();
    let assembled: Vec<(u32, u64)> = world
        .native
        .assembled
        .as_ref()
        .expect("an assembled context")
        .fragments
        .iter()
        .map(|message| (message.id.partition_id, message.id.offset))
        .collect();
    let earlier: Vec<(u32, u64)> = turns.iter().take(assembled.len()).map(position).collect();
    assert_eq!(listed, assembled);
    assert_eq!(listed, earlier);
}

#[given(regex = r"^sessions publish heartbeats every (\d+) milliseconds$")]
async fn heartbeat_every(world: &mut LaserWorld, millis: u64) {
    world.native.config = Some(SessionConfig::new().heartbeat(Duration::from_millis(millis)));
}

#[when(regex = r#"^agent "([^"]+)" opens the session "([^"]+)"$"#)]
async fn open_lens(world: &mut LaserWorld, owner: String, label: String) {
    let sessions = sessions(world);
    let id = sessions.create(label).id().expect("an id");
    world.session = Some(sessions.open(id).as_agent(agent(&owner)));
}

// Every heartbeat on `stream`, in log order.
async fn heartbeats(laser: &Laser, stream: &str) -> Vec<SessionHeartbeat> {
    let mut cursor = laser
        .stream(stream)
        .topic(AGENT_HEARTBEATS)
        .replay()
        .expect("a heartbeat reader");
    let mut beats = Vec::new();
    loop {
        let batch = cursor.poll().await.expect("the heartbeats read");
        if batch.is_empty() {
            return beats;
        }
        for message in batch {
            let envelope: AgentEnvelope =
                decode_named(&message.payload).expect("a heartbeat envelope");
            beats.push(decode_named(&envelope.body).expect("a heartbeat body"));
        }
    }
}

fn stream_of(world: &LaserWorld) -> String {
    world.laser().default_stream().expect("a stream").to_owned()
}

#[then(regex = r"^no heartbeat is published within (\d+) seconds?$")]
async fn no_heartbeat(world: &mut LaserWorld, seconds: u64) {
    tokio::time::sleep(Duration::from_secs(seconds)).await;
    let beats = heartbeats(world.laser(), &stream_of(world)).await;
    assert!(beats.is_empty(), "heartbeats: {beats:?}");
    if let Some(worker) = world.worker.take() {
        worker.shutdown().await.expect("the worker stops");
    }
}

async fn start_leased(world: &mut LaserWorld, sessions: Sessions, owner: &str, label: String) {
    let stream = sessions.stream().expect("a stream").to_owned();
    let (session, lease) = sessions
        .create(label.clone())
        .agent(agent(owner))
        .begin()
        .await
        .expect("the session starts");
    let id = session.conversation();
    world
        .native
        .labels
        .insert(label.clone(), (stream.clone(), id));
    world.native.leases.insert(label, (stream, id, lease));
}

#[when(regex = r#"^agent "([^"]+)" starts the sessions "([^"]+)" and "([^"]+)"$"#)]
async fn start_two(world: &mut LaserWorld, owner: String, first: String, second: String) {
    for label in [first, second] {
        start_leased(world, sessions(world), &owner, label).await;
    }
}

fn listed(beat: &SessionHeartbeat) -> BTreeSet<String> {
    beat.sessions
        .iter()
        .map(|session| ConversationId::from(*session).to_string())
        .collect()
}

#[then(regex = r#"^a heartbeat lists the sessions (".+")$"#)]
async fn heartbeat_lists(world: &mut LaserWorld, list: String) {
    let expected: BTreeSet<String> = names(&list)
        .iter()
        .map(|label| {
            let (_, session) = world.native.labels.get(label).expect("a started session");
            session.to_string()
        })
        .collect();
    let laser = world.laser().clone();
    let stream = stream_of(world);
    eventually(async || {
        let beats = heartbeats(&laser, &stream).await;
        beats
            .iter()
            .any(|beat| beat.stream == stream && listed(beat) == expected)
            .then_some(())
    })
    .await;
}

#[when(regex = r#"^the lease on the session "([^"]+)" is released$"#)]
async fn release_lease(world: &mut LaserWorld, label: String) {
    let (_, _, lease) = world
        .native
        .leases
        .remove(&label)
        .expect("a leased session");
    lease.release();
}

#[then("the heartbeats stop")]
async fn heartbeats_stop(world: &mut LaserWorld) {
    let stream = stream_of(world);
    // A beat already in flight when the last lease dropped may still land.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let settled = heartbeats(world.laser(), &stream).await.len();
    assert!(settled > 0, "no heartbeat was ever published");
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(heartbeats(world.laser(), &stream).await.len(), settled);
}

#[given("a second stream bootstrapped for sessions on the same connection")]
async fn second_stream(world: &mut LaserWorld) {
    let stream = format!("{}-second", stream_of(world));
    let config = world
        .native
        .config
        .clone()
        .unwrap_or_default()
        .stream(stream.clone())
        .register_source(false);
    world
        .laser()
        .sessions_with(config)
        .bootstrap(4, retention())
        .await
        .expect("bootstrap the second stream");
    world.native.second_stream = Some(stream);
}

#[when(
    regex = r#"^agent "([^"]+)" starts the session "([^"]+)" on this stream and the session "([^"]+)" on the second stream$"#
)]
async fn start_on_two_streams(world: &mut LaserWorld, owner: String, here: String, there: String) {
    let second = world.native.second_stream.clone().expect("a second stream");
    start_leased(world, sessions(world), &owner, here).await;
    let config = world
        .native
        .config
        .clone()
        .unwrap_or_default()
        .stream(second);
    let elsewhere = world.laser().sessions_with(config);
    start_leased(world, elsewhere, &owner, there).await;
}

#[then("the heartbeats of each stream list only that stream's session")]
async fn heartbeats_per_stream(world: &mut LaserWorld) {
    let laser = world.laser().clone();
    let streams: BTreeMap<String, ConversationId> = world.native.labels.values().cloned().collect();
    assert_eq!(streams.len(), 2, "one session on each of two streams");
    // The later session's stream beats only while both leases are held.
    eventually(async || {
        let mut counts = Vec::new();
        for stream in streams.keys() {
            counts.push(heartbeats(&laser, stream).await.len());
        }
        counts.iter().all(|count| *count >= 2).then_some(())
    })
    .await;
    for (stream, session) in &streams {
        let beats = heartbeats(&laser, stream).await;
        for beat in &beats {
            assert_eq!(&beat.stream, stream);
            assert_eq!(
                listed(beat),
                BTreeSet::from([session.to_string()]),
                "{beat:?}"
            );
        }
    }
    world.native.leases.clear();
}

#[when("the policy refuses the next state snapshot publish")]
async fn refuse_snapshot(world: &mut LaserWorld) {
    world
        .native
        .snapshot_refusals
        .as_ref()
        .expect("a refusing policy")
        .store(1, Ordering::SeqCst);
}

#[when("I write a state snapshot")]
async fn captured_snapshot(world: &mut LaserWorld) {
    world.last_result = Some(
        world
            .session()
            .state()
            .snapshot()
            .await
            .map(|_| ())
            .map_err(|error| format!("{error:?}")),
    );
}
