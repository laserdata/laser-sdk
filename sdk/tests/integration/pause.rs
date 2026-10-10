use crate::harness;
use async_trait::async_trait;
use iggy::prelude::{Identifier, StreamClient, TopicClient};
use laser_sdk::agent::{ParkedRecords, SessionControl};
use laser_sdk::govern::{ActionDecision, ActionGovernor, ActionKind, GovernedAction, GovernorMode};
use laser_sdk::prelude::full::*;
use laser_sdk::wire::agent::{
    AgentId, AgentKind, CorrelationId, LogPosition, OPERATION_SESSION, OPERATION_SESSION_PARKED,
    OPERATION_SESSION_UNPARKED, SessionParking, SessionPauseRequest, SessionTransition, TaskState,
};
use laser_sdk::wire::content::ContentType;
use laser_sdk::wire::dispatch::OPERATION_SESSION_PAUSE;
use laser_sdk::wire::framing::{decode_named, encode_named};
use laser_sdk::wire::graph::SourceRef;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

fn worker_id() -> AgentId {
    "pauser".parse().expect("valid agent id")
}

fn client_id() -> AgentId {
    "client".parse().expect("valid agent id")
}

fn operator() -> AgentId {
    "operator".parse().expect("valid agent id")
}

// Every work body the handler saw, in order.
#[derive(Clone, Default)]
struct Seen(Arc<Mutex<Vec<String>>>);

impl Seen {
    fn bodies(&self) -> Vec<String> {
        self.0.lock().expect("seen lock").clone()
    }

    fn count(&self, body: &str) -> usize {
        self.bodies().iter().filter(|seen| *seen == body).count()
    }
}

// The handler hangs forever the first time it sees `body`, after recording
// the effect: a process that dies between the effect and its completion.
struct Hang {
    body: &'static str,
    armed: AtomicBool,
    reached: tokio::sync::Notify,
}

impl Hang {
    fn on(body: &'static str) -> Arc<Self> {
        Arc::new(Self {
            body,
            armed: AtomicBool::new(true),
            reached: tokio::sync::Notify::new(),
        })
    }
}

struct Recorder {
    seen: Seen,
    hang: Option<Arc<Hang>>,
}

impl AgentHandler for Recorder {
    async fn handle(&self, message: &AgentMessage, _ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        let body = String::from_utf8_lossy(message.body()).into_owned();
        self.seen.0.lock().expect("seen lock").push(body.clone());
        if let Some(hang) = &self.hang
            && hang.body == body
            && hang.armed.swap(false, Ordering::SeqCst)
        {
            hang.reached.notify_one();
            std::future::pending::<()>().await;
        }
        Ok(())
    }
}

// Fails the publish of the armed record shapes: a parking, a completion, or
// a pause or resume acknowledgment. The governor runs before every AGDX
// publish, so an armed shape never reaches the log.
#[derive(Clone, Default)]
struct Faults {
    armed: Arc<Mutex<HashSet<&'static str>>>,
    // Every refused publish, by shape.
    refused: Arc<Mutex<Vec<&'static str>>>,
}

const ACKNOWLEDGMENT: &str = "acknowledgment";

impl Faults {
    fn arm(&self, shape: &'static str) {
        self.armed.lock().expect("faults lock").insert(shape);
    }

    fn disarm(&self) {
        self.armed.lock().expect("faults lock").clear();
    }

    fn refused(&self, shape: &str) -> usize {
        let refused = self.refused.lock().expect("faults lock");
        refused.iter().filter(|refused| **refused == shape).count()
    }
}

#[async_trait]
impl ActionGovernor for Faults {
    async fn decide(&self, action: &GovernedAction<'_>) -> Result<ActionDecision, LaserError> {
        let shape = match (action.kind, action.operation) {
            (ActionKind::Event, Some(OPERATION_SESSION_PARKED)) => Some(OPERATION_SESSION_PARKED),
            (ActionKind::Event, Some(OPERATION_SESSION_UNPARKED)) => {
                Some(OPERATION_SESSION_UNPARKED)
            }
            (ActionKind::Status, Some(OPERATION_SESSION))
                if decode_named::<SessionTransition>(action.payload)
                    .is_ok_and(|transition| transition.acknowledges.is_some()) =>
            {
                Some(ACKNOWLEDGMENT)
            }
            _ => None,
        };
        if let Some(shape) = shape
            && self.armed.lock().expect("faults lock").contains(shape)
        {
            self.refused.lock().expect("faults lock").push(shape);
            return Err(LaserError::Invalid("injected publish fault".to_owned()));
        }
        Ok(ActionDecision::allow())
    }
}

async fn setup() -> Laser {
    let laser = harness::laser().await;
    laser
        .topic(AgentTopic::Control.topic_string())
        .ensure(4)
        .await
        .expect("provisioning creates the control topic");
    laser
}

async fn spawn(
    laser: &Laser,
    seen: &Seen,
    hang: Option<Arc<Hang>>,
    concurrency: ConcurrencyPolicy,
) -> AgentHandle {
    let mut worker = Agent::builder()
        .id(worker_id().as_str().parse().expect("valid agent id"))
        .listen_on(AgentTopic::Sessions)
        .concurrency(concurrency)
        .handler(Recorder {
            seen: seen.clone(),
            hang,
        })
        .build()
        .spawn(laser.clone());
    worker.ready().await.expect("the worker is ready");
    worker
}

async fn send_work(laser: &Laser, session: ConversationId, body: &str) -> (u32, u64) {
    let receipt = laser
        .agdx(AgentTopic::Sessions, client_id(), session.into())
        .command(
            <CorrelationId as laser_sdk::types::MintUlid>::mint(),
            body.as_bytes().to_vec(),
        )
        .with_target(worker_id())
        .send_receipt()
        .await
        .expect("the work is sent");
    position(receipt)
}

fn position(receipt: laser_sdk::agent::AgdxReceipt) -> (u32, u64) {
    (
        receipt.partition_id.expect("a confirmed partition"),
        receipt.offset.expect("a confirmed offset"),
    )
}

fn control(laser: &Laser, session: ConversationId) -> SessionControl {
    laser
        .sessions()
        .control(laser.default_stream().expect("stream"), session)
        .as_operator(operator())
        .participants([worker_id()])
}

async fn lane(
    laser: &Laser,
    session: ConversationId,
) -> Vec<laser_sdk::wire::agent::AgentEnvelope> {
    laser
        .context(session)
        .fetch_with(vec![AgentTopic::Sessions], Box::new(LastN(usize::MAX)))
        .await
        .expect("reading the lane succeeds")
        .into_iter()
        .filter_map(|record| record.envelope)
        .collect()
}

// The worker's acknowledgments on the lane: the state and the control
// position each one answers.
async fn acknowledgments(laser: &Laser, session: ConversationId) -> Vec<(TaskState, (u32, u64))> {
    lane(laser, session)
        .await
        .into_iter()
        .filter(|envelope| {
            envelope.kind == AgentKind::Status
                && envelope.operation.as_deref() == Some(OPERATION_SESSION)
        })
        .filter_map(|envelope| {
            let transition: SessionTransition = decode_named(&envelope.body).ok()?;
            let at = transition.acknowledges?;
            let state = envelope.task_state?;
            (transition.actor == Some(worker_id())).then_some((state, (at.partition_id, at.offset)))
        })
        .collect()
}

async fn wait_acknowledged(
    laser: &Laser,
    session: ConversationId,
    state: TaskState,
    request: (u32, u64),
) {
    harness::eventually(|| async {
        acknowledgments(laser, session)
            .await
            .contains(&(state, request))
            .then_some(())
    })
    .await;
}

async fn parkings(laser: &Laser, session: ConversationId, operation: &str) -> Vec<SessionParking> {
    lane(laser, session)
        .await
        .into_iter()
        .filter(|envelope| {
            envelope.kind == AgentKind::Event && envelope.operation.as_deref() == Some(operation)
        })
        .map(|envelope| decode_named(&envelope.body).expect("a parking body"))
        .collect()
}

async fn parked(laser: &Laser, session: ConversationId) -> ParkedRecords {
    laser
        .sessions()
        .open(session)
        .parked()
        .await
        .expect("reading held records succeeds")
}

async fn wait_parked(laser: &Laser, session: ConversationId, count: usize) -> ParkedRecords {
    harness::eventually(|| async {
        let parked = parked(laser, session).await;
        (parked.records.len() == count).then_some(parked)
    })
    .await
}

async fn wait_seen(seen: &Seen, body: &str, count: usize) {
    harness::eventually(|| async { (seen.count(body) >= count).then_some(()) }).await;
    assert_eq!(seen.count(body), count, "`{body}` is handled {count} times");
}

fn offset_of(parking: &SessionParking) -> u64 {
    match parking.source {
        SourceRef::Message { offset, .. } => offset,
        _ => panic!("a parking names a log record"),
    }
}

// Pause, hold one record, resume, and handle it, three times over.
async fn pause_cycles(laser: &Laser, concurrency: ConcurrencyPolicy) {
    let seen = Seen::default();
    let worker = spawn(laser, &seen, None, concurrency).await;
    let session = ConversationId::new();
    let control = control(laser, session);
    send_work(laser, session, "before").await;
    wait_seen(&seen, "before", 1).await;
    for cycle in 0..3 {
        let pause = position(control.pause().await.expect("pause is sent"));
        wait_acknowledged(laser, session, TaskState::Paused, pause).await;
        let body = format!("held-{cycle}");
        let source = send_work(laser, session, &body).await;
        let held = wait_parked(laser, session, 1).await;
        assert!(held.complete);
        assert_eq!(held.records[0].role, worker_id());
        assert_eq!(
            (
                held.records[0].request.partition_id,
                held.records[0].request.offset
            ),
            pause
        );
        assert_eq!(offset_of(&held.records[0]), source.1);
        assert_eq!(seen.count(&body), 0, "a held record is not handled");
        let resume = position(control.resume().await.expect("resume is sent"));
        wait_acknowledged(laser, session, TaskState::Working, resume).await;
        wait_seen(&seen, &body, 1).await;
        wait_parked(laser, session, 0).await;
    }
    send_work(laser, session, "after").await;
    wait_seen(&seen, "after", 1).await;
    assert_eq!(
        seen.bodies(),
        vec!["before", "held-0", "held-1", "held-2", "after"],
        "every record is handled once, in order"
    );
    assert_eq!(
        parkings(laser, session, OPERATION_SESSION_PARKED)
            .await
            .len(),
        3
    );
    assert_eq!(
        parkings(laser, session, OPERATION_SESSION_UNPARKED)
            .await
            .len(),
        3
    );
    worker.shutdown().await.expect("the worker stops");
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_repeated_pause_cycles_when_work_arrives_while_paused_then_should_park_it_and_handle_it_once_after_each_resume()
 {
    let laser = setup().await;
    pause_cycles(&laser, ConcurrencyPolicy::Serial).await;
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_the_group_engine_and_partition_lanes_when_paused_and_resumed_then_should_park_and_handle_once()
 {
    let laser = harness::connected_laser().await;
    laser
        .topic(AgentTopic::Control.topic_string())
        .ensure(1)
        .await
        .expect("provisioning creates the control topic");
    pause_cycles(
        &laser,
        ConcurrencyPolicy::SerialPerPartition { max_partitions: 4 },
    )
    .await;
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_work_on_the_lane_when_paused_without_participants_then_should_name_the_working_agent_and_acknowledge()
 {
    let laser = setup().await;
    let seen = Seen::default();
    let worker = spawn(&laser, &seen, None, ConcurrencyPolicy::Serial).await;
    let session = ConversationId::new();
    send_work(&laser, session, "first").await;
    wait_seen(&seen, "first", 1).await;
    let pause = position(
        laser
            .sessions()
            .control(laser.default_stream().expect("stream"), session)
            .as_operator(operator())
            .pause()
            .await
            .expect("pause is sent"),
    );
    let request = laser
        .context(session)
        .fetch_with(vec![AgentTopic::Control], Box::new(LastN(10)))
        .await
        .expect("reading the control topic succeeds")
        .into_iter()
        .filter_map(|record| record.envelope)
        .find(|envelope| envelope.operation.as_deref() == Some(OPERATION_SESSION_PAUSE))
        .expect("the pause request is on the control topic");
    let body: SessionPauseRequest =
        serde_json::from_slice(&request.body).expect("the pause body decodes");
    assert_eq!(body.participants, vec![worker_id()]);
    wait_acknowledged(&laser, session, TaskState::Paused, pause).await;
    worker.shutdown().await.expect("the worker stops");
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_paused_session_when_canceled_then_should_end_canceled_and_keep_held_records_unprocessed()
 {
    let laser = setup().await;
    let seen = Seen::default();
    let worker = spawn(&laser, &seen, None, ConcurrencyPolicy::Serial).await;
    let session = ConversationId::new();
    let control = control(&laser, session);
    let pause = position(control.pause().await.expect("pause is sent"));
    wait_acknowledged(&laser, session, TaskState::Paused, pause).await;
    let source = send_work(&laser, session, "held").await;
    wait_parked(&laser, session, 1).await;
    control.cancel().await.expect("cancel is sent");
    harness::eventually(|| async {
        lane(&laser, session)
            .await
            .iter()
            .any(|envelope| {
                envelope.task_state == Some(TaskState::Canceled) && envelope.source == worker_id()
            })
            .then_some(())
    })
    .await;
    let resume = position(control.resume().await.expect("resume is sent"));
    // Work after the resume is the handler's to judge, or held when it
    // arrives before the runtime reads the resume: control and work ride
    // different topics. Either way the runtime decided on it after the held
    // record, which it would have handled first.
    send_work(&laser, session, "late").await;
    let held = harness::eventually(|| async {
        let held = parked(&laser, session).await;
        (seen.count("late") == 1 || held.records.len() == 2).then_some(held)
    })
    .await;
    assert_eq!(
        seen.count("held"),
        0,
        "a canceled session's held record is never handled"
    );
    assert!(
        held.records
            .iter()
            .any(|record| offset_of(record) == source.1),
        "the held record stays listed"
    );
    assert!(held.complete);
    assert!(
        !acknowledgments(&laser, session)
            .await
            .contains(&(TaskState::Working, resume)),
        "a canceled session does not resume"
    );
    worker.shutdown().await.expect("the worker stops");
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_failed_parking_publish_when_work_arrives_while_paused_then_should_leave_it_uncommitted()
 {
    let laser = setup().await;
    let faults = Faults::default();
    let governed = laser.with_governor(Arc::new(faults.clone()), GovernorMode::Enforce);
    let seen = Seen::default();
    let worker = spawn(&governed, &seen, None, ConcurrencyPolicy::Serial).await;
    let session = ConversationId::new();
    let control = control(&laser, session);
    let pause = position(control.pause().await.expect("pause is sent"));
    wait_acknowledged(&laser, session, TaskState::Paused, pause).await;
    faults.arm(OPERATION_SESSION_PARKED);
    send_work(&laser, session, "held").await;
    let stopped = tokio::time::timeout(Duration::from_secs(15), worker.join())
        .await
        .expect("the worker stops on the failed parking");
    assert!(stopped.is_err(), "a failed parking stops the runtime");
    assert_eq!(seen.count("held"), 0);
    assert!(parked(&laser, session).await.records.is_empty());

    faults.disarm();
    let worker = spawn(&governed, &seen, None, ConcurrencyPolicy::Serial).await;
    wait_parked(&laser, session, 1).await;
    let resume = position(control.resume().await.expect("resume is sent"));
    wait_acknowledged(&laser, session, TaskState::Working, resume).await;
    wait_seen(&seen, "held", 1).await;
    wait_parked(&laser, session, 0).await;
    worker.shutdown().await.expect("the worker stops");
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_failed_completion_publish_when_resumed_then_should_handle_the_held_record_again() {
    let laser = setup().await;
    let faults = Faults::default();
    let governed = laser.with_governor(Arc::new(faults.clone()), GovernorMode::Enforce);
    let seen = Seen::default();
    let worker = spawn(&governed, &seen, None, ConcurrencyPolicy::Serial).await;
    let session = ConversationId::new();
    let control = control(&laser, session);
    let pause = position(control.pause().await.expect("pause is sent"));
    wait_acknowledged(&laser, session, TaskState::Paused, pause).await;
    send_work(&laser, session, "held").await;
    wait_parked(&laser, session, 1).await;
    faults.arm(OPERATION_SESSION_UNPARKED);
    let resume = position(control.resume().await.expect("resume is sent"));
    wait_acknowledged(&laser, session, TaskState::Working, resume).await;
    wait_seen(&seen, "held", 1).await;
    assert_eq!(
        parked(&laser, session).await.records.len(),
        1,
        "without its completion the record stays held"
    );

    faults.disarm();
    send_work(&laser, session, "next").await;
    wait_seen(&seen, "next", 1).await;
    assert_eq!(
        seen.bodies(),
        vec!["held", "held", "next"],
        "the held record is handled again before new work"
    );
    wait_parked(&laser, session, 0).await;
    assert_eq!(
        parkings(&laser, session, OPERATION_SESSION_UNPARKED)
            .await
            .len(),
        1
    );
    worker.shutdown().await.expect("the worker stops");
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_failed_pause_acknowledgment_when_work_arrives_then_should_leave_it_uncommitted_and_acknowledge_after_restart()
 {
    let laser = setup().await;
    let faults = Faults::default();
    let governed = laser.with_governor(Arc::new(faults.clone()), GovernorMode::Enforce);
    let seen = Seen::default();
    let worker = spawn(&governed, &seen, None, ConcurrencyPolicy::Serial).await;
    let session = ConversationId::new();
    let control = control(&laser, session);
    faults.arm(ACKNOWLEDGMENT);
    let pause = position(control.pause().await.expect("pause is sent"));
    // The refused eager acknowledgment proves the runtime read the pause, so
    // the work below meets a paused session.
    harness::eventually(|| async { (faults.refused(ACKNOWLEDGMENT) > 0).then_some(()) }).await;
    send_work(&laser, session, "held").await;
    let stopped = tokio::time::timeout(Duration::from_secs(15), worker.join())
        .await
        .expect("the worker stops on the failed acknowledgment");
    assert!(
        stopped.is_err(),
        "a failed acknowledgment stops the runtime"
    );
    assert!(acknowledgments(&laser, session).await.is_empty());
    assert!(parked(&laser, session).await.records.is_empty());

    faults.disarm();
    let worker = spawn(&governed, &seen, None, ConcurrencyPolicy::Serial).await;
    wait_acknowledged(&laser, session, TaskState::Paused, pause).await;
    wait_parked(&laser, session, 1).await;
    assert_eq!(
        acknowledgments(&laser, session).await,
        vec![(TaskState::Paused, pause)],
        "the pause is acknowledged once"
    );
    let resume = position(control.resume().await.expect("resume is sent"));
    wait_acknowledged(&laser, session, TaskState::Working, resume).await;
    wait_seen(&seen, "held", 1).await;
    worker.shutdown().await.expect("the worker stops");
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_crash_after_the_effect_when_restarted_then_should_handle_the_held_record_again_and_complete_it()
 {
    let laser = setup().await;
    let seen = Seen::default();
    let hang = Hang::on("held");
    let worker = spawn(
        &laser,
        &seen,
        Some(Arc::clone(&hang)),
        ConcurrencyPolicy::Serial,
    )
    .await;
    let session = ConversationId::new();
    let control = control(&laser, session);
    let pause = position(control.pause().await.expect("pause is sent"));
    wait_acknowledged(&laser, session, TaskState::Paused, pause).await;
    send_work(&laser, session, "held").await;
    wait_parked(&laser, session, 1).await;
    let reached = hang.reached.notified();
    control.resume().await.expect("resume is sent");
    tokio::time::timeout(Duration::from_secs(15), reached)
        .await
        .expect("the held record reaches the handler");
    worker.abort();
    drop(worker);
    assert_eq!(seen.count("held"), 1);
    assert_eq!(
        parked(&laser, session).await.records.len(),
        1,
        "a crash before the completion leaves the record held"
    );

    let worker = spawn(&laser, &seen, None, ConcurrencyPolicy::Serial).await;
    wait_seen(&seen, "held", 2).await;
    wait_parked(&laser, session, 0).await;
    assert_eq!(
        parkings(&laser, session, OPERATION_SESSION_UNPARKED)
            .await
            .len(),
        1
    );
    worker.shutdown().await.expect("the worker stops");
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_crash_after_parking_before_the_commit_when_restarted_then_should_not_park_or_handle_twice()
 {
    let laser = setup().await;
    let session = ConversationId::new();
    let control = control(&laser, session);
    let pause = position(control.pause().await.expect("pause is sent"));
    // The log a worker leaves when it dies after appending the parking and
    // before committing the source: the record, its parking, no commit.
    let source = send_work(&laser, session, "held").await;
    let (stream_id, sessions_topic, generation, control_topic) = topic_ids(&laser).await;
    let parking = SessionParking {
        source: SourceRef::Message {
            stream: stream_id,
            topic: sessions_topic,
            partition: source.0,
            offset: source.1,
            generation: Some(generation),
            conversation: Some(session.to_string()),
        },
        role: worker_id(),
        request: LogPosition::new(stream_id, control_topic, pause.0, pause.1),
    };
    write_parking(&laser, session, &parking).await;

    let seen = Seen::default();
    let worker = spawn(&laser, &seen, None, ConcurrencyPolicy::Serial).await;
    wait_acknowledged(&laser, session, TaskState::Paused, pause).await;
    let resume = position(control.resume().await.expect("resume is sent"));
    wait_acknowledged(&laser, session, TaskState::Working, resume).await;
    wait_seen(&seen, "held", 1).await;
    // The source shares the session's partition, so once later work is
    // handled the redelivered source was settled too.
    send_work(&laser, session, "after").await;
    wait_seen(&seen, "after", 1).await;
    assert_eq!(seen.bodies(), vec!["held", "after"]);
    assert_eq!(
        parkings(&laser, session, OPERATION_SESSION_PARKED)
            .await
            .len(),
        1,
        "the redelivered record is not parked again"
    );
    assert_eq!(
        parkings(&laser, session, OPERATION_SESSION_UNPARKED)
            .await
            .len(),
        1
    );
    worker.shutdown().await.expect("the worker stops");
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_resume_while_the_agent_was_offline_when_restarted_then_should_acknowledge_and_handle_the_held_record()
 {
    let laser = setup().await;
    let seen = Seen::default();
    let worker = spawn(&laser, &seen, None, ConcurrencyPolicy::Serial).await;
    let session = ConversationId::new();
    let control = control(&laser, session);
    let pause = position(control.pause().await.expect("pause is sent"));
    wait_acknowledged(&laser, session, TaskState::Paused, pause).await;
    send_work(&laser, session, "held").await;
    wait_parked(&laser, session, 1).await;
    worker.shutdown().await.expect("the worker stops");

    let resume = position(control.resume().await.expect("resume is sent"));
    let worker = spawn(&laser, &seen, None, ConcurrencyPolicy::Serial).await;
    wait_acknowledged(&laser, session, TaskState::Working, resume).await;
    wait_seen(&seen, "held", 1).await;
    wait_parked(&laser, session, 0).await;
    assert_eq!(
        acknowledgments(&laser, session).await,
        vec![(TaskState::Paused, pause), (TaskState::Working, resume)],
        "each request is acknowledged once"
    );
    worker.shutdown().await.expect("the worker stops");
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_held_record_of_a_recreated_topic_when_resumed_then_should_report_it_and_keep_it_listed()
 {
    let laser = setup().await;
    let session = ConversationId::new();
    let control = control(&laser, session);
    let pause = position(control.pause().await.expect("pause is sent"));
    let source = send_work(&laser, session, "stale").await;
    let (stream_id, sessions_topic, generation, control_topic) = topic_ids(&laser).await;
    let parking = SessionParking {
        source: SourceRef::Message {
            stream: stream_id,
            topic: sessions_topic,
            partition: source.0,
            offset: source.1,
            generation: Some(generation + 1),
            conversation: Some(session.to_string()),
        },
        role: worker_id(),
        request: LogPosition::new(stream_id, control_topic, pause.0, pause.1),
    };
    write_parking(&laser, session, &parking).await;
    let resume = position(control.resume().await.expect("resume is sent"));

    let seen = Seen::default();
    let worker = spawn(&laser, &seen, None, ConcurrencyPolicy::Serial).await;
    send_work(&laser, session, "after").await;
    wait_seen(&seen, "after", 1).await;
    let held = parked(&laser, session).await;
    assert_eq!(
        held.records,
        vec![parking],
        "the unrecoverable record stays listed"
    );
    assert!(
        !held.complete,
        "an unreadable held record makes the read incomplete"
    );
    assert!(
        !acknowledgments(&laser, session)
            .await
            .contains(&(TaskState::Working, resume)),
        "a role that never acknowledged the pause does not acknowledge the resume"
    );
    worker.shutdown().await.expect("the worker stops");
}

async fn topic_ids(laser: &Laser) -> (u32, u32, u64, u32) {
    let client = laser.client();
    let stream = Identifier::named(laser.default_stream().expect("stream")).expect("stream id");
    let stream_id = client
        .get_stream(&stream)
        .await
        .expect("reading the stream succeeds")
        .expect("the stream exists")
        .id;
    let sessions = client
        .get_topic(&stream, &AgentTopic::Sessions.as_identifier())
        .await
        .expect("reading agent.sessions succeeds")
        .expect("agent.sessions exists");
    let control = client
        .get_topic(&stream, &AgentTopic::Control.as_identifier())
        .await
        .expect("reading agent.control succeeds")
        .expect("agent.control exists");
    (
        stream_id,
        sessions.id,
        sessions.created_at.as_micros(),
        control.id,
    )
}

async fn write_parking(laser: &Laser, session: ConversationId, parking: &SessionParking) {
    laser
        .agdx(AgentTopic::Sessions, worker_id(), session.into())
        .emit(encode_named(parking).expect("the parking encodes"))
        .with_operation(OPERATION_SESSION_PARKED)
        .content_type(ContentType::Cbor)
        .send()
        .await
        .expect("the parking is written");
}
