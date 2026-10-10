use crate::agent::consumer::{
    AgentMessage, MAX_CONSECUTIVE_POLL_ERRORS, backoff_for, decode_agent_record,
};
use crate::agent::control::{
    ControlBook, ControlPosition, ControlState, PlacedRequest, control_request,
};
use crate::agent::{Session, Sessions};
use crate::context::READ_BATCH;
use crate::error::LaserError;
use crate::laser::Laser;
use crate::poll::{DrainRange, bounded_partitions, drain_partition, tail_anchored_offset};
use crate::provenance::AgentTopic;
use crate::stream::ConsumerMessage;
use crate::types::ConversationId;
use futures::future::BoxFuture;
use iggy::prelude::{
    Identifier, IggyClient, IggyMessage, MessageClient, PollingStrategy, StreamClient, TopicClient,
};
use laser_wire::agent::{
    AgentId, AgentKind, LogPosition, OPERATION_CHAT, OPERATION_SESSION, OPERATION_SESSION_PARKED,
    OPERATION_SESSION_UNPARKED, SessionParking, SessionTransition, TaskState, features,
};
use laser_wire::content::ContentType;
use laser_wire::dispatch::{
    CONTROL_OPERATIONS, OPERATION_EXECUTE_TOOL, OPERATION_GENERATE_CONTENT,
    OPERATION_TEXT_COMPLETION,
};
use laser_wire::framing::{decode_named, encode_named};
use laser_wire::graph::SourceRef;
use laser_wire::topics::{AGENT_CONTROL, AGENT_SESSIONS};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, warn};

/// The held work of one session that no agent reported handled, read by
/// [`Session::parked`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParkedRecords {
    /// The held records, by source position.
    pub records: Vec<SessionParking>,
    /// False when the bounded read of `agent.sessions` did not reach the
    /// oldest retained record of the session's partition, so older held
    /// records may be missing, or when a held record can no longer be read
    /// back because its source expired or its topic was recreated.
    pub complete: bool,
}

impl Session {
    /// The work records this session's agents held while it was paused and
    /// have not reported handled. A session canceled while paused keeps its
    /// held records here unprocessed. The read covers the newest
    /// [`CONTEXT_READ_WINDOW`](crate::context::CONTEXT_READ_WINDOW) records
    /// of each `agent.sessions` partition.
    pub async fn parked(&self) -> Result<ParkedRecords, LaserError> {
        let session = self.conversation();
        let mut scan = scan_lane(&self.laser, None, Some(session)).await?;
        let mut complete = scan.complete_for(session);
        let pending = scan
            .sessions
            .remove(&session)
            .map(|facts| facts.pending())
            .unwrap_or_default();
        let mut retained = SourceCheck::new(&self.laser);
        for key in pending.keys() {
            if !retained.readable(key.source).await? {
                complete = false;
            }
        }
        Ok(ParkedRecords {
            records: pending.into_values().collect(),
            complete,
        })
    }
}

// Whether held records can still be read back from their sources, with the
// topic generations and partition starts looked up once each.
struct SourceCheck<'a> {
    laser: &'a Laser,
    generations: HashMap<(u32, u32), Option<u64>>,
    first: HashMap<(u32, u32, u32), Option<u64>>,
}

impl<'a> SourceCheck<'a> {
    fn new(laser: &'a Laser) -> Self {
        Self {
            laser,
            generations: HashMap::new(),
            first: HashMap::new(),
        }
    }

    async fn readable(&mut self, address: SourceAddress) -> Result<bool, LaserError> {
        let stream = Identifier::numeric(address.stream)?;
        let topic = Identifier::numeric(address.topic)?;
        let client = self.laser.client();
        let generation = match self.generations.get(&(address.stream, address.topic)) {
            Some(generation) => *generation,
            None => {
                let generation = client
                    .get_topic(&stream, &topic)
                    .await?
                    .map(|details| details.created_at.as_micros());
                self.generations
                    .insert((address.stream, address.topic), generation);
                generation
            }
        };
        if generation.is_none() || generation != address.generation {
            return Ok(false);
        }
        let partition = (address.stream, address.topic, address.partition);
        let first = match self.first.get(&partition) {
            Some(first) => *first,
            None => {
                let first = client
                    .poll_messages(
                        &stream,
                        &topic,
                        Some(address.partition),
                        &iggy::prelude::Consumer::new(Identifier::named(PARKED_READER)?),
                        &PollingStrategy::first(),
                        1,
                        false,
                    )
                    .await?
                    .messages
                    .first()
                    .map(|message| message.header.offset);
                self.first.insert(partition, first);
                first
            }
        };
        Ok(first.is_some_and(|first| first <= address.offset))
    }
}

/// The agents the lane of `session` shows working on it: the addressees of
/// its work commands and the agents that picked it up or acknowledged a
/// control request. Model, tool, and control records are not work.
pub(crate) async fn lane_participants(
    laser: &Laser,
    session: ConversationId,
) -> Result<Vec<AgentId>, LaserError> {
    let records = laser
        .context(session)
        .fetch_with(
            vec![AgentTopic::Sessions],
            Box::new(crate::context::LastN(usize::MAX)),
        )
        .await?;
    let mut participants = BTreeSet::new();
    for envelope in records.into_iter().filter_map(|record| record.envelope) {
        let operation = envelope.operation.as_deref().unwrap_or_default();
        match envelope.kind {
            AgentKind::Command if is_work(operation) => {
                participants.extend(envelope.target);
            }
            AgentKind::Status if operation == OPERATION_SESSION => {
                if let Ok(SessionTransition {
                    actor: Some(actor), ..
                }) = decode_named::<SessionTransition>(&envelope.body)
                {
                    participants.insert(actor);
                }
            }
            _ => {}
        }
    }
    Ok(participants.into_iter().collect())
}

fn is_work(operation: &str) -> bool {
    !CONTROL_OPERATIONS.contains(&operation)
        && !matches!(
            operation,
            OPERATION_CHAT
                | OPERATION_TEXT_COMPLETION
                | OPERATION_GENERATE_CONTENT
                | OPERATION_EXECUTE_TOOL
        )
}

/// Handles a held record again after the resume, through the same decode,
/// gates, handler, retry, and dead letter as live delivery, but without the
/// pause check. `Err` only when a dead letter could not be published.
pub(crate) trait Replay: Send + Sync {
    fn replay<'a>(&'a self, received: &'a ConsumerMessage)
    -> BoxFuture<'a, Result<(), LaserError>>;
}

/// What the pause check decided for one work record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Hold {
    /// Handle the record now.
    Handle,
    /// Commit the record without handling it: it is durably parked, or it
    /// was already handled through its parking.
    Commit,
}

/// Where a runtime's work records come from.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SourceTopic {
    pub(crate) stream_id: u32,
    pub(crate) topic_id: u32,
    /// The topic's creation time, which proves the numeric topic id.
    pub(crate) generation: u64,
}

/// `agent.control` as the startup read found it: its ids and the next offset
/// to follow on each partition.
pub(crate) struct ControlFeed {
    pub(crate) stream_id: u32,
    pub(crate) topic_id: u32,
    next: BTreeMap<u32, u64>,
    /// Some partition holds more than the bounded read covered, or already
    /// expired records, so requests older than the read may be missing.
    truncated: bool,
}

/// Read the newest records of every `agent.control` partition into `book`
/// and return where to follow from, `None` when the stream has no control
/// topic. The book is not marked live here.
pub(crate) async fn load_control(
    laser: &Laser,
    me: Option<&AgentId>,
    book: &ControlBook,
) -> Result<Option<ControlFeed>, LaserError> {
    let stream = Identifier::named(laser.stream_required()?)?;
    let topic = Identifier::named(AGENT_CONTROL)?;
    let client = laser.client();
    let Some(details) = client.get_topic(&stream, &topic).await? else {
        return Ok(None);
    };
    let stream_id = client
        .get_stream(&stream)
        .await?
        .map(|stream| stream.id)
        .unwrap_or_default();
    let reader = iggy::prelude::Consumer::new(Identifier::named(CONTROL_READER)?);
    let mut next = BTreeMap::new();
    let mut truncated = false;
    let mut records = Vec::new();
    for partition in 0..bounded_partitions(details.partitions_count) {
        let window = read_window(&client, &stream, &topic, &reader, partition).await?;
        truncated |= window.truncated;
        next.insert(partition, window.next_offset);
        for message in window.messages {
            let Ok(decoded) = decode_agent_record(&message, features::NONE) else {
                continue;
            };
            let Some(envelope) = decoded.envelope else {
                continue;
            };
            if let Some(request) = control_request(&envelope, me) {
                records.push((
                    envelope.conversation.into(),
                    (partition, message.header.offset),
                    request,
                ));
            }
        }
    }
    book.load(records);
    Ok(Some(ControlFeed {
        stream_id,
        topic_id: details.id,
        next,
        truncated,
    }))
}

/// The control follower: polls every `agent.control` partition from where
/// the startup read ended and records each request for `me` in the book, so
/// every instance of a role sees every request whatever partitions its
/// consumer group assigns it. Each request is handed to the pause runtime
/// through `notify`. When the follower stops, the book stops claiming to be
/// current and every read folds the log.
pub(crate) async fn follow_control(
    laser: Laser,
    mut feed: ControlFeed,
    me: Option<AgentId>,
    book: Arc<ControlBook>,
    notify: Option<mpsc::UnboundedSender<ConversationId>>,
    poll_interval: Duration,
    mut stop: oneshot::Receiver<()>,
) {
    let (stream, topic, reader) = match (
        laser
            .stream_required()
            .and_then(|stream| Ok(Identifier::named(stream)?)),
        Identifier::named(AGENT_CONTROL),
        Identifier::named(CONTROL_READER),
    ) {
        (Ok(stream), Ok(topic), Ok(reader)) => {
            (stream, topic, iggy::prelude::Consumer::new(reader))
        }
        _ => {
            book.set_live(false);
            return;
        }
    };
    let mut failures = 0u32;
    loop {
        let pass = async {
            let client = laser.client();
            let mut read = false;
            for (partition, next) in &mut feed.next {
                let batch = drain_partition(
                    &client,
                    &stream,
                    &topic,
                    &reader,
                    DrainRange::open(*partition, *next),
                    READ_BATCH,
                )
                .await?;
                read |= !batch.messages.is_empty();
                for message in batch.messages {
                    observe_control(&message, *partition, me.as_ref(), &book, notify.as_ref());
                }
                *next = batch.next_offset;
            }
            Ok::<bool, LaserError>(read)
        };
        let outcome = tokio::select! {
            _ = &mut stop => break,
            outcome = pass => outcome,
        };
        let idle = match outcome {
            Ok(read) => {
                failures = 0;
                !read
            }
            Err(error) => {
                failures += 1;
                if failures >= MAX_CONSECUTIVE_POLL_ERRORS {
                    warn!(%error, "the control follower stopped, session control is read from the log");
                    break;
                }
                debug!(%error, attempt = failures, "reading agent.control failed, retrying");
                tokio::select! {
                    _ = &mut stop => break,
                    () = tokio::time::sleep(backoff_for(failures)) => {}
                }
                continue;
            }
        };
        if idle {
            tokio::select! {
                _ = &mut stop => break,
                () = tokio::time::sleep(poll_interval) => {}
            }
        }
    }
    book.set_live(false);
}

fn observe_control(
    message: &IggyMessage,
    partition: u32,
    me: Option<&AgentId>,
    book: &ControlBook,
    notify: Option<&mpsc::UnboundedSender<ConversationId>>,
) {
    let Ok(decoded) = decode_agent_record(message, features::NONE) else {
        return;
    };
    let Some(envelope) = decoded.envelope else {
        return;
    };
    let Some(request) = control_request(&envelope, me) else {
        return;
    };
    let session: ConversationId = envelope.conversation.into();
    book.observe(session, (partition, message.header.offset), request);
    if let Some(notify) = notify {
        let _ = notify.send(session);
    }
}

/// The pause and resume runtime of one agent role (D7a). Work for a paused
/// session is durably parked on the session lane before its source offset is
/// committed, the role acknowledges each pause and resume request it takes
/// part in on the lane, and a resume handles the held work again before new
/// work for the session. Every decision is rebuilt from the log after a
/// restart, a rebalance, or a reopened consumer.
pub(crate) struct PauseRuntime {
    laser: Laser,
    sessions: Sessions,
    me: AgentId,
    book: Arc<ControlBook>,
    control: (u32, u32),
    control_truncated: bool,
    source: SourceTopic,
    reconcile: mpsc::UnboundedSender<ConversationId>,
    gates: Mutex<HashMap<ConversationId, Arc<tokio::sync::Mutex<()>>>>,
    roles: Mutex<HashMap<ConversationId, RoleState>>,
    assignment: Mutex<Option<BTreeSet<u32>>>,
    epoch: AtomicU64,
    recovered_epoch: AtomicU64,
    recovering: tokio::sync::Mutex<()>,
}

impl PauseRuntime {
    pub(crate) fn new(
        sessions: Sessions,
        me: AgentId,
        book: Arc<ControlBook>,
        feed: &ControlFeed,
        source: SourceTopic,
        reconcile: mpsc::UnboundedSender<ConversationId>,
    ) -> Self {
        Self {
            laser: sessions.laser().clone(),
            sessions,
            me,
            book,
            control: (feed.stream_id, feed.topic_id),
            control_truncated: feed.truncated,
            source,
            reconcile,
            gates: Mutex::default(),
            roles: Mutex::default(),
            assignment: Mutex::default(),
            epoch: AtomicU64::new(1),
            recovered_epoch: AtomicU64::new(0),
            recovering: tokio::sync::Mutex::new(()),
        }
    }

    /// Serialize the work and control handling of one session in this
    /// runtime. A pause acknowledgment taken under the gate comes after every
    /// record of the session already in flight.
    pub(crate) async fn gate(&self, session: ConversationId) -> Gate<'_> {
        let mutex = Arc::clone(lock(&self.gates).entry(session).or_default());
        let guard = mutex.lock_owned().await;
        Gate {
            runtime: self,
            session,
            guard: Some(guard),
        }
    }

    /// Decide what to do with one work record of a session, under its gate.
    /// A paused session acknowledges the pause and parks the record. A
    /// resumed session acknowledges the resume and handles its held records
    /// first. A failed acknowledgment, parking, or completion is an error, so
    /// the record stays uncommitted.
    pub(crate) async fn hold(
        &self,
        worker: &dyn Replay,
        message: &AgentMessage,
    ) -> Result<Hold, LaserError> {
        let session = message.provenance.conversation_id;
        self.ensure_recovered().await?;
        loop {
            let control = self.control_state(session).await?;
            if let Some(pause) = control.paused() {
                self.acknowledge(session, TaskState::Paused, pause).await?;
                if control.flags.cancel_requested {
                    self.cancel_paused(session, true).await;
                }
                self.park(session, message, pause).await?;
                return Ok(Hold::Commit);
            }
            self.acknowledge_resume(session, &control).await?;
            if !control.flags.cancel_requested
                && self.has_pending(session)
                && self.drain(worker, session, false).await? == Drained::Interrupted
            {
                continue;
            }
            // A record redelivered after a crash between its parking and its
            // commit was just handled through the parking.
            return Ok(if self.handled(session, self.address(message)) {
                Hold::Commit
            } else {
                Hold::Handle
            });
        }
    }

    /// Bring one session up to date with its control requests outside the
    /// work path: acknowledge a pause the role is named in or already takes
    /// part in, end a paused session canceled, acknowledge a resume, and
    /// handle held records on partitions this member reads.
    pub(crate) async fn reconcile(&self, worker: &dyn Replay, session: ConversationId) {
        let _gate = self.gate(session).await;
        if let Err(error) = self.reconcile_locked(worker, session).await {
            warn!(%error, %session, "bringing a session up to date with its control requests failed, the next record or request retries");
        }
    }

    /// Run the recovery read before the first dispatch.
    pub(crate) async fn recover(&self) -> Result<(), LaserError> {
        self.ensure_recovered().await
    }

    /// Note the partitions this member reads. A changed assignment means
    /// another member may have written held work for a session this member
    /// now reads, so the next dispatch reads the lane again.
    pub(crate) fn observe_assignment(&self, assigned: Option<BTreeSet<u32>>) {
        let Some(assigned) = assigned else {
            return;
        };
        let mut current = lock(&self.assignment);
        match current.as_ref() {
            Some(known) if *known == assigned => {}
            Some(_) => {
                *current = Some(assigned);
                drop(current);
                self.invalidate();
            }
            None => *current = Some(assigned),
        }
    }

    /// Read the lane again before the next dispatch, after the consumer was
    /// reopened.
    pub(crate) fn invalidate(&self) {
        self.epoch.fetch_add(1, Ordering::AcqRel);
    }

    async fn reconcile_locked(
        &self,
        worker: &dyn Replay,
        session: ConversationId,
    ) -> Result<(), LaserError> {
        self.ensure_recovered().await?;
        let control = self.control_state(session).await?;
        if let Some(pause) = control.paused() {
            if control.flags.cancel_requested {
                self.cancel_paused(session, pause.named).await;
            } else if pause.named || self.involved(session) {
                self.acknowledge(session, TaskState::Paused, pause).await?;
            }
            return Ok(());
        }
        self.acknowledge_resume(session, &control).await?;
        if !control.flags.cancel_requested && self.has_pending(session) {
            self.drain(worker, session, true).await?;
        }
        Ok(())
    }

    // The bounded recovery read of the lane, once per assignment epoch, and
    // only when some session has a pause history or the control read could
    // not prove there is none.
    async fn ensure_recovered(&self) -> Result<(), LaserError> {
        if self.recovered_epoch.load(Ordering::Acquire) >= self.epoch.load(Ordering::Acquire) {
            return Ok(());
        }
        let _recovering = self.recovering.lock().await;
        let epoch = self.epoch.load(Ordering::Acquire);
        if self.recovered_epoch.load(Ordering::Acquire) >= epoch {
            return Ok(());
        }
        if self.control_truncated || self.book.has_pause_history() {
            let scan = scan_lane(&self.laser, Some(&self.me), None).await?;
            if !scan.truncated.is_empty() {
                warn!(
                    partitions = ?scan.truncated,
                    "session recovery read only the newest records of these agent.sessions partitions, held work older than that read is not recovered"
                );
            }
            let mut touched: std::collections::HashSet<ConversationId> =
                scan.sessions.keys().copied().collect();
            self.merge(scan);
            touched.extend(self.book.paused_or_resumed());
            for session in touched {
                let _ = self.reconcile.send(session);
            }
        }
        self.recovered_epoch.store(epoch, Ordering::Release);
        Ok(())
    }

    fn merge(&self, scan: LaneScan) {
        let mut roles = lock(&self.roles);
        for (session, facts) in scan.sessions {
            let role = roles.entry(session).or_default();
            role.handled.extend(facts.handled);
            for (key, parking) in facts.parked {
                role.parked.entry(key).or_insert(parking);
            }
            let handled = role.handled.clone();
            role.parked.retain(|key, _| !handled.contains(key));
            role.terminal |= facts.terminal;
            if let Some(ack) = facts.ack {
                role.record_ack(ack);
            }
        }
    }

    async fn control_state(&self, session: ConversationId) -> Result<ControlState, LaserError> {
        self.lens(session).control_state().await
    }

    fn lens(&self, session: ConversationId) -> Session {
        let mut lens = self.sessions.open(session).as_agent(self.me.clone());
        lens.control = Some(Arc::clone(&self.book));
        lens
    }

    fn control_position(&self, at: ControlPosition) -> LogPosition {
        LogPosition::new(self.control.0, self.control.1, at.0, at.1)
    }

    // Write `status(session, state)` acknowledging `request`, unless this
    // role's newest acknowledgment already is that one.
    async fn acknowledge(
        &self,
        session: ConversationId,
        state: TaskState,
        request: PlacedRequest,
    ) -> Result<(), LaserError> {
        let acknowledged = lock(&self.roles)
            .get(&session)
            .and_then(|role| role.ack)
            .is_some_and(|ack| ack.state == state && ack.request == request.at);
        if acknowledged {
            return Ok(());
        }
        let at = self.control_position(request.at);
        let transition = SessionTransition {
            actor: Some(self.me.clone()),
            acknowledges: Some(at),
        };
        let lens = self.lens(session);
        let lane = lens.lane()?;
        let mut status = lane
            .status(OPERATION_SESSION)
            .with_task_state(state)
            .body(encode_named(&transition)?)
            .content_type(ContentType::Cbor);
        if let Some(record) = request.record {
            status = status.with_cause(record, Some(at));
        }
        let receipt = status.send_receipt().await?;
        debug!(%session, %state, request = ?request.at, "acknowledged a session control request");
        lock(&self.roles)
            .entry(session)
            .or_default()
            .record_ack(Ack {
                state,
                request: request.at,
                lane_offset: receipt.offset,
            });
        Ok(())
    }

    // Acknowledge the resume when this role acknowledged a pause it lifts,
    // unless the session was canceled.
    async fn acknowledge_resume(
        &self,
        session: ConversationId,
        control: &ControlState,
    ) -> Result<(), LaserError> {
        let paused = !control.flags.cancel_requested
            && lock(&self.roles)
                .get(&session)
                .and_then(|role| role.ack)
                .is_some_and(|ack| ack.state == TaskState::Paused);
        match control.resume {
            Some(resume) if paused => self.acknowledge(session, TaskState::Working, resume).await,
            _ => Ok(()),
        }
    }

    // Append and confirm the parking record of `message` under `pause`. A
    // source this role already parked or handled is not parked again.
    async fn park(
        &self,
        session: ConversationId,
        message: &AgentMessage,
        pause: PlacedRequest,
    ) -> Result<(), LaserError> {
        let address = self.address(message);
        let known = lock(&self.roles).get(&session).is_some_and(|role| {
            role.parked
                .keys()
                .chain(role.handled.iter())
                .any(|key| key.source == address)
        });
        if known {
            debug!(%session, source = %message.id, "the record is already parked");
            return Ok(());
        }
        let parking = SessionParking {
            source: SourceRef::Message {
                stream: address.stream,
                topic: address.topic,
                partition: address.partition,
                offset: address.offset,
                generation: address.generation,
                conversation: Some(session.to_string()),
            },
            role: self.me.clone(),
            request: self.control_position(pause.at),
        };
        let key = ParkingKey::of(&parking).expect("a parking names a log record");
        let lens = self.lens(session);
        let lane = lens.lane()?;
        let mut event = lane
            .emit(encode_named(&parking)?)
            .with_operation(OPERATION_SESSION_PARKED)
            .content_type(ContentType::Cbor);
        if let Some(record) = message
            .envelope
            .as_ref()
            .and_then(|envelope| envelope.record)
        {
            event = event.with_cause(
                record,
                Some(LogPosition::new(
                    address.stream,
                    address.topic,
                    address.partition,
                    address.offset,
                )),
            );
        }
        event.send_receipt().await?;
        debug!(%session, source = %message.id, "parked a work record of a paused session");
        lock(&self.roles)
            .entry(session)
            .or_default()
            .parked
            .insert(key, parking);
        Ok(())
    }

    // Handle the held records of `session` once each, oldest source first,
    // and confirm a completion for each. Stops when the session is paused or
    // canceled again. A record whose source expired or whose topic was
    // recreated is reported and left held.
    async fn drain(
        &self,
        worker: &dyn Replay,
        session: ConversationId,
        assigned_only: bool,
    ) -> Result<Drained, LaserError> {
        for (address, held) in self.pending(session) {
            if assigned_only && !self.reads(address.partition) {
                continue;
            }
            let control = self.control_state(session).await?;
            if control.paused().is_some() || control.flags.cancel_requested {
                return Ok(Drained::Interrupted);
            }
            let Some(received) = self.fetch_source(address).await? else {
                warn!(%session, source = ?address, "a held record cannot be recovered: its source expired or its topic was recreated, it stays listed as held");
                let mut roles = lock(&self.roles);
                let role = roles.entry(session).or_default();
                role.unrecoverable
                    .extend(held.iter().map(|(key, _)| key.clone()));
                continue;
            };
            worker.replay(&received).await?;
            let lens = self.lens(session);
            let lane = lens.lane()?;
            for (key, parking) in held {
                lane.emit(encode_named(&parking)?)
                    .with_operation(OPERATION_SESSION_UNPARKED)
                    .content_type(ContentType::Cbor)
                    .send_receipt()
                    .await?;
                let mut roles = lock(&self.roles);
                let role = roles.entry(session).or_default();
                role.parked.remove(&key);
                role.handled.insert(key);
            }
            debug!(%session, source = ?address, "handled a held record after the resume");
        }
        Ok(Drained::All)
    }

    // The held records of `session` this runtime can handle: from its own
    // source topic and not already found unrecoverable, grouped by source.
    fn pending(
        &self,
        session: ConversationId,
    ) -> BTreeMap<SourceAddress, Vec<(ParkingKey, SessionParking)>> {
        let roles = lock(&self.roles);
        let mut pending: BTreeMap<SourceAddress, Vec<(ParkingKey, SessionParking)>> =
            BTreeMap::new();
        let Some(role) = roles.get(&session) else {
            return pending;
        };
        for (key, parking) in &role.parked {
            if key.source.stream != self.source.stream_id
                || key.source.topic != self.source.topic_id
                || role.unrecoverable.contains(key)
            {
                continue;
            }
            pending
                .entry(key.source)
                .or_default()
                .push((key.clone(), parking.clone()));
        }
        pending
    }

    fn address(&self, message: &AgentMessage) -> SourceAddress {
        SourceAddress {
            stream: self.source.stream_id,
            topic: self.source.topic_id,
            partition: message.id.partition_id,
            offset: message.id.offset,
            generation: Some(self.source.generation),
        }
    }

    fn handled(&self, session: ConversationId, address: SourceAddress) -> bool {
        lock(&self.roles)
            .get(&session)
            .is_some_and(|role| role.handled.iter().any(|key| key.source == address))
    }

    fn has_pending(&self, session: ConversationId) -> bool {
        !self.pending(session).is_empty()
    }

    fn involved(&self, session: ConversationId) -> bool {
        lock(&self.roles)
            .get(&session)
            .is_some_and(|role| role.ack.is_some() || !role.parked.is_empty())
    }

    fn reads(&self, partition: u32) -> bool {
        lock(&self.assignment)
            .as_ref()
            .is_none_or(|assigned| assigned.contains(&partition))
    }

    // End a paused session canceled as this role, once. Best effort: a
    // failed write is retried by the next record or request.
    async fn cancel_paused(&self, session: ConversationId, named: bool) {
        let (involved, terminal) = lock(&self.roles)
            .get(&session)
            .map_or((false, false), |role| {
                (role.ack.is_some() || !role.parked.is_empty(), role.terminal)
            });
        if terminal || !(named || involved) {
            return;
        }
        match self.lens(session).cancel().await {
            Ok(()) => {
                lock(&self.roles).entry(session).or_default().terminal = true;
                debug!(%session, "ended a paused session canceled, its held records stay unprocessed");
            }
            Err(error) => {
                warn!(%error, %session, "failed to end a paused session canceled");
            }
        }
    }

    // Read one held record back from its source. `None` when the record no
    // longer exists there or the topic was recreated since it was parked.
    async fn fetch_source(
        &self,
        address: SourceAddress,
    ) -> Result<Option<ConsumerMessage>, LaserError> {
        if address.generation != Some(self.source.generation) {
            return Ok(None);
        }
        let polled = self
            .laser
            .client()
            .poll_messages(
                &Identifier::numeric(address.stream)?,
                &Identifier::numeric(address.topic)?,
                Some(address.partition),
                &iggy::prelude::Consumer::new(Identifier::named(PARKED_READER)?),
                &PollingStrategy::offset(address.offset),
                1,
                false,
            )
            .await?;
        Ok(polled
            .messages
            .into_iter()
            .find(|message| message.header.offset == address.offset)
            .map(|message| ConsumerMessage::of(message, address.partition, address.offset, None)))
    }
}

/// The per-session gate of a [`PauseRuntime`].
pub(crate) struct Gate<'a> {
    runtime: &'a PauseRuntime,
    session: ConversationId,
    guard: Option<tokio::sync::OwnedMutexGuard<()>>,
}

impl Drop for Gate<'_> {
    fn drop(&mut self) {
        drop(self.guard.take());
        let mut gates = lock(&self.runtime.gates);
        if gates
            .get(&self.session)
            .is_some_and(|mutex| Arc::strong_count(mutex) == 1)
        {
            gates.remove(&self.session);
        }
    }
}

/// The pause driver: brings each session a control request names up to
/// date, one task per request, until `stop` fires.
pub(crate) async fn drive(
    runtime: Arc<PauseRuntime>,
    worker: Arc<dyn Replay>,
    mut requests: mpsc::UnboundedReceiver<ConversationId>,
    mut stop: oneshot::Receiver<()>,
) {
    let mut tasks = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            _ = &mut stop => break,
            next = requests.recv() => match next {
                Some(session) => {
                    let runtime = Arc::clone(&runtime);
                    let worker = Arc::clone(&worker);
                    tasks.spawn(async move { runtime.reconcile(worker.as_ref(), session).await });
                }
                None => break,
            },
            Some(_) = tasks.join_next(), if !tasks.is_empty() => {}
        }
    }
    while tasks.join_next().await.is_some() {}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Drained {
    All,
    Interrupted,
}

// The consumer name the bounded reads poll under. Reads never commit.
const CONTROL_READER: &str = "laser-control-follower";
const LANE_READER: &str = "laser-session-recovery";
const PARKED_READER: &str = "laser-parked-reader";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Ack {
    state: TaskState,
    request: ControlPosition,
    lane_offset: Option<u64>,
}

/// What one role knows of one session's pause history.
#[derive(Default)]
struct RoleState {
    /// The role's newest acknowledgment.
    ack: Option<Ack>,
    /// Held records without a completion.
    parked: BTreeMap<ParkingKey, SessionParking>,
    /// Held records with a completion.
    handled: BTreeSet<ParkingKey>,
    /// Held records whose source cannot be read back. They stay listed.
    unrecoverable: BTreeSet<ParkingKey>,
    /// The role wrote a terminal status.
    terminal: bool,
}

impl RoleState {
    // Keep the acknowledgment latest on the lane.
    fn record_ack(&mut self, ack: Ack) {
        if self
            .ack
            .is_some_and(|current| current.lane_offset > ack.lane_offset)
        {
            return;
        }
        self.ack = Some(ack);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct SourceAddress {
    stream: u32,
    topic: u32,
    partition: u32,
    offset: u64,
    generation: Option<u64>,
}

/// The identity of one parking: the source address, the role, and the pause
/// request position.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct ParkingKey {
    source: SourceAddress,
    role: String,
    request: [u8; 20],
}

impl ParkingKey {
    fn of(parking: &SessionParking) -> Option<Self> {
        let SourceRef::Message {
            stream,
            topic,
            partition,
            offset,
            generation,
            ..
        } = parking.source
        else {
            return None;
        };
        Some(Self {
            source: SourceAddress {
                stream,
                topic,
                partition,
                offset,
                generation,
            },
            role: parking.role.as_str().to_owned(),
            request: parking.request.to_bytes(),
        })
    }
}

/// The pause facts a bounded read of `agent.sessions` found.
#[derive(Default)]
struct LaneScan {
    sessions: HashMap<ConversationId, SessionFacts>,
    /// Partitions whose read did not reach their oldest retained record.
    truncated: BTreeSet<u32>,
}

#[derive(Default)]
struct SessionFacts {
    partition: Option<u32>,
    ack: Option<Ack>,
    parked: BTreeMap<ParkingKey, SessionParking>,
    handled: BTreeSet<ParkingKey>,
    terminal: bool,
}

impl SessionFacts {
    fn pending(&self) -> BTreeMap<ParkingKey, SessionParking> {
        self.parked
            .iter()
            .filter(|(key, _)| !self.handled.contains(*key))
            .map(|(key, parking)| (key.clone(), parking.clone()))
            .collect()
    }
}

impl LaneScan {
    fn complete_for(&self, session: ConversationId) -> bool {
        match self
            .sessions
            .get(&session)
            .and_then(|facts| facts.partition)
        {
            Some(partition) => !self.truncated.contains(&partition),
            None => self.truncated.is_empty(),
        }
    }

    // Fold one lane record. `role` keeps only that role's facts. `only`
    // keeps only one session and notes its partition.
    fn fold(
        &mut self,
        partition: u32,
        message: &IggyMessage,
        role: Option<&AgentId>,
        only: Option<ConversationId>,
    ) {
        let Ok(decoded) = decode_agent_record(message, features::NONE) else {
            return;
        };
        let Some(envelope) = decoded.envelope else {
            return;
        };
        let session: ConversationId = envelope.conversation.into();
        if only.is_some_and(|only| only != session) {
            return;
        }
        if only.is_some() {
            self.sessions.entry(session).or_default().partition = Some(partition);
        }
        match (envelope.kind, envelope.operation.as_deref()) {
            (AgentKind::Status, Some(OPERATION_SESSION)) => match envelope.task_state {
                Some(state) if state.is_terminal() => {
                    if role.is_some_and(|role| &envelope.source == role) {
                        self.sessions.entry(session).or_default().terminal = true;
                    }
                }
                Some(state @ (TaskState::Paused | TaskState::Working)) => {
                    let Some(role) = role else {
                        return;
                    };
                    if let Ok(SessionTransition {
                        actor: Some(actor),
                        acknowledges: Some(request),
                    }) = decode_named::<SessionTransition>(&envelope.body)
                        && &actor == role
                    {
                        self.sessions.entry(session).or_default().ack = Some(Ack {
                            state,
                            request: (request.partition_id, request.offset),
                            lane_offset: Some(message.header.offset),
                        });
                    }
                }
                _ => {}
            },
            (
                AgentKind::Event,
                Some(operation @ (OPERATION_SESSION_PARKED | OPERATION_SESSION_UNPARKED)),
            ) => {
                let Ok(parking) = decode_named::<SessionParking>(&envelope.body) else {
                    return;
                };
                if role.is_some_and(|role| &parking.role != role) {
                    return;
                }
                let Some(key) = ParkingKey::of(&parking) else {
                    return;
                };
                let facts = self.sessions.entry(session).or_default();
                if operation == OPERATION_SESSION_PARKED {
                    facts.parked.insert(key, parking);
                } else {
                    facts.handled.insert(key);
                }
            }
            _ => {}
        }
    }
}

// Read the newest records of every `agent.sessions` partition, up to the
// context read window, and fold the pause facts of `role` (every role when
// `None`) for `only` (every session when `None`).
async fn scan_lane(
    laser: &Laser,
    role: Option<&AgentId>,
    only: Option<ConversationId>,
) -> Result<LaneScan, LaserError> {
    let stream = Identifier::named(laser.stream_required()?)?;
    let topic = Identifier::named(AGENT_SESSIONS)?;
    let client = laser.client();
    let mut scan = LaneScan::default();
    let Some(details) = client.get_topic(&stream, &topic).await? else {
        return Ok(scan);
    };
    let reader = iggy::prelude::Consumer::new(Identifier::named(LANE_READER)?);
    for partition in 0..bounded_partitions(details.partitions_count) {
        let window = read_window(&client, &stream, &topic, &reader, partition).await?;
        if window.truncated {
            scan.truncated.insert(partition);
        }
        for message in &window.messages {
            scan.fold(partition, message, role, only);
        }
    }
    Ok(scan)
}

struct Window {
    messages: Vec<IggyMessage>,
    next_offset: u64,
    /// The partition holds or held records before the window.
    truncated: bool,
}

// The newest records of one partition, up to the context read window.
async fn read_window(
    client: &IggyClient,
    stream: &Identifier,
    topic: &Identifier,
    reader: &iggy::prelude::Consumer,
    partition: u32,
) -> Result<Window, LaserError> {
    let first = client
        .poll_messages(
            stream,
            topic,
            Some(partition),
            reader,
            &PollingStrategy::first(),
            1,
            false,
        )
        .await?
        .messages
        .first()
        .map(|message| message.header.offset);
    let Some(first) = first else {
        return Ok(Window {
            messages: Vec::new(),
            next_offset: 0,
            truncated: false,
        });
    };
    let start = tail_anchored_offset(client, stream, topic, reader, partition, first).await?;
    let batch = drain_partition(
        client,
        stream,
        topic,
        reader,
        DrainRange::open(partition, start),
        READ_BATCH,
    )
    .await?;
    Ok(Window {
        messages: batch.messages,
        next_offset: batch.next_offset,
        truncated: first > 0 || start > first,
    })
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parking(offset: u64, request: u64) -> SessionParking {
        SessionParking {
            source: SourceRef::Message {
                stream: 1,
                topic: 2,
                partition: 0,
                offset,
                generation: Some(9),
                conversation: None,
            },
            role: "worker".parse().expect("valid agent id"),
            request: LogPosition::new(1, 3, 0, request),
        }
    }

    #[test]
    fn given_parkings_and_completions_when_folded_then_should_keep_only_unhandled_records_by_source()
     {
        let mut facts = SessionFacts::default();
        for (offset, request) in [(7, 1), (4, 1), (7, 2)] {
            let parking = parking(offset, request);
            facts
                .parked
                .insert(ParkingKey::of(&parking).expect("a log record"), parking);
        }
        facts
            .handled
            .insert(ParkingKey::of(&parking(4, 1)).expect("a log record"));
        let pending: Vec<(u64, u64)> = facts
            .pending()
            .keys()
            .map(|key| {
                (
                    key.source.offset,
                    LogPosition::from_bytes(key.request).offset,
                )
            })
            .collect();
        assert_eq!(pending, vec![(7, 1), (7, 2)]);
    }

    #[test]
    fn given_acknowledgments_when_recorded_then_should_keep_the_latest_on_the_lane() {
        let mut role = RoleState::default();
        let ack = |state, offset| Ack {
            state,
            request: (0, offset),
            lane_offset: Some(offset),
        };
        role.record_ack(ack(TaskState::Working, 9));
        role.record_ack(ack(TaskState::Paused, 4));
        assert_eq!(role.ack.map(|ack| ack.state), Some(TaskState::Working));
        role.record_ack(ack(TaskState::Paused, 12));
        assert_eq!(role.ack.map(|ack| ack.state), Some(TaskState::Paused));
    }

    #[test]
    fn given_lane_operations_when_classified_then_should_count_only_work_commands() {
        assert!(is_work("invoke_agent"));
        assert!(is_work(""));
        assert!(!is_work(OPERATION_CHAT));
        assert!(!is_work(OPERATION_EXECUTE_TOOL));
        assert!(!is_work(laser_wire::dispatch::OPERATION_SESSION_PAUSE));
    }
}
