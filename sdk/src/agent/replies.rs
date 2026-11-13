use crate::agent::consumer::{AgentMessage, DEAD_LETTER_REASON_DETAIL, decode_agent_record};
use crate::error::LaserError;
use crate::poll::{TopicFollower, bounded_partitions};
use crate::types::MessageId;
use dashmap::DashMap;
use iggy::prelude::*;
use laser_wire::agent::{
    AgentErrorBody, AgentKind, DeadLetterReason, OPERATION_TASK, RecordId, TaskState,
};
use laser_wire::framing::decode_named;
use laser_wire::query::Value;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use tokio::sync::oneshot;
use tracing::debug;

const REPLY_BATCH: u32 = 200;
// How long the server holds the hub's read while no reply lands. A reply ends
// the wait at once.
const REPLY_MAX_WAIT: Duration = Duration::from_secs(5);
// How long the hub waits before it looks again for a reply topic that does not
// exist yet or a read that failed.
const REPLY_RETRY_INTERVAL: Duration = Duration::from_millis(20);
const PENDING_REPLY_LIMIT: usize = 64;
/// How often a parked dispatcher checks that its hub still exists.
const IDLE_HUB_CHECK: Duration = Duration::from_secs(5);

/// One shared reply consumer per `(stream, reply topic)`. It decodes each record
/// once and completes the one waiter whose correlation matches, so N concurrent
/// requests on the same reply topic read the topic once between them instead of
/// each running its own tail-seeded scan. Cached on the connection and driven by
/// a background task that stops when the last `Laser` clone drops.
#[derive(Clone)]
pub(crate) struct ReplyHub {
    inner: Arc<ReplyHubInner>,
}

struct ReplyHubInner {
    // Correlation -> the one-shot completing that request. A reply for an unknown
    // correlation (already completed, or never waited) is dropped.
    waiters: DashMap<String, ReplyWaiter>,
    // Woken by every subscribe, so an idle dispatcher parks instead of
    // polling the reply topic with nobody waiting.
    subscribed: tokio::sync::Notify,
    // Woken when the last waiter leaves, so the dispatcher ends its held
    // reads at once instead of when they answer. Shared with the dispatcher
    // without keeping the hub alive.
    emptied: Arc<tokio::sync::Notify>,
    task: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    #[cfg(feature = "sign")]
    verifier: Option<Arc<crate::sign::KeyRegistry>>,
}

struct ReplyWaiter {
    state: Mutex<ReplyWaiterState>,
    expected: ExpectedReply,
    // Set by a `Working` status for this correlation in the request's session,
    // the pickup acknowledgment a contract reads as consumption.
    consumed: Arc<AtomicBool>,
    #[cfg(feature = "sign")]
    expected_signer: Option<String>,
}

struct ReplyWaiterState {
    sender: Option<oneshot::Sender<AgentMessage>>,
    request: Option<Option<MessageId>>,
    pending: Vec<AgentMessage>,
}

// What a record must carry to answer one request: the request's session, a
// reply kind, and, when the request named its sender, that sender as the
// addressee. A request that names its command record is not answered by an
// earlier copy of it that expired unconsumed.
pub(crate) struct ExpectedReply {
    pub(crate) session: crate::types::ConversationId,
    pub(crate) requester: Option<String>,
    pub(crate) command: Option<RecordId>,
}

impl ExpectedReply {
    pub(crate) fn accepts(&self, message: &AgentMessage) -> bool {
        if message.provenance.conversation_id != self.session {
            return false;
        }
        let addressee = match &message.envelope {
            Some(envelope) => {
                if !matches!(envelope.kind, AgentKind::Response | AgentKind::Error) {
                    return false;
                }
                if let (Some(command), Some(cause)) = (self.command, envelope.cause)
                    && cause != command
                    && envelope.kind == AgentKind::Error
                    && dropped_unconsumed(message)
                {
                    return false;
                }
                envelope
                    .target
                    .as_ref()
                    .map(|target| target.as_str().to_owned())
            }
            None => message
                .provenance
                .target_agent_id
                .as_ref()
                .map(|target| target.as_str().to_owned()),
        };
        match (&self.requester, addressee) {
            (Some(requester), Some(addressee)) => *requester == addressee,
            (Some(_), None) => false,
            (None, _) => true,
        }
    }

    pub(crate) fn acknowledges(&self, message: &AgentMessage) -> bool {
        message.provenance.conversation_id == self.session
            && message.envelope.as_ref().is_some_and(|envelope| {
                envelope.kind == AgentKind::Status
                    && envelope.operation.as_deref() == Some(OPERATION_TASK)
                    && envelope.task_state == Some(TaskState::Working)
            })
    }
}

// Whether the target answered that its consumer dropped the command past its
// deadline, before any handler ran.
pub(crate) fn dropped_unconsumed(reply: &AgentMessage) -> bool {
    let Some(envelope) = &reply.envelope else {
        return false;
    };
    decode_named::<AgentErrorBody>(&envelope.body)
        .ok()
        .and_then(|body| body.detail)
        .and_then(|detail| match detail.get(DEAD_LETTER_REASON_DETAIL) {
            Some(Value::Uint(code)) => Some(*code),
            Some(Value::Int(code)) => u64::try_from(*code).ok(),
            _ => None,
        })
        .is_some_and(|code| code == u64::from(DeadLetterReason::DeadlineExceeded.code()))
}

impl ReplyHubInner {
    fn forget(&self, correlation: &str) {
        self.waiters.remove(correlation);
        if self.waiters.is_empty() {
            self.emptied.notify_waiters();
        }
    }
}

impl Drop for ReplyHubInner {
    fn drop(&mut self) {
        if let Some(task) = self
            .task
            .lock()
            .expect("reply-hub task lock is not poisoned")
            .take()
        {
            task.abort();
        }
    }
}

impl ReplyHub {
    /// Create the hub and spawn its dispatcher, seeded at the reply topic's tail
    /// so it reads only records appended after creation. A waiter must
    /// [`subscribe`](Self::subscribe) before sending its request (registration is
    /// synchronous), so a reply, which cannot exist until the request is sent, is
    /// dispatched rather than missed.
    pub(crate) async fn create(
        client: Arc<IggyClient>,
        stream: String,
        topic: Identifier,
        #[cfg(feature = "sign")] verifier: Option<Arc<crate::sign::KeyRegistry>>,
    ) -> Result<Self, LaserError> {
        let stream_id = Identifier::named(&stream)?;
        let consumer = Consumer::new(Identifier::named("laser-reply-hub")?);
        let mut offsets = BTreeMap::new();
        if let Some(details) = client.get_topic(&stream_id, &topic).await? {
            for partition in 0..bounded_partitions(details.partitions_count) {
                let polled = client
                    .poll_messages(
                        &stream_id,
                        &topic,
                        partition,
                        &consumer,
                        &PollingStrategy::last(),
                        PollOptions {
                            count: 1,
                            ..PollOptions::default()
                        },
                    )
                    .await?;
                offsets.insert(
                    partition,
                    polled
                        .messages
                        .last()
                        .map_or(0, |last| last.header.offset.saturating_add(1)),
                );
            }
        }
        let emptied = Arc::new(tokio::sync::Notify::new());
        let inner = Arc::new(ReplyHubInner {
            waiters: DashMap::new(),
            subscribed: tokio::sync::Notify::new(),
            emptied: Arc::clone(&emptied),
            task: std::sync::Mutex::new(None),
            #[cfg(feature = "sign")]
            verifier,
        });
        let handle = tokio::spawn(dispatch_loop(
            client,
            stream_id,
            topic,
            consumer,
            offsets,
            Arc::downgrade(&inner),
            emptied,
        ));
        *inner
            .task
            .lock()
            .expect("reply-hub task lock is not poisoned") = Some(handle);
        Ok(Self { inner })
    }

    /// Register a waiter for `correlation`, returning a ticket. Synchronous, so a
    /// caller registers before sending the request the reply answers.
    pub(crate) fn subscribe(
        &self,
        correlation: String,
        expected: ExpectedReply,
        expected_signer: Option<String>,
    ) -> ReplyTicket {
        let (tx, rx) = oneshot::channel();
        let consumed = Arc::new(AtomicBool::new(false));
        #[cfg(not(feature = "sign"))]
        let _ = expected_signer;
        self.inner.waiters.insert(
            correlation.clone(),
            ReplyWaiter {
                state: Mutex::new(ReplyWaiterState {
                    sender: Some(tx),
                    request: None,
                    pending: Vec::new(),
                }),
                expected,
                consumed: Arc::clone(&consumed),
                #[cfg(feature = "sign")]
                expected_signer,
            },
        );
        self.inner.subscribed.notify_one();
        ReplyTicket {
            hub: Arc::downgrade(&self.inner),
            correlation,
            rx,
            consumed,
        }
    }
}

/// A registered wait for one correlated reply. Consumed by [`wait`](Self::wait).
/// Dropping it (here or on timeout) deregisters the waiter.
pub(crate) struct ReplyTicket {
    hub: Weak<ReplyHubInner>,
    correlation: String,
    rx: oneshot::Receiver<AgentMessage>,
    consumed: Arc<AtomicBool>,
}

impl ReplyTicket {
    /// Bind the committed request address before a correlated record can complete the wait.
    pub(crate) fn arm(&self, request: Option<MessageId>) {
        let Some(hub) = self.hub.upgrade() else {
            return;
        };
        let Some(waiter) = hub.waiters.get(&self.correlation) else {
            return;
        };
        let mut state = waiter
            .state
            .lock()
            .expect("reply waiter lock is not poisoned");
        state.request = Some(request);
        let reply = state
            .pending
            .iter()
            .position(|message| request.is_none_or(|address| message.id != address))
            .map(|index| state.pending.remove(index));
        let sender = reply.as_ref().and_then(|_| state.sender.take());
        drop(state);
        drop(waiter);
        if let (Some(sender), Some(reply)) = (sender, reply) {
            hub.forget(&self.correlation);
            let _ = sender.send(reply);
        }
    }

    /// Await the correlated reply up to `timeout`, then deregister.
    pub(crate) async fn wait(mut self, timeout: Duration) -> Result<AgentMessage, LaserError> {
        self.next_within(timeout)
            .await?
            .ok_or(LaserError::Timeout("reply"))
    }

    /// Await the correlated reply up to `timeout`, keeping the registration
    /// when none landed so the caller can wait again.
    pub(crate) async fn next_within(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<AgentMessage>, LaserError> {
        match tokio::time::timeout(timeout, &mut self.rx).await {
            Ok(Ok(message)) => Ok(Some(message)),
            // The sender dropped without a reply: the hub went away.
            Ok(Err(_)) => Err(LaserError::Timeout("reply")),
            Err(_) => Ok(None),
        }
    }

    /// Whether the target acknowledged pickup with a `Working` status.
    pub(crate) fn consumed(&self) -> bool {
        self.consumed.load(Ordering::Acquire)
    }
}

impl Drop for ReplyTicket {
    fn drop(&mut self) {
        if let Some(hub) = self.hub.upgrade() {
            hub.forget(&self.correlation);
        }
    }
}

// Follow the reply topic with held reads and dispatch each record to its
// waiter. Holds only a `Weak` to the hub, upgrading per batch, so the task
// exits once the last `Laser` clone (and any outstanding ticket) is gone.
// The held reads exist only while a waiter does: the last waiter leaving
// drops them, which cancels them on the server.
async fn dispatch_loop(
    client: Arc<IggyClient>,
    stream: Identifier,
    topic: Identifier,
    consumer: Consumer,
    mut offsets: BTreeMap<u32, u64>,
    hub: Weak<ReplyHubInner>,
    emptied: Arc<tokio::sync::Notify>,
) {
    let mut follower: Option<TopicFollower> = None;
    loop {
        // Registered before the emptiness check, so a waiter leaving after
        // it still ends the held read below.
        let last_waiter_left = emptied.notified();
        tokio::pin!(last_waiter_left);
        last_waiter_left.as_mut().enable();
        let Some(inner) = hub.upgrade() else {
            return;
        };
        // Nobody waits: stop the held read and park until a subscribe. The
        // permit `notify_one` stores covers a subscribe that lands before
        // this wait starts.
        if inner.waiters.is_empty() {
            follower = None;
            let parked = async {
                inner.subscribed.notified().await;
            };
            // Wake now and then to notice a hub that was dropped while idle.
            let _ = tokio::time::timeout(IDLE_HUB_CHECK, parked).await;
            drop(inner);
            continue;
        }
        drop(inner);
        let active = match follower.as_mut() {
            Some(active) => active,
            None => match follow_replies(&client, &stream, &topic, &consumer, &mut offsets).await {
                Some(started) => follower.insert(started),
                None => {
                    tokio::time::sleep(REPLY_RETRY_INTERVAL).await;
                    continue;
                }
            },
        };
        let next = tokio::select! {
            outcome = active.next(REPLY_MAX_WAIT) => Some(outcome),
            () = &mut last_waiter_left => None,
        };
        let Some(outcome) = next else {
            offsets.clone_from(active.positions());
            follower = None;
            continue;
        };
        match outcome {
            Ok(Some((partition, messages))) => {
                offsets.clone_from(active.positions());
                let Some(inner) = hub.upgrade() else {
                    return;
                };
                dispatch_batch(&inner, partition, messages);
            }
            // An idle wait also notices a reply topic that gained partitions.
            Ok(None) => {
                if let Ok(Some(details)) = client.get_topic(&stream, &topic).await
                    && bounded_partitions(details.partitions_count) as usize > offsets.len()
                {
                    follower = None;
                }
            }
            Err(error) => {
                debug!(%error, "following the reply topic failed, reading it again");
                offsets.clone_from(active.positions());
                follower = None;
                tokio::time::sleep(REPLY_RETRY_INTERVAL).await;
            }
        }
    }
}

// Start following every partition of the reply topic from `offsets`, a
// partition the hub has not read from its first record. `None` while the
// topic does not exist, since a fan-out reply topic is created by its first
// reply.
async fn follow_replies(
    client: &IggyClient,
    stream: &Identifier,
    topic: &Identifier,
    consumer: &Consumer,
    offsets: &mut BTreeMap<u32, u64>,
) -> Option<TopicFollower> {
    let details = client.get_topic(stream, topic).await.ok()??;
    for partition in 0..bounded_partitions(details.partitions_count) {
        offsets.entry(partition).or_insert(0);
    }
    match TopicFollower::start(
        client,
        stream,
        topic,
        consumer,
        offsets.clone(),
        REPLY_BATCH,
        REPLY_MAX_WAIT,
    )
    .await
    {
        Ok(follower) => Some(follower),
        Err(error) => {
            debug!(%error, "starting the reply topic read failed, trying again");
            None
        }
    }
}

// Complete the waiter of each reply in one partition's batch.
fn dispatch_batch(inner: &ReplyHubInner, partition: u32, messages: Vec<IggyMessage>) {
    for message in messages {
        let Ok(decoded) = decode_agent_record(&message, laser_wire::agent::features::NONE) else {
            continue;
        };
        let provenance = decoded.provenance;
        let Some(correlation) = provenance.correlation_id.clone() else {
            continue;
        };
        let Some(waiter) = inner.waiters.get(&correlation) else {
            continue;
        };
        #[cfg(feature = "sign")]
        let verified_principal = match &inner.verifier {
            Some(registry) => {
                let Some(envelope) = decoded.envelope.as_ref() else {
                    continue;
                };
                let Some(context) = decoded.signature_context.as_ref() else {
                    continue;
                };
                let Ok(verified) =
                    registry.verify_observed_at(envelope, context, decoded.observed_at_micros)
                else {
                    continue;
                };
                if waiter
                    .expected_signer
                    .as_deref()
                    .is_some_and(|expected| expected != verified.principal.as_str())
                {
                    continue;
                }
                Some(verified.principal)
            }
            None => None,
        };
        #[cfg(not(feature = "sign"))]
        let verified_principal = None;
        let reply = AgentMessage {
            provenance,
            id: MessageId::new(partition, message.header.offset),
            payload: message.payload.to_vec(),
            envelope: decoded.envelope,
            content_type: decoded.content_type,
            verified_principal,
        };
        if waiter.expected.acknowledges(&reply) {
            waiter.consumed.store(true, Ordering::Release);
            continue;
        }
        if !waiter.expected.accepts(&reply) {
            continue;
        }
        let mut state = waiter
            .state
            .lock()
            .expect("reply waiter lock is not poisoned");
        let Some(request) = state.request else {
            if state.pending.len() == PENDING_REPLY_LIMIT {
                state.pending.remove(0);
            }
            state.pending.push(reply);
            continue;
        };
        if request.is_some_and(|address| reply.id == address) {
            continue;
        }
        let sender = state.sender.take();
        drop(state);
        drop(waiter);
        if let Some(sender) = sender {
            inner.forget(&correlation);
            let _ = sender.send(reply);
        }
    }
}
