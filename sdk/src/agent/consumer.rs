use crate::agent::ctx::AgentCtx;
use crate::error::LaserError;
use crate::laser::Laser;
use crate::provenance::{AgentTopic, Provenance};
use crate::types::{AgentId, ConversationId, MessageId};
use async_trait::async_trait;
use iggy::consumer_ext::MessageConsumer;
use iggy::prelude::*;
use laser_wire::agent::{AgentDeadLetter, AgentEnvelope, DeadLetterReason, LogPosition};
use laser_wire::content::ContentType;
use laser_wire::framing::{decode_named, encode_named};
use laser_wire::headers::{AGENT_VERSION, CONTENT_TYPE};
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::str::FromStr;
use std::sync::Mutex;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::time::sleep;
use tracing::{debug, error, warn};

// Iggy defaults to a one-second poll, tuned for throughput. An agent runtime is
// latency-bound, so each hop would wait up to a second. Override per agent with
// `Agent::builder().poll_interval(..)`.
const POLL_INTERVAL: Duration = Duration::from_millis(10);

/// What you implement: one async `handle` per message. (`AgentHandler` is the `Send` variant the runtime drives.)
#[trait_variant::make(AgentHandler: Send)]
pub trait LocalAgentHandler {
    async fn handle(&self, message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError>;
}

/// A message delivered to a handler: decoded provenance, raw payload, and log position.
#[derive(Debug, Clone)]
pub struct AgentMessage {
    /// Provenance headers decoded off the message. For an AGDX message it is
    /// synthesized from the decoded [`envelope`](Self::envelope), so routing,
    /// dedup, and deadline work uniformly for both message shapes.
    pub provenance: Provenance,
    /// The raw message body. Owned `Vec<u8>` so the public API never leaks the
    /// `bytes` crate. Decode it with whatever codec the producer used.
    pub payload: Vec<u8>,
    /// Where the message sits on the log (partition and offset).
    pub id: MessageId,
    /// The decoded AGDX envelope when the message carries one (the `agdx.av`
    /// header is present). `None` for a plain `send_agent` message.
    pub envelope: Option<AgentEnvelope>,
}

impl AgentMessage {
    fn from_received(received: ReceivedMessage) -> Result<Self, LaserError> {
        // The message's own offset, not `received.current_offset` (the partition
        // high-water, shared across a polled batch).
        let id = MessageId::new(received.partition_id, received.message.header.offset);
        let (provenance, envelope) = provenance_and_envelope(&received.message)?;
        Ok(Self {
            provenance,
            payload: received.message.payload.to_vec(),
            id,
            envelope,
        })
    }
}

// Decode a log message into its runtime provenance and, when it is an AGDX
// message (the `agdx.av` header is present), its envelope. An AGDX message routes
// off the decoded envelope, whose typed fields the string-header provenance
// decoder cannot read. Everything else routes off the provenance headers. The
// read paths (the reliable consumer, context assembly, the stream reader) share
// this so AGDX and `send_agent` messages decode identically everywhere.
pub(crate) fn provenance_and_envelope(
    message: &IggyMessage,
) -> Result<(Provenance, Option<AgentEnvelope>), LaserError> {
    // Parse the header map once. An AGDX message (the `Agdx` producer always
    // stamps the `agdx.av` version header) routes off its decoded envelope, whose
    // typed fields the string-header provenance decoder cannot read. Everything
    // else routes off the provenance headers built from the same map.
    let headers = message.user_headers_map()?.unwrap_or_default();
    let version_key = HeaderKey::from_str(AGENT_VERSION)?;
    if headers.contains_key(&version_key) {
        let envelope: AgentEnvelope = decode_named(&message.payload)?;
        let provenance = provenance_from_envelope(&envelope);
        Ok((provenance, Some(envelope)))
    } else {
        Ok((crate::provenance::provenance_from_headers(&headers)?, None))
    }
}

// Synthesize the runtime provenance from an AGDX envelope, so the consumer's
// target filter, dedup, and deadline checks read one shape for both message
// kinds. Agent ids are name strings on both sides, so `source`/`target` map
// straight across, and a name the SDK validator rejects simply drops out.
fn provenance_from_envelope(envelope: &AgentEnvelope) -> Provenance {
    Provenance::builder()
        .conversation_id(envelope.conversation.into())
        .maybe_agent(AgentId::try_from(envelope.source.as_str()).ok())
        .maybe_target_agent_id(
            envelope
                .target
                .as_ref()
                .and_then(|target| AgentId::try_from(target.as_str()).ok()),
        )
        .maybe_idempotency_key(
            envelope
                .idempotency_key
                .as_ref()
                .map(|key| key.as_str().to_owned()),
        )
        .maybe_deadline(envelope.deadline_micros.map(IggyTimestamp::from))
        .build()
}

/// How the reliable consumer retries a transient handler error: capped attempts with exponential backoff.
#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    /// Total attempts before dead-lettering.
    pub max_attempts: u32,
    /// First backoff delay, doubled each attempt.
    pub base_delay: Duration,
}

impl RetryPolicy {
    /// A policy of `max_attempts` with exponential backoff from `base_delay`.
    pub fn backoff(max_attempts: u32, base_delay: Duration) -> Self {
        Self {
            max_attempts,
            base_delay,
        }
    }

    fn delay_for(&self, attempt: u32) -> Duration {
        self.base_delay
            .saturating_mul(2u32.saturating_pow(attempt.min(16)))
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            base_delay: Duration::from_millis(200),
        }
    }
}

/// The reliable consumer (consumer-group delivery + dedup + retry + DLQ). Most callers use `Agent::builder`, not this directly.
#[derive(bon::Builder)]
pub struct AgentConsumer {
    #[builder(into)]
    pub group: String,
    #[builder(into)]
    pub topic: String,
    #[builder(default = 10_000)]
    pub dedup_window: usize,
    #[builder(default)]
    pub retry: RetryPolicy,
    /// Poll interval, default a reactive 10ms. Raise for throughput-bound work.
    #[builder(default = POLL_INTERVAL)]
    pub poll_interval: Duration,
    pub respond_on: Option<AgentTopic<'static>>,
    // Override the dedup backend. Defaults to an in-memory `SlidingWindow` of
    // `dedup_window` keys, and a durable backend is a drop-in via this seam.
    pub deduplicator: Option<Box<dyn Deduplicator>>,
    // Replay the partition tail into the dedup window on startup so a restart does
    // not reprocess duplicates that are still inside the window. Off by default
    // (the at-least-once + idempotent-handler default tolerates the replay).
    #[builder(default)]
    pub warm_dedup: bool,
}

impl AgentConsumer {
    /// Consume until `shutdown` fires, dispatching each message to `handler`.
    /// `ready` fires once the consumer has joined its group and is polling.
    pub async fn run<H>(
        self,
        laser: &Laser,
        handler: H,
        ready: oneshot::Sender<()>,
        shutdown: oneshot::Receiver<()>,
    ) -> Result<(), LaserError>
    where
        H: AgentHandler + Sync,
    {
        let mut consumer = laser
            .client()
            .consumer_group(&self.group, laser.stream_required()?, &self.topic)?
            .auto_commit(AutoCommit::After(AutoCommitAfter::ConsumingEachMessage))
            .create_consumer_group_if_not_exists()
            .auto_join_consumer_group()
            .poll_interval(IggyDuration::new(self.poll_interval))
            .build();
        consumer.init().await?;

        let deduplicator = self
            .deduplicator
            .unwrap_or_else(|| Box::new(SlidingWindow::new(self.dedup_window)));
        if self.warm_dedup {
            warm_dedup_window(
                laser,
                &self.group,
                &self.topic,
                deduplicator.as_ref(),
                self.dedup_window,
            )
            .await?;
        }
        // Joined and dedup-warmed: signal readiness. A dropped receiver is fine.
        let _ = ready.send(());
        let agent = match self.group.parse() {
            Ok(id) => Some(id),
            Err(error) => {
                warn!(
                    %error,
                    group = %self.group,
                    "consumer group name is not a valid AgentId, so target-agent routing and \
                     `AgentCtx::respond`'s back-routing will not apply for this consumer",
                );
                None
            }
        };
        // Resolve the subscribed stream and topic to their numeric ids once, so
        // every dead-letter capsule can carry a complete `LogPosition` for the
        // poison message without a server round-trip per failure. The consumer has
        // already joined this stream/topic, so a missing id is a should-never
        // happen: warn loudly rather than silently stamping a wrong locator -
        // the partition and offset (the locate-within-topic half) stay correct.
        let stream_ident = Identifier::named(laser.stream_required()?)?;
        let topic_ident = Identifier::named(&self.topic)?;
        let stream_id = laser
            .client()
            .get_stream(&stream_ident)
            .await?
            .map(|details| details.id);
        let topic_id = laser
            .client()
            .get_topic(&stream_ident, &topic_ident)
            .await?
            .map(|details| details.id);
        if stream_id.is_none() || topic_id.is_none() {
            warn!(
                topic = %self.topic,
                "could not resolve the numeric stream/topic id, dead-letter capsules \
                 carry 0 for the unresolved locator half (partition and offset stay correct)"
            );
        }
        let (stream_id, topic_id) = (stream_id.unwrap_or_default(), topic_id.unwrap_or_default());
        let reliable = ReliableConsumer {
            handler,
            laser,
            retry: self.retry,
            dedup: deduplicator,
            agent,
            respond_on: self.respond_on,
            stream_id,
            topic_id,
        };
        // `consume_messages` needs its own shutdown receiver, so we keep that sender
        // alive so the loop only stops on our external `shutdown`. An explicit
        // shutdown (Ok) returns. A dropped handle (Err) leaves the consumer
        // running, matching the detached default.
        let (_keep_tx, keep_rx) = oneshot::channel();
        let consume = consumer.consume_messages(&reliable, keep_rx);
        tokio::pin!(consume);
        tokio::select! {
            result = &mut consume => result.map_err(LaserError::from),
            signal = shutdown => match signal {
                Ok(()) => Ok(()),
                Err(_) => consume.await.map_err(LaserError::from),
            },
        }
    }
}

struct ReliableConsumer<'a, H> {
    handler: H,
    laser: &'a Laser,
    retry: RetryPolicy,
    dedup: Box<dyn Deduplicator>,
    agent: Option<AgentId>,
    respond_on: Option<AgentTopic<'static>>,
    stream_id: u32,
    topic_id: u32,
}

impl<H> ReliableConsumer<'_, H> {
    fn log_position(&self, id: MessageId) -> LogPosition {
        LogPosition::new(self.stream_id, self.topic_id, id.partition_id, id.offset)
    }

    // Dead-letters a decoded message: the capsule carries the poison message's
    // log position, the reason code, the attempt count, a human detail, and the
    // original payload VERBATIM, so redrive is republishing those bytes.
    async fn dead_letter(
        &self,
        message: &AgentMessage,
        reason: DeadLetterReason,
        attempts: u32,
        detail: &str,
    ) {
        let capsule = AgentDeadLetter {
            source: self.log_position(message.id),
            reason,
            attempts,
            detail: Some(detail.to_owned()),
            payload: message.payload.clone(),
        };
        // Carry the original provenance for inspection, repointed at the poison
        // message. Clear the deadline so a deadline-bound DLQ consumer does not
        // re-drop the capsule for the very deadline that killed the original.
        let mut provenance = message.provenance.clone();
        provenance.causal_parent = Some(message.id);
        provenance.deadline = None;
        self.publish_dead_letter(provenance, message.id, capsule)
            .await;
    }

    // Dead-letters a message whose provenance could not be decoded. The original
    // payload rides verbatim so nothing is lost, and the synthetic provenance carries
    // only the source offset as the causal parent (there are no original headers
    // to keep - failing to decode them is why this path ran).
    async fn dead_letter_undecodable(&self, source: MessageId, payload: Vec<u8>) {
        let capsule = AgentDeadLetter {
            source: self.log_position(source),
            reason: DeadLetterReason::DecodeFailed,
            attempts: 0,
            detail: None,
            payload,
        };
        let provenance = Provenance::builder()
            .conversation_id(ConversationId::new())
            .causal_parent(source)
            .build();
        self.publish_dead_letter(provenance, source, capsule).await;
    }

    async fn publish_dead_letter(
        &self,
        provenance: Provenance,
        source: MessageId,
        capsule: AgentDeadLetter,
    ) {
        // DLQ publication is best-effort: the wrapper returns `Ok` either way so
        // the offset commits, so a failure here loses the poison message. Log it
        // loudly with the reason so the loss is never silent.
        let reason = capsule.reason;
        let payload = match encode_named(&capsule) {
            Ok(bytes) => bytes,
            Err(error) => {
                error!(%error, source = %source, ?reason, "failed to encode the dead-letter capsule, losing the poison message as its offset commits");
                return;
            }
        };
        let mut headers = match BTreeMap::<HeaderKey, HeaderValue>::try_from(&provenance) {
            Ok(headers) => headers,
            Err(error) => {
                error!(%error, source = %source, ?reason, "failed to encode the dead-letter headers, losing the poison message as its offset commits");
                return;
            }
        };
        // Mark the capsule body as cbor so a DLQ consumer is self-describing.
        match HeaderKey::from_str(CONTENT_TYPE) {
            Ok(key) => {
                headers.insert(key, HeaderValue::from(ContentType::Cbor.code()));
            }
            Err(error) => {
                error!(%error, "the content-type header key is invalid");
                return;
            }
        }
        let topic = AgentTopic::Dlq.topic_string();
        let key = provenance.partition_key();
        if let Err(error) = self
            .laser
            .send_with_headers(&topic, payload, headers, Some(&key))
            .await
        {
            error!(%error, source = %source, ?reason, "failed to publish the dead-letter capsule, losing the poison message as its offset commits");
        }
    }
}

impl<H> MessageConsumer for ReliableConsumer<'_, H>
where
    H: AgentHandler + Sync,
{
    async fn consume(&self, received: ReceivedMessage) -> Result<(), IggyError> {
        let source = MessageId::new(received.partition_id, received.current_offset);
        let raw = received.message.payload.clone();
        let message = match AgentMessage::from_received(received) {
            Ok(message) => message,
            Err(error) => {
                warn!(%error, source = %source, "undecodable provenance, dead-lettering raw payload");
                self.dead_letter_undecodable(source, raw.to_vec()).await;
                return Ok(());
            }
        };

        // Target-agent routing filter (defensive). Iggy's consumer-group
        // semantics already guarantee one delivery per group, so the
        // canonical one-agent-one-group setup (see `Agent` docstring) makes
        // this check a no-op in steady state. Bites in two cases:
        //   1. a publisher mis-addresses `target_agent_id` to the wrong
        //      agent that happens to subscribe to the same topic - drop
        //      cleanly instead of corrupting state with a misrouted handler
        //      invocation.
        //   2. operator error: two distinct agent ids accidentally joined
        //      the same consumer group, in which case Iggy delivers each
        //      message to ONE member and we want the other member's
        //      messages skipped, not handled.
        // Tolerating one-message-loss in case (2) is by design: the operator
        // is supposed to fix the group-per-agent setup, not have the SDK
        // paper over it by handling unrelated agents' work.
        if let (Some(target), Some(agent)) = (&message.provenance.target_agent_id, &self.agent)
            && target != agent
        {
            debug!(target = %target, agent = %agent, source = %message.id, "skipping message targeted at another agent");
            return Ok(());
        }

        if let Some(key) = &message.provenance.idempotency_key {
            // Dedup marks the key seen before the handler runs: a duplicate arriving
            // while the original is still in the window is skipped even if the
            // original later dead-letters. This is the at-least-once + idempotent
            // model, and a durable `Deduplicator` is the drop-in upgrade.
            if !self.dedup.observe(key).await {
                debug!(idempotency_key = %key, source = %message.id, "skipping duplicate message");
                return Ok(());
            }
        }

        if let Some(deadline) = message.provenance.deadline
            && IggyTimestamp::now().as_micros() > deadline.as_micros()
        {
            warn!(source = %message.id, "message past its deadline, dead-lettering");
            self.dead_letter(
                &message,
                DeadLetterReason::DeadlineExceeded,
                0,
                "message past its deadline",
            )
            .await;
            return Ok(());
        }

        let ctx = AgentCtx::new(
            self.laser,
            &message,
            self.agent.clone(),
            self.respond_on.clone(),
        );
        let mut attempt = 0;
        loop {
            match self.handler.handle(&message, &ctx).await {
                Ok(()) => {
                    debug!(source = %message.id, "message handled");
                    return Ok(());
                }
                Err(error) => {
                    if !error.is_retryable() {
                        warn!(%error, source = %message.id, "handler rejected message, dead-lettering without retry");
                        self.dead_letter(
                            &message,
                            DeadLetterReason::Rejected,
                            attempt + 1,
                            &error.to_string(),
                        )
                        .await;
                        return Ok(());
                    }
                    if attempt + 1 >= self.retry.max_attempts {
                        error!(%error, source = %message.id, attempts = attempt + 1, "handler exhausted retries, dead-lettering");
                        self.dead_letter(
                            &message,
                            DeadLetterReason::RetryExhausted,
                            attempt + 1,
                            &error.to_string(),
                        )
                        .await;
                        return Ok(());
                    }
                    warn!(%error, source = %message.id, attempt = attempt + 1, "handler failed, retrying");
                    sleep(self.retry.delay_for(attempt)).await;
                    attempt += 1;
                }
            }
        }
    }
}

// Pre-fills the dedup window from each partition so a freshly started consumer
// recognizes duplicates of messages it processed before the restart. Reads only
// up to the group's stored (already-consumed) offset and at most `depth` per
// partition: reading past the stored offset would pre-mark un-consumed messages
// and cause them to be skipped (data loss).
async fn warm_dedup_window(
    laser: &Laser,
    group: &str,
    topic: &str,
    dedup: &dyn Deduplicator,
    depth: usize,
) -> Result<(), LaserError> {
    let stream = Identifier::named(laser.stream_required()?)?;
    let topic_id = Identifier::named(topic)?;
    let Some(details) = laser.client().get_topic(&stream, &topic_id).await? else {
        return Ok(());
    };
    let group_consumer = Consumer::group(Identifier::named(group)?);
    let reader = Consumer::new(Identifier::named("laser-dedup-warmer")?);
    let depth = u64::try_from(depth).unwrap_or(u64::MAX);
    for partition in 0..details.partitions_count {
        let Some(offset) = laser
            .client()
            .get_consumer_offset(&group_consumer, &stream, &topic_id, Some(partition))
            .await?
        else {
            continue;
        };
        let stored = offset.stored_offset;
        let start = stored.saturating_sub(depth.saturating_sub(1));
        let count = u32::try_from(stored - start + 1).unwrap_or(u32::MAX);
        let polled = laser
            .client()
            .poll_messages(
                &stream,
                &topic_id,
                Some(partition),
                &reader,
                &PollingStrategy::offset(start),
                count,
                false,
            )
            .await?;
        for message in polled.messages {
            if message.header.offset > stored {
                continue;
            }
            if let Ok(provenance) = Provenance::try_from(&message)
                && let Some(key) = &provenance.idempotency_key
            {
                dedup.observe(key).await;
            }
        }
    }
    Ok(())
}

/// The dedup seam: decides whether an idempotency key has been seen before. The
/// default `SlidingWindow` is an in-memory bounded set. A durable backend (a
/// `StateStore`, or infrastructure-side dedup) is a drop-in. `observe` is async
/// and the trait is `dyn`-safe so a premium backend can do I/O behind it.
#[async_trait]
pub trait Deduplicator: Send + Sync {
    // Records the key and returns true if it is new, false if already seen.
    async fn observe(&self, key: &str) -> bool;
}

/// The default `Deduplicator`: an in-memory bounded set of recent keys.
pub struct SlidingWindow {
    inner: Mutex<DedupWindow>,
}

impl SlidingWindow {
    /// A window that remembers the most recent `capacity` keys.
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(DedupWindow::new(capacity)),
        }
    }
}

#[async_trait]
impl Deduplicator for SlidingWindow {
    async fn observe(&self, key: &str) -> bool {
        self.inner
            .lock()
            .expect("the dedup mutex is not poisoned")
            .insert(key)
    }
}

struct DedupWindow {
    capacity: usize,
    seen: HashSet<String>,
    order: VecDeque<String>,
}

impl DedupWindow {
    fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            seen: HashSet::new(),
            order: VecDeque::new(),
        }
    }

    fn insert(&mut self, key: &str) -> bool {
        if self.seen.contains(key) {
            return false;
        }
        if self.order.len() >= self.capacity
            && let Some(evicted) = self.order.pop_front()
        {
            self.seen.remove(&evicted);
        }
        self.seen.insert(key.to_owned());
        self.order.push_back(key.to_owned());
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_a_seen_key_when_inserting_again_then_should_report_a_duplicate() {
        let mut window = DedupWindow::new(8);
        assert!(window.insert("a"));
        assert!(!window.insert("a"));
        assert!(window.insert("b"));
    }

    #[test]
    fn given_a_full_window_when_inserting_then_should_evict_the_oldest_key() {
        let mut window = DedupWindow::new(2);
        assert!(window.insert("a"));
        assert!(window.insert("b"));
        assert!(window.insert("c"));
        assert!(window.insert("a"));
    }

    #[test]
    fn given_increasing_attempts_when_computing_backoff_then_should_grow_and_stay_bounded() {
        let policy = RetryPolicy::backoff(5, Duration::from_millis(100));
        assert_eq!(policy.delay_for(0), Duration::from_millis(100));
        assert_eq!(policy.delay_for(1), Duration::from_millis(200));
        assert_eq!(policy.delay_for(2), Duration::from_millis(400));
        assert!(policy.delay_for(60) >= policy.delay_for(2));
    }
}
