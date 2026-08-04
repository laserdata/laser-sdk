use crate::agent::consumer::{AgentMessage, decode_agent_record};
use crate::error::LaserError;
use crate::types::MessageId;
use dashmap::DashMap;
use iggy::prelude::*;
use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio::sync::oneshot;

const REPLY_BATCH: u32 = 200;
const REPLY_POLL_INTERVAL: Duration = Duration::from_millis(20);

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
    task: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    #[cfg(feature = "sign")]
    verifier: Option<Arc<crate::sign::KeyRegistry>>,
}

struct ReplyWaiter {
    sender: oneshot::Sender<AgentMessage>,
    #[cfg(feature = "sign")]
    expected_signer: Option<String>,
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
        let mut offsets: Vec<u64> = Vec::new();
        if let Some(details) = client.get_topic(&stream_id, &topic).await? {
            let partitions = crate::poll::bounded_partitions(details.partitions_count);
            offsets = vec![0u64; partitions as usize];
            for partition in 0..partitions {
                let polled = client
                    .poll_messages(
                        &stream_id,
                        &topic,
                        Some(partition),
                        &consumer,
                        &PollingStrategy::last(),
                        1,
                        false,
                    )
                    .await?;
                if let Some(last) = polled.messages.last() {
                    offsets[partition as usize] = last.header.offset.saturating_add(1);
                }
            }
        }
        let inner = Arc::new(ReplyHubInner {
            waiters: DashMap::new(),
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
        expected_signer: Option<String>,
    ) -> ReplyTicket {
        let (tx, rx) = oneshot::channel();
        #[cfg(not(feature = "sign"))]
        let _ = expected_signer;
        self.inner.waiters.insert(
            correlation.clone(),
            ReplyWaiter {
                sender: tx,
                #[cfg(feature = "sign")]
                expected_signer,
            },
        );
        ReplyTicket {
            hub: Arc::downgrade(&self.inner),
            correlation,
            rx,
        }
    }
}

/// A registered wait for one correlated reply. Consumed by [`wait`](Self::wait).
/// Dropping it (here or on timeout) deregisters the waiter.
pub(crate) struct ReplyTicket {
    hub: Weak<ReplyHubInner>,
    correlation: String,
    rx: oneshot::Receiver<AgentMessage>,
}

impl ReplyTicket {
    /// Await the correlated reply up to `timeout`, then deregister.
    pub(crate) async fn wait(mut self, timeout: Duration) -> Result<AgentMessage, LaserError> {
        match tokio::time::timeout(timeout, &mut self.rx).await {
            Ok(Ok(message)) => Ok(message),
            // The sender dropped without a reply (the hub went away), or the wait
            // timed out. Either way the caller sees a reply timeout.
            Ok(Err(_)) | Err(_) => Err(LaserError::Timeout("reply")),
        }
    }
}

impl Drop for ReplyTicket {
    fn drop(&mut self) {
        if let Some(hub) = self.hub.upgrade() {
            hub.waiters.remove(&self.correlation);
        }
    }
}

// Poll the reply topic forward and dispatch each record to its waiter. Holds only
// a `Weak` to the hub, upgrading per pass, so the task exits once the last `Laser`
// clone (and any outstanding ticket) is gone.
async fn dispatch_loop(
    client: Arc<IggyClient>,
    stream: Identifier,
    topic: Identifier,
    consumer: Consumer,
    mut offsets: Vec<u64>,
    hub: Weak<ReplyHubInner>,
) {
    loop {
        let Some(inner) = hub.upgrade() else {
            return;
        };
        // The reply topic may not exist at creation (a fan-out reply topic is
        // created by the first reply), so resolve partitions each pass until it is.
        let partitions = match client.get_topic(&stream, &topic).await {
            Ok(Some(details)) => crate::poll::bounded_partitions(details.partitions_count),
            _ => {
                drop(inner);
                tokio::time::sleep(REPLY_POLL_INTERVAL).await;
                continue;
            }
        };
        if (offsets.len() as u32) < partitions {
            offsets.resize(partitions as usize, 0);
        }
        let mut dispatched = false;
        for partition in 0..partitions {
            let from = offsets[partition as usize];
            let batch = match crate::poll::drain_partition(
                &client,
                &stream,
                &topic,
                &consumer,
                partition,
                from,
                REPLY_BATCH,
            )
            .await
            {
                Ok(batch) => batch,
                Err(_) => continue,
            };
            offsets[partition as usize] = batch.next_offset;
            for message in batch.messages {
                let Ok(decoded) = decode_agent_record(&message, laser_wire::agent::features::NONE)
                else {
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
                        let Ok(verified) = registry.verify_observed_at(
                            envelope,
                            context,
                            decoded.observed_at_micros,
                        ) else {
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
                drop(waiter);
                let Some((_, waiter)) = inner.waiters.remove(&correlation) else {
                    continue;
                };
                let reply = AgentMessage {
                    provenance,
                    id: MessageId::new(partition, message.header.offset),
                    payload: message.payload.to_vec(),
                    envelope: decoded.envelope,
                    content_type: decoded.content_type,
                    verified_principal,
                };
                let _ = waiter.sender.send(reply);
                dispatched = true;
            }
        }
        drop(inner);
        if !dispatched {
            tokio::time::sleep(REPLY_POLL_INTERVAL).await;
        }
    }
}
