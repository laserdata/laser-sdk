use crate::error::LaserError;
use crate::laser::Laser;
use iggy::prelude::{HeaderKey, HeaderValue, IggyMessage};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Notify};

/// Flush at this many queued records unless overridden.
pub const DEFAULT_MAX_RECORDS: usize = 512;
/// Flush at this many queued payload bytes unless overridden (1 MiB).
pub const DEFAULT_MAX_BYTES: usize = 1024 * 1024;
/// Flush a non-empty queue after at most this long unless overridden.
pub const DEFAULT_LINGER: Duration = Duration::from_millis(5);
/// The smallest linger the timer runs at. `tokio::time::interval` panics on a
/// zero period, and a sub-millisecond linger spins the timer hot for no gain
/// on a batching producer, so a zero or tiny value is floored here.
pub const MIN_LINGER: Duration = Duration::from_millis(1);

/// Builder for a [`BatchingProducer`], opened with
/// [`Topic::batching`](crate::stream::Topic::batching). Every bound is
/// explicit: the batch flushes on whichever of `max_records`, `max_bytes`, or
/// `linger` trips first.
pub struct BatchingProducerBuilder {
    laser: Laser,
    stream: String,
    topic: String,
    partition_key: Option<String>,
    max_records: usize,
    max_bytes: usize,
    linger: Duration,
}

impl BatchingProducerBuilder {
    pub(crate) fn new(laser: Laser, stream: String, topic: String) -> Self {
        Self {
            laser,
            stream,
            topic,
            partition_key: None,
            max_records: DEFAULT_MAX_RECORDS,
            max_bytes: DEFAULT_MAX_BYTES,
            linger: DEFAULT_LINGER,
        }
    }

    /// Flush once this many records are queued.
    #[must_use]
    pub fn max_records(mut self, n: usize) -> Self {
        self.max_records = n.max(1);
        self
    }

    /// Flush once the queued payload bytes reach this bound.
    #[must_use]
    pub fn max_bytes(mut self, payload: usize) -> Self {
        self.max_bytes = payload.max(1);
        self
    }

    /// Flush a non-empty queue after at most this long, so a trickle of
    /// records never waits for a full batch.
    #[must_use]
    pub fn linger(mut self, linger: Duration) -> Self {
        self.linger = linger;
        self
    }

    /// Pin every record in this handle to one partition key. One key per
    /// handle by construction: batches are flushed whole under a single
    /// partitioning, so ordering within the key is never silently
    /// interleaved. Without a key, Iggy's balanced partitioner spreads each
    /// flushed batch.
    #[must_use]
    pub fn partition_key(mut self, key: impl Into<String>) -> Self {
        self.partition_key = Some(key.into());
        self
    }

    /// Build the handle and start its linger timer.
    pub fn build(self) -> BatchingProducer {
        let inner = Arc::new(Inner {
            laser: self.laser,
            stream: self.stream,
            topic: self.topic,
            partition_key: self.partition_key,
            max_records: self.max_records,
            max_bytes: self.max_bytes,
            queue: Mutex::new(Queue::default()),
            kept: Mutex::new(None),
        });
        let shutdown = Arc::new(Notify::new());
        let timer = {
            let inner = Arc::clone(&inner);
            let shutdown = Arc::clone(&shutdown);
            let linger = self.linger.max(MIN_LINGER);
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(linger);
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    // A graceful shutdown drains the queue and exits. Aborting
                    // instead would drop a flush future mid-send (the batch is
                    // already taken out of the queue), losing it silently, so
                    // `close` signals here rather than calling `abort`.
                    tokio::select! {
                        _ = ticker.tick() => inner.flush_on_timer().await,
                        _ = shutdown.notified() => {
                            inner.flush_on_timer().await;
                            break;
                        }
                    }
                }
            })
        };
        BatchingProducer {
            inner,
            timer: Some(timer),
            shutdown,
        }
    }
}

/// A size-and-time batching publisher over one topic: `send` enqueues, the
/// queue flushes as ONE Iggy `send_messages` call when `max_records`,
/// `max_bytes`, or `linger` trips, whichever first. Opt-in construction: the
/// unbatched publish path is untouched.
///
/// `flush().await` is the guaranteed path. Dropping the handle flushes
/// best-effort on a background task and logs a failure. A caller that needs
/// the last batch on the log awaits `flush` (or [`close`](Self::close))
/// before dropping.
///
/// A failed linger flush never stops the timer. Its failure is kept and
/// returned by the next `flush` or `close`, after that call has drained the
/// queue. When several batches fail before the caller asks, the report is
/// one [`LaserError::PublishFailed`] that lists the records of all of them.
pub struct BatchingProducer {
    inner: Arc<Inner>,
    timer: Option<tokio::task::JoinHandle<()>>,
    shutdown: Arc<Notify>,
}

#[derive(Default)]
struct Queue {
    messages: Vec<IggyMessage>,
    payload: usize,
}

#[derive(Clone, Copy)]
enum Trigger {
    Send,
    Timer,
    Caller,
}

struct Inner {
    laser: Laser,
    stream: String,
    topic: String,
    partition_key: Option<String>,
    max_records: usize,
    max_bytes: usize,
    queue: Mutex<Queue>,
    // Held across the append, so batches reach the log in queue order. It
    // guards the failure a timer flush left for the caller.
    kept: Mutex<Option<LaserError>>,
}

impl BatchingProducer {
    /// Enqueue one payload with optional headers. Flushes inline when a size
    /// bound trips, so backpressure lands on the sender, not the timer. An
    /// error is the failure of that inline flush and lists this record as
    /// unconfirmed. A failed linger flush is reported by [`flush`](Self::flush)
    /// or [`close`](Self::close), never here.
    pub async fn send(
        &self,
        payload: impl Into<Vec<u8>>,
        headers: BTreeMap<HeaderKey, HeaderValue>,
    ) -> Result<(), LaserError> {
        let payload = payload.into();
        let message = IggyMessage::builder()
            .payload(payload.into())
            .user_headers(headers)
            .build()?;
        let flush_now = {
            let mut queue = self.inner.queue.lock().await;
            queue.payload += message.payload.len();
            queue.messages.push(message);
            queue.messages.len() >= self.inner.max_records || queue.payload >= self.inner.max_bytes
        };
        if flush_now {
            self.inner.flush(Trigger::Send).await?;
        }
        Ok(())
    }

    /// Flush everything queued as one batch append, then report the failure
    /// an earlier linger flush left, if any. A no-op on an empty queue.
    pub async fn flush(&self) -> Result<(), LaserError> {
        self.inner.flush(Trigger::Caller).await
    }

    /// Flush and stop the linger timer. The graceful shutdown spelling: the
    /// timer is signalled (never aborted mid-flush, which would drop a batch
    /// already taken from the queue) and awaited so its final drain completes,
    /// then a last flush covers anything enqueued in the meantime and
    /// reports a kept linger failure.
    pub async fn close(mut self) -> Result<(), LaserError> {
        self.shutdown.notify_one();
        if let Some(timer) = self.timer.take() {
            let _ = timer.await;
        }
        self.inner.flush(Trigger::Caller).await
    }
}

impl Inner {
    async fn flush(&self, trigger: Trigger) -> Result<(), LaserError> {
        let mut kept = self.kept.lock().await;
        let batch = {
            let mut queue = self.queue.lock().await;
            queue.payload = 0;
            std::mem::take(&mut queue.messages)
        };
        let sent = if batch.is_empty() {
            Ok(())
        } else {
            self.laser
                .send_batch_on(
                    &self.stream,
                    &self.topic,
                    batch,
                    self.partition_key.as_deref(),
                )
                .await
                .map(|_| ())
        };
        settle(trigger, &mut kept, sent)
    }

    async fn flush_on_timer(&self) {
        // A timer flush keeps its failure for the caller, so it has nothing to return.
        let _ = self.flush(Trigger::Timer).await;
    }
}

// Decides who hears about a flush result. A sender hears its own inline
// flush. The timer keeps a failure. The caller hears everything kept so far
// together with its own flush.
fn settle(
    trigger: Trigger,
    kept: &mut Option<LaserError>,
    sent: Result<(), LaserError>,
) -> Result<(), LaserError> {
    match trigger {
        Trigger::Send => sent,
        Trigger::Timer => {
            if let Err(error) = sent {
                tracing::warn!(%error, "linger flush failed");
                *kept = Some(match kept.take() {
                    Some(earlier) => merge(earlier, error),
                    None => error,
                });
            }
            Ok(())
        }
        Trigger::Caller => match (kept.take(), sent) {
            (None, sent) => sent,
            (Some(earlier), Ok(())) => Err(earlier),
            (Some(earlier), Err(error)) => Err(merge(earlier, error)),
        },
    }
}

// One report for every batch that failed: the first cause, with the records
// of the later batch added. A later failure that carries no records is logged.
fn merge(earlier: LaserError, later: LaserError) -> LaserError {
    match (earlier, later) {
        (LaserError::PublishFailed(mut first), LaserError::PublishFailed(next)) => {
            let next = *next;
            first.committed.extend(next.committed);
            first.unconfirmed.extend(next.unconfirmed);
            LaserError::PublishFailed(first)
        }
        (earlier, later) => {
            tracing::warn!(error = %later, "a later batch flush failed while an earlier failure was kept");
            earlier
        }
    }
}

impl Drop for BatchingProducer {
    fn drop(&mut self) {
        // Signal the timer to drain and exit rather than aborting it (abort
        // could drop a flush future mid-send). Best-effort: the guaranteed
        // path is an awaited `flush`/`close`. `close` already took the timer
        // handle, so this only fires on a bare drop.
        if self.timer.take().is_some() {
            self.shutdown.notify_one();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::PublishFailure;

    fn producer() -> BatchingProducer {
        let laser = Laser::from_client(iggy::prelude::IggyClient::default());
        BatchingProducerBuilder::new(laser, "fleet".into(), "readings".into()).build()
    }

    fn publish_failure(payloads: &[&'static str]) -> LaserError {
        LaserError::PublishFailed(Box::new(PublishFailure {
            source: LaserError::Invalid("append refused".into()),
            stream: "fleet".into(),
            topic: "readings".into(),
            committed: Vec::new(),
            unconfirmed: payloads
                .iter()
                .map(|payload| {
                    IggyMessage::builder()
                        .payload((*payload).into())
                        .build()
                        .expect("a test message should build")
                })
                .collect(),
        }))
    }

    fn unconfirmed(error: &LaserError) -> Vec<&[u8]> {
        match error {
            LaserError::PublishFailed(failure) => failure
                .unconfirmed
                .iter()
                .map(|message| message.payload.as_ref())
                .collect(),
            other => panic!("expected a publish failure, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn given_a_kept_timer_failure_when_flushing_then_should_report_it_once() {
        let producer = producer();
        *producer.inner.kept.lock().await = Some(LaserError::Invalid("failed batch".into()));
        producer.inner.flush_on_timer().await;
        assert!(matches!(
            producer.flush().await,
            Err(LaserError::Invalid(_))
        ));
        producer
            .close()
            .await
            .expect("the reported failure is not repeated");
    }

    #[tokio::test]
    async fn given_a_kept_timer_failure_when_closing_then_should_not_report_success() {
        let producer = producer();
        *producer.inner.kept.lock().await = Some(LaserError::Invalid("failed batch".into()));
        assert!(matches!(
            producer.close().await,
            Err(LaserError::Invalid(_))
        ));
    }

    #[test]
    fn given_a_failed_timer_flush_when_settled_then_should_keep_it_and_report_success() {
        let mut kept = None;
        settle(Trigger::Timer, &mut kept, Err(publish_failure(&["a"])))
            .expect("the timer keeps the failure");
        assert_eq!(
            unconfirmed(kept.as_ref().expect("the failure is kept")),
            [b"a"]
        );
    }

    #[test]
    fn given_two_failed_timer_flushes_when_settled_then_should_keep_the_records_of_both() {
        let mut kept = None;
        settle(Trigger::Timer, &mut kept, Err(publish_failure(&["a"])))
            .expect("the timer keeps the first failure");
        settle(Trigger::Timer, &mut kept, Err(publish_failure(&["b", "c"])))
            .expect("the timer keeps the second failure");
        assert_eq!(
            unconfirmed(kept.as_ref().expect("the failures are kept")),
            [b"a".as_slice(), b"b", b"c"]
        );
    }

    #[test]
    fn given_a_kept_failure_when_a_sender_flush_succeeds_then_should_not_report_it_to_the_sender() {
        let mut kept = Some(publish_failure(&["a"]));
        settle(Trigger::Send, &mut kept, Ok(())).expect("the sender hears only its own flush");
        assert!(kept.is_some());
    }

    #[test]
    fn given_a_kept_failure_when_the_caller_flush_also_fails_then_should_report_both_once() {
        let mut kept = Some(publish_failure(&["a"]));
        let error = settle(Trigger::Caller, &mut kept, Err(publish_failure(&["b"])))
            .expect_err("the caller hears both failures");
        assert_eq!(unconfirmed(&error), [b"a".as_slice(), b"b"]);
        assert!(kept.is_none());
    }

    #[test]
    fn given_a_kept_failure_when_the_caller_flush_succeeds_then_should_report_the_kept_one() {
        let mut kept = Some(publish_failure(&["a"]));
        let error = settle(Trigger::Caller, &mut kept, Ok(()))
            .expect_err("the caller hears the kept failure");
        assert_eq!(unconfirmed(&error), [b"a".as_slice()]);
        assert!(kept.is_none());
    }
}
