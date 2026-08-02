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
                        _ = ticker.tick() => {
                            if let Err(error) = inner.flush().await {
                                tracing::warn!(%error, "linger flush failed (error: {error})");
                            }
                        }
                        _ = shutdown.notified() => {
                            if let Err(error) = inner.flush().await {
                                tracing::warn!(%error, "shutdown flush failed (error: {error})");
                            }
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

struct Inner {
    laser: Laser,
    stream: String,
    topic: String,
    partition_key: Option<String>,
    max_records: usize,
    max_bytes: usize,
    queue: Mutex<Queue>,
}

impl BatchingProducer {
    /// Enqueue one payload with optional headers. Flushes inline when a size
    /// bound trips, so backpressure lands on the sender, not the timer.
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
            self.inner.flush().await?;
        }
        Ok(())
    }

    /// Flush everything queued as one batch append. A no-op on an empty queue.
    pub async fn flush(&self) -> Result<(), LaserError> {
        self.inner.flush().await
    }

    /// Flush and stop the linger timer. The graceful shutdown spelling: the
    /// timer is signalled (never aborted mid-flush, which would drop a batch
    /// already taken from the queue) and awaited so its final drain completes,
    /// then a last flush covers anything enqueued in the meantime.
    pub async fn close(mut self) -> Result<(), LaserError> {
        self.shutdown.notify_one();
        if let Some(timer) = self.timer.take() {
            let _ = timer.await;
        }
        self.inner.flush().await
    }
}

impl Inner {
    async fn flush(&self) -> Result<(), LaserError> {
        let batch = {
            let mut queue = self.queue.lock().await;
            if queue.messages.is_empty() {
                return Ok(());
            }
            queue.payload = 0;
            std::mem::take(&mut queue.messages)
        };
        self.laser
            .send_batch_on(
                &self.stream,
                &self.topic,
                batch,
                self.partition_key.as_deref(),
            )
            .await
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
