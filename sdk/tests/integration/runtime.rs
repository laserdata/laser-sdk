use crate::harness;
use async_trait::async_trait;
use bytes::Bytes;
use laser_sdk::prelude::full::*;
use laser_sdk::wire::agent::AgentDeadLetter;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::time::sleep;

// R2: a handler that records when it starts and when it finishes, so a shutdown
// can be issued mid-flight and the finish observed.
struct SlowHandler {
    started: Arc<AtomicBool>,
    finished: Arc<AtomicBool>,
    work: Duration,
}

impl AgentHandler for SlowHandler {
    async fn handle(&self, _message: &AgentMessage, _ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        self.started.store(true, Ordering::SeqCst);
        sleep(self.work).await;
        self.finished.store(true, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_message_in_flight_when_shut_down_then_should_drain_before_returning() {
    let laser = harness::laser().await;
    let started = Arc::new(AtomicBool::new(false));
    let finished = Arc::new(AtomicBool::new(false));
    let handle = Agent::builder()
        .id("drainer".parse().expect("valid agent id"))
        .listen_on(AgentTopic::Commands)
        .handler(SlowHandler {
            started: started.clone(),
            finished: finished.clone(),
            work: Duration::from_millis(800),
        })
        .build()
        .spawn(laser.clone());

    let provenance = Provenance::builder()
        .conversation_id(ConversationId::new())
        .build();
    laser
        .send_agent(
            AgentTopic::Commands,
            Bytes::from_static(b"tick"),
            &provenance,
        )
        .await
        .expect("send");

    // Wait until the handler is mid-flight, then shut down. A graceful drain must
    // let the in-flight message finish before returning, so `finished` holds.
    harness::eventually(|| {
        let started = started.clone();
        async move { started.load(Ordering::SeqCst).then_some(()) }
    })
    .await;
    handle
        .shutdown()
        .await
        .expect("graceful shutdown returns Ok");
    assert!(
        finished.load(Ordering::SeqCst),
        "shutdown must drain the in-flight message, not abort it"
    );
}

// R14: a dead-letter sink that records every capsule it is handed.
struct RecordingSink {
    capsules: Arc<std::sync::Mutex<Vec<AgentDeadLetter>>>,
}

#[async_trait]
impl DeadLetterSink for RecordingSink {
    async fn on_dead_letter(
        &self,
        _message: Option<&AgentMessage>,
        capsule: &AgentDeadLetter,
        _publish_result: &Result<(), LaserError>,
    ) {
        self.capsules
            .lock()
            .expect("sink lock is not poisoned")
            .push(capsule.clone());
    }
}

// A middleware that counts how many times a handler outcome was observed.
struct CountingMiddleware {
    after: Arc<AtomicUsize>,
}

#[async_trait]
impl AgentMiddleware for CountingMiddleware {
    async fn after_handle(
        &self,
        _message: &AgentMessage,
        _result: &Result<(), LaserError>,
        _attempt: u32,
    ) {
        self.after.fetch_add(1, Ordering::SeqCst);
    }
}

struct RejectingHandler;

impl AgentHandler for RejectingHandler {
    async fn handle(&self, _message: &AgentMessage, _ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        Err(LaserError::rejected("nope"))
    }
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_rejected_message_when_dead_lettered_then_should_notify_the_sink_and_middleware() {
    let laser = harness::laser().await;
    let capsules = Arc::new(std::sync::Mutex::new(Vec::new()));
    let after = Arc::new(AtomicUsize::new(0));
    let handle = Agent::builder()
        .id("rejecter".parse().expect("valid agent id"))
        .listen_on(AgentTopic::Commands)
        .handler(RejectingHandler)
        .on_dead_letter(Arc::new(RecordingSink {
            capsules: capsules.clone(),
        }))
        .middleware(vec![Arc::new(CountingMiddleware {
            after: after.clone(),
        })])
        .build()
        .spawn(laser.clone());

    let provenance = Provenance::builder()
        .conversation_id(ConversationId::new())
        .build();
    laser
        .send_agent(
            AgentTopic::Commands,
            Bytes::from_static(b"work"),
            &provenance,
        )
        .await
        .expect("send");

    let seen = harness::eventually(|| {
        let capsules = capsules.clone();
        async move {
            let items = capsules.lock().expect("lock").clone();
            (!items.is_empty()).then_some(items)
        }
    })
    .await;
    assert_eq!(seen.len(), 1, "the sink observes exactly one dead-letter");
    assert!(
        after.load(Ordering::SeqCst) >= 1,
        "the middleware observed the handler outcome"
    );
    handle.shutdown().await.expect("shutdown");
}

// R1: a handler that blocks on a "slow" payload while flagging in-flight, and
// counts "fast" payloads handled during that window. Under serial-per-partition
// a fast message on another partition is handled while the slow one blocks its
// own lane. Under strict serial nothing would run until the slow one returns.
struct LaneHandler {
    slow_in_flight: Arc<AtomicBool>,
    fast_during_slow: Arc<AtomicUsize>,
    work: Duration,
}

impl AgentHandler for LaneHandler {
    async fn handle(&self, message: &AgentMessage, _ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        if message.body() == b"slow" {
            self.slow_in_flight.store(true, Ordering::SeqCst);
            sleep(self.work).await;
            self.slow_in_flight.store(false, Ordering::SeqCst);
        } else if self.slow_in_flight.load(Ordering::SeqCst) {
            self.fast_during_slow.fetch_add(1, Ordering::SeqCst);
        }
        Ok(())
    }
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_serial_per_partition_when_one_partition_blocks_then_others_still_progress() {
    let laser = harness::laser().await;
    let slow_in_flight = Arc::new(AtomicBool::new(false));
    let fast_during_slow = Arc::new(AtomicUsize::new(0));
    let handle = Agent::builder()
        .id("lanes".parse().expect("valid agent id"))
        .listen_on(AgentTopic::Commands)
        .handler(LaneHandler {
            slow_in_flight: slow_in_flight.clone(),
            fast_during_slow: fast_during_slow.clone(),
            work: Duration::from_millis(2000),
        })
        .concurrency(ConcurrencyPolicy::SerialPerPartition { max_partitions: 4 })
        .build()
        .spawn(laser.clone());

    // The slow message first, then several fast ones on distinct conversation
    // keys so at least one lands on a different partition than the slow message.
    let slow = Provenance::builder()
        .conversation_id(ConversationId::new())
        .build();
    laser
        .send_agent(AgentTopic::Commands, Bytes::from_static(b"slow"), &slow)
        .await
        .expect("send slow");

    harness::eventually(|| {
        let slow_in_flight = slow_in_flight.clone();
        async move { slow_in_flight.load(Ordering::SeqCst).then_some(()) }
    })
    .await;

    for _ in 0..8 {
        let fast = Provenance::builder()
            .conversation_id(ConversationId::new())
            .build();
        laser
            .send_agent(AgentTopic::Commands, Bytes::from_static(b"fast"), &fast)
            .await
            .expect("send fast");
    }

    // A fast message is handled while the slow one is still in flight.
    harness::eventually(|| {
        let fast_during_slow = fast_during_slow.clone();
        async move { (fast_during_slow.load(Ordering::SeqCst) >= 1).then_some(()) }
    })
    .await;
    handle.shutdown().await.expect("shutdown");
}
