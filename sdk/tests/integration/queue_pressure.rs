use crate::harness;
use bytes::Bytes;
use laser_sdk::prelude::full::*;
use std::sync::{Arc, Mutex};
use std::time::Duration;

// Parks on `gate` for the "block" payload (announcing itself on `entered`
// first) and records everything else, so a test can hold one partition lane
// open while watching what the scheduler still lets through.
struct GatedWorker {
    gate: Arc<tokio::sync::Semaphore>,
    entered: Arc<tokio::sync::Semaphore>,
    handled: Arc<Mutex<Vec<String>>>,
}

impl AgentHandler for GatedWorker {
    async fn handle(&self, message: &AgentMessage, _ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        let payload = String::from_utf8_lossy(&message.payload).into_owned();
        if payload == "block" {
            self.entered.add_permits(1);
            let _permit = self
                .gate
                .acquire()
                .await
                .map_err(|_| LaserError::Handler("the gate closed".to_owned()))?;
        }
        self.handled
            .lock()
            .expect("the lock should not be poisoned")
            .push(payload);
        Ok(())
    }
}

async fn send(laser: &Laser, conversation: ConversationId, payload: String) {
    let provenance = Provenance::builder().conversation_id(conversation).build();
    laser
        .send_agent(AgentTopic::Commands, Bytes::from(payload), &provenance)
        .await
        .expect("the message should publish");
}

fn snapshot(handled: &Arc<Mutex<Vec<String>>>) -> Vec<String> {
    handled
        .lock()
        .expect("the lock should not be poisoned")
        .clone()
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_blocked_partition_when_the_record_bound_fills_then_should_stall_intake_and_recover()
 {
    let laser = harness::laser().await;
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let handled = Arc::new(Mutex::new(Vec::new()));
    let mut worker = Agent::builder()
        .id("bounded".parse().expect("the agent id is valid"))
        .listen_on(AgentTopic::Commands)
        .handler(GatedWorker {
            gate: gate.clone(),
            entered: entered.clone(),
            handled: handled.clone(),
        })
        .concurrency(ConcurrencyPolicy::SerialPerPartition { max_partitions: 4 })
        .max_queued_records(3)
        .build()
        .spawn(laser.clone());
    worker.ready().await.expect("the worker should be ready");

    // A fast conversation drains completely while nothing is blocked.
    let fast = ConversationId::new();
    for index in 0..10 {
        send(&laser, fast, format!("fast-{index}")).await;
    }
    harness::eventually(|| {
        let handled = handled.clone();
        async move { (snapshot(&handled).len() == 10).then_some(()) }
    })
    .await;

    // Block one conversation's lane, then flood it past the three-record
    // bound. The scheduler may buffer at most the bound, so intake stalls and
    // records published afterwards must not reach the handler.
    let blocked = ConversationId::new();
    send(&laser, blocked, "block".to_owned()).await;
    let _in_flight = entered
        .acquire()
        .await
        .expect("the blocking message should enter the handler");
    for index in 0..20 {
        send(&laser, blocked, format!("queued-{index}")).await;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    let late = ConversationId::new();
    for index in 0..5 {
        send(&laser, late, format!("late-{index}")).await;
    }
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert_eq!(
        snapshot(&handled).len(),
        10,
        "a full record bound must stall intake instead of buffering the flood",
    );

    // Releasing the lane drains everything exactly once, in partition order.
    gate.add_permits(100);
    harness::eventually(|| {
        let handled = handled.clone();
        async move { (snapshot(&handled).len() == 36).then_some(()) }
    })
    .await;
    let queued: Vec<String> = snapshot(&handled)
        .into_iter()
        .filter(|payload| payload.starts_with("queued-"))
        .collect();
    let expected: Vec<String> = (0..20).map(|index| format!("queued-{index}")).collect();
    assert_eq!(queued, expected);
    worker.shutdown().await.expect("the worker should drain");
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_blocked_partition_when_the_byte_bound_fills_then_should_stall_intake_and_recover()
{
    let laser = harness::laser().await;
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let handled = Arc::new(Mutex::new(Vec::new()));
    let mut worker = Agent::builder()
        .id("byte-bounded".parse().expect("the agent id is valid"))
        .listen_on(AgentTopic::Commands)
        .handler(GatedWorker {
            gate: gate.clone(),
            entered: entered.clone(),
            handled: handled.clone(),
        })
        .concurrency(ConcurrencyPolicy::SerialPerPartition { max_partitions: 4 })
        .max_queued_bytes(64 * 1024)
        .build()
        .spawn(laser.clone());
    worker.ready().await.expect("the worker should be ready");

    // Hold the lane open, then queue payloads that overflow the byte bound:
    // the first large record fits, the second must stall the poll loop.
    let blocked = ConversationId::new();
    send(&laser, blocked, "block".to_owned()).await;
    let _in_flight = entered
        .acquire()
        .await
        .expect("the blocking message should enter the handler");
    for index in 0..3 {
        send(
            &laser,
            blocked,
            format!("big-{index}-{}", "x".repeat(40 * 1024)),
        )
        .await;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    let probe = ConversationId::new();
    send(&laser, probe, "probe".to_owned()).await;
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert!(
        snapshot(&handled).is_empty(),
        "a full byte bound must stall intake instead of buffering the flood",
    );

    gate.add_permits(100);
    harness::eventually(|| {
        let handled = handled.clone();
        async move { (snapshot(&handled).len() == 5).then_some(()) }
    })
    .await;
    worker.shutdown().await.expect("the worker should drain");
}
