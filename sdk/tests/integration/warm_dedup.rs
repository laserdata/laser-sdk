use crate::harness;
use bytes::Bytes;
use laser_sdk::prelude::full::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::time::sleep;

struct Counter {
    handled: Arc<AtomicUsize>,
}

impl AgentHandler for Counter {
    async fn handle(&self, _message: &AgentMessage, _ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        self.handled.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

fn job(conversation: ConversationId, key: &str) -> Provenance {
    Provenance::builder()
        .conversation_id(conversation)
        .idempotency_key(key.to_owned())
        .build()
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_warmed_consumer_when_a_duplicate_arrives_after_restart_then_should_skip_it() {
    let laser = harness::laser().await;
    let handled = Arc::new(AtomicUsize::new(0));
    // One conversation keeps every message on the same partition, so the restarted
    // consumer sees them in order.
    let conversation = ConversationId::new();

    // First incarnation processes the original job and commits its offset.
    laser
        .send_agent(
            AgentTopic::Commands,
            Bytes::from_static(b"first"),
            &job(conversation, "job"),
        )
        .await
        .expect("the first job should be sent");

    let first = Agent::builder()
        .id("warmer".parse().expect("warmer is a valid agent id"))
        .listen_on(AgentTopic::Commands)
        .warm_dedup(true)
        .handler(Counter {
            handled: handled.clone(),
        })
        .build()
        .spawn(laser.clone());

    harness::eventually(|| {
        let handled = handled.clone();
        async move { (handled.load(Ordering::SeqCst) == 1).then_some(()) }
    })
    .await;
    first
        .shutdown()
        .await
        .expect("the first incarnation should stop");

    // While it is down, a duplicate of the original arrives plus a brand-new job.
    laser
        .send_agent(
            AgentTopic::Commands,
            Bytes::from_static(b"dup"),
            &job(conversation, "job"),
        )
        .await
        .expect("the duplicate should be sent");
    laser
        .send_agent(
            AgentTopic::Commands,
            Bytes::from_static(b"third"),
            &job(conversation, "other"),
        )
        .await
        .expect("the fresh job should be sent");

    // The restarted consumer warms its window from the already-consumed tail, so
    // the duplicate is skipped while the fresh job is handled: total stays at 2.
    let second = Agent::builder()
        .id("warmer".parse().expect("warmer is a valid agent id"))
        .listen_on(AgentTopic::Commands)
        .warm_dedup(true)
        .handler(Counter {
            handled: handled.clone(),
        })
        .build()
        .spawn(laser.clone());

    harness::eventually(|| {
        let handled = handled.clone();
        async move { (handled.load(Ordering::SeqCst) == 2).then_some(()) }
    })
    .await;
    // Settle, then confirm the duplicate did not also count (would be 3 unwarmed).
    sleep(Duration::from_millis(500)).await;
    assert_eq!(handled.load(Ordering::SeqCst), 2);
    second
        .shutdown()
        .await
        .expect("the second incarnation should stop");
}
