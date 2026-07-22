use crate::harness;
use bytes::Bytes;
use laser_sdk::prelude::full::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

struct Counter {
    handled: Arc<AtomicUsize>,
}

impl AgentHandler for Counter {
    async fn handle(&self, _message: &AgentMessage, _ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        self.handled.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_running_agent_when_shut_down_then_should_stop_cleanly() {
    let laser = harness::laser().await;
    let handled = Arc::new(AtomicUsize::new(0));
    let handle = Agent::builder()
        .id("counter".parse().expect("counter is a valid agent id"))
        .listen_on(AgentTopic::Commands)
        .handler(Counter {
            handled: handled.clone(),
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
        .expect("the message should be sent");

    harness::eventually(|| {
        let handled = handled.clone();
        async move { (handled.load(Ordering::SeqCst) == 1).then_some(()) }
    })
    .await;

    handle
        .shutdown()
        .await
        .expect("a graceful shutdown should return Ok");
}
