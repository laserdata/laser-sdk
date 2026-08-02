use crate::common::world::LaserWorld;
use cucumber::{given, then, when};
use laser_sdk::LaserError;
use laser_sdk::govern::{
    ActionDecision, ActionGovernor, GovernedAction, GovernorMode, POLICY_DECISION_OPERATION,
    PolicyEvidence,
};
use laser_sdk::prelude::full::*;
use std::sync::Arc;
use std::time::Duration;

// The reference policy of the governance scenarios: block any payload starting
// with the configured needle, allow everything else.
struct BlockNeedle {
    needle: String,
}

#[async_trait::async_trait]
impl ActionGovernor for BlockNeedle {
    async fn decide(&self, action: &GovernedAction<'_>) -> Result<ActionDecision, LaserError> {
        if action.payload.starts_with(self.needle.as_bytes()) {
            return Ok(ActionDecision::block("blocked by policy"));
        }
        Ok(ActionDecision::allow())
    }
}

#[given(regex = r#"^the laser is governed by a policy that blocks "([^"]+)" in "([^"]+)" mode$"#)]
async fn govern_the_laser(world: &mut LaserWorld, needle: String, mode: String) {
    let mode = match mode.as_str() {
        "observe" => GovernorMode::Observe,
        _ => GovernorMode::Enforce,
    };
    world.governed = Some(
        world
            .laser()
            .with_governor(Arc::new(BlockNeedle { needle }), mode),
    );
}

#[when(regex = r#"^I send a governed agent command "([^"]+)"$"#)]
async fn send_governed(world: &mut LaserWorld, payload: String) {
    let provenance = Provenance::builder()
        .conversation_id(world.conversation())
        .build();
    let result = world
        .governed
        .as_ref()
        .expect("a governed laser")
        .send_agent(AgentTopic::Commands, payload.into_bytes(), &provenance)
        .await;
    world.last_result = Some(result.map_err(|error| format!("{error:?}")));
}

#[when(regex = r#"^I publish a governed business record "([^"]+)"$"#)]
async fn publish_governed(world: &mut LaserWorld, payload: String) {
    let governed = world.governed.as_ref().expect("a governed laser");
    governed
        .topic("business.audit")
        .ensure(1)
        .await
        .expect("business topic ready");
    let provenance = Provenance::builder()
        .conversation_id(world.conversation())
        .build();
    let result = governed
        .topic("business.audit")
        .publish()
        .provenance(&provenance)
        .payload(payload.into_bytes())
        .send()
        .await;
    world.last_result = Some(result.map_err(|error| format!("{error:?}")));
}

#[then("the send is rejected by policy")]
fn rejected_by_policy(world: &mut LaserWorld) {
    let result = world.last_result.as_ref().expect("a send ran");
    let error = result.as_ref().expect_err("the send is rejected");
    assert!(
        error.contains("PolicyBlocked"),
        "expected a policy block, got: {error}"
    );
}

#[then(regex = r#"^the audit topic records a "([^"]+)" decision with outcome "([^"]+)"$"#)]
async fn audit_records(world: &mut LaserWorld, decision: String, outcome: String) {
    let conversation = world.conversation();
    let laser = world.laser().clone();
    // Evidence lands asynchronously with the send, so poll briefly.
    for _ in 0..50 {
        let messages = ContextAssembler::builder()
            .conversation_id(conversation)
            .topics(vec![AgentTopic::Audit])
            .build()
            .assemble(&laser)
            .await
            .expect("assemble the audit topic");
        let found = messages
            .iter()
            .filter_map(|message| message.envelope.as_ref())
            .filter(|envelope| envelope.operation.as_deref() == Some(POLICY_DECISION_OPERATION))
            .filter_map(|envelope| PolicyEvidence::decode(&envelope.body).ok())
            .any(|evidence| evidence.decision == decision && evidence.outcome == outcome);
        if found {
            return;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("no `{decision}` decision with outcome `{outcome}` on the audit topic");
}
