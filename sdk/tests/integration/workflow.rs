use crate::harness;
use bytes::Bytes;
use laser_sdk::agent::StepContext;
use laser_sdk::prelude::*;
use laser_sdk::wire::agent::{AgentCard, CapabilityDescriptor};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

// A worker that tags its reply with its role and echoes the input, so a step's
// output reflects both its own work and what the prior step produced.
struct Tagger {
    tag: String,
}

impl AgentHandler for Tagger {
    async fn handle(&self, message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        // `body()` is the task body for both AGDX commands (workflow/contract) and
        // plain sends, so the worker need not know how it was reached.
        let reply = format!("{}:{}", self.tag, String::from_utf8_lossy(message.body()));
        ctx.respond(Bytes::from(reply)).await
    }
}

fn card(skill: &str) -> AgentCard {
    AgentCard {
        name: None,
        version: None,
        capabilities: vec![CapabilityDescriptor {
            skill_id: skill.to_owned(),
            input: None,
            output: None,
            cost_class: None,
            latency_class: None,
            max_concurrency: None,
            health: None,
            load: None,
        }],
        ttl_micros: None,
    }
}

fn worker(laser: &Laser, id: &str, skill: &str, tag: &str) -> AgentHandle {
    Agent::builder()
        .id(id.parse().expect("worker id is valid"))
        .listen_on(AgentTopic::Commands)
        .respond_on(AgentTopic::Responses)
        .capabilities(card(skill).capabilities)
        .handler(Tagger {
            tag: tag.to_owned(),
        })
        .build()
        .spawn(laser.clone())
}

#[tokio::test]
async fn given_a_two_step_workflow_when_run_then_should_dispatch_in_dependency_order_and_thread_outputs()
 {
    let laser = harness::laser().await;
    let mut triager = worker(&laser, "triager", "triage", "triaged");
    let mut diagnoser = worker(&laser, "diagnoser", "diagnose", "diagnosed");
    triager.ready().await.expect("triager joins its group");
    diagnoser.ready().await.expect("diagnoser joins its group");

    // A fixed inbox route, since the stock Apache Iggy harness has no presence
    // command. Each step is target-filtered to its worker on the shared topic.
    let outcome = laser
        .workflow("incidentflow")
        .inbox_route(InboxRoute::Fixed(AgentTopic::Commands))
        .step(
            "triage",
            Router::to_capable("triage", RoutePolicy::Any),
            |_ctx: &StepContext<'_>| Bytes::from_static(b"incident").to_vec(),
        )
        .step(
            "diagnose",
            Router::to_capable("diagnose", RoutePolicy::Any),
            // Thread the triage output into the diagnose task.
            |ctx: &StepContext<'_>| ctx.outputs.get("triage").cloned().unwrap_or_default(),
        )
        .after("triage")
        .run()
        .await
        .expect("the workflow runs to completion");

    assert_eq!(
        outcome.outputs.get("triage").map(|o| o.as_slice()),
        Some(b"triaged:incident".as_slice()),
    );
    assert_eq!(
        outcome.outputs.get("diagnose").map(|o| o.as_slice()),
        Some(b"diagnosed:triaged:incident".as_slice()),
        "the diagnose step must receive the triage step's output",
    );

    triager.shutdown().await.expect("triager shuts down");
    diagnoser.shutdown().await.expect("diagnoser shuts down");
}

#[tokio::test]
async fn given_an_all_capable_step_when_run_then_should_scatter_and_fold_every_reply() {
    let laser = harness::laser().await;
    let mut reviewer_a = worker(&laser, "rev-a", "review", "a-reviewed");
    let mut reviewer_b = worker(&laser, "rev-b", "review", "b-reviewed");
    reviewer_a
        .ready()
        .await
        .expect("reviewer a joins its group");
    reviewer_b
        .ready()
        .await
        .expect("reviewer b joins its group");

    let outcome = laser
        .workflow("reviewflow")
        .inbox_route(InboxRoute::Fixed(AgentTopic::Commands))
        .step(
            "review",
            Router::all_capable("review", RoutePolicy::Any),
            |_ctx: &StepContext<'_>| Bytes::from_static(b"check").to_vec(),
        )
        .run()
        .await
        .expect("the all-capable workflow runs");

    let folded = String::from_utf8(
        outcome
            .outputs
            .get("review")
            .cloned()
            .expect("the review step produced an output"),
    )
    .expect("the folded output is utf8");
    // Both reviewers' replies are folded (order is nondeterministic across the
    // concurrent scatter).
    assert!(
        folded.contains("a-reviewed:check") && folded.contains("b-reviewed:check"),
        "both reviewers' replies must be folded, got {folded:?}",
    );

    reviewer_a.shutdown().await.expect("reviewer a shuts down");
    reviewer_b.shutdown().await.expect("reviewer b shuts down");
}

// Counts how many times it is dispatched to, so a resumed run can prove a
// journalled step is not re-executed.
struct Counter {
    calls: Arc<AtomicUsize>,
}

impl AgentHandler for Counter {
    async fn handle(&self, _message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        ctx.respond(Bytes::from_static(b"counted")).await
    }
}

#[tokio::test]
async fn given_a_completed_step_when_the_workflow_resumes_then_should_not_re_dispatch_it() {
    let laser = harness::laser().await;
    let calls = Arc::new(AtomicUsize::new(0));
    let mut counter = Agent::builder()
        .id("counter".parse().expect("counter id is valid"))
        .listen_on(AgentTopic::Commands)
        .respond_on(AgentTopic::Responses)
        .capabilities(card("count").capabilities)
        .handler(Counter {
            calls: calls.clone(),
        })
        .build()
        .spawn(laser.clone());
    counter.ready().await.expect("counter joins its group");

    // First run dispatches the step once.
    let first = laser
        .workflow("countflow")
        .inbox_route(InboxRoute::Fixed(AgentTopic::Commands))
        .step(
            "count",
            Router::to_capable("count", RoutePolicy::Any),
            |_ctx: &StepContext<'_>| Bytes::from_static(b"go").to_vec(),
        )
        .run()
        .await
        .expect("the first run completes");
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    // Resuming the same run id replays the journal: the completed step keeps its
    // recorded output and is not dispatched again.
    let resumed = laser
        .workflow("countflow")
        .run_id(first.run_id)
        .inbox_route(InboxRoute::Fixed(AgentTopic::Commands))
        .step(
            "count",
            Router::to_capable("count", RoutePolicy::Any),
            |_ctx: &StepContext<'_>| Bytes::from_static(b"go").to_vec(),
        )
        .run()
        .await
        .expect("the resumed run completes");

    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "a journalled step must not be re-dispatched on resume",
    );
    assert_eq!(
        resumed.outputs.get("count").map(|o| o.as_slice()),
        Some(b"counted".as_slice()),
        "resume re-derives the recorded output",
    );

    counter.shutdown().await.expect("counter shuts down");
}
