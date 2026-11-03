use crate::harness;
use laser_sdk::agent::{ModelRequest, ModelResponse, StepContext};
use laser_sdk::prelude::full::*;
use laser_sdk::wire::agent::{AgentId, AgentKind, Budget, SessionEnd, TaskState, TokenUsage};
use laser_sdk::wire::framing::decode_named;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

fn agent() -> AgentId {
    "budgeted".parse().expect("budgeted is a valid agent id")
}

fn usage(input_tokens: u64, output_tokens: u64, cost_micros: Option<u64>) -> ModelResponse {
    ModelResponse {
        body: b"answer".to_vec(),
        usage: Some(TokenUsage {
            input_tokens,
            output_tokens,
            cost_micros,
            ..TokenUsage::default()
        }),
        ..ModelResponse::default()
    }
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_token_budget_when_usage_passes_it_then_should_fold_the_lane_over_budget() {
    let laser = harness::laser().await;
    let (session, lease) = laser
        .sessions()
        .start()
        .agent(agent())
        .budget(Budget {
            tokens: Some(100),
            cost_micros: None,
        })
        .begin()
        .await
        .expect("the session starts");
    session
        .record_model_call(
            ModelRequest::new("m", b"q".to_vec()),
            usage(40, 20, None),
            None,
        )
        .await
        .expect("the first call is recorded");
    assert!(
        !session.over_budget().await.expect("the lane folds"),
        "60 of 100 tokens is within the budget",
    );
    session
        .record_model_call(
            ModelRequest::new("m", b"q".to_vec()),
            usage(30, 10, None),
            None,
        )
        .await
        .expect("the second call is recorded");
    assert!(
        !session.over_budget().await.expect("the lane folds"),
        "exactly 100 of 100 tokens is not over the budget",
    );
    session
        .record_model_call(
            ModelRequest::new("m", b"q".to_vec()),
            usage(1, 0, None),
            None,
        )
        .await
        .expect("the third call is recorded");
    assert!(
        session.over_budget().await.expect("the lane folds"),
        "101 of 100 tokens is over the budget",
    );
    drop(lease);
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_cost_budget_or_none_when_usage_is_recorded_then_should_fold_each_ceiling() {
    let laser = harness::laser().await;
    let (costed, costed_lease) = laser
        .sessions()
        .start()
        .agent(agent())
        .budget(Budget {
            tokens: None,
            cost_micros: Some(500),
        })
        .begin()
        .await
        .expect("the costed session starts");
    costed
        .record_model_call(
            ModelRequest::new("m", b"q".to_vec()),
            usage(10_000, 10_000, Some(400)),
            None,
        )
        .await
        .expect("the call is recorded");
    assert!(
        !costed.over_budget().await.expect("the lane folds"),
        "tokens without a token ceiling never breach",
    );
    costed
        .record_model_call(
            ModelRequest::new("m", b"q".to_vec()),
            usage(1, 1, Some(101)),
            None,
        )
        .await
        .expect("the call is recorded");
    assert!(costed.over_budget().await.expect("the lane folds"));

    let (unbounded, unbounded_lease) = laser
        .sessions()
        .start()
        .agent(agent())
        .begin()
        .await
        .expect("the unbounded session starts");
    unbounded
        .record_model_call(
            ModelRequest::new("m", b"q".to_vec()),
            usage(u64::MAX / 2, u64::MAX / 2, Some(u64::MAX)),
            None,
        )
        .await
        .expect("the call is recorded");
    assert!(
        !unbounded.over_budget().await.expect("the lane folds"),
        "a session without a budget is never over it",
    );
    drop((costed_lease, unbounded_lease));
}

// A step worker whose `charge` step records a model call on the run session
// that passes the run's token budget, so the next step boundary stops.
struct SpendingWorker {
    laser: Laser,
    shipped: Arc<AtomicUsize>,
    compensated: Arc<AtomicUsize>,
}

impl AgentHandler for SpendingWorker {
    async fn handle(&self, message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        match message.body() {
            b"undo" => {
                self.compensated.fetch_add(1, Ordering::SeqCst);
            }
            b"ship" => {
                self.shipped.fetch_add(1, Ordering::SeqCst);
            }
            _ => {
                let run = message
                    .provenance
                    .parent_conversation_id
                    .expect("a workflow step names its run as parent");
                self.laser
                    .sessions()
                    .open(run)
                    .as_agent(
                        "spender"
                            .parse::<laser_sdk::wire::agent::AgentId>()
                            .expect("valid agent id"),
                    )
                    .record_model_call(
                        ModelRequest::new("m", b"charge".to_vec()),
                        usage(80, 40, None),
                        None,
                    )
                    .await?;
            }
        }
        ctx.respond(b"done".to_vec()).await
    }
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_run_over_its_token_budget_when_the_next_step_starts_then_should_compensate_and_fail_with_reason_budget()
 {
    let laser = harness::laser().await;
    let shipped = Arc::new(AtomicUsize::new(0));
    let compensated = Arc::new(AtomicUsize::new(0));
    let mut worker = Agent::builder()
        .id("spender".parse().expect("worker id is valid"))
        .listen_on(AgentTopic::Sessions)
        .respond_on(AgentTopic::Sessions)
        .handler(SpendingWorker {
            laser: laser.clone(),
            shipped: shipped.clone(),
            compensated: compensated.clone(),
        })
        .build()
        .spawn(laser.clone());
    worker.ready().await.expect("worker joins its group");

    let run = ConversationId::new();
    let result = laser
        .workflow("spendflow")
        .run_id(run)
        .budget(WorkflowBudget::tokens(100))
        .inbox_route(InboxRoute::Fixed(AgentTopic::Sessions))
        .step(
            "charge",
            Router::to("spender".parse().expect("valid agent id")),
            |_ctx: &StepContext<'_>| b"charge".to_vec(),
        )
        .compensate_with(|_ctx: &StepContext<'_>| b"undo".to_vec())
        .step(
            "ship",
            Router::to("spender".parse().expect("valid agent id")),
            |_ctx: &StepContext<'_>| b"ship".to_vec(),
        )
        .after("charge")
        .run()
        .await;

    assert!(
        matches!(
            result,
            Err(LaserError::BudgetExceeded {
                ceiling: 100,
                spent: 120
            })
        ),
        "the run stops on the lane's 120 of 100 tokens, got {result:?}",
    );
    assert_eq!(
        shipped.load(Ordering::SeqCst),
        0,
        "no step runs past the budget"
    );
    harness::eventually(|| async { (compensated.load(Ordering::SeqCst) == 1).then_some(()) }).await;

    let end = harness::eventually(|| async {
        let turns = laser.sessions().open(run).context().await.ok()?;
        turns.into_iter().find_map(|turn| {
            let envelope = turn.message.envelope?;
            (envelope.kind == AgentKind::Status
                && envelope.operation.as_deref() == Some("session")
                && envelope.task_state == Some(TaskState::Failed))
            .then(|| decode_named::<SessionEnd>(&envelope.body).expect("the end decodes"))
        })
    })
    .await;
    assert_eq!(end.reason.as_deref(), Some("budget"));
    let error = end.error.expect("the failure names the budget");
    assert!(
        error
            .message
            .as_deref()
            .is_some_and(|message| message.contains("budget")),
    );
    worker.shutdown().await.expect("worker shuts down");
}
