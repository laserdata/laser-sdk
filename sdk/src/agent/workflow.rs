use crate::agent::Session;
use crate::agent::budget::BudgetBreach;
use crate::agent::contract::Contract;
use crate::agent::ctx::GatherPolicy;
use crate::agent::router::{CapabilitySelector, InboxRoute, Router};
use crate::context::{ContextAssembler, LastN};
use crate::error::LaserError;
#[cfg(feature = "kv")]
use crate::kv::{KV_LEASE_OP_VERSION, Kv, KvError, KvLeaseRenew, LeaseGuard};
use crate::laser::Laser;
use crate::provenance::{AgentTopic, Provenance};
use crate::types::{AgentId, ConversationId};
use iggy::prelude::{
    Consumer, Identifier, MessageClient, PollOptions, PollingStrategy, TopicClient,
};
use laser_wire::agent::{CorrelationId, IdempotencyKey};
use laser_wire::framing::{decode_named, encode_named};
use laser_wire::result::ResultCode;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;
use tokio::time::Instant;

/// The key-value namespace holding the workflow run's fenced lease, the
/// orchestrator-fence an exclusive step claims.
#[cfg(feature = "kv")]
pub const WORKFLOW_FENCE_NAMESPACE: &str = "agdx.workflow.fence";

/// How long an exclusive step's fenced lease is held before it must be renewed.
/// Shorter than the default step deadline, so a resumed run waits out the lease
/// a crashed run left behind instead of failing the step.
#[cfg(feature = "kv")]
const WORKFLOW_LEASE_TTL: Duration = Duration::from_secs(15);

/// How long an exclusive step waits before it asks again for a lease another
/// holder still has.
#[cfg(feature = "kv")]
const LEASE_RETRY_INTERVAL: Duration = Duration::from_millis(500);

/// The completion deadline of a step that sets none with
/// [`StepHandle::deadline`].
const DEFAULT_STEP_DEADLINE: Duration = Duration::from_secs(30);

/// The least wall-clock a step is worth dispatching with. A budget with less than
/// this remaining is treated as spent, so the workflow breaches as a budget
/// exceeded rather than handing the step a near-zero deadline that surfaces as a
/// generic timeout.
const STEP_BUDGET_FLOOR: Duration = Duration::from_millis(100);

/// The most times an exclusive step is reassigned on timeout before it fails.
const MAX_REASSIGNMENTS: u32 = 2;

/// One workflow-journal record, keyed on the log by the run id so one journal
/// topic carries every run. A resume folds the run's records: a step recorded
/// complete keeps its recorded output and is never dispatched again, a step
/// dispatched without an outcome is looked for on the reply topic before it is
/// sent again, and a compensated step counts as undone.
#[derive(Debug, Serialize, Deserialize)]
enum WorkflowJournalEntry {
    RunStarted {
        workflow: String,
    },
    StepDispatched {
        label: String,
        attempt: u32,
        reply_from: Vec<u64>,
    },
    StepCompleted {
        label: String,
        output: Vec<u8>,
        agents: Vec<String>,
    },
    StepCompensated {
        label: String,
    },
}

/// A spend ceiling for a workflow, summed from the advisory `usage` on replies and
/// the engine's own counters. Any unset dimension is unbounded. A breach is a
/// [`LaserError::BudgetExceeded`], read from the engine's running totals.
#[derive(Clone, Copy, Debug, Default)]
pub struct WorkflowBudget {
    tokens: Option<u64>,
    wall_clock: Option<Duration>,
    invocations: Option<u32>,
}

impl WorkflowBudget {
    /// An unbounded budget (the default).
    pub fn unlimited() -> Self {
        Self::default()
    }

    /// Cap the summed input-plus-output tokens across all step replies. Only
    /// counts the `usage` a step's reply carries, which an AGDX `respond` sets
    /// with `with_usage`. A plain `send_agent` reply (no envelope) and an
    /// all-capable scatter report zero, so this ceiling is advisory and bites only
    /// where handlers attach usage.
    pub fn tokens(tokens: u64) -> Self {
        Self {
            tokens: Some(tokens),
            ..Self::default()
        }
    }

    /// Cap the wall-clock time the whole workflow may run.
    pub fn wall_clock(mut self, wall_clock: Duration) -> Self {
        self.wall_clock = Some(wall_clock);
        self
    }

    /// Cap how many steps the workflow may dispatch.
    pub fn invocations(mut self, invocations: u32) -> Self {
        self.invocations = Some(invocations);
        self
    }
}

/// The prior steps' outputs, by label, handed to a [`StepFn`] so it can build this
/// step's payload from what came before.
pub struct StepContext<'a> {
    pub outputs: &'a BTreeMap<String, Vec<u8>>,
}

/// Builds a step's task payload from the outputs of the steps before it. Async
/// and fallible, so a step that must call a model or a service to build its input
/// can, and a build failure fails the step rather than panicking. A plain
/// synchronous closure `Fn(&StepContext) -> Vec<u8>` still is a `StepFn` (the
/// blanket impl wraps it), so the common step stays a one-line closure. A build
/// is bounded by the step deadline.
#[async_trait::async_trait]
pub trait StepFn: Send + Sync {
    async fn build(&self, ctx: &StepContext<'_>) -> Result<Vec<u8>, LaserError>;
}

#[async_trait::async_trait]
impl<F> StepFn for F
where
    F: Fn(&StepContext<'_>) -> Vec<u8> + Send + Sync,
{
    async fn build(&self, ctx: &StepContext<'_>) -> Result<Vec<u8>, LaserError> {
        Ok(self(ctx))
    }
}

/// Checks a step's reply before it counts as done. Async and fallible, so an
/// LLM-judge verifier panel (the advertised use) can call a model and a check
/// can say why it failed, not just that it did. A `false` verdict fails the step
/// and compensates it with the steps before it. A plain synchronous closure
/// `Fn(&[u8]) -> bool` still is a `Verifier` via the blanket impl. A
/// verification is bounded by the step deadline.
#[async_trait::async_trait]
pub trait Verifier: Send + Sync {
    async fn verify(&self, output: &[u8]) -> Result<bool, LaserError>;
}

#[async_trait::async_trait]
impl<F> Verifier for F
where
    F: Fn(&[u8]) -> bool + Send + Sync,
{
    async fn verify(&self, output: &[u8]) -> Result<bool, LaserError> {
        Ok(self(output))
    }
}

/// How a step responds to its contract timing out without a terminal reply.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum OnTimeout {
    /// Fail the step and run compensations (the default).
    #[default]
    Fail,
    /// Hand the task to a fresh holder after releasing the timed-out assignment's
    /// lease, preferring a capable agent other than the ones that timed out and
    /// reusing one of them only when no other is capable. The fresh acquisition
    /// bumps the fence and gates the old holder out at the sink. Needs an
    /// exclusive step so reassignment cannot double-commit.
    Reassign,
}

/// Why a workflow step did not complete, carried by
/// [`LaserError::StepFailed`].
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    strum::Display,
    strum::EnumString,
    strum::IntoStaticStr,
    strum::VariantArray,
)]
#[strum(serialize_all = "snake_case")]
pub enum StepFailure {
    /// The step did not finish within its deadline: no terminal reply, a build
    /// or verification that ran past it, or an exclusive lease another holder
    /// kept. Retryable.
    TimedOut,
    /// The target replied with a terminal error.
    Failed,
    /// The command was not consumed within its consumption expiry.
    NotConsumed,
    /// The step's verifier rejected the reply.
    Unverified,
    /// No capable agent completed an all-capable step.
    NoCompletion,
}

impl StepFailure {
    // Whether a later attempt of the same step can succeed.
    pub(crate) fn is_retryable(self) -> bool {
        self == Self::TimedOut
    }

    // The result code the failure classifies as.
    pub(crate) fn code(self) -> ResultCode {
        match self {
            Self::TimedOut | Self::NotConsumed => ResultCode::DeadlineExceeded,
            Self::Failed | Self::Unverified | Self::NoCompletion => ResultCode::Backend,
        }
    }
}

struct Step {
    label: String,
    target: Router,
    run: Box<dyn StepFn>,
    after: Vec<String>,
    verifier: Option<Box<dyn Verifier>>,
    exclusive: bool,
    fence_namespace: Option<String>,
    on_timeout: OnTimeout,
    compensate: Option<Box<dyn StepFn>>,
    deadline: Duration,
}

/// A journalled directed-acyclic workflow over the built coordination primitives.
/// Each step is a directed request (the task [`contract`](Laser::contract)) to its
/// `target`, ordered by its declared dependencies, with an optional verifier,
/// exclusivity, a deadline, and a compensation. Built by [`Laser::workflow`],
/// described with fluent [`step`](Self::step) calls, and driven by
/// [`run`](Self::run).
///
/// The single decision the developer makes per step is whether it has an exclusive
/// side effect (`.exclusive()`). Everything else (lease, fence, journal, replay)
/// is the engine's, never a word in a user-facing signature.
pub struct Workflow {
    laser: Laser,
    name: String,
    budget: WorkflowBudget,
    inbox_route: InboxRoute,
    run_id: Option<ConversationId>,
    steps: Vec<Step>,
}

impl Workflow {
    /// Set the workflow's spend ceiling.
    pub fn budget(mut self, budget: WorkflowBudget) -> Self {
        self.budget = budget;
        self
    }

    /// How each step resolves its target to a topic (default
    /// [`InboxRoute::Advertised`]). The same flexible addressing every directed
    /// send uses, so a deployment without a presence command sets
    /// [`InboxRoute::Fixed`].
    pub fn inbox_route(mut self, inbox_route: InboxRoute) -> Self {
        self.inbox_route = inbox_route;
        self
    }

    /// Run under `run_id` (from a prior [`WorkflowOutcome::run_id`], or one the
    /// caller derives for a task): the engine replays that run's whole journal,
    /// skips the steps recorded complete, and dispatches only the unfinished
    /// ones. A step dispatched before the run stopped is answered by its reply
    /// if one landed, and sent again under the same idempotency key otherwise.
    /// A journal that retention shortened past the run's first record is a
    /// [`LaserError::JournalTruncated`]. The run session reopens through
    /// [`SessionBuilder::resume`](crate::agent::SessionBuilder::resume), so a
    /// run that ended completed or failed runs again under the same id and a
    /// canceled one is refused with [`LaserError::SessionEnded`]. A fresh run
    /// mints its own id, so omit this to start anew.
    pub fn run_id(mut self, run_id: ConversationId) -> Self {
        self.run_id = Some(run_id);
        self
    }

    /// Add a step `label` that dispatches to `target`, building its payload from
    /// the prior outputs with `run`. Returns a [`StepHandle`] so the step can be
    /// refined (`after`, `verify_with`, `exclusive`, `deadline`,
    /// `compensate_with`) before the next `step`.
    pub fn step(mut self, label: &str, target: Router, run: impl StepFn + 'static) -> StepHandle {
        self.steps.push(Step {
            label: label.to_owned(),
            target,
            run: Box::new(run),
            after: Vec::new(),
            verifier: None,
            exclusive: false,
            fence_namespace: None,
            on_timeout: OnTimeout::Fail,
            compensate: None,
            deadline: DEFAULT_STEP_DEADLINE,
        });
        let current = self.steps.len() - 1;
        StepHandle {
            workflow: self,
            current,
        }
    }

    /// Run the workflow to completion, returning each step's output. Steps run in
    /// dependency order. A step that does not complete fails the workflow with
    /// [`LaserError::StepFailed`] after running the compensations of the steps
    /// with an effect in reverse (the saga rollback). A misconfigured workflow
    /// (a duplicate label, a broadcast step, an exclusive all-capable step,
    /// reassignment without exclusivity, an exclusive step without fenced
    /// leases, a dependency cycle) is refused before anything is written.
    ///
    /// The run is a session on `agent.sessions` whose id is the run id, and every
    /// step and compensation is a child session of it. The run session starts
    /// before the first step and ends by the outcome. Between steps the engine
    /// checks `agent.control` for a cancel request, which compensates and
    /// returns [`LaserError::Cancelled`], and then whether the run session is
    /// [over its budget](crate::agent::Session::over_budget), which
    /// compensates and returns [`LaserError::BudgetExceeded`]. A failed check
    /// compensates before its error returns too. Any budget breach ends the run
    /// session failed with reason `budget`. A lifecycle record that fails to
    /// publish surfaces as the error, except when the run itself already
    /// failed, where the run's error wins. Dropping the run future stops it at
    /// once without compensating, releases a held exclusive lease, and leaves
    /// the run resumable by its id.
    #[tracing::instrument(target = "laser", level = "info", skip_all, fields(agent = %self.name, operation = "workflow"))]
    pub async fn run(mut self) -> Result<WorkflowOutcome, LaserError> {
        let source: AgentId = self.name.parse().map_err(|_| {
            LaserError::Invalid(format!(
                "workflow name `{}` is not a valid agent id",
                self.name
            ))
        })?;
        let order = self.validate()?;
        let resumed = self.run_id.is_some();
        let run_id = self.run_id.unwrap_or_default();
        self.run_id = Some(run_id);
        let journal = self.replay(run_id, resumed).await?;
        let mut builder = self
            .laser
            .sessions()
            .start()
            .with_id(run_id)
            .agent(source.wire_id());
        if let Some(tokens) = self.budget.tokens {
            builder = builder.budget(laser_wire::agent::Budget {
                tokens: Some(tokens),
                cost_micros: None,
            });
        }
        // A run id the caller named may name a run that already ended
        // completed or failed, which a resume reopens. A canceled run stays
        // ended.
        let (session, lease) = if resumed {
            builder.resume().await?
        } else {
            builder.begin().await?
        };
        let result = self
            .execute(&session, &source, &order, run_id, journal)
            .await;
        let ended = match &result {
            Ok(_) => session.end().await,
            Err(LaserError::Cancelled { .. }) => session.cancel().await,
            Err(LaserError::BudgetExceeded { ceiling, spent }) => {
                session
                    .fail_over_budget(&BudgetBreach::exceeded(*ceiling, *spent))
                    .await
            }
            Err(error) => {
                session
                    .fail(laser_wire::agent::AgentErrorBody {
                        code: laser_wire::agent::AgentErrorCode::Internal,
                        message: Some(error.to_string()),
                        retryable: error.is_retryable(),
                        detail: None,
                    })
                    .await
            }
        };
        drop(lease);
        match (result, ended) {
            (Ok(outcome), Ok(())) => Ok(outcome),
            (Ok(_), Err(error)) => Err(error),
            (Err(error), _) => Err(error),
        }
    }

    // Every configuration error, before the run writes anything.
    fn validate(&self) -> Result<Vec<usize>, LaserError> {
        let mut labels = BTreeSet::new();
        for step in &self.steps {
            if !labels.insert(step.label.as_str()) {
                return Err(LaserError::Invalid(format!(
                    "workflow step label `{}` is declared twice",
                    step.label
                )));
            }
            if matches!(step.target, Router::Broadcast) {
                return Err(LaserError::Invalid(
                    "a broadcast workflow step has no gather target".to_owned(),
                ));
            }
            // Only a directed step can be exclusive (a scatter has no single
            // target to fence), and reassign-on-timeout needs an exclusive step,
            // since the fence is what makes handing the task to a fresh holder
            // safe from the timed-out one double-executing.
            if step.exclusive && matches!(step.target, Router::AllCapable(_)) {
                return Err(LaserError::Invalid(
                    "an exclusive step must be directed (to / to_capable)".to_owned(),
                ));
            }
            if !step.exclusive && step.on_timeout == OnTimeout::Reassign {
                return Err(LaserError::Invalid(
                    "on_timeout(Reassign) needs an exclusive step, so the reassignment \
                     bumps the fence and the timed-out holder is gated out"
                        .to_owned(),
                ));
            }
        }
        if self.steps.iter().any(|step| step.exclusive) {
            self.require_fenced_leases()?;
        }
        topological_order(&self.steps)
    }

    #[cfg(feature = "kv")]
    fn require_fenced_leases(&self) -> Result<(), LaserError> {
        if self.laser.current_capabilities().kv.fenced_leases {
            return Ok(());
        }
        Err(LaserError::unsupported_feature(
            "workflow",
            "kv_fenced_leases",
            "an exclusive step needs the managed backend's monotonic fence sequence",
        ))
    }

    #[cfg(not(feature = "kv"))]
    fn require_fenced_leases(&self) -> Result<(), LaserError> {
        Err(LaserError::unsupported_feature(
            "workflow",
            "kv_fenced_leases",
            "exclusive workflow steps need the managed lease and fence",
        ))
    }

    // The engine body: steps in dependency order, the cancel and budget checks
    // on the run session at every step boundary.
    async fn execute(
        &self,
        session: &Session,
        source: &AgentId,
        order: &[usize],
        run_id: ConversationId,
        journal: Journal,
    ) -> Result<WorkflowOutcome, LaserError> {
        let started = Instant::now();
        let Journal {
            started: journaled,
            mut outputs,
            mut agents,
            in_flight,
            attempts,
            ..
        } = journal;
        if !journaled {
            self.record(
                run_id,
                &WorkflowJournalEntry::RunStarted {
                    workflow: self.name.clone(),
                },
            )
            .await?;
        }
        let mut effects: Vec<Effect> = order
            .iter()
            .copied()
            .filter(|&index| outputs.contains_key(&self.steps[index].label))
            .map(|index| Effect {
                index,
                agents: agents.remove(&self.steps[index].label).unwrap_or_default(),
            })
            .collect();
        let mut tokens_spent: u64 = 0;
        let mut invocations: u32 = 0;

        for &index in order {
            let step = &self.steps[index];
            if outputs.contains_key(&step.label) {
                continue;
            }
            let checked = match self.boundary(session, run_id, started).await {
                Ok(()) => {
                    invocations += 1;
                    self.check_budget(invocations, tokens_spent, started)
                }
                Err(error) => Err(error),
            };
            if let Err(error) = checked {
                self.compensate(run_id, &effects, &outputs).await;
                return Err(error);
            }
            let attempt = match in_flight.get(&step.label) {
                Some(dispatched) => Attempt {
                    number: dispatched.attempt,
                    reply_from: Some(dispatched.reply_from.clone()),
                },
                None => Attempt {
                    number: attempts.get(&step.label).map_or(1, |last| last + 1),
                    reply_from: None,
                },
            };
            match self
                .run_step(source, step, &outputs, run_id, attempt, started)
                .await
            {
                Ok(done) => {
                    tokens_spent = tokens_spent.saturating_add(done.tokens);
                    outputs.insert(step.label.clone(), done.output);
                    effects.push(Effect {
                        index,
                        agents: done.agents,
                    });
                }
                Err(failure) => {
                    if let Some(agents) = failure.effect {
                        effects.push(Effect { index, agents });
                    }
                    self.compensate(run_id, &effects, &outputs).await;
                    return Err(*failure.error);
                }
            }
            if let Err(error) = self.check_budget(invocations, tokens_spent, started) {
                self.compensate(run_id, &effects, &outputs).await;
                return Err(error);
            }
        }

        Ok(WorkflowOutcome { outputs, run_id })
    }

    // The step-boundary checks: a cancel request, a run session over its
    // budget, and a wall-clock budget too spent to dispatch with.
    async fn boundary(
        &self,
        session: &Session,
        run_id: ConversationId,
        started: Instant,
    ) -> Result<(), LaserError> {
        if session.cancel_requested().await? {
            return Err(LaserError::Cancelled {
                run: run_id.to_string(),
            });
        }
        if let Some(breach) = session.budget_breach().await? {
            return Err(breach.into_error());
        }
        if let Some(total) = self.budget.wall_clock
            && total.saturating_sub(started.elapsed()) < STEP_BUDGET_FLOOR
        {
            return Err(LaserError::BudgetExceeded {
                ceiling: micros(total),
                spent: micros(started.elapsed()),
            });
        }
        Ok(())
    }

    // One step: build its payload, dispatch it, verify the reply, and journal
    // the completion, all inside the step deadline.
    async fn run_step(
        &self,
        source: &AgentId,
        step: &Step,
        outputs: &BTreeMap<String, Vec<u8>>,
        run_id: ConversationId,
        attempt: Attempt,
        started: Instant,
    ) -> Result<StepDone, StepError> {
        let deadline_at = Instant::now() + self.step_deadline(step, started);
        let payload =
            match tokio::time::timeout_at(deadline_at, step.run.build(&StepContext { outputs }))
                .await
            {
                Ok(built) => built?,
                Err(_) => return Err(self.failed(run_id, step, StepFailure::TimedOut).into()),
            };
        match &step.target {
            Router::AllCapable(selector) => {
                self.run_all_capable(
                    source,
                    step,
                    selector,
                    &payload,
                    run_id,
                    attempt,
                    deadline_at,
                )
                .await
            }
            _ if step.exclusive => {
                self.run_exclusive(
                    source,
                    step,
                    &payload,
                    run_id,
                    attempt,
                    started,
                    deadline_at,
                )
                .await
            }
            _ => {
                self.run_directed(source, step, payload, run_id, attempt, deadline_at)
                    .await
            }
        }
    }

    // A directed step is one contract to the routed target.
    async fn run_directed(
        &self,
        source: &AgentId,
        step: &Step,
        payload: Vec<u8>,
        run_id: ConversationId,
        attempt: Attempt,
        deadline_at: Instant,
    ) -> Result<StepDone, StepError> {
        let dispatch = Dispatch::new(run_id, &step.label, attempt.number)?;
        let reply_from = self.dispatch_point(source, run_id, step, &attempt).await?;
        let (agent, outcome) = self
            .laser
            .contract(step.target.clone())
            .from(source.clone())
            .payload(payload)
            .inbox_route(self.inbox_route.clone())
            .deadline(deadline_at.saturating_duration_since(Instant::now()))
            .conversation(dispatch.conversation)
            .parent(run_id, run_id)
            .correlation(dispatch.correlation)
            .idempotency_key(dispatch.key)
            .resume_from(reply_from)
            .send_resolved()
            .await?;
        let (output, tokens) = self.completion(run_id, step, outcome)?;
        self.finish(run_id, step, output, tokens, vec![agent], deadline_at)
            .await
    }

    // An exclusive step claims a fenced lease, pins a task conversation stable
    // across re-dispatch, and on a timeout may reassign under a new holder,
    // whose acquisition bumps the fence and gates the stale holder out.
    #[allow(clippy::too_many_arguments)]
    async fn run_exclusive(
        &self,
        source: &AgentId,
        step: &Step,
        payload: &[u8],
        run_id: ConversationId,
        attempt: Attempt,
        started: Instant,
        deadline_at: Instant,
    ) -> Result<StepDone, StepError> {
        let task_conversation = ConversationId::derive(&format!("{run_id}/{}", step.label));
        let last = match step.on_timeout {
            OnTimeout::Reassign => attempt.number.saturating_add(MAX_REASSIGNMENTS),
            OnTimeout::Fail => attempt.number,
        };
        let mut attempt = attempt;
        let mut deadline_at = deadline_at;
        let mut timed_out = Vec::new();
        loop {
            let number = attempt.number;
            match self
                .exclusive_attempt(
                    source,
                    step,
                    payload,
                    run_id,
                    task_conversation,
                    attempt,
                    &timed_out,
                    deadline_at,
                )
                .await?
            {
                AttemptOutcome::Done(done) => return Ok(done),
                AttemptOutcome::TimedOut(agent) if number < last => {
                    timed_out.push(agent);
                    attempt = Attempt {
                        number: number + 1,
                        reply_from: None,
                    };
                    deadline_at = Instant::now() + self.step_deadline(step, started);
                }
                AttemptOutcome::TimedOut(_) => {
                    return Err(self.failed(run_id, step, StepFailure::TimedOut).into());
                }
            }
        }
    }

    /// Claim, maintain, and release one assignment's lease. A live lease of
    /// an earlier holder, a crashed run being resumed, is waited out inside the
    /// step deadline. Renewal runs at half of the granted TTL while the
    /// contract, verification, and completion journal are active, and stops at
    /// the step deadline. Every exit releases the lease, so a timeout
    /// reassignment can acquire immediately under a fresh holder and bump the
    /// fence sequence before the stale task can commit.
    #[cfg(feature = "kv")]
    #[allow(clippy::too_many_arguments)]
    async fn exclusive_attempt(
        &self,
        source: &AgentId,
        step: &Step,
        payload: &[u8],
        run_id: ConversationId,
        task_conversation: ConversationId,
        attempt: Attempt,
        avoid: &[AgentId],
        deadline_at: Instant,
    ) -> Result<AttemptOutcome, StepError> {
        let namespace = step
            .fence_namespace
            .as_deref()
            .unwrap_or(WORKFLOW_FENCE_NAMESPACE);
        let key = run_id.to_string();
        let holder = workflow_lease_holder(run_id, attempt.number);
        let kv = self.laser.kv(namespace);
        let coordination = self.laser.coordination_client().await?;
        let Some(guard) = acquire_lease(&kv, &key, &holder, deadline_at).await? else {
            return Err(self.failed(run_id, step, StepFailure::TimedOut).into());
        };
        let mut lease = *guard.lease();
        let fence_token = lease.token;
        let dispatch = Dispatch::new(run_id, &step.label, attempt.number)?;
        let mut completion = Box::pin(async {
            let reply_from = self.dispatch_point(source, run_id, step, &attempt).await?;
            let (agent, outcome) = self
                .laser
                .contract(step.target.clone())
                .from(source.clone())
                .payload(payload.to_vec())
                .inbox_route(self.inbox_route.clone())
                .deadline(deadline_at.saturating_duration_since(Instant::now()))
                .fence(fence_token)
                .conversation(task_conversation)
                .parent(run_id, run_id)
                .correlation(dispatch.correlation)
                .idempotency_key(dispatch.key)
                .resume_from(reply_from)
                .excluding(avoid.to_vec())
                .send_resolved()
                .await?;
            if matches!(outcome, Contract::TimedOut) {
                return Ok(AttemptOutcome::TimedOut(agent));
            }
            let (output, tokens) = self.completion(run_id, step, outcome)?;
            self.finish(run_id, step, output, tokens, vec![agent], deadline_at)
                .await
                .map(AttemptOutcome::Done)
        });
        let outcome = 'maintain: loop {
            let renewal_delay = workflow_renewal_delay(lease.granted_ttl)
                .ok_or_else(|| LaserError::from(laser_wire::kv::KvError::LeaseLost))?;
            tokio::select! {
                result = &mut completion => break result,
                () = tokio::time::sleep(renewal_delay) => {
                    let request = KvLeaseRenew {
                        v: KV_LEASE_OP_VERSION,
                        namespace: kv.resource_namespace().to_owned(),
                        key: key.as_bytes().to_vec(),
                        holder_id: holder.clone(),
                        subject_user_id: None,
                        lease_token: lease.token,
                        lease_ttl_micros: micros(WORKFLOW_LEASE_TTL),
                    };
                    let prepared = coordination.prepare_renew(&request)?;
                    let renewal_deadline = lease.valid_until.min(deadline_at.into_std());
                    if renewal_deadline <= std::time::Instant::now() {
                        break Err(LaserError::from(laser_wire::kv::KvError::LeaseLost).into());
                    }
                    let mut renewal = Box::pin(
                        coordination.renew_resolving(&prepared, renewal_deadline),
                    );
                    let renewed = tokio::select! {
                        result = &mut completion => {
                            let renewal_result = (&mut renewal).await;
                            if let Err(error) = renewal_result {
                                break 'maintain Err(error.into());
                            }
                            break 'maintain result;
                        }
                        result = &mut renewal => result,
                    };
                    match renewed {
                        Ok(next) => {
                            self.laser.observe_kv_position(next.position);
                            lease = next;
                        }
                        Err(error) => break Err(error.into()),
                    }
                }
            }
        };
        drop(completion);
        // A renewal keeps the token, so the guard releases the lease it holds.
        let released = guard.release().await;
        let outcome = outcome?;
        let effect = match &outcome {
            AttemptOutcome::Done(done) => Some(done.agents.clone()),
            AttemptOutcome::TimedOut(_) => None,
        };
        match released {
            Ok(true) => Ok(outcome),
            Ok(false) => Err(StepError {
                error: Box::new(laser_wire::kv::KvError::LeaseLost.into()),
                effect,
            }),
            Err(error) => Err(StepError {
                error: Box::new(error),
                effect,
            }),
        }
    }

    /// Without the managed key-value store there is no lease to fence with, so an
    /// exclusive step cannot run. `validate` refuses it before the run starts.
    #[cfg(not(feature = "kv"))]
    #[allow(clippy::too_many_arguments)]
    async fn exclusive_attempt(
        &self,
        _source: &AgentId,
        _step: &Step,
        _payload: &[u8],
        _run_id: ConversationId,
        _task_conversation: ConversationId,
        _attempt: Attempt,
        _avoid: &[AgentId],
        _deadline_at: Instant,
    ) -> Result<AttemptOutcome, StepError> {
        Err(StepError::from(LaserError::unsupported_feature(
            "workflow",
            "kv_fenced_leases",
            "exclusive workflow steps need the managed lease and fence",
        )))
    }

    /// Scatter a contract to every agent advertising the selector's skill,
    /// concurrently, and fold the completed replies (each body on its own line) into
    /// the step output. The step succeeds when at least one capable agent completes,
    /// the partial-panel tolerance a verifier or diagnostic pool wants, and its
    /// compensation goes to every agent that completed. No capable agent at all
    /// is a [`LaserError::NoCapableAgent`]. A scatter in flight when the run
    /// stopped is scattered again on resume.
    #[allow(clippy::too_many_arguments)]
    async fn run_all_capable(
        &self,
        source: &AgentId,
        step: &Step,
        selector: &CapabilitySelector,
        payload: &[u8],
        run_id: ConversationId,
        attempt: Attempt,
        deadline_at: Instant,
    ) -> Result<StepDone, StepError> {
        if attempt.reply_from.is_none() {
            self.record(
                run_id,
                &WorkflowJournalEntry::StepDispatched {
                    label: step.label.clone(),
                    attempt: attempt.number,
                    reply_from: Vec::new(),
                },
            )
            .await?;
        }
        let report = self
            .laser
            .scatter_report(
                source.clone(),
                selector,
                payload,
                &self.inbox_route,
                GatherPolicy::RequireAll,
                deadline_at.saturating_duration_since(Instant::now()),
            )
            .await?;
        let (agents, bodies): (Vec<AgentId>, Vec<String>) = report
            .completed()
            .map(|(agent, reply)| {
                (
                    agent.clone(),
                    String::from_utf8_lossy(reply.body()).into_owned(),
                )
            })
            .unzip();
        if agents.is_empty() {
            return Err(self.failed(run_id, step, StepFailure::NoCompletion).into());
        }
        self.finish(
            run_id,
            step,
            bodies.join("\n").into_bytes(),
            0,
            agents,
            deadline_at,
        )
        .await
    }

    // Where the reply to this attempt lands at or after. A fresh attempt
    // journals the reply topic's tail before it sends, a resumed one hands the
    // journaled positions to the contract.
    async fn dispatch_point(
        &self,
        source: &AgentId,
        run_id: ConversationId,
        step: &Step,
        attempt: &Attempt,
    ) -> Result<Option<Vec<u64>>, LaserError> {
        if attempt.reply_from.is_some() {
            return Ok(attempt.reply_from.clone());
        }
        let reply_from = self
            .laser
            .reply_positions(AgentTopic::Sessions, &source.wire_id())
            .await?;
        self.record(
            run_id,
            &WorkflowJournalEntry::StepDispatched {
                label: step.label.clone(),
                attempt: attempt.number,
                reply_from,
            },
        )
        .await?;
        Ok(None)
    }

    // The completing reply's body and tokens, or the step failure the other
    // terminals mean.
    fn completion(
        &self,
        run_id: ConversationId,
        step: &Step,
        outcome: Contract,
    ) -> Result<(Vec<u8>, u64), LaserError> {
        let reason = match outcome {
            Contract::Completed(reply) => {
                let tokens = reply
                    .envelope
                    .as_ref()
                    .and_then(|envelope| envelope.usage)
                    .map_or(0, |usage| usage.input_tokens + usage.output_tokens);
                return Ok((reply.body().to_vec(), tokens));
            }
            Contract::Failed(_) => StepFailure::Failed,
            Contract::NotConsumed => StepFailure::NotConsumed,
            Contract::TimedOut => StepFailure::TimedOut,
        };
        Err(self.failed(run_id, step, reason))
    }

    // Verify the reply and journal the completion inside the step deadline. A
    // reply the step cannot accept still had its effect, so the failure carries
    // the agents that ran it for the rollback.
    async fn finish(
        &self,
        run_id: ConversationId,
        step: &Step,
        output: Vec<u8>,
        tokens: u64,
        agents: Vec<AgentId>,
        deadline_at: Instant,
    ) -> Result<StepDone, StepError> {
        let verdict = match &step.verifier {
            Some(verifier) => {
                match tokio::time::timeout_at(deadline_at, verifier.verify(&output)).await {
                    Ok(Ok(true)) => Ok(()),
                    Ok(Ok(false)) => Err(self.failed(run_id, step, StepFailure::Unverified)),
                    Ok(Err(error)) => Err(error),
                    Err(_) => Err(self.failed(run_id, step, StepFailure::TimedOut)),
                }
            }
            None => Ok(()),
        };
        let recorded = match verdict {
            Ok(()) => {
                self.record(
                    run_id,
                    &WorkflowJournalEntry::StepCompleted {
                        label: step.label.clone(),
                        output: output.clone(),
                        agents: agents.iter().map(ToString::to_string).collect(),
                    },
                )
                .await
            }
            Err(error) => Err(error),
        };
        match recorded {
            Ok(()) => Ok(StepDone {
                output,
                tokens,
                agents,
            }),
            Err(error) => Err(StepError {
                error: Box::new(error),
                effect: Some(agents),
            }),
        }
    }

    /// Replay this run's whole journal, every retained record of the run's
    /// partition, never a window. A freshly minted run id has no records. A
    /// journal whose retained records do not begin with the run's start, or an
    /// empty one while retention has removed the head of the journal topic,
    /// cannot say which steps completed, so it is refused rather than read as
    /// a run with nothing done.
    async fn replay(&self, run_id: ConversationId, resumed: bool) -> Result<Journal, LaserError> {
        if !resumed {
            return Ok(Journal::default());
        }
        let records = ContextAssembler::builder()
            .conversation_id(run_id)
            .topics(vec![AgentTopic::WorkflowJournal])
            .policy(Box::new(LastN(usize::MAX)))
            .build()
            .replay(&self.laser)
            .await?;
        let journal = Journal::fold(
            records
                .iter()
                .filter_map(|record| decode_named::<WorkflowJournalEntry>(&record.payload).ok()),
        );
        if !journal.started && (journal.records > 0 || self.journal_trimmed().await?) {
            return Err(LaserError::JournalTruncated {
                run: run_id.to_string(),
            });
        }
        Ok(journal)
    }

    // Whether retention removed records from the head of any journal
    // partition.
    async fn journal_trimmed(&self) -> Result<bool, LaserError> {
        let client = self.laser.client();
        let stream = Identifier::named(self.laser.stream_required()?)?;
        let topic = AgentTopic::WorkflowJournal.as_identifier();
        let Some(details) = client.get_topic(&stream, &topic).await? else {
            return Ok(false);
        };
        let consumer = Consumer::new(Identifier::named("laser-workflow-journal")?);
        for partition in 0..crate::poll::bounded_partitions(details.partitions_count) {
            let polled = client
                .poll_messages(
                    &stream,
                    &topic,
                    partition,
                    &consumer,
                    &PollingStrategy::first(),
                    PollOptions {
                        count: 1,
                        ..PollOptions::default()
                    },
                )
                .await?;
            if polled
                .messages
                .first()
                .is_some_and(|first| first.header.offset > 0)
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    // Append one record of this run to the journal topic, keyed by the run.
    async fn record(
        &self,
        run_id: ConversationId,
        entry: &WorkflowJournalEntry,
    ) -> Result<(), LaserError> {
        let payload = encode_named(entry)
            .map_err(|error| LaserError::Codec(format!("encode journal entry: {error}")))?;
        let provenance = Provenance::builder().conversation_id(run_id).build();
        self.laser
            .send_agent(AgentTopic::WorkflowJournal, payload, &provenance)
            .await
    }

    /// Run the compensations of the steps with an effect in reverse order (the
    /// saga rollback), each to the agents that ran its step under the step's
    /// principal requirement. A compensation that completes is journaled, so a
    /// resume runs the undone step again. One that does not is logged and the
    /// rollback goes on, it never replaces the original error.
    async fn compensate(
        &self,
        run_id: ConversationId,
        effects: &[Effect],
        outputs: &BTreeMap<String, Vec<u8>>,
    ) {
        let Ok(source) = self.name.parse::<AgentId>() else {
            return;
        };
        for effect in effects.iter().rev() {
            let step = &self.steps[effect.index];
            let label = step.label.as_str();
            let Some(compensate) = &step.compensate else {
                continue;
            };
            let deadline_at = Instant::now() + step.deadline;
            let payload = match tokio::time::timeout_at(
                deadline_at,
                compensate.build(&StepContext { outputs }),
            )
            .await
            {
                Ok(Ok(payload)) => payload,
                Ok(Err(error)) => {
                    tracing::warn!(run = %run_id, step = label, %error, "Building the compensation of workflow step {label} failed");
                    continue;
                }
                Err(_) => {
                    tracing::warn!(run = %run_id, step = label, "Building the compensation of workflow step {label} timed out");
                    continue;
                }
            };
            let targets: Vec<Router> = if effect.agents.is_empty() {
                vec![step.target.clone()]
            } else {
                effect
                    .agents
                    .iter()
                    .map(|agent| compensation_route(&step.target, agent))
                    .collect()
            };
            let mut undone = true;
            for (index, target) in targets.into_iter().enumerate() {
                let outcome = self
                    .laser
                    .contract(target)
                    .from(source.clone())
                    .payload(payload.clone())
                    .inbox_route(self.inbox_route.clone())
                    .deadline(deadline_at.saturating_duration_since(Instant::now()))
                    .conversation(ConversationId::derive(&format!(
                        "{run_id}/{label}/compensate/{index}"
                    )))
                    .parent(run_id, run_id)
                    .send()
                    .await;
                match outcome {
                    Ok(Contract::Completed(_)) => {}
                    Ok(other) => {
                        undone = false;
                        tracing::warn!(run = %run_id, step = label, outcome = ?other, "Compensation of workflow step {label} did not complete");
                    }
                    Err(error) => {
                        undone = false;
                        tracing::warn!(run = %run_id, step = label, %error, "Compensation of workflow step {label} failed");
                    }
                }
            }
            if undone
                && let Err(error) = self
                    .record(
                        run_id,
                        &WorkflowJournalEntry::StepCompensated {
                            label: label.to_owned(),
                        },
                    )
                    .await
            {
                tracing::warn!(run = %run_id, step = label, %error, "Journaling the compensation of workflow step {label} failed");
            }
        }
    }

    // The step's completion deadline, never past what the wall-clock budget
    // has left.
    fn step_deadline(&self, step: &Step, started: Instant) -> Duration {
        match self.budget.wall_clock {
            Some(total) => total.saturating_sub(started.elapsed()).min(step.deadline),
            None => step.deadline,
        }
    }

    fn failed(&self, run_id: ConversationId, step: &Step, reason: StepFailure) -> LaserError {
        LaserError::StepFailed {
            run: run_id.to_string(),
            step: step.label.clone(),
            reason,
        }
    }

    /// Check the running totals against the budget, erroring on the first breach.
    fn check_budget(
        &self,
        invocations: u32,
        tokens_spent: u64,
        started: Instant,
    ) -> Result<(), LaserError> {
        if let Some(ceiling) = self.budget.invocations
            && invocations > ceiling
        {
            return Err(LaserError::BudgetExceeded {
                ceiling: u64::from(ceiling),
                spent: u64::from(invocations),
            });
        }
        if let Some(ceiling) = self.budget.tokens
            && tokens_spent > ceiling
        {
            return Err(LaserError::BudgetExceeded {
                ceiling,
                spent: tokens_spent,
            });
        }
        if let Some(ceiling) = self.budget.wall_clock
            && started.elapsed() > ceiling
        {
            return Err(LaserError::BudgetExceeded {
                ceiling: micros(ceiling),
                spent: micros(started.elapsed()),
            });
        }
        Ok(())
    }
}

/// A step under construction, returned by [`Workflow::step`]. Refines the step it
/// was returned for, then chains the next `step` or `run`.
pub struct StepHandle {
    workflow: Workflow,
    current: usize,
}

impl StepHandle {
    /// Declare that this step runs only after `label` has completed.
    pub fn after(mut self, label: &str) -> Self {
        self.workflow.steps[self.current]
            .after
            .push(label.to_owned());
        self
    }

    /// Check this step's reply with `verifier` before it counts as done.
    pub fn verify_with(mut self, verifier: impl Verifier + 'static) -> Self {
        self.workflow.steps[self.current].verifier = Some(Box::new(verifier));
        self
    }

    /// Opt this step into the lease and fence (an exclusive, at-most-once effect).
    pub fn exclusive(mut self) -> Self {
        self.workflow.steps[self.current].exclusive = true;
        self
    }

    /// Opt this step into a lease and fence whose sequence lives in
    /// `namespace`. Use the same namespace for the handler's
    /// [`Kv::cas_fenced`](crate::kv::Kv::cas_fenced) commit and pass the run id
    /// as its fence key, so the external effect and the workflow reassignment
    /// validate one fence sequence atomically. Plain [`exclusive`](Self::exclusive)
    /// keeps the SDK-owned coordination namespace for consumers that only need
    /// the local stale-holder gate.
    pub fn exclusive_in(mut self, namespace: impl Into<String>) -> Self {
        let step = &mut self.workflow.steps[self.current];
        step.exclusive = true;
        step.fence_namespace = Some(namespace.into());
        self
    }

    /// Set what the step does when its contract times out (default
    /// [`OnTimeout::Fail`]). [`OnTimeout::Reassign`] hands the task to a fresh
    /// holder with a bumped fence, and requires [`exclusive`](Self::exclusive).
    pub fn on_timeout(mut self, on_timeout: OnTimeout) -> Self {
        self.workflow.steps[self.current].on_timeout = on_timeout;
        self
    }

    /// Bound the step's build, dispatch, verification, and completion record
    /// by `deadline` (default 30 s), never past the wall-clock budget left. The
    /// command carries it, so a target that picks the task up later drops it.
    /// An exclusive step that resumes a crashed run waits out that run's lease,
    /// which lasts 15 s, inside this deadline.
    pub fn deadline(mut self, deadline: Duration) -> Self {
        self.workflow.steps[self.current].deadline = deadline;
        self
    }

    /// Provide the compensation to run if a later step fails (the saga rollback).
    pub fn compensate_with(mut self, run: impl StepFn + 'static) -> Self {
        self.workflow.steps[self.current].compensate = Some(Box::new(run));
        self
    }

    /// Set the workflow budget (forwards to [`Workflow::budget`]).
    pub fn budget(mut self, budget: WorkflowBudget) -> Self {
        self.workflow.budget = budget;
        self
    }

    /// Set the inbox route (forwards to [`Workflow::inbox_route`]).
    pub fn inbox_route(mut self, inbox_route: InboxRoute) -> Self {
        self.workflow.inbox_route = inbox_route;
        self
    }

    /// Resume an earlier run (forwards to [`Workflow::run_id`]).
    pub fn run_id(mut self, run_id: ConversationId) -> Self {
        self.workflow.run_id = Some(run_id);
        self
    }

    /// Add the next step (forwards to [`Workflow::step`]).
    pub fn step(self, label: &str, target: Router, run: impl StepFn + 'static) -> StepHandle {
        self.workflow.step(label, target, run)
    }

    /// Run the workflow (forwards to [`Workflow::run`]).
    pub async fn run(self) -> Result<WorkflowOutcome, LaserError> {
        self.workflow.run().await
    }
}

/// The result of a completed [`Workflow::run`]: each step's output by label, plus
/// the run id. Pass the run id to [`Workflow::run_id`] to resume the same run.
#[derive(Debug)]
pub struct WorkflowOutcome {
    pub outputs: BTreeMap<String, Vec<u8>>,
    pub run_id: ConversationId,
}

impl Laser {
    /// Open a [`Workflow`] named `name` (also the orchestrator identity it
    /// dispatches as). Describe it with fluent [`step`](Workflow::step) calls, then
    /// [`run`](Workflow::run).
    pub fn workflow(&self, name: &str) -> Workflow {
        Workflow {
            laser: self.clone(),
            name: name.to_owned(),
            budget: WorkflowBudget::unlimited(),
            inbox_route: InboxRoute::default(),
            run_id: None,
            steps: Vec::new(),
        }
    }
}

// What a run's journal records: whether it holds the run's first record, each
// completed step's output and the agents that ran it, the dispatch a stop left
// without an outcome, and the last attempt of every step.
#[derive(Default)]
struct Journal {
    started: bool,
    records: usize,
    outputs: BTreeMap<String, Vec<u8>>,
    agents: BTreeMap<String, Vec<AgentId>>,
    in_flight: BTreeMap<String, InFlight>,
    attempts: BTreeMap<String, u32>,
}

impl Journal {
    fn fold(entries: impl IntoIterator<Item = WorkflowJournalEntry>) -> Self {
        let mut journal = Self::default();
        for entry in entries {
            journal.records += 1;
            match entry {
                WorkflowJournalEntry::RunStarted { .. } => journal.started = true,
                WorkflowJournalEntry::StepDispatched {
                    label,
                    attempt,
                    reply_from,
                } => {
                    let last = journal.attempts.entry(label.clone()).or_default();
                    *last = (*last).max(attempt);
                    journal.in_flight.insert(
                        label,
                        InFlight {
                            attempt,
                            reply_from,
                        },
                    );
                }
                WorkflowJournalEntry::StepCompleted {
                    label,
                    output,
                    agents,
                } => {
                    journal.in_flight.remove(&label);
                    journal.agents.insert(
                        label.clone(),
                        agents
                            .iter()
                            .filter_map(|agent| agent.parse().ok())
                            .collect(),
                    );
                    journal.outputs.insert(label, output);
                }
                WorkflowJournalEntry::StepCompensated { label } => {
                    journal.in_flight.remove(&label);
                    journal.agents.remove(&label);
                    journal.outputs.remove(&label);
                }
            }
        }
        journal
    }
}

// A dispatch the journal holds without an outcome.
struct InFlight {
    attempt: u32,
    reply_from: Vec<u64>,
}

// One dispatch of a step: its number, and the journaled reply positions when
// it resumes a dispatch sent before the run stopped.
struct Attempt {
    number: u32,
    reply_from: Option<Vec<u64>>,
}

// The ids one attempt keeps across every send: its child conversation, its
// correlation, and the idempotency key the target deduplicates by.
struct Dispatch {
    conversation: ConversationId,
    correlation: CorrelationId,
    key: IdempotencyKey,
}

impl Dispatch {
    fn new(run_id: ConversationId, label: &str, attempt: u32) -> Result<Self, LaserError> {
        let conversation = ConversationId::derive(&format!("{run_id}/{label}/{attempt}"));
        let correlation =
            ConversationId::derive(&format!("{run_id}/{label}/{attempt}/correlation"));
        Ok(Self {
            conversation,
            correlation: CorrelationId::from_u128(correlation.as_u128()),
            key: format!("workflow:{conversation}").parse()?,
        })
    }
}

// A step whose effect a rollback must undo, and the agents that ran it.
struct Effect {
    index: usize,
    agents: Vec<AgentId>,
}

// A step that completed: its output, the tokens its reply reported, and the
// agents that ran it.
struct StepDone {
    output: Vec<u8>,
    tokens: u64,
    agents: Vec<AgentId>,
}

// How one exclusive assignment ended.
#[cfg_attr(not(feature = "kv"), allow(dead_code))]
enum AttemptOutcome {
    Done(StepDone),
    TimedOut(AgentId),
}

// A step that did not complete, with the agents that ran it when its effect
// happened anyway (a rejected or unrecorded reply).
struct StepError {
    error: Box<LaserError>,
    effect: Option<Vec<AgentId>>,
}

impl From<LaserError> for StepError {
    fn from(error: LaserError) -> Self {
        Self {
            error: Box::new(error),
            effect: None,
        }
    }
}

// Acquire the step's lease, waiting out a live lease of an earlier holder
// until `deadline_at`. `None` when the deadline passed first. The guard
// releases the lease when the run future is dropped mid-step, so a cancelled
// run does not hold the key until the lease lapses.
#[cfg(feature = "kv")]
async fn acquire_lease(
    kv: &Kv,
    key: &str,
    holder: &str,
    deadline_at: Instant,
) -> Result<Option<LeaseGuard>, LaserError> {
    loop {
        match kv
            .lease_guard(key, holder.to_owned(), WORKFLOW_LEASE_TTL)
            .await
        {
            Ok(guard) => return Ok(Some(guard)),
            Err(LaserError::Kv(KvError::LeaseHeld { .. })) => {
                if Instant::now() + LEASE_RETRY_INTERVAL >= deadline_at {
                    return Ok(None);
                }
                tokio::time::sleep(LEASE_RETRY_INTERVAL).await;
            }
            Err(error) => return Err(error),
        }
    }
}

// The route a compensation takes to `agent`, keeping the principal the step's
// own route required.
fn compensation_route(target: &Router, agent: &AgentId) -> Router {
    let principal = match target {
        Router::ToPrincipal { principal, .. } => Some(*principal),
        Router::ToCapable(selector) | Router::AllCapable(selector) => selector.principal,
        Router::To(_) | Router::Broadcast => None,
    };
    match principal {
        Some(principal) => Router::to_principal(agent.clone(), principal),
        None => Router::to(agent.clone()),
    }
}

fn micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

#[cfg(feature = "kv")]
fn workflow_lease_holder(run_id: ConversationId, attempt: u32) -> String {
    format!("workflow:{run_id}:{attempt}")
}

#[cfg(feature = "kv")]
fn workflow_renewal_delay(granted_ttl: Duration) -> Option<Duration> {
    let delay = granted_ttl / 2;
    (!delay.is_zero()).then_some(delay)
}

/// Order the steps so every step follows its `after` dependencies (Kahn's
/// algorithm). Errors on an unknown dependency or a cycle.
fn topological_order(steps: &[Step]) -> Result<Vec<usize>, LaserError> {
    let index_of: BTreeMap<&str, usize> = steps
        .iter()
        .enumerate()
        .map(|(index, step)| (step.label.as_str(), index))
        .collect();
    let mut in_degree = vec![0usize; steps.len()];
    let mut dependents: Vec<Vec<usize>> = vec![Vec::new(); steps.len()];
    for (index, step) in steps.iter().enumerate() {
        for dependency in &step.after {
            let &dep = index_of.get(dependency.as_str()).ok_or_else(|| {
                LaserError::Invalid(format!(
                    "workflow step `{}` depends on unknown step `{dependency}`",
                    step.label
                ))
            })?;
            in_degree[index] += 1;
            dependents[dep].push(index);
        }
    }
    // Seed with the dependency-free steps in declared order, so an independent
    // chain keeps its authored sequence.
    let mut ready: Vec<usize> = (0..steps.len()).filter(|&i| in_degree[i] == 0).collect();
    let mut order = Vec::with_capacity(steps.len());
    let mut cursor = 0;
    while cursor < ready.len() {
        let index = ready[cursor];
        cursor += 1;
        order.push(index);
        for &dependent in &dependents[index] {
            in_degree[dependent] -= 1;
            if in_degree[dependent] == 0 {
                ready.push(dependent);
            }
        }
    }
    if order.len() != steps.len() {
        return Err(LaserError::Invalid(
            "workflow steps form a dependency cycle".to_owned(),
        ));
    }
    Ok(order)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_a_linear_chain_when_ordered_then_should_follow_the_dependencies() {
        let steps = vec![
            step("rollback", &["diagnose"]),
            step("triage", &[]),
            step("diagnose", &["triage"]),
        ];
        let order = topological_order(&steps).expect("a chain is acyclic");
        assert_eq!(labels(&steps, &order), ["triage", "diagnose", "rollback"]);
    }

    #[test]
    fn given_independent_steps_when_ordered_then_should_keep_the_authored_sequence() {
        let steps = vec![step("a", &[]), step("b", &[]), step("c", &[])];
        let order = topological_order(&steps).expect("independent steps are acyclic");
        assert_eq!(labels(&steps, &order), ["a", "b", "c"]);
    }

    #[test]
    fn given_a_cycle_when_ordered_then_should_error() {
        let steps = vec![step("a", &["b"]), step("b", &["a"])];
        let error = topological_order(&steps).expect_err("a cycle is refused");
        assert!(matches!(error, LaserError::Invalid(message) if message.contains("cycle")));
    }

    #[test]
    fn given_an_unknown_dependency_when_ordered_then_should_error() {
        let steps = vec![step("a", &["ghost"])];
        let error = topological_order(&steps).expect_err("an unknown dependency is refused");
        assert!(matches!(error, LaserError::Invalid(message) if message.contains("unknown step")));
    }

    #[test]
    fn given_a_journal_when_folded_then_should_keep_completions_and_drop_compensated_steps() {
        let journal = Journal::fold([
            WorkflowJournalEntry::RunStarted {
                workflow: "flow".to_owned(),
            },
            dispatched("triage", 1),
            completed("triage", b"sev2"),
            dispatched("patch", 1),
            completed("patch", b"patched"),
            WorkflowJournalEntry::StepCompensated {
                label: "patch".to_owned(),
            },
            dispatched("notify", 2),
        ]);
        assert!(journal.started);
        assert_eq!(
            journal.outputs,
            BTreeMap::from([("triage".to_owned(), b"sev2".to_vec())])
        );
        assert_eq!(journal.agents.get("triage"), Some(&vec![agent()]));
        assert_eq!(journal.attempts.get("patch"), Some(&1));
        assert_eq!(
            journal
                .in_flight
                .get("notify")
                .map(|in_flight| (in_flight.attempt, in_flight.reply_from.clone())),
            Some((2, vec![4, 9]))
        );
        assert!(!journal.in_flight.contains_key("triage"));
    }

    #[test]
    fn given_journal_records_without_the_run_start_when_folded_then_should_report_no_start() {
        let journal = Journal::fold([completed("triage", b"sev2")]);
        assert!(!journal.started);
        assert_eq!(journal.records, 1);
    }

    #[test]
    fn given_more_entries_than_the_default_context_window_when_folded_then_should_keep_every_completion()
     {
        let mut entries = vec![WorkflowJournalEntry::RunStarted {
            workflow: "flow".to_owned(),
        }];
        for index in 0..120 {
            entries.push(completed(&format!("step-{index}"), b"done"));
        }
        let journal = Journal::fold(entries);
        assert_eq!(journal.outputs.len(), 120);
    }

    #[test]
    fn given_journal_entries_when_encoded_then_should_round_trip() {
        let entry = dispatched("triage", 3);
        let decoded: WorkflowJournalEntry =
            decode_named(&encode_named(&entry).expect("encodes")).expect("decodes");
        assert!(matches!(
            decoded,
            WorkflowJournalEntry::StepDispatched { label, attempt: 3, reply_from }
                if label == "triage" && reply_from == [4, 9]
        ));
    }

    #[test]
    fn given_a_dispatch_when_derived_then_should_stay_stable_and_differ_per_attempt() {
        let run_id = ConversationId::new();
        let label = "a label longer than any idempotency key allows on its own";
        let first = Dispatch::new(run_id, label, 1).expect("derives");
        let again = Dispatch::new(run_id, label, 1).expect("derives");
        let next = Dispatch::new(run_id, label, 2).expect("derives");
        assert_eq!(first.conversation, again.conversation);
        assert_eq!(first.correlation, again.correlation);
        assert_eq!(first.key, again.key);
        assert_ne!(first.key, next.key);
        assert_ne!(first.correlation, next.correlation);
        assert_ne!(first.correlation.as_u128(), first.conversation.as_u128());
    }

    #[test]
    fn given_a_step_failure_when_classified_then_should_retry_only_a_timeout() {
        for reason in <StepFailure as strum::VariantArray>::VARIANTS {
            let error = LaserError::StepFailed {
                run: "run".to_owned(),
                step: "triage".to_owned(),
                reason: *reason,
            };
            assert_eq!(
                error.is_retryable(),
                *reason == StepFailure::TimedOut,
                "{reason}"
            );
        }
        let timed_out = LaserError::StepFailed {
            run: "run".to_owned(),
            step: "triage".to_owned(),
            reason: StepFailure::TimedOut,
        };
        assert_eq!(timed_out.code(), ResultCode::DeadlineExceeded);
        assert_eq!(StepFailure::Unverified.to_string(), "unverified");
    }

    #[test]
    fn given_a_step_deadline_when_the_budget_has_less_left_then_should_use_the_budget() {
        let laser = Laser::from_client(iggy::prelude::IggyClient::default());
        let mut long = step("triage", &[]);
        long.deadline = Duration::from_secs(90);
        let started = Instant::now();
        assert_eq!(
            laser.workflow("flow").step_deadline(&long, started),
            Duration::from_secs(90)
        );
        let budgeted = laser
            .workflow("flow")
            .budget(WorkflowBudget::unlimited().wall_clock(Duration::from_secs(5)));
        assert!(budgeted.step_deadline(&long, started) <= Duration::from_secs(5));
    }

    #[tokio::test]
    async fn given_an_owned_workflow_when_spawned_then_should_run_on_its_own_task() {
        let laser = Laser::from_client(iggy::prelude::IggyClient::default());
        let run = tokio::spawn(
            laser
                .workflow("flow")
                .step("a", Router::to(agent()), |_: &StepContext<'_>| Vec::new())
                .step("a", Router::to(agent()), |_: &StepContext<'_>| Vec::new())
                .run(),
        );
        assert!(run.await.expect("the workflow task joins").is_err_and(
            |error| matches!(error, LaserError::Invalid(message) if message.contains("twice"))
        ));
    }

    #[test]
    fn given_misconfigured_steps_when_validated_then_should_refuse_before_the_run() {
        let laser = Laser::from_client(iggy::prelude::IggyClient::default());
        let duplicate = laser
            .workflow("flow")
            .step("a", Router::to(agent()), |_: &StepContext<'_>| Vec::new())
            .step("a", Router::to(agent()), |_: &StepContext<'_>| Vec::new())
            .workflow;
        assert!(
            matches!(duplicate.validate(), Err(LaserError::Invalid(message)) if message.contains("twice"))
        );
        let reassign = laser
            .workflow("flow")
            .step("a", Router::to(agent()), |_: &StepContext<'_>| Vec::new())
            .on_timeout(OnTimeout::Reassign)
            .workflow;
        assert!(matches!(reassign.validate(), Err(LaserError::Invalid(_))));
        let exclusive = laser
            .workflow("flow")
            .step("a", Router::to(agent()), |_: &StepContext<'_>| Vec::new())
            .exclusive()
            .workflow;
        assert!(matches!(
            exclusive.validate(),
            Err(LaserError::Unsupported { .. })
        ));
    }

    #[test]
    fn given_a_principal_bound_step_when_compensated_then_should_keep_the_principal() {
        let principal = crate::types::PrincipalId::new(7);
        let route = compensation_route(&Router::to_principal(agent(), principal), &agent());
        assert!(matches!(route, Router::ToPrincipal { principal: kept, .. } if kept == principal));
        let route = compensation_route(
            &Router::to_capable("triage", crate::agent::RoutePolicy::Any),
            &agent(),
        );
        assert!(matches!(route, Router::To(target) if target == agent()));
    }

    #[cfg(feature = "kv")]
    #[test]
    fn given_reassignments_when_holders_are_derived_then_should_use_distinct_bounded_identities() {
        let run_id = ConversationId::new();
        let first = workflow_lease_holder(run_id, 1);
        let second = workflow_lease_holder(run_id, 2);
        assert_ne!(first, second);
        assert!(first.len() <= laser_wire::limits::MAX_HOLDER_ID_BYTES);
        assert!(second.len() <= laser_wire::limits::MAX_HOLDER_ID_BYTES);
    }

    #[cfg(feature = "kv")]
    #[test]
    fn given_a_short_granted_ttl_when_scheduling_renewal_then_should_fail_before_expiry() {
        assert_eq!(workflow_renewal_delay(Duration::from_nanos(1)), None);
        assert_eq!(
            workflow_renewal_delay(Duration::from_secs(60)),
            Some(Duration::from_secs(30))
        );
    }

    #[cfg(feature = "kv")]
    #[test]
    fn given_the_lease_ttl_when_compared_to_the_default_step_deadline_then_should_be_shorter() {
        assert!(WORKFLOW_LEASE_TTL < DEFAULT_STEP_DEADLINE);
    }

    fn step(label: &str, after: &[&str]) -> Step {
        Step {
            label: label.to_owned(),
            target: Router::broadcast(),
            run: Box::new(|_: &StepContext<'_>| Vec::new()),
            after: after
                .iter()
                .map(|&dependency| dependency.to_owned())
                .collect(),
            verifier: None,
            exclusive: false,
            fence_namespace: None,
            on_timeout: OnTimeout::Fail,
            compensate: None,
            deadline: DEFAULT_STEP_DEADLINE,
        }
    }

    fn labels(steps: &[Step], order: &[usize]) -> Vec<String> {
        order.iter().map(|&i| steps[i].label.clone()).collect()
    }

    fn agent() -> AgentId {
        "worker".parse().expect("valid agent id")
    }

    fn dispatched(label: &str, attempt: u32) -> WorkflowJournalEntry {
        WorkflowJournalEntry::StepDispatched {
            label: label.to_owned(),
            attempt,
            reply_from: vec![4, 9],
        }
    }

    fn completed(label: &str, output: &[u8]) -> WorkflowJournalEntry {
        WorkflowJournalEntry::StepCompleted {
            label: label.to_owned(),
            output: output.to_vec(),
            agents: vec!["worker".to_owned()],
        }
    }
}
