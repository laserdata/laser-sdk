use crate::agent::contract::Contract;
use crate::agent::router::{CapabilitySelector, InboxRoute, Router};
use crate::context::ContextAssembler;
use crate::error::LaserError;
use crate::laser::Laser;
use crate::provenance::{AgentTopic, Provenance};
use crate::types::{AgentId, ConversationId};
use laser_wire::framing::{decode_named, encode_named};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::time::Instant;

/// One workflow-journal record. The engine records a step's outcome so a crashed
/// run resumes by replaying the journal rather than re-executing. Replay
/// re-derives the recorded output, it does not re-run the step. Keyed on the log
/// by the run's conversation id, so one journal topic carries every run.
#[derive(Serialize, Deserialize)]
enum WorkflowJournalEntry {
    StepCompleted { label: String, output: Vec<u8> },
}

/// The key-value namespace holding the workflow run's fenced lease, the
/// orchestrator-fence an exclusive step claims.
#[cfg(feature = "kv")]
const WORKFLOW_FENCE_NAMESPACE: &str = "agdx.workflow.fence";

/// How long an exclusive step's fenced lease is held before it must be renewed.
#[cfg(feature = "kv")]
const WORKFLOW_LEASE_TTL: Duration = Duration::from_secs(60);

/// The least wall-clock a step is worth dispatching with. A budget with less than
/// this remaining is treated as spent, so the workflow breaches as a budget
/// exceeded rather than handing the step a near-zero deadline that surfaces as a
/// generic timeout.
const STEP_BUDGET_FLOOR: Duration = Duration::from_millis(100);

/// A spend ceiling for a workflow, summed from the advisory `usage` on replies and
/// the engine's own counters. Any unset dimension is unbounded. A breach is a
/// [`LaserError::BudgetExceeded`], read from the engine's running totals.
#[derive(Clone, Copy, Debug, Default)]
pub struct Budget {
    tokens: Option<u64>,
    wall_clock: Option<Duration>,
    invocations: Option<u32>,
}

impl Budget {
    /// An unbounded budget (the default).
    pub fn unlimited() -> Self {
        Self::default()
    }

    /// Cap the summed input-plus-output tokens across all step replies. Only
    /// counts the `usage` a step's reply carries, which an AGDX `respond` sets
    /// with `with_usage`; a plain `send_agent` reply (no envelope) and an
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

/// Builds a step's task payload from the outputs of the steps before it. A plain
/// closure `Fn(&StepContext) -> Vec<u8>` is a `StepFn`, so most steps are a
/// closure that formats the prior outputs into the next task.
pub trait StepFn: Send + Sync {
    fn build(&self, ctx: &StepContext<'_>) -> Vec<u8>;
}

impl<F> StepFn for F
where
    F: Fn(&StepContext<'_>) -> Vec<u8> + Send + Sync,
{
    fn build(&self, ctx: &StepContext<'_>) -> Vec<u8> {
        self(ctx)
    }
}

/// Checks a step's reply before it counts as done. A plain closure
/// `Fn(&[u8]) -> bool` is a `Verifier`. A `false` verdict fails the step (and
/// triggers compensation), so a verifier panel guards a step's correctness, not
/// just its completion.
pub trait Verifier: Send + Sync {
    fn verify(&self, output: &[u8]) -> bool;
}

impl<F> Verifier for F
where
    F: Fn(&[u8]) -> bool + Send + Sync,
{
    fn verify(&self, output: &[u8]) -> bool {
        self(output)
    }
}

/// How a step responds to its contract timing out without a terminal reply.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum OnTimeout {
    /// Fail the step and run compensations (the default).
    #[default]
    Fail,
    /// Hand the task to a fresh holder, re-acquiring the lease so the fence bumps
    /// and the timed-out holder is gated out at the sink. Needs an exclusive step,
    /// since the fence is what makes reassignment safe from double-execution.
    Reassign,
}

/// The most times an exclusive step is reassigned on timeout before it fails.
const MAX_REASSIGNMENTS: u32 = 2;

/// One directed dispatch's outcome, with a timeout kept distinct from other
/// non-completions so an exclusive step can reassign on it.
enum StepDispatch {
    Completed { body: Vec<u8>, tokens: u64 },
    TimedOut,
    NotCompleted,
}

impl StepDispatch {
    /// The completing reply's body and tokens, or a step failure with `on_failure`
    /// for a timeout or other non-completion (the no-reassign collapse).
    fn completed(self, on_failure: &str) -> Result<(Vec<u8>, u64), LaserError> {
        match self {
            StepDispatch::Completed { body, tokens } => Ok((body, tokens)),
            _ => Err(LaserError::Handler(on_failure.to_owned())),
        }
    }
}

impl From<Contract> for StepDispatch {
    fn from(outcome: Contract) -> Self {
        match outcome {
            Contract::Completed(reply) => {
                let tokens = reply
                    .envelope
                    .as_ref()
                    .and_then(|envelope| envelope.usage)
                    .map_or(0, |usage| usage.input_tokens + usage.output_tokens);
                StepDispatch::Completed {
                    body: reply.body().to_vec(),
                    tokens,
                }
            }
            Contract::TimedOut => StepDispatch::TimedOut,
            Contract::Failed(_) | Contract::NotConsumed => StepDispatch::NotCompleted,
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
    on_timeout: OnTimeout,
    compensate: Option<Box<dyn StepFn>>,
}

/// A journalled directed-acyclic workflow over the built coordination primitives.
/// Each step is a directed request (the task [`contract`](Laser::contract)) to its
/// `target`, ordered by its declared dependencies, with an optional verifier,
/// exclusivity, and a compensation. Built by [`Laser::workflow`], described with
/// fluent [`step`](Self::step) calls, and driven by [`run`](Self::run).
///
/// The single decision the developer makes per step is whether it has an exclusive
/// side effect (`.exclusive()`). Everything else (lease, fence, journal, replay)
/// is the engine's, never a word in a user-facing signature.
pub struct Workflow<'a> {
    laser: &'a Laser,
    name: String,
    budget: Budget,
    inbox_route: InboxRoute,
    run_id: Option<ConversationId>,
    steps: Vec<Step>,
}

impl<'a> Workflow<'a> {
    /// Set the workflow's spend ceiling.
    pub fn budget(mut self, budget: Budget) -> Self {
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

    /// Resume an earlier run by its id (from a prior [`WorkflowOutcome::run_id`]):
    /// the engine replays that run's journal and skips the steps already recorded
    /// complete, re-dispatching only the unfinished ones. A fresh run mints its own
    /// id, so omit this to start anew.
    pub fn run_id(mut self, run_id: ConversationId) -> Self {
        self.run_id = Some(run_id);
        self
    }

    /// Add a step `label` that dispatches to `target`, building its payload from
    /// the prior outputs with `run`. Returns a [`StepHandle`] so the step can be
    /// refined (`after`, `verify_with`, `exclusive`, `compensate_with`) before the
    /// next `step`.
    pub fn step(
        mut self,
        label: &str,
        target: Router,
        run: impl StepFn + 'static,
    ) -> StepHandle<'a> {
        self.steps.push(Step {
            label: label.to_owned(),
            target,
            run: Box::new(run),
            after: Vec::new(),
            verifier: None,
            exclusive: false,
            on_timeout: OnTimeout::Fail,
            compensate: None,
        });
        let current = self.steps.len() - 1;
        StepHandle {
            workflow: self,
            current,
        }
    }

    /// Run the workflow to completion, returning each step's output. Steps run in
    /// dependency order. A step's reply that fails its verifier, or a dispatch that
    /// does not complete, fails the workflow after running the completed steps'
    /// compensations in reverse (the saga rollback).
    pub async fn run(self) -> Result<WorkflowOutcome, LaserError> {
        let order = topological_order(&self.steps)?;
        let source: AgentId = self.name.parse().map_err(|_| {
            LaserError::Invalid(format!(
                "workflow name `{}` is not a valid agent id",
                self.name
            ))
        })?;
        let run_id = self.run_id.unwrap_or_default();

        let started = Instant::now();
        // Replay the journal: a step recorded complete keeps its recorded output
        // and is not re-dispatched (replay re-derives, never re-executes).
        let mut outputs = self.replay(run_id).await?;
        let mut completed: Vec<usize> = order
            .iter()
            .copied()
            .filter(|&index| outputs.contains_key(&self.steps[index].label))
            .collect();
        let mut tokens_spent: u64 = 0;
        let mut invocations: u32 = 0;

        for index in order {
            let step = &self.steps[index];
            if outputs.contains_key(&step.label) {
                // Recorded complete on a prior run, skip it.
                continue;
            }
            if matches!(step.target, Router::Broadcast) {
                self.compensate(&completed, &outputs).await;
                return Err(LaserError::Unsupported(
                    "a broadcast workflow step has no gather target".to_owned(),
                ));
            }
            // A near-spent wall-clock budget would hand the step a near-zero
            // deadline that surfaces as a generic timeout. Treat it as the budget
            // breach it is, before claiming any lease.
            if let Some(total) = self.budget.wall_clock
                && total.saturating_sub(started.elapsed()) < STEP_BUDGET_FLOOR
            {
                self.compensate(&completed, &outputs).await;
                return Err(LaserError::BudgetExceeded {
                    ceiling: total.as_micros().min(u128::from(u64::MAX)) as u64,
                    spent: started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64,
                });
            }
            // Only a directed step can be exclusive (a scatter has no single target
            // to fence), and reassign-on-timeout needs an exclusive step, since the
            // fence is what makes handing the task to a fresh holder safe from the
            // timed-out one double-executing.
            if step.exclusive && matches!(step.target, Router::AllCapable(_)) {
                self.compensate(&completed, &outputs).await;
                return Err(LaserError::Invalid(
                    "an exclusive step must be directed (to / to_capable)".to_owned(),
                ));
            }
            if !step.exclusive && step.on_timeout == OnTimeout::Reassign {
                self.compensate(&completed, &outputs).await;
                return Err(LaserError::Invalid(
                    "on_timeout(Reassign) needs an exclusive step, so the reassignment \
                     bumps the fence and the timed-out holder is gated out"
                        .to_owned(),
                ));
            }

            invocations += 1;
            self.check_budget(invocations, tokens_spent, started)?;

            let payload = step.run.build(&StepContext { outputs: &outputs });
            // A directed step is one contract. An exclusive step claims a fenced
            // lease, pins a task conversation stable across re-dispatch, and on a
            // timeout may reassign (re-leasing bumps the fence, gating the stale
            // holder). An all-capable step scatters to every capable agent and
            // folds their replies. All return the step output and reply tokens.
            let dispatched = match &step.target {
                Router::AllCapable(selector) => {
                    self.scatter(&source, selector, &payload, started).await
                }
                _ if step.exclusive => {
                    self.dispatch_exclusive(&source, step, &payload, started, run_id)
                        .await
                }
                _ => self
                    .dispatch_one(&source, step.target.clone(), payload, started)
                    .await
                    .and_then(|outcome| outcome.completed("a workflow step did not complete")),
            };
            let (output, step_tokens) = match dispatched {
                Ok(result) => result,
                Err(error) => {
                    self.compensate(&completed, &outputs).await;
                    return Err(error);
                }
            };
            tokens_spent = tokens_spent.saturating_add(step_tokens);

            if let Some(verifier) = &step.verifier
                && !verifier.verify(&output)
            {
                self.compensate(&completed, &outputs).await;
                return Err(LaserError::Handler(format!(
                    "workflow step `{}` failed verification",
                    step.label
                )));
            }

            self.journal(run_id, &step.label, &output).await?;
            outputs.insert(step.label.clone(), output);
            completed.push(index);
            self.check_budget(invocations, tokens_spent, started)?;
        }

        Ok(WorkflowOutcome { outputs, run_id })
    }

    /// Dispatch a directed step as one contract. A completion returns its body and
    /// reply tokens; a timeout is surfaced distinctly so an exclusive step can
    /// reassign; any other non-completion fails the step.
    async fn dispatch_one(
        &self,
        source: &AgentId,
        target: Router,
        payload: Vec<u8>,
        started: Instant,
    ) -> Result<StepDispatch, LaserError> {
        let outcome = self
            .laser
            .contract(target)
            .from(source.clone())
            .payload(payload)
            .inbox_route(self.inbox_route.clone())
            .deadline(self.step_deadline(started))
            .send()
            .await?;
        Ok(StepDispatch::from(outcome))
    }

    /// Dispatch an exclusive step: claim a fenced lease, stamp its token on a
    /// command pinned to a task conversation stable across re-dispatch, and send.
    /// On a timeout with [`OnTimeout::Reassign`], re-acquire the lease (which bumps
    /// the plane's fence sequence, so the new token is strictly greater and the
    /// timed-out holder is gated out) and re-dispatch, up to a bounded number of
    /// reassignments. Returns the completing reply's body and tokens.
    async fn dispatch_exclusive(
        &self,
        source: &AgentId,
        step: &Step,
        payload: &[u8],
        started: Instant,
        run_id: ConversationId,
    ) -> Result<(Vec<u8>, u64), LaserError> {
        let task_conversation = ConversationId::derive(&format!("{run_id}/{}", step.label));
        let max_attempts = match step.on_timeout {
            OnTimeout::Reassign => MAX_REASSIGNMENTS.saturating_add(1),
            OnTimeout::Fail => 1,
        };
        let mut attempt = 0;
        loop {
            attempt += 1;
            let fence = self.acquire_fence(run_id).await?;
            let outcome = self
                .laser
                .contract(step.target.clone())
                .from(source.clone())
                .payload(payload.to_vec())
                .inbox_route(self.inbox_route.clone())
                .deadline(self.step_deadline(started))
                .fence(fence)
                .conversation(task_conversation)
                .send()
                .await?;
            match StepDispatch::from(outcome) {
                StepDispatch::Completed { body, tokens } => return Ok((body, tokens)),
                StepDispatch::TimedOut if attempt < max_attempts => continue,
                _ => {
                    return Err(LaserError::Handler(
                        "an exclusive workflow step did not complete".to_owned(),
                    ));
                }
            }
        }
    }

    /// Claim a fenced lease on the workflow run and return its monotonic fence
    /// token. The lease grant bumps the never-expiring fence sequence plane-side
    /// and returns the new value, so an exclusive step's command carries a token
    /// strictly greater than any prior holder's, and a worker fences out the
    /// stale one. The token's monotonicity is the plane's contract: a server must
    /// back it with the dedicated fence sequence of the protocol, not a lease
    /// version that resets on re-acquire. The SDK fails closed when the server has
    /// not advertised the fenced compare-and-swap capability that the same
    /// sequence underpins, rather than trust a token that may not be monotonic.
    #[cfg(feature = "kv")]
    async fn acquire_fence(&self, run_id: ConversationId) -> Result<u64, LaserError> {
        if !self.laser.capabilities.kv.cas_fenced {
            return Err(LaserError::Unsupported(
                "an exclusive step needs the plane's monotonic fence (the fenced \
                 compare-and-swap sequence); this server does not advertise it"
                    .to_owned(),
            ));
        }
        let lease = self
            .laser
            .kv(WORKFLOW_FENCE_NAMESPACE)
            .lease(run_id.to_string(), WORKFLOW_LEASE_TTL)
            .await?;
        Ok(lease.token)
    }

    /// Without the managed key-value store there is no lease to fence with, so an
    /// exclusive step cannot run.
    #[cfg(not(feature = "kv"))]
    async fn acquire_fence(&self, _run_id: ConversationId) -> Result<u64, LaserError> {
        Err(LaserError::Unsupported(
            "exclusive workflow steps need the managed lease and fence (the plane)".to_owned(),
        ))
    }

    /// Scatter a contract to every agent advertising the selector's skill,
    /// concurrently, and fold the completed replies (each body on its own line) into
    /// the step output. The step succeeds when at least one capable agent completes,
    /// the partial-panel tolerance a verifier or diagnostic pool wants. No capable
    /// agent at all is a [`LaserError::NoCapableAgent`].
    async fn scatter(
        &self,
        source: &AgentId,
        selector: &CapabilitySelector,
        payload: &[u8],
        started: Instant,
    ) -> Result<(Vec<u8>, u64), LaserError> {
        let bodies = self
            .laser
            .scatter(
                source.clone(),
                selector,
                payload,
                &self.inbox_route,
                self.step_deadline(started),
            )
            .await?;
        if bodies.is_empty() {
            return Err(LaserError::Handler(
                "no capable agent completed the all-capable step".to_owned(),
            ));
        }
        // Fold each completed reply onto its own line for the next step.
        let folded = bodies
            .iter()
            .map(|body| String::from_utf8_lossy(body).into_owned())
            .collect::<Vec<_>>()
            .join("\n");
        Ok((folded.into_bytes(), 0))
    }

    /// Replay this run's journal into the completed-step outputs, so a resumed run
    /// skips what already finished. Reads the workflow-journal topic for the run's
    /// conversation and folds each recorded outcome.
    async fn replay(
        &self,
        run_id: ConversationId,
    ) -> Result<BTreeMap<String, Vec<u8>>, LaserError> {
        let records = ContextAssembler::builder()
            .conversation_id(run_id)
            .topics(vec![AgentTopic::WorkflowJournal])
            .build()
            .assemble(self.laser)
            .await?;
        let mut outputs = BTreeMap::new();
        for record in records {
            if let Ok(WorkflowJournalEntry::StepCompleted { label, output }) =
                decode_named::<WorkflowJournalEntry>(&record.payload)
            {
                outputs.insert(label, output);
            }
        }
        Ok(outputs)
    }

    /// Record a step's completion on the journal topic, keyed by the run's
    /// conversation, so a later resume replays it instead of re-dispatching.
    async fn journal(
        &self,
        run_id: ConversationId,
        label: &str,
        output: &[u8],
    ) -> Result<(), LaserError> {
        let entry = WorkflowJournalEntry::StepCompleted {
            label: label.to_owned(),
            output: output.to_vec(),
        };
        let payload = encode_named(&entry)
            .map_err(|error| LaserError::Codec(format!("encode journal entry: {error}")))?;
        let provenance = Provenance::builder().conversation_id(run_id).build();
        self.laser
            .send_agent(AgentTopic::WorkflowJournal, payload, &provenance)
            .await
    }

    /// Run the compensations of the completed steps in reverse order (the saga
    /// rollback). Best-effort: a compensation that the step did not declare is
    /// skipped, and a compensating dispatch failure is not surfaced over the
    /// original error.
    async fn compensate(&self, completed: &[usize], outputs: &BTreeMap<String, Vec<u8>>) {
        for &index in completed.iter().rev() {
            let step = &self.steps[index];
            let Some(compensate) = &step.compensate else {
                continue;
            };
            let Ok(source) = self.name.parse::<AgentId>() else {
                return;
            };
            let payload = compensate.build(&StepContext { outputs });
            let _ = self
                .laser
                .contract(step.target.clone())
                .from(source)
                .payload(payload)
                .inbox_route(self.inbox_route.clone())
                .deadline(Duration::from_secs(30))
                .send()
                .await;
        }
    }

    /// The deadline for one step's dispatch: whatever wall-clock budget remains,
    /// clamped to a sensible per-step default so a step without a budget still
    /// bounds its wait.
    fn step_deadline(&self, started: Instant) -> Duration {
        let default = Duration::from_secs(30);
        match self.budget.wall_clock {
            // Use whatever wall-clock remains, never longer than the per-step
            // default. A fully-spent budget is caught by the budget check first.
            Some(total) => total.saturating_sub(started.elapsed()).min(default),
            None => default,
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
                ceiling: ceiling.as_micros().min(u128::from(u64::MAX)) as u64,
                spent: started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64,
            });
        }
        Ok(())
    }
}

/// A step under construction, returned by [`Workflow::step`]. Refines the step it
/// was returned for, then chains the next `step` or `run`.
pub struct StepHandle<'a> {
    workflow: Workflow<'a>,
    current: usize,
}

impl<'a> StepHandle<'a> {
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

    /// Set what the step does when its contract times out (default
    /// [`OnTimeout::Fail`]). [`OnTimeout::Reassign`] hands the task to a fresh
    /// holder with a bumped fence, and requires [`exclusive`](Self::exclusive).
    pub fn on_timeout(mut self, on_timeout: OnTimeout) -> Self {
        self.workflow.steps[self.current].on_timeout = on_timeout;
        self
    }

    /// Provide the compensation to run if a later step fails (the saga rollback).
    pub fn compensate_with(mut self, run: impl StepFn + 'static) -> Self {
        self.workflow.steps[self.current].compensate = Some(Box::new(run));
        self
    }

    /// Set the workflow budget (forwards to [`Workflow::budget`]).
    pub fn budget(mut self, budget: Budget) -> Self {
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
    pub fn step(self, label: &str, target: Router, run: impl StepFn + 'static) -> StepHandle<'a> {
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
    pub fn workflow(&self, name: &str) -> Workflow<'_> {
        Workflow {
            laser: self,
            name: name.to_owned(),
            budget: Budget::unlimited(),
            inbox_route: InboxRoute::default(),
            run_id: None,
            steps: Vec::new(),
        }
    }
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

    fn step(label: &str, after: &[&str]) -> Step {
        Step {
            label: label.to_owned(),
            target: Router::broadcast(),
            run: Box::new(|_: &StepContext<'_>| Vec::new()),
            after: after.iter().map(|s| s.to_owned().to_owned()).collect(),
            verifier: None,
            exclusive: false,
            on_timeout: OnTimeout::Fail,
            compensate: None,
        }
    }

    fn labels(steps: &[Step], order: &[usize]) -> Vec<String> {
        order.iter().map(|&i| steps[i].label.clone()).collect()
    }

    #[test]
    fn given_a_linear_chain_when_ordered_then_should_follow_the_dependencies() {
        let steps = vec![
            step("credit", &["diagnose"]),
            step("triage", &[]),
            step("diagnose", &["triage"]),
        ];
        let order = topological_order(&steps).expect("a chain is acyclic");
        assert_eq!(labels(&steps, &order), ["triage", "diagnose", "credit"]);
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
        let error = topological_order(&steps).unwrap_err();
        assert!(matches!(error, LaserError::Invalid(message) if message.contains("cycle")));
    }

    #[test]
    fn given_an_unknown_dependency_when_ordered_then_should_error() {
        let steps = vec![step("a", &["ghost"])];
        let error = topological_order(&steps).unwrap_err();
        assert!(matches!(error, LaserError::Invalid(message) if message.contains("unknown step")));
    }
}
