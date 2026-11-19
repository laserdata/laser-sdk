use crate::common::test_iggy::fresh_laser;
use crate::common::world::LaserWorld;
use cucumber::{given, then, when};
use laser_bdd::wait_for_partition_reads;
use laser_sdk::agent::{
    Agent, AgentCtx, AgentHandle, AgentHandler, AgentMessage, InboxRoute, Router, StepContext,
    StepFn, TopicRetention, WorkflowOutcome,
};
use laser_sdk::prelude::{AgentTopic, ConversationId, LaserError};
use laser_sdk::wire::agent::{CapabilityDescriptor, SessionStatus};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// The workflow scenarios' state: the agents that answer steps and what each
/// received, the last run's outcome or error, and the local step bodies run.
#[derive(Default)]
pub struct WorkflowWorld {
    agents: Vec<AgentHandle>,
    received: Arc<Mutex<Vec<(String, String)>>>,
    outcome: Option<Result<WorkflowOutcome, LaserError>>,
    run_id: Option<ConversationId>,
    local_runs: Arc<Mutex<Vec<String>>>,
    compensations: Arc<Mutex<Vec<String>>>,
    function_labels: Vec<String>,
}

impl std::fmt::Debug for WorkflowWorld {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WorkflowWorld")
            .field("agents", &self.agents.len())
            .field("run_id", &self.run_id)
            .finish_non_exhaustive()
    }
}

#[given("a fresh stream bootstrapped with the defaults")]
async fn fresh_stream_bootstrapped_with_defaults(world: &mut LaserWorld) {
    let fresh = fresh_laser().await;
    fresh
        .laser
        .bootstrap(1, TopicRetention::default())
        .await
        .expect("bootstrap the stream with the defaults");
    let context = fresh.laser.context(ConversationId::new());
    wait_for_partition_reads(async || {
        context
            .fetch(vec![AgentTopic::Sessions], 1, None)
            .await
            .map(|_| ())
    })
    .await
    .expect("the session partitions are readable");
    world.platform = fresh.iggy;
    world.laser = Some(fresh.laser);
}

#[given(regex = r#"^agent "([^"]+)" answers workflow steps on the session topic$"#)]
async fn answering_agent(world: &mut LaserWorld, name: String) {
    spawn(world, &name, None, Answer::Done).await;
}

#[given(regex = r#"^agent "([^"]+)" fails every workflow step on the session topic$"#)]
async fn failing_agent(world: &mut LaserWorld, name: String) {
    spawn(world, &name, None, Answer::Fail).await;
}

#[given(
    regex = r#"^agent "([^"]+)" advertising the skill "([^"]+)" answers workflow steps on the session topic$"#
)]
async fn advertising_agent(world: &mut LaserWorld, name: String, skill: String) {
    spawn(world, &name, Some(skill), Answer::Done).await;
}

#[when(
    regex = r#"^the workflow "([^"]+)" runs "([^"]+)", then "([^"]+)" after "([^"]+)", then "([^"]+)" after "([^"]+)" on "([^"]+)"$"#
)]
#[allow(clippy::too_many_arguments)]
async fn three_routed_steps(
    world: &mut LaserWorld,
    name: String,
    first: String,
    second: String,
    second_after: String,
    third: String,
    third_after: String,
    agent: String,
) {
    let target = || Router::to(agent.parse().expect("a valid agent id"));
    let outcome = world
        .laser()
        .workflow(&name)
        .inbox_route(InboxRoute::Fixed(AgentTopic::Sessions))
        .step(&first, target(), body(&first))
        .step(&second, target(), body(&second))
        .after(&second_after)
        .step(&third, target(), body(&third))
        .after(&third_after)
        .run()
        .await;
    world.workflow.outcome = Some(outcome);
}

#[when(
    regex = r#"^the workflow "([^"]+)" runs the step "([^"]+)" on "([^"]+)" with a verifier that rejects every reply$"#
)]
async fn rejected_step(world: &mut LaserWorld, name: String, label: String, agent: String) {
    let outcome = world
        .laser()
        .workflow(&name)
        .inbox_route(InboxRoute::Fixed(AgentTopic::Sessions))
        .step(
            &label,
            Router::to(agent.parse().expect("a valid agent id")),
            body(&label),
        )
        .verify_with(|_: &[u8]| false)
        .run()
        .await;
    world.workflow.outcome = Some(outcome);
}

#[when(
    regex = r#"^the workflow "([^"]+)" runs "([^"]+)" and "([^"]+)" on "([^"]+)" and then "([^"]+)" on "([^"]+)", each with a compensation$"#
)]
async fn compensated_steps(
    world: &mut LaserWorld,
    name: String,
    first: String,
    second: String,
    agent: String,
    third: String,
    breaker: String,
) {
    let to = |agent: &str| Router::to(agent.parse().expect("a valid agent id"));
    let outcome = world
        .laser()
        .workflow(&name)
        .inbox_route(InboxRoute::Fixed(AgentTopic::Sessions))
        .step(&first, to(&agent), body(&first))
        .compensate_with(undo(&first))
        .step(&second, to(&agent), body(&second))
        .compensate_with(undo(&second))
        .step(&third, to(&breaker), body(&third))
        .compensate_with(undo(&third))
        .run()
        .await;
    world.workflow.outcome = Some(outcome);
}

#[when(
    regex = r#"^the workflow "([^"]+)" runs the step "([^"]+)" on an agent with the skill "([^"]+)"$"#
)]
async fn capable_step(world: &mut LaserWorld, name: String, label: String, skill: String) {
    let outcome = world
        .laser()
        .workflow(&name)
        .step(
            &label,
            Router::to_capable(skill, Default::default()),
            body(&label),
        )
        .run()
        .await;
    world.workflow.outcome = Some(outcome);
}

#[when(
    regex = r#"^the workflow "([^"]+)" runs the step "([^"]+)" on "([^"]+)" with a deadline of (\d+) milliseconds$"#
)]
async fn silent_step(
    world: &mut LaserWorld,
    name: String,
    label: String,
    agent: String,
    deadline_ms: u64,
) {
    let outcome = world
        .laser()
        .workflow(&name)
        .inbox_route(InboxRoute::Fixed(AgentTopic::Sessions))
        .step(
            &label,
            Router::to(agent.parse().expect("a valid agent id")),
            body(&label),
        )
        .deadline(Duration::from_millis(deadline_ms))
        .run()
        .await;
    world.workflow.outcome = Some(outcome);
}

#[when(
    regex = r#"^the workflow "([^"]+)" runs the step "([^"]+)" on "([^"]+)" through the advertised inbox route$"#
)]
async fn advertised_step(world: &mut LaserWorld, name: String, label: String, agent: String) {
    let outcome = world
        .laser()
        .workflow(&name)
        .step(
            &label,
            Router::to(agent.parse().expect("a valid agent id")),
            body(&label),
        )
        .run()
        .await;
    world.workflow.outcome = Some(outcome);
}

#[when(regex = r#"^the workflow "([^"]+)" runs the local steps "([^"]+)", "([^"]+)", "([^"]+)"$"#)]
async fn local_steps(
    world: &mut LaserWorld,
    name: String,
    first: String,
    second: String,
    third: String,
) {
    let outcome = local_workflow(world, &name, [&first, &second, &third], None, None).await;
    world.workflow.outcome = Some(outcome);
}

#[when(
    regex = r#"^the workflow "([^"]+)" runs the local steps "([^"]+)", "([^"]+)", "([^"]+)" and step "([^"]+)" fails once$"#
)]
async fn local_steps_failing_once(
    world: &mut LaserWorld,
    name: String,
    first: String,
    second: String,
    third: String,
    failing: String,
) {
    let run_id = ConversationId::new();
    world.workflow.run_id = Some(run_id);
    let outcome = local_workflow(
        world,
        &name,
        [&first, &second, &third],
        Some(run_id),
        Some(&failing),
    )
    .await;
    assert!(outcome.is_err(), "the first run fails at {failing}");
    world.workflow.outcome = Some(outcome);
}

#[when(regex = r#"^the workflow "([^"]+)" resumes the same run of local steps$"#)]
async fn local_steps_resumed(world: &mut LaserWorld, name: String) {
    let run_id = world.workflow.run_id.expect("a run to resume");
    let labels = ["generate", "process", "finish"];
    let outcome = local_workflow(world, &name, labels, Some(run_id), None).await;
    world.workflow.outcome = Some(outcome);
}

#[when(
    regex = r#"^the workflow "([^"]+)" runs the local steps "([^"]+)" and "([^"]+)" with compensations and then "([^"]+)" which fails$"#
)]
async fn local_steps_compensated(
    world: &mut LaserWorld,
    name: String,
    first: String,
    second: String,
    failing: String,
) {
    let compensations = Arc::clone(&world.workflow.compensations);
    let undo_locally = |label: &str| {
        let compensations = Arc::clone(&compensations);
        let text = format!("undo {label}");
        move |_: &StepContext<'_>| {
            compensations
                .lock()
                .expect("the compensation log")
                .push(text.clone());
            Vec::new()
        }
    };
    let runs = Arc::clone(&world.workflow.local_runs);
    let outcome = world
        .laser()
        .workflow(&name)
        .step_local(&first, LocalBody::new(&first, &runs, false))
        .compensate_with(undo_locally(&first))
        .step_local(&second, LocalBody::new(&second, &runs, false))
        .compensate_with(undo_locally(&second))
        .step_local(&failing, LocalBody::new(&failing, &runs, true))
        .run()
        .await;
    world.workflow.outcome = Some(outcome);
}

#[when(
    regex = r#"^the workflow function "([^"]+)" calls the steps "([^"]+)", "([^"]+)", "([^"]+)"$"#
)]
async fn function_steps(
    world: &mut LaserWorld,
    name: String,
    first: String,
    second: String,
    third: String,
) {
    function_run(world, &name, vec![first, second, third], None).await;
}

#[when(
    regex = r#"^the workflow function "([^"]+)" calls the steps "([^"]+)", "([^"]+)", "([^"]+)" and step "([^"]+)" fails once$"#
)]
async fn function_steps_failing_once(
    world: &mut LaserWorld,
    name: String,
    first: String,
    second: String,
    third: String,
    failing: String,
) {
    function_run(world, &name, vec![first, second, third], Some(failing)).await;
}

#[when(
    regex = r#"^the workflow function "([^"]+)" calls the step "([^"]+)" twice and then "([^"]+)"$"#
)]
async fn function_step_twice(world: &mut LaserWorld, name: String, twice: String, last: String) {
    function_run(world, &name, vec![twice.clone(), twice, last], None).await;
}

#[when(regex = r#"^the workflow function "([^"]+)" resumes the same run$"#)]
async fn function_resumed(world: &mut LaserWorld, name: String) {
    let labels = world.workflow.function_labels.clone();
    function_run(world, &name, labels, None).await;
}

#[then(regex = r#"^the workflow function returned "([^"]+)"$"#)]
async fn function_returned(world: &mut LaserWorld, expected: String) {
    let value = completed(world)
        .value
        .as_deref()
        .expect("the workflow's value");
    let value: String = serde_json::from_slice(value).expect("the value is JSON text");
    assert_eq!(value, expected);
}

#[then(regex = r#"^the workflow completed the steps (".+") in order$"#)]
async fn completed_in_order(world: &mut LaserWorld, labels: String) {
    let outcome = completed(world);
    assert_eq!(outcome.completed_steps, quoted(&labels));
    assert_eq!(outcome.status, SessionStatus::Completed);
}

#[then(regex = r#"^the workflow output of step "([^"]+)" is "([^"]+)"$"#)]
async fn workflow_output(world: &mut LaserWorld, label: String, expected: String) {
    assert_eq!(
        completed(world).text(&label).as_deref(),
        Some(expected.as_str())
    );
}

#[then(regex = r#"^the workflow output text of step "([^"]+)" is "([^"]+)"$"#)]
async fn workflow_output_text(world: &mut LaserWorld, label: String, expected: String) {
    assert_eq!(
        completed(world).text(&label).as_deref(),
        Some(expected.as_str())
    );
}

#[then(regex = r#"^the workflow fails at step "([^"]+)" with reason "([^"]+)"$"#)]
async fn fails_at_step(world: &mut LaserWorld, label: String, expected: String) {
    match failure(world) {
        LaserError::StepFailed { step, reason, .. } => {
            assert_eq!(step, &label);
            assert_eq!(reason.to_string(), expected);
        }
        other => panic!("the workflow fails at step {label}, not with {other}"),
    }
}

#[then(regex = r#"^the failure names the target "([^"]+)" and a deadline of (\d+) milliseconds$"#)]
async fn failure_names_target(world: &mut LaserWorld, agent: String, deadline_ms: u64) {
    let error = failure(world);
    assert!(
        matches!(error, LaserError::StepFailed { target, deadline_ms: deadline, .. } if target.contains(&agent) && *deadline == deadline_ms),
        "{error:?}"
    );
    let text = error.to_string();
    assert!(text.contains(&agent), "{text}");
    assert!(
        text.contains(&format!("deadline {deadline_ms} ms")),
        "{text}"
    );
}

#[then(regex = r#"^agent "([^"]+)" received the compensations "([^"]+)" then "([^"]+)"$"#)]
async fn received_compensations(
    world: &mut LaserWorld,
    agent: String,
    first: String,
    second: String,
) {
    let received: Vec<String> = world
        .workflow
        .received
        .lock()
        .expect("the received log")
        .iter()
        .filter(|(by, body)| by == &agent && body.starts_with("undo "))
        .map(|(_, body)| body.clone())
        .collect();
    assert_eq!(received, [first, second]);
}

#[then("the error names the agent capabilities and the fixed inbox route")]
async fn names_inbox_fixes(world: &mut LaserWorld) {
    let error = failure(world);
    assert!(matches!(error, LaserError::NoInbox { .. }), "{error:?}");
    let text = error.to_string();
    assert!(text.contains("capabilities"), "{text}");
    assert!(text.contains("fixed_inbox"), "{text}");
}

#[then(regex = r#"^the local step bodies ran (".+")$"#)]
async fn local_bodies_ran(world: &mut LaserWorld, labels: String) {
    assert_eq!(
        *world.workflow.local_runs.lock().expect("the run log"),
        quoted(&labels)
    );
}

#[then(regex = r#"^the workflow fails with the local step error "([^"]+)"$"#)]
async fn local_step_error(world: &mut LaserWorld, expected: String) {
    let error = failure(world);
    assert!(error.to_string().contains(&expected), "{error}");
}

#[then(regex = r#"^the local compensations ran "([^"]+)" then "([^"]+)"$"#)]
async fn local_compensations(world: &mut LaserWorld, first: String, second: String) {
    assert_eq!(
        *world
            .workflow
            .compensations
            .lock()
            .expect("the compensation log"),
        [first, second]
    );
}

async fn spawn(world: &mut LaserWorld, name: &str, skill: Option<String>, answer: Answer) {
    let mut agent = Agent::builder()
        .id(name.parse().expect("a valid agent id"))
        .listen_on(AgentTopic::Sessions)
        .respond_on(AgentTopic::Sessions)
        .maybe_capabilities(skill.map(|skill| {
            vec![CapabilityDescriptor {
                skill_id: skill,
                ..CapabilityDescriptor::default()
            }]
        }))
        .handler(StepAnswerer {
            name: name.to_owned(),
            answer,
            received: Arc::clone(&world.workflow.received),
        })
        .build()
        .spawn(world.laser().clone());
    if let Err(error) = agent.ready().await {
        panic!(
            "agent {name} did not start: {error}, {:?}",
            agent.shutdown().await
        );
    }
    world.workflow.agents.push(agent);
}

async fn local_workflow(
    world: &LaserWorld,
    name: &str,
    labels: [&str; 3],
    run_id: Option<ConversationId>,
    failing: Option<&str>,
) -> Result<WorkflowOutcome, LaserError> {
    let runs = &world.workflow.local_runs;
    let mut workflow = world.laser().workflow(name);
    if let Some(run_id) = run_id {
        workflow = workflow.run_id(run_id);
    }
    let [first, second, third] = labels;
    let fails = |label: &str| failing == Some(label);
    workflow
        .step_local(first, LocalBody::new(first, runs, fails(first)))
        .step_local(second, LocalBody::new(second, runs, fails(second)))
        .step_local(third, LocalBody::new(third, runs, fails(third)))
        .run()
        .await
}

// Runs the labels as steps of a workflow function under one run id, so a later
// call resumes the run. A step function records each run and answers "<label> done".
async fn function_run(
    world: &mut LaserWorld,
    name: &str,
    labels: Vec<String>,
    failing: Option<String>,
) {
    let run_id = *world
        .workflow
        .run_id
        .get_or_insert_with(ConversationId::new);
    world.workflow.function_labels = labels.clone();
    let runs = Arc::clone(&world.workflow.local_runs);
    let outcome = world
        .laser()
        .workflow(name)
        .run_id(run_id)
        .run_with(async |run| {
            let mut value = String::new();
            for label in &labels {
                value = run
                    .step(label, async || {
                        runs.lock().expect("the run log").push(label.clone());
                        if failing.as_deref() == Some(label.as_str()) {
                            return Err(LaserError::Handler(format!("{label} failed")));
                        }
                        Ok(format!("{label} done"))
                    })
                    .await?;
            }
            Ok(value)
        })
        .await;
    world.workflow.outcome = Some(outcome);
}

fn completed(world: &LaserWorld) -> &WorkflowOutcome {
    match world.workflow.outcome.as_ref().expect("a workflow ran") {
        Ok(outcome) => outcome,
        Err(error) => panic!("the workflow completes, not with {error}"),
    }
}

fn failure(world: &LaserWorld) -> &LaserError {
    match world.workflow.outcome.as_ref().expect("a workflow ran") {
        Ok(outcome) => panic!("the workflow fails, not with {:?}", outcome.completed_steps),
        Err(error) => error,
    }
}

// A step body is the step label.
fn body(label: &str) -> impl Fn(&StepContext<'_>) -> Vec<u8> + Send + Sync + 'static {
    let label = label.to_owned();
    move |_: &StepContext<'_>| label.clone().into_bytes()
}

fn undo(label: &str) -> impl Fn(&StepContext<'_>) -> Vec<u8> + Send + Sync + 'static {
    let text = format!("undo {label}");
    move |_: &StepContext<'_>| text.clone().into_bytes()
}

fn quoted(list: &str) -> Vec<String> {
    list.split('"')
        .skip(1)
        .step_by(2)
        .map(str::to_owned)
        .collect()
}

#[derive(Clone, Copy)]
enum Answer {
    Done,
    Fail,
}

// Answers a workflow step with "<body> done by <agent>", or rejects it, and
// records every body it received.
struct StepAnswerer {
    name: String,
    answer: Answer,
    received: Arc<Mutex<Vec<(String, String)>>>,
}

impl AgentHandler for StepAnswerer {
    async fn handle(&self, message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        let body = String::from_utf8_lossy(message.body()).into_owned();
        self.received
            .lock()
            .expect("the received log")
            .push((self.name.clone(), body.clone()));
        match self.answer {
            Answer::Done => {
                ctx.respond(format!("{body} done by {}", self.name).into_bytes())
                    .await
            }
            Answer::Fail => Err(LaserError::rejected(format!(
                "{} fails every step",
                self.name
            ))),
        }
    }
}

// A local step body that records each run, answers "<label> done in
// process", and fails its first run when asked.
struct LocalBody {
    label: String,
    runs: Arc<Mutex<Vec<String>>>,
    fail_once: AtomicBool,
}

impl LocalBody {
    fn new(label: &str, runs: &Arc<Mutex<Vec<String>>>, fail_once: bool) -> Self {
        Self {
            label: label.to_owned(),
            runs: Arc::clone(runs),
            fail_once: AtomicBool::new(fail_once),
        }
    }
}

#[async_trait::async_trait]
impl StepFn for LocalBody {
    async fn build(&self, _context: &StepContext<'_>) -> Result<Vec<u8>, LaserError> {
        self.runs
            .lock()
            .expect("the run log")
            .push(self.label.clone());
        if self.fail_once.swap(false, Ordering::SeqCst) {
            return Err(LaserError::Handler(format!("{} failed", self.label)));
        }
        Ok(format!("{} done in process", self.label).into_bytes())
    }
}
