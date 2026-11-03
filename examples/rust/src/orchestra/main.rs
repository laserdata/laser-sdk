use laser_examples::{PARTITIONS, env_bool, fresh_run, init_tracing, laser, phase, stream_for};
use laser_sdk::prelude::full::*;
use laser_sdk::wire::agent::{AgentCard, CapabilityDescriptor, Health};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};

// THE orchestration example: one orchestrator coordinating a pool of long-running
// capability agents, entirely over the log, never a direct call. It is
// INTERACTIVE and paced: it stops at each phase and waits for Enter, so you can
// open the LaserData console's Orchestration view (`/orchestration`) and watch every
// transition happen live, presence, the registry, contracts, and the workflow
// journal. `LASER_NON_INTERACTIVE=1` runs it straight through.
//
// The agents connect once at the start and stay up for the whole run, so the
// console shows a live, populated fabric the entire time. Each phase:
//
//   1. DISCOVERY    six agents connect and advertise a capability card + presence.
//                   The orchestrator resolves them from the fused registry, so it
//                   never hard-codes who can do what.
//   2. CONTRACT     a directed task to one capable agent (Router::to_capable).
//   3. FAN-OUT      a panel scattered to every capable agent (Router::all_capable).
//                   One agent advertises Unavailable, so routing leaves it out.
//   4. WORKFLOW     a journalled run: triage, then a diagnose panel, then remediate,
//                   each step building its task from the prior steps' outputs.
//   5. QUARANTINE   an operator pulls a misbehaving agent, and the panel routes around it.
//   6. RECOVERY     the operator reinstates it (un-quarantine), and the panel is whole.
//   7. EXPIRY       a tight-deadline task to a slow agent times out, and the
//                   orchestrator recovers by re-dispatching to a healthy one.
//
// Routing uses a fixed inbox topic so it runs against Apache Iggy: every
// branch is addressed to its agent on `agent.sessions`. Presence advertisement
// is best-effort: a managed deployment records it, and it is a no-op against
// Apache Iggy. The registry, contracts, and workflows work on both.
//
//   cargo run --release --example orchestra

const EXAMPLE: &str = "orchestra";
const CLASSIFY: &str = "classify";
const DIAGNOSE: &str = "diagnose";
const REMEDIATE: &str = "remediate";
const SLOW_TASK: &str = "slow-task";
const INCIDENT: &str = "auth API latency spike";
const ORCHESTRATOR: &str = "orchestrator";

#[tokio::main]
async fn main() -> Result<(), LaserError> {
    init_tracing();
    let stream = stream_for(EXAMPLE);
    let laser = laser(&stream, Capabilities::OPEN).await?;
    fresh_run(&laser, &stream, async {
        laser
            .bootstrap(
                PARTITIONS,
                laser_sdk::agent::TopicRetention::expire_after(Duration::from_secs(86_400)),
            )
            .await?;

        phase("Discovery: a pool of long-running capability agents connects");
        // Kept alive for the whole run so the console stays populated. Health is a
        // property of the card: diag-gamma advertises Unavailable to prove routing
        // reads it, and laggard is deliberately slow to drive the expiry phase.
        let agents = vec![
            spawn_worker(&stream, "triager", CLASSIFY, Some(Health::Healthy), ms(200)).await?,
            spawn_worker(
                &stream,
                "diag-alpha",
                DIAGNOSE,
                Some(Health::Healthy),
                ms(400),
            )
            .await?,
            spawn_worker(
                &stream,
                "diag-beta",
                DIAGNOSE,
                Some(Health::Healthy),
                ms(400),
            )
            .await?,
            spawn_worker(
                &stream,
                "diag-gamma",
                DIAGNOSE,
                Some(Health::Unavailable),
                ms(400),
            )
            .await?,
            spawn_worker(
                &stream,
                "executor",
                REMEDIATE,
                Some(Health::Healthy),
                ms(300),
            )
            .await?,
            spawn_worker(
                &stream,
                "laggard",
                SLOW_TASK,
                Some(Health::Healthy),
                secs(6),
            )
            .await?,
        ];
        println!("six agents connected and advertised their capability cards");
        pause("DISCOVERY: six agents are live in the registry (one unavailable)").await;

        phase("Contract: a directed task to one capable agent, with a deadline");
        // The orchestrator names a capability, not an agent. Routing resolves the one
        // classifier from the registry and waits for the reply or the deadline.
        let classified = completed_body(contract_skill(&laser, CLASSIFY, secs(10)).await?);
        println!(
            "classifier replied: {}",
            classified.as_deref().unwrap_or("<did not complete>")
        );
        pause("CONTRACT: a directed task completed (see it in the Contracts panel)").await;

        phase("Fan-out: a panel scattered to every capable agent");
        // Three agents advertise diagnose, but one is Unavailable, so the scatter
        // reaches the two healthy ones without the orchestrator knowing their ids.
        let findings = diagnose_panel(&laser).await?;
        println!(
            "panel gathered {} findings (the unavailable agent was skipped)",
            findings.len()
        );
        pause("FAN-OUT: two healthy diagnosers answered, the unavailable one was skipped").await;

        phase("Workflow: triage, then a diagnose panel, then remediate (journalled)");
        // The run is a session and every step a child session of it, so the
        // sessions view shows the whole tree on any deployment.
        let workflow = laser
            .workflow("incident-response")
            .inbox_route(InboxRoute::Fixed(AgentTopic::Sessions))
            // Cap the dispatches and wall clock so a runaway fan-out cannot spin.
            .budget(
                WorkflowBudget::unlimited()
                    .invocations(8)
                    .wall_clock(secs(60)),
            );
        let run = workflow
            .step(
                "triage",
                Router::to_capable(CLASSIFY, RoutePolicy::Any),
                move |_ctx: &StepContext<'_>| INCIDENT.as_bytes().to_vec(),
            )
            .step(
                "diagnose",
                Router::all_capable(DIAGNOSE, RoutePolicy::Any),
                |ctx: &StepContext<'_>| {
                    // Each step reads the prior steps' outputs from the journal, so
                    // the dependency edge is data, not a shared variable.
                    let severity = ctx.outputs.get("triage").cloned().unwrap_or_default();
                    [b"diagnose: ".to_vec(), severity].concat()
                },
            )
            .after("triage")
            .verify_with(|folded: &[u8]| !folded.is_empty())
            .step(
                "remediate",
                Router::to_capable(REMEDIATE, RoutePolicy::Any),
                |ctx: &StepContext<'_>| {
                    let findings = ctx.outputs.get("diagnose").cloned().unwrap_or_default();
                    [b"remediate: ".to_vec(), findings].concat()
                },
            )
            .after("diagnose")
            .run()
            .await?;
        println!(
            "workflow completed and journalled: {} steps",
            run.outputs.len()
        );
        pause("WORKFLOW: the run journalled triage -> diagnose -> remediate (Workflow panel)")
            .await;

        phase("Quarantine: an operator pulls a misbehaving agent");
        // Quarantine is a registry fact every fused registry folds, so the next
        // panel routes around diag-alpha with no change to the orchestrator.
        laser
            .quarantine(operator(), &agent_id("diag-alpha"))
            .await?;
        let after = diagnose_panel(&laser).await?;
        println!(
            "panel after quarantine: {} findings (alpha routed around)",
            after.len()
        );
        pause("QUARANTINE: diag-alpha is quarantined in the registry, the panel routes around it")
            .await;

        phase("Recovery: the operator reinstates the agent");
        laser
            .unquarantine(operator(), &agent_id("diag-alpha"))
            .await?;
        let reinstated = diagnose_panel(&laser).await?;
        println!(
            "panel after un-quarantine: {} findings (alpha is back)",
            reinstated.len()
        );
        pause("RECOVERY: diag-alpha is reinstated, the panel is whole again").await;

        phase("Expiry + recovery: a tight deadline times out, the orchestrator recovers");
        // The slow agent acks pickup but cannot finish inside the one-second deadline,
        // so the contract expires. The orchestrator recovers by re-dispatching to a
        // healthy fast agent, the pattern any real coordinator uses for a stuck task.
        let slow = contract_skill(&laser, SLOW_TASK, secs(1)).await?;
        if let Some(body) = completed_body(slow) {
            println!("unexpectedly fast: {body}");
        } else {
            println!("the slow agent missed the deadline, recovering on a healthy agent");
            let recovered = completed_body(contract_skill(&laser, REMEDIATE, secs(10)).await?);
            println!(
                "recovered: {}",
                recovered.as_deref().unwrap_or("<did not complete>")
            );
        }
        pause("EXPIRY: the slow agent timed out, the task recovered on a healthy agent").await;

        println!(
            "\norchestra: discovery, routing, fan-out, a journalled workflow, health,\n\
             reversible quarantine, and deadline recovery, all coordinated over the log."
        );

        for agent in agents {
            agent.shutdown().await?;
        }
        Ok(())
    })
    .await
}

// A capability agent: reads the task body, waits its handling delay (so the
// in-flight Working state is visible in the console), and replies with the work
// its skill produces.
struct Worker {
    name: String,
    skill: String,
    delay: Duration,
}

impl AgentHandler for Worker {
    async fn handle(&self, message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        tokio::time::sleep(self.delay).await;
        let task = String::from_utf8_lossy(message.body());
        let reply = match self.skill.as_str() {
            CLASSIFY => format!("severity=high ({task})"),
            DIAGNOSE => format!("{}: cache stampede on the hot key [{task}]", self.name),
            REMEDIATE => format!(
                "{}: drained the hot key, scaled the cache [{task}]",
                self.name
            ),
            other => format!("{}: {other} done [{task}]", self.name),
        };
        ctx.respond(reply).await
    }
}

// Spawn one long-running capability agent on its OWN connection, so each is a
// distinct live presence in the console (presence is per connection). It
// advertises its card and inbox on start, and the returned handle owns the
// connection until the run ends.
async fn spawn_worker(
    stream: &str,
    id: &str,
    skill: &str,
    health: Option<Health>,
    delay: Duration,
) -> Result<AgentHandle, LaserError> {
    let connection = laser(stream, Capabilities::OPEN).await?;
    let mut handle = Agent::builder()
        .id(agent_id(id))
        .listen_on(AgentTopic::Sessions)
        .respond_on(AgentTopic::Sessions)
        .capabilities(card(skill, health).capabilities)
        // Ack on pickup so the orchestrator can tell a consumed task from an
        // expired one, which is what makes the expiry phase legible.
        .ack_on_pickup(true)
        .handler(Worker {
            name: id.to_owned(),
            skill: skill.to_owned(),
            delay,
        })
        .build()
        .spawn(connection);
    handle.ready().await?;
    Ok(handle)
}

// A one-skill capability card at the given health, the fact the registry folds
// so routing can resolve this agent by what it can do.
fn card(skill: &str, health: Option<Health>) -> AgentCard {
    AgentCard {
        name: None,
        version: None,
        capabilities: vec![CapabilityDescriptor {
            skill_id: skill.to_owned(),
            health,
            ..Default::default()
        }],
        ttl_micros: None,
    }
}

// Contract a task to one agent advertising `skill`, on a fixed inbox, with a
// deadline. The orchestrator names the capability, routing picks the agent.
async fn contract_skill(
    laser: &Laser,
    skill: &str,
    deadline: Duration,
) -> Result<Contract, LaserError> {
    laser
        .contract(Router::to_capable(skill, RoutePolicy::Any))
        .from(agent_id(ORCHESTRATOR))
        .payload(INCIDENT.as_bytes())
        .inbox_route(InboxRoute::Fixed(AgentTopic::Sessions))
        .deadline(deadline)
        .send()
        .await
}

// Scatter a diagnose panel to every capable agent and return the distinct
// findings. Unavailable agents are left out by capability resolution, so the
// count reflects who could actually answer.
async fn diagnose_panel(laser: &Laser) -> Result<Vec<Vec<u8>>, LaserError> {
    laser
        .scatter(
            agent_id(ORCHESTRATOR),
            &CapabilitySelector::new(DIAGNOSE, RoutePolicy::Any),
            INCIDENT.as_bytes(),
            &InboxRoute::Fixed(AgentTopic::Sessions),
            secs(10),
        )
        .await
}

// The reply text of a completed contract, or `None` for any other outcome.
fn completed_body(outcome: Contract) -> Option<String> {
    match outcome {
        Contract::Completed(reply) => Some(String::from_utf8_lossy(reply.body()).into_owned()),
        _ => None,
    }
}

// Print what to watch, then block on Enter so the operator can flip to the
// LaserData console and observe the phase live. The read is async, so the
// spawned agents keep handling while the orchestrator waits.
// `LASER_NON_INTERACTIVE=1` skips the wait.
async fn pause(prompt: &str) {
    println!("\n  >>> {prompt}\n      (watch the console's /orchestration view, then press Enter)");
    if env_bool("LASER_NON_INTERACTIVE", false) {
        return;
    }
    let mut line = String::new();
    let _ = BufReader::new(tokio::io::stdin())
        .read_line(&mut line)
        .await;
}

// The ids in this run are all fixed strings, so a parse failure is a bug, not a
// runtime condition to handle.
fn agent_id(id: &str) -> AgentId {
    id.parse().expect("agent id is valid")
}

fn operator() -> AgentId {
    agent_id("operator")
}

fn ms(value: u64) -> Duration {
    Duration::from_millis(value)
}

fn secs(value: u64) -> Duration {
    Duration::from_secs(value)
}
