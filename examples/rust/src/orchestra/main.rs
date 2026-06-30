use bytes::Bytes;
use laser_examples::{PARTITIONS, init_tracing, laser, phase, stream_for};
use laser_sdk::prelude::*;
use laser_sdk::wire::agent::{AgentCard, CapabilityDescriptor, Health};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tracing::info;

// THE orchestration example: one orchestrator coordinating a pool of long-running
// capability agents, entirely over the log. It is INTERACTIVE and paced: it stops
// at each phase and waits for Enter, so you can open the stream-ui Orchestration
// console (`/orchestration`) and watch every transition happen live, presence,
// the registry, contracts, and the workflow journal.
//
// The agents connect once at the start and stay up for the whole run, so the
// console shows a live, populated fabric the entire time. Each phase:
//
//   1. DISCOVERY    six agents connect and advertise a capability card + presence.
//   2. CONTRACT     a directed task to one capable agent (Router::to_capable).
//   3. FAN-OUT      a panel scattered to every capable agent (Router::all_capable);
//                   the unavailable agent is routed around by health.
//   4. WORKFLOW     a journalled run: triage, then a diagnose panel, then remediate.
//   5. QUARANTINE   an operator pulls a misbehaving agent; the panel routes around it.
//   6. RECOVERY     the operator reinstates it (un-quarantine); the panel is whole.
//   7. EXPIRY       a tight-deadline task to a slow agent times out, and the
//                   orchestrator recovers by re-dispatching to a healthy one.
//
// Routing uses a fixed inbox topic so it runs against a stock local Apache Iggy.
// Presence advertisement is best-effort: it lights up the console's presence
// panel against the LaserData fork, and is a harmless no-op against stock Iggy
// (the registry, contracts, and workflow panels work on both).
//
//   cargo run --release --example orchestra

const EXAMPLE: &str = "orchestra";
const CLASSIFY: &str = "classify";
const DIAGNOSE: &str = "diagnose";
const REMEDIATE: &str = "remediate";
const SLOW_TASK: &str = "slow-task";
const INCIDENT: &str = "checkout API latency spike";
const ORCHESTRATOR: &str = "orchestrator";

// A capability agent: reads the task body (an AGDX command or a plain send),
// waits its handling delay (so the in-flight Working state is visible in the
// console), and replies with the work its skill produces.
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
        ctx.respond(Bytes::from(reply)).await
    }
}

// A one-skill capability card at the given health.
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

// Spawn one long-running capability agent on its OWN connection, so each agent
// is a distinct live presence in the console (presence is per connection). It
// advertises its card and inbox on start, and the handle owns the connection
// until the run ends.
async fn spawn_worker(
    stream: &str,
    id: &str,
    skill: &str,
    health: Option<Health>,
    delay: Duration,
) -> Result<AgentHandle, LaserError> {
    let connection = laser(stream, Capabilities::OPEN).await?;
    let mut handle = Agent::builder()
        .id(id.parse().expect("agent id is valid"))
        .listen_on(AgentTopic::Commands)
        .respond_on(AgentTopic::Responses)
        .capabilities(card(skill, health).capabilities)
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

// Print what to watch, then block on Enter so the operator can flip to the
// stream-ui console and observe the phase live. The read is async, so the spawned
// agents keep handling while the orchestrator waits.
async fn pause(prompt: &str) {
    println!("\n  >>> {prompt}\n      (watch stream-ui /orchestration, then press Enter)");
    let mut line = String::new();
    let _ = BufReader::new(tokio::io::stdin())
        .read_line(&mut line)
        .await;
}

// Scatter a diagnose panel and return the distinct findings.
async fn diagnose_panel(laser: &Laser) -> Result<Vec<Vec<u8>>, LaserError> {
    laser
        .scatter(
            ORCHESTRATOR.parse().expect("orchestrator id is valid"),
            &CapabilitySelector::new(DIAGNOSE, RoutePolicy::Any),
            INCIDENT.as_bytes(),
            &InboxRoute::Fixed(AgentTopic::Commands),
            Duration::from_secs(10),
        )
        .await
}

// Contract a task to one agent advertising `skill`, on a fixed inbox, with a
// deadline.
async fn contract_skill(
    laser: &Laser,
    skill: &str,
    deadline: Duration,
) -> Result<Contract, LaserError> {
    laser
        .contract(Router::to_capable(skill, RoutePolicy::Any))
        .from(ORCHESTRATOR.parse().expect("orchestrator id is valid"))
        .payload(Bytes::from_static(INCIDENT.as_bytes()))
        .inbox_route(InboxRoute::Fixed(AgentTopic::Commands))
        .deadline(deadline)
        .send()
        .await
}

#[tokio::main]
async fn main() -> Result<(), LaserError> {
    init_tracing();
    let stream = stream_for(EXAMPLE);
    let laser = laser(&stream, Capabilities::OPEN).await?;
    laser.bootstrap(PARTITIONS).await?;

    phase("Discovery: a pool of long-running capability agents connects");
    // Spawned once, kept alive for the whole run so the console stays populated.
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
    info!("six agents connected and advertised their capability cards");
    pause("DISCOVERY: six agents are live in the registry (one unavailable)").await;

    phase("Contract: a directed task to one capable agent, with a deadline");
    match laser
        .contract(Router::to_capable(CLASSIFY, RoutePolicy::Any))
        .from(ORCHESTRATOR.parse().expect("orchestrator id is valid"))
        .payload(Bytes::from_static(INCIDENT.as_bytes()))
        .inbox_route(InboxRoute::Fixed(AgentTopic::Commands))
        .deadline(Duration::from_secs(10))
        .send()
        .await?
    {
        Contract::Completed(reply) => {
            info!(result = %String::from_utf8_lossy(reply.body()), "classifier replied")
        }
        other => info!(?other, "the contract did not complete"),
    }
    pause("CONTRACT: a directed task completed (see it in the Contracts panel)").await;

    phase("Fan-out: a panel scattered to every capable agent");
    let findings = diagnose_panel(&laser).await?;
    info!(
        findings = findings.len(),
        "panel gathered findings (the unavailable agent was skipped)"
    );
    pause("FAN-OUT: two healthy diagnosers answered, the unavailable one was skipped").await;

    phase("Workflow: triage, then a diagnose panel, then remediate (journalled)");
    let run = laser
        .workflow("incident-response")
        .inbox_route(InboxRoute::Fixed(AgentTopic::Commands))
        .budget(
            Budget::unlimited()
                .invocations(8)
                .wall_clock(Duration::from_secs(60)),
        )
        .step(
            "triage",
            Router::to_capable(CLASSIFY, RoutePolicy::Any),
            move |_ctx: &StepContext<'_>| INCIDENT.as_bytes().to_vec(),
        )
        .step(
            "diagnose",
            Router::all_capable(DIAGNOSE, RoutePolicy::Any),
            |ctx: &StepContext<'_>| {
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
    info!(
        steps = run.outputs.len(),
        "workflow completed and journalled"
    );
    pause("WORKFLOW: the run journalled triage -> diagnose -> remediate (Workflow panel)").await;

    phase("Quarantine: an operator pulls a misbehaving agent");
    laser
        .quarantine(
            "operator".parse().expect("operator id is valid"),
            &"diag-alpha".parse().expect("agent id is valid"),
        )
        .await?;
    let after = diagnose_panel(&laser).await?;
    info!(
        findings = after.len(),
        "panel after quarantine (alpha routed around)"
    );
    pause("QUARANTINE: diag-alpha is quarantined in the registry, the panel routes around it")
        .await;

    phase("Recovery: the operator reinstates the agent");
    laser
        .unquarantine(
            "operator".parse().expect("operator id is valid"),
            &"diag-alpha".parse().expect("agent id is valid"),
        )
        .await?;
    let reinstated = diagnose_panel(&laser).await?;
    info!(
        findings = reinstated.len(),
        "panel after un-quarantine (alpha is back)"
    );
    pause("RECOVERY: diag-alpha is reinstated, the panel is whole again").await;

    phase("Expiry + recovery: a tight deadline times out, the orchestrator recovers");
    // The slow agent acks pickup but cannot finish inside the deadline, so the
    // contract times out, and the orchestrator recovers by re-dispatching the task
    // to a healthy fast agent.
    match contract_skill(&laser, SLOW_TASK, Duration::from_secs(1)).await? {
        Contract::Completed(reply) => {
            info!(result = %String::from_utf8_lossy(reply.body()), "unexpectedly fast")
        }
        other => {
            info!(?other, "the slow agent missed the deadline, recovering");
            if let Contract::Completed(reply) =
                contract_skill(&laser, REMEDIATE, Duration::from_secs(10)).await?
            {
                info!(result = %String::from_utf8_lossy(reply.body()), "recovered on a healthy agent");
            }
        }
    }
    pause("EXPIRY: the slow agent timed out, the task recovered on a healthy agent").await;

    println!(
        "\norchestra: discovery, routing, fan-out, a journalled workflow, health,\nreversible quarantine, and deadline recovery, all coordinated over the log."
    );

    for agent in agents {
        agent.shutdown().await?;
    }
    Ok(())
}

fn ms(value: u64) -> Duration {
    Duration::from_millis(value)
}

fn secs(value: u64) -> Duration {
    Duration::from_secs(value)
}
