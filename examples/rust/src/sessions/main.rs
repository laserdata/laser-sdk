use laser_examples::{PARTITIONS, fresh_run, init_tracing, laser, phase, stream_for};
use laser_sdk::agent::{ModelRequest, ModelResponse, Session, TopicRetention};
use laser_sdk::prelude::full::*;
use laser_sdk::types::MintUlid;
use laser_sdk::wire::agent::{AgentEnvelope, LogPosition, RecordId};
use serde_json::json;
use std::time::Duration;

async fn acting_on_last(
    session: &Session,
    sessions: &laser_sdk::agent::Sessions,
    managed: bool,
) -> Result<Session, LaserError> {
    let turns = session.context().await?;
    let message = &turns
        .last()
        .ok_or_else(|| LaserError::Invalid("the recorded source is unavailable".to_owned()))?
        .message;
    let generation = if managed {
        sessions
            .sources(session.conversation())
            .await?
            .lane
            .map(|(_, generation, _)| generation)
    } else {
        None
    };
    Ok(session
        .clone()
        .acting_on(laser_sdk::wire::graph::SourceRef::Message {
            stream: message.stream_id,
            topic: message.topic_id,
            partition: message.id.partition_id,
            offset: message.id.offset,
            generation,
            conversation: Some(session.conversation().to_string()),
        }))
}

async fn task(
    sessions: &laser_sdk::agent::Sessions,
    root: &Session,
    label: &str,
    agent: &str,
    result: &str,
) -> Result<Session, LaserError> {
    let (child, lease) = sessions
        .create(label)
        .namespace(format!("ops/{}", root.conversation()))
        .agent(agent.parse::<laser_sdk::wire::agent::AgentId>()?)
        .parent(root.conversation(), root.conversation())
        .begin()
        .await?;
    let call = child
        .tool("inspect_service", json!({"service":"api", "task":label}))
        .await?;
    let correlation = call.correlation();
    let receipt = call.complete(result.as_bytes().to_vec()).await?;
    child.state().set("result", json!(result)).await?;
    let turns = child.context().await?;
    let turn = turns
        .iter()
        .find(|turn| {
            turn.message
                .envelope
                .as_ref()
                .is_some_and(|envelope| envelope.record == receipt.record)
        })
        .ok_or_else(|| {
            LaserError::Invalid("the recorded child result is unavailable".to_owned())
        })?;
    let at = LogPosition {
        stream_id: turn.message.stream_id,
        topic_id: turn.message.topic_id,
        partition_id: turn.message.id.partition_id,
        offset: turn.message.id.offset,
    };
    let reply = AgentEnvelope::event(
        RecordId::mint(),
        root.conversation().into(),
        "triage".parse()?,
        result.as_bytes().to_vec(),
    )
    .with_correlation(correlation)
    .with_cause(
        receipt.record.expect("a tool result has a record ID"),
        Some(at),
    );
    root.append(reply).await?;
    root.state().set(label, json!(result)).await?;
    child.end().await?;
    drop(lease);
    Ok(child)
}

async fn report(
    session: &Session,
    label: &str,
    sessions: &laser_sdk::agent::Sessions,
    managed: bool,
) -> Result<(), LaserError> {
    let events = session.context().await?.len();
    let links = if managed {
        sessions
            .links(session.conversation(), None)
            .await?
            .links
            .len()
    } else {
        0
    };
    println!("  {label}: {events} events, {links} resource links");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), LaserError> {
    init_tracing();
    let stream = stream_for("sessions");
    let laser = laser(&stream, Capabilities::OPEN).await?;
    fresh_run(&laser, &stream, async {
        let sessions = laser.sessions();
        let registered = sessions
            .bootstrap(
                PARTITIONS,
                TopicRetention::expire_after(Duration::from_secs(86_400)),
            )
            .await?
            .registered;
        let capabilities = laser.capabilities().await;
        if capabilities.sessions && !registered {
            return Err(LaserError::Invalid(
                "the session source registration is unavailable".to_owned(),
            ));
        }
        phase("one root incident, two child tasks, explicit result collection");
        let (root, root_lease) = sessions
            .create("incident-42")
            .namespace("ops")
            .agent("triage".parse::<laser_sdk::wire::agent::AgentId>()?)
            .begin()
            .await?;
        let observer = root
            .clone()
            .as_agent("specialist".parse::<laser_sdk::wire::agent::AgentId>()?);
        observer
            .tool("read_metrics", json!({"service":"api"}))
            .await?
            .complete(b"latency increased".to_vec())
            .await?;
        let diagnosis = task(
            &sessions,
            &root,
            "diagnosis",
            "specialist",
            "cache saturation",
        )
        .await?;
        let remediation = task(
            &sessions,
            &root,
            "remediation",
            "resolver",
            "reduce cache pressure",
        )
        .await?;
        let assembled = root.assemble(Box::new(LastN(20))).await?;
        let model = root
            .model(
                ModelRequest::new("mock", b"summarize the recorded findings".to_vec()),
                Some(&assembled),
            )
            .await?;
        model
            .complete(ModelResponse {
                body: b"reduce cache pressure and observe latency".to_vec(),
                model: None,
                finish_reason: None,
                usage: None,
                duration: None,
            })
            .await?;
        phase("a separate maintenance session shares resources in the same stream");
        let (maintenance, maintenance_lease) = sessions
            .create("maintenance-7")
            .namespace("ops")
            .agent("resolver".parse::<laser_sdk::wire::agent::AgentId>()?)
            .begin()
            .await?;
        maintenance
            .state()
            .set("task", json!("verify cache capacity"))
            .await?;
        let root_resources = acting_on_last(&root, &sessions, capabilities.sessions).await?;
        let maintenance_resources =
            acting_on_last(&maintenance, &sessions, capabilities.sessions).await?;
        if capabilities.kv.available {
            root.kv("infra")?
                .set("service:api")
                .json(&json!({"finding":"cache saturation"}))?
                .send()
                .await?;
            maintenance
                .kv("infra")?
                .set("maintenance:api")
                .json(&json!({"task":"verify cache capacity"}))?
                .send()
                .await?;
        }
        if capabilities.graph {
            root_resources
                .linked_graph("infra")?
                .link("service:api", "depends_on", "service:cache")
                .await?;
            maintenance_resources
                .linked_graph("infra")?
                .link("service:api", "observed_by", "agent:resolver")
                .await?;
        }
        root_resources
            .linked_memory()
            .remember("api latency increased when the cache saturated")
            .send()
            .await?;
        root.end().await?;
        maintenance.end().await?;
        drop(root_lease);
        drop(maintenance_lease);
        if capabilities.sessions {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
            loop {
                let root_done = sessions.get(root.conversation()).await.is_ok_and(|info| {
                    info.status == laser_sdk::wire::agent::SessionStatus::Completed
                });
                let memory_linked =
                    sessions
                        .links(root.conversation(), None)
                        .await
                        .is_ok_and(|view| {
                            view.links.iter().any(|link| {
                                link.surface == laser_sdk::wire::session::LinkSurface::Memory
                            })
                        });
                if root_done && memory_linked {
                    break;
                }
                if tokio::time::Instant::now() >= deadline {
                    return Err(LaserError::Timeout("the session example views"));
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        for (session, label) in [
            (&root, "incident-42"),
            (&diagnosis, "diagnosis"),
            (&remediation, "remediation"),
            (&maintenance, "maintenance-7"),
        ] {
            report(session, label, &sessions, capabilities.sessions).await?;
        }
        println!("  2 independent roots, 2 child sessions, explicit parent results");
        Ok(())
    })
    .await
}
