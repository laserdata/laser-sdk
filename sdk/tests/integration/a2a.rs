use crate::harness;
use laser_sdk::prelude::full::*;
use std::sync::Arc;

struct Echo;

impl AgentHandler for Echo {
    async fn handle(&self, message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        // A worker behind the bridge speaks AGDX: it reads the decoded command
        // envelope and answers with an AGDX `response` echoing the correlation.
        let command = message
            .envelope
            .as_ref()
            .ok_or_else(|| LaserError::Handler("expected an AGDX command".to_owned()))?;
        let correlation = command
            .correlation
            .ok_or_else(|| LaserError::Handler("the command carries no correlation".to_owned()))?;
        let reply = format!("echo: {}", String::from_utf8_lossy(&command.body)).into_bytes();
        ctx.laser()
            .agdx(
                AgentTopic::Sessions,
                "a2a-worker"
                    .parse::<laser_sdk::types::AgentId>()
                    .expect("a2a-worker is a valid agent id"),
                command.conversation,
            )
            .respond(correlation, reply)
            .send()
            .await?;
        Ok(())
    }
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_message_send_when_the_agent_replies_then_tasks_get_should_complete() {
    let laser = harness::laser().await;
    // A worker behind the bridge: consumes the request topic, replies on responses.
    let _agent_lifetime_1 = Agent::builder()
        .id("a2a-worker"
            .parse()
            .expect("a2a-worker is a valid agent id"))
        .listen_on(AgentTopic::Sessions)
        .handler(Echo)
        .build()
        .spawn(laser.clone());

    let bridge = Arc::new(A2aBridge::new(
        laser.clone(),
        "a2a-bridge"
            .parse::<laser_sdk::types::AgentId>()
            .expect("a2a-bridge is a valid agent id"),
        AgentTopic::Sessions,
        AgentTopic::Sessions,
    ));

    // message/send creates a task in the Submitted state, tunneling the whole
    // A2A params object byte-identical in the AGDX command body.
    let params = br#"{"message":{"role":"user","parts":[{"kind":"text","text":"ping"}]}}"#.to_vec();
    let task = bridge.submit(params).await.expect("submit should succeed");
    assert_eq!(task.status.state, TaskState::Submitted);

    // tasks/get reaches Completed once the worker's AGDX reply lands on the log.
    let completed = harness::eventually(|| {
        let bridge = bridge.clone();
        let id = task.id.clone();
        async move {
            let task = bridge.task(&id).await.expect("task lookup should succeed");
            (task.status.state == TaskState::Completed).then_some(task)
        }
    })
    .await;

    assert_eq!(completed.artifacts.len(), 1);
    // The artifact is the worker's reply over the tunneled request (which still
    // carries the original "ping" text part).
    assert!(completed.artifacts[0].text.contains("echo:"));
    assert!(completed.artifacts[0].text.contains("ping"));
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_submitted_task_when_canceled_then_tasks_get_should_report_canceled() {
    let laser = harness::laser().await;
    let bridge = Arc::new(A2aBridge::new(
        laser.clone(),
        "a2a-bridge"
            .parse::<laser_sdk::types::AgentId>()
            .expect("a2a-bridge is a valid agent id"),
        AgentTopic::Sessions,
        AgentTopic::Sessions,
    ));

    // The card advertises the bridge's identity and A2A capabilities.
    let card = bridge.card();
    assert_eq!(card.name, "a2a-bridge");
    let interface = card
        .supported_interfaces
        .first()
        .expect("the card names its endpoint");
    assert_eq!(interface.protocol_version, "1.0");
    assert_eq!(interface.protocol_binding, "JSONRPC");
    assert!(card.capabilities.streaming);

    let params = br#"{"message":{"role":"user","parts":[{"kind":"text","text":"hi"}]}}"#.to_vec();
    let task = bridge.submit(params).await.expect("submit should succeed");

    // No worker replies, so cancelling writes the canceled terminal itself.
    let canceled = bridge
        .cancel(&task.id)
        .await
        .expect("cancel should succeed");
    assert_eq!(canceled.status.state, TaskState::Canceled);

    // tasks/get reads the canceled terminal back off the log.
    let got = harness::eventually(|| {
        let bridge = bridge.clone();
        let id = task.id.clone();
        async move {
            let task = bridge.task(&id).await.expect("task lookup should succeed");
            (task.status.state == TaskState::Canceled).then_some(task)
        }
    })
    .await;
    assert_eq!(got.status.state, TaskState::Canceled);
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_an_agent_and_bridge_on_a_custom_stream_when_used_then_should_run_on_that_stream() {
    let laser = harness::laser().await;
    // A second, independently-named stream on the SAME connection: the unit of
    // multi-stream topologies and per-stream Iggy RBAC. `with_default_stream` is a cheap
    // view (it shares the one connection), so a deployment scales to many
    // streams x topics by handing each agent/bridge the stream-scoped `Laser`.
    let default_stream = laser
        .default_stream()
        .expect("the test laser has a default stream");
    let scoped = laser.with_default_stream(format!("{default_stream}-scoped"));
    scoped
        .bootstrap(
            2,
            laser_sdk::agent::TopicRetention::expire_after(std::time::Duration::from_secs(86_400)),
        )
        .await
        .expect("the scoped stream bootstraps");

    let _agent_lifetime_2 = Agent::builder()
        .id("a2a-worker"
            .parse()
            .expect("a2a-worker is a valid agent id"))
        .listen_on(AgentTopic::Sessions)
        .handler(Echo)
        .build()
        .spawn(scoped.clone());

    let bridge = Arc::new(A2aBridge::new(
        scoped.clone(),
        "a2a-bridge"
            .parse::<laser_sdk::types::AgentId>()
            .expect("a2a-bridge is a valid agent id"),
        AgentTopic::Sessions,
        AgentTopic::Sessions,
    ));
    let params =
        br#"{"message":{"role":"user","parts":[{"kind":"text","text":"on the scoped stream"}]}}"#
            .to_vec();
    let task = bridge.submit(params).await.expect("submit should succeed");

    let completed = harness::eventually(|| {
        let bridge = bridge.clone();
        let id = task.id.clone();
        async move {
            let task = bridge.task(&id).await.expect("task lookup should succeed");
            (task.status.state == TaskState::Completed).then_some(task)
        }
    })
    .await;
    assert!(completed.artifacts[0].text.contains("echo:"));
}

// A worker that records each task it handles and answers it in its own name.
struct Named {
    name: &'static str,
    seen: Arc<std::sync::Mutex<Vec<(String, ConversationId)>>>,
}

impl AgentHandler for Named {
    async fn handle(&self, message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        let Some(command) = message.envelope.as_ref() else {
            return Ok(());
        };
        let Some(correlation) = command.correlation else {
            return Ok(());
        };
        if command.kind != laser_sdk::wire::agent::AgentKind::Command {
            return Ok(());
        }
        self.seen
            .lock()
            .expect("the seen list is not poisoned")
            .push((self.name.to_owned(), command.conversation.into()));
        ctx.laser()
            .agdx(
                AgentTopic::Sessions,
                self.name
                    .parse::<laser_sdk::types::AgentId>()
                    .expect("the worker name is a valid agent id"),
                command.conversation,
            )
            .respond(correlation, self.name.as_bytes().to_vec())
            .send()
            .await?;
        Ok(())
    }
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_two_workers_when_submitting_to_one_then_only_that_worker_should_handle_the_task() {
    let laser = harness::laser().await;
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let _workers: Vec<_> = ["alpha", "beta"]
        .into_iter()
        .map(|name| {
            Agent::builder()
                .id(name.parse().expect("the worker name is a valid agent id"))
                .listen_on(AgentTopic::Sessions)
                .handler(Named {
                    name,
                    seen: seen.clone(),
                })
                .build()
                .spawn(laser.clone())
        })
        .collect();
    let bridge = Arc::new(A2aBridge::new(
        laser.clone(),
        "a2a-bridge"
            .parse::<laser_sdk::types::AgentId>()
            .expect("a2a-bridge is a valid agent id"),
        AgentTopic::Sessions,
        AgentTopic::Sessions,
    ));
    let parent = ConversationId::new();
    let to_beta = bridge
        .submit_to(
            "beta"
                .parse::<laser_sdk::types::AgentId>()
                .expect("beta is a valid agent id"),
            br#"{"message":{"role":"user","text":"for beta"}}"#.to_vec(),
        )
        .await
        .expect("submit_to should succeed");
    let to_alpha = bridge
        .submit_in_to(
            "alpha"
                .parse::<laser_sdk::types::AgentId>()
                .expect("alpha is a valid agent id"),
            parent,
            parent,
            br#"{"message":{"role":"user","text":"for alpha"}}"#.to_vec(),
        )
        .await
        .expect("submit_in_to should succeed");
    for (task, worker) in [(&to_beta, "beta"), (&to_alpha, "alpha")] {
        let completed = harness::eventually(|| {
            let bridge = bridge.clone();
            let id = task.id.clone();
            async move {
                let task = bridge.task(&id).await.expect("task lookup should succeed");
                (task.status.state == TaskState::Completed).then_some(task)
            }
        })
        .await;
        assert_eq!(completed.artifacts[0].text, worker);
    }
    let seen = seen.lock().expect("the seen list is not poisoned").clone();
    let handled = |task: &Task| {
        seen.iter()
            .filter(|(_, conversation)| conversation.to_string() == task.id)
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>()
    };
    assert_eq!(handled(&to_beta), ["beta"]);
    assert_eq!(handled(&to_alpha), ["alpha"]);
}
