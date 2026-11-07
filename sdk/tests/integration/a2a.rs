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
    assert!(completed.artifacts[0].parts[0].text.contains("echo:"));
    assert!(completed.artifacts[0].parts[0].text.contains("ping"));
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_submitted_task_when_canceled_then_tasks_get_should_report_canceled() {
    let laser = harness::laser().await;
    let bridge = Arc::new(
        A2aBridge::new(
            laser.clone(),
            "a2a-bridge"
                .parse::<laser_sdk::types::AgentId>()
                .expect("a2a-bridge is a valid agent id"),
            AgentTopic::Sessions,
            AgentTopic::Sessions,
        )
        .with_public_url("https://agents.example.com/a2a")
        .expect("an absolute URL"),
    );

    // The card advertises the bridge's identity and A2A capabilities.
    let card = bridge.card().expect("the card builds");
    assert_eq!(card.name, "a2a-bridge");
    let interface = card
        .supported_interfaces
        .first()
        .expect("the card names its endpoint");
    assert_eq!(interface.protocol_version, "1.0");
    assert_eq!(interface.protocol_binding, "JSONRPC");
    assert!(!card.capabilities.streaming);
    assert_eq!(interface.url, "https://agents.example.com/a2a");

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
    assert!(completed.artifacts[0].parts[0].text.contains("echo:"));
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
        assert_eq!(completed.artifacts[0].parts[0].text, worker);
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

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_task_binding_when_another_caller_uses_the_task_id_then_should_refuse_it() {
    let laser = harness::laser().await;
    let bridge = A2aBridge::new(
        laser.clone(),
        "a2a-bound"
            .parse::<laser_sdk::types::AgentId>()
            .expect("a2a-bound is a valid agent id"),
        AgentTopic::Sessions,
        AgentTopic::Sessions,
    )
    .with_task_binding([3; 32], std::time::Duration::from_secs(60));
    let params = br#"{"message":{"role":"user","parts":[{"kind":"text","text":"hi"}]}}"#.to_vec();

    // A bound bridge refuses an anonymous submit before publishing anything.
    assert!(matches!(
        bridge.submit(params.clone()).await,
        Err(LaserError::HandlerConfig(_))
    ));
    let task = bridge
        .caller("alice")
        .submit(params)
        .await
        .expect("the caller submit should succeed");
    assert_eq!(task.id.split('.').count(), 3);

    for refused in [
        bridge.caller("bob").cancel(&task.id).await,
        bridge.caller("bob").task(&task.id).await,
        bridge.cancel(&task.id).await,
    ] {
        assert!(matches!(refused, Err(LaserError::HandlerConfig(_))));
    }
    let anonymous = bridge
        .handle_rpc(serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "CancelTask", "params": {"id": task.id}
        }))
        .await;
    assert!(anonymous.error.is_some());

    let canceled = bridge
        .caller("alice")
        .cancel(&task.id)
        .await
        .expect("the owner cancel should succeed");
    assert_eq!(canceled.status.state, TaskState::Canceled);
    let owned = bridge
        .caller("alice")
        .handle_rpc(serde_json::json!({
            "jsonrpc": "2.0", "id": 2, "method": "GetTask", "params": {"id": task.id}
        }))
        .await;
    assert!(owned.error.is_none());
}

// A dead-letter sink that counts the capsules a worker writes.
struct CountingSink {
    capsules: Arc<std::sync::atomic::AtomicUsize>,
}

#[async_trait::async_trait]
impl DeadLetterSink for CountingSink {
    async fn on_dead_letter(
        &self,
        _message: Option<&AgentMessage>,
        _capsule: &laser_sdk::wire::agent::AgentDeadLetter,
        _publish_result: &Result<(), LaserError>,
    ) {
        self.capsules
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_verified_worker_when_bridges_submit_then_should_complete_the_signed_task_and_dead_letter_the_unsigned_one()
 {
    let laser = harness::laser().await;
    let bridge_key = laser_sdk::sign::SigningKey::from_bytes(&[71u8; 32]);
    let registry = laser_sdk::sign::KeyRegistry::new();
    registry.enroll("a2a-signer", bridge_key.verifying_key());
    let capsules = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut worker = Agent::builder()
        .id("a2a-worker"
            .parse()
            .expect("a2a-worker is a valid agent id"))
        .listen_on(AgentTopic::Sessions)
        .verifier(Arc::new(registry))
        .handler(Echo)
        .on_dead_letter(Arc::new(CountingSink {
            capsules: capsules.clone(),
        }))
        .build()
        .spawn(laser.clone());
    worker.ready().await.expect("the worker is ready");
    let params =
        br#"{"message":{"messageId":"m-1","role":"ROLE_USER","parts":[{"text":"signed"}]}}"#;

    // Without a key the worker refuses the command before its handler runs.
    let unsigned = A2aBridge::new(
        laser.clone(),
        "a2a-signer"
            .parse::<laser_sdk::types::AgentId>()
            .expect("a2a-signer is a valid agent id"),
        AgentTopic::Sessions,
        AgentTopic::Sessions,
    )
    .with_target(
        "a2a-worker"
            .parse::<laser_sdk::types::AgentId>()
            .expect("a2a-worker is a valid agent id"),
    );
    let refused = unsigned
        .submit(params.to_vec())
        .await
        .expect("the unsigned submit publishes");
    harness::eventually(|| {
        let capsules = capsules.clone();
        async move { (capsules.load(std::sync::atomic::Ordering::SeqCst) == 1).then_some(()) }
    })
    .await;
    assert_eq!(
        unsigned
            .task(&refused.id)
            .await
            .expect("the task lookup succeeds")
            .status
            .state,
        TaskState::Working
    );

    // With the enrolled key the same worker handles the task and answers it.
    let signed = Arc::new(
        A2aBridge::new(
            laser.clone(),
            "a2a-signer"
                .parse::<laser_sdk::types::AgentId>()
                .expect("a2a-signer is a valid agent id"),
            AgentTopic::Sessions,
            AgentTopic::Sessions,
        )
        .with_signing_key(bridge_key)
        .with_target(
            "a2a-worker"
                .parse::<laser_sdk::types::AgentId>()
                .expect("a2a-worker is a valid agent id"),
        ),
    );
    let task = signed
        .submit(params.to_vec())
        .await
        .expect("the signed submit publishes");
    let completed = harness::eventually(|| {
        let bridge = signed.clone();
        let id = task.id.clone();
        async move {
            let task = bridge.task(&id).await.expect("task lookup should succeed");
            (task.status.state == TaskState::Completed).then_some(task)
        }
    })
    .await;
    assert!(completed.artifacts[0].parts[0].text.contains("signed"));
    assert_eq!(capsules.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_targeted_task_under_a_verifier_when_another_signer_answers_then_should_settle_only_on_the_target()
 {
    let laser = harness::laser().await;
    let beta_key = laser_sdk::sign::SigningKey::from_bytes(&[72u8; 32]);
    let other_key = laser_sdk::sign::SigningKey::from_bytes(&[73u8; 32]);
    let registry = laser_sdk::sign::KeyRegistry::new();
    registry.enroll("beta", beta_key.verifying_key());
    registry.enroll("other", other_key.verifying_key());
    let verified = harness::verified(&laser, Arc::new(registry)).await;
    let bridge = A2aBridge::new(
        verified.clone(),
        "a2a-bridge"
            .parse::<laser_sdk::types::AgentId>()
            .expect("a2a-bridge is a valid agent id"),
        AgentTopic::Sessions,
        AgentTopic::Sessions,
    );
    let task = bridge
        .submit_to(
            "beta"
                .parse::<laser_sdk::types::AgentId>()
                .expect("beta is a valid agent id"),
            br#"{"message":{"messageId":"m-1","role":"ROLE_USER","parts":[]}}"#.to_vec(),
        )
        .await
        .expect("the submit publishes");
    let conversation: ConversationId = task.id.parse().expect("the task id is a conversation");
    let correlation = laser_sdk::wire::agent::CorrelationId::from_u128(conversation.as_u128());
    let answer = |signer: &'static str, key: &laser_sdk::sign::SigningKey, body: &'static str| {
        let laser = laser.clone();
        let key = key.clone();
        async move {
            laser
                .agdx(
                    AgentTopic::Sessions,
                    signer
                        .parse::<laser_sdk::types::AgentId>()
                        .expect("the signer is a valid agent id"),
                    conversation.into(),
                )
                .respond(correlation, body.as_bytes().to_vec())
                .signed_by(&key)
                .send()
                .await
                .expect("the answer publishes");
        }
    };
    answer("other", &other_key, "forged").await;
    assert_eq!(
        bridge
            .task(&task.id)
            .await
            .expect("the lookup succeeds")
            .status
            .state,
        TaskState::Working,
        "an answer signed by another enrolled key does not settle the task"
    );
    answer("beta", &beta_key, "honest").await;
    let settled = bridge.task(&task.id).await.expect("the lookup succeeds");
    assert_eq!(settled.status.state, TaskState::Completed);
    assert_eq!(settled.artifacts[0].parts[0].text, "honest");
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_an_input_required_task_when_a_message_names_it_then_should_continue_it_in_its_conversation()
 {
    let laser = harness::laser().await;
    let bridge = A2aBridge::new(
        laser.clone(),
        "a2a-bridge"
            .parse::<laser_sdk::types::AgentId>()
            .expect("a2a-bridge is a valid agent id"),
        AgentTopic::Sessions,
        AgentTopic::Sessions,
    );
    let task = bridge
        .submit_to(
            "beta"
                .parse::<laser_sdk::types::AgentId>()
                .expect("beta is a valid agent id"),
            br#"{"message":{"messageId":"m-1","role":"ROLE_USER","contextId":"trip-7","parts":[]}}"#
                .to_vec(),
        )
        .await
        .expect("the submit publishes");
    assert_eq!(task.context_id, "trip-7");
    let conversation: ConversationId = task.id.parse().expect("the task id is a conversation");
    let correlation = laser_sdk::wire::agent::CorrelationId::from_u128(conversation.as_u128());
    let answer = |state: TaskState, body: &'static str| {
        let laser = laser.clone();
        async move {
            laser
                .agdx(
                    AgentTopic::Sessions,
                    "beta"
                        .parse::<laser_sdk::types::AgentId>()
                        .expect("beta is a valid agent id"),
                    conversation.into(),
                )
                .respond(correlation, body.as_bytes().to_vec())
                .with_task_state(state)
                .send()
                .await
                .expect("the answer publishes");
        }
    };
    answer(TaskState::InputRequired, "which city").await;
    let continue_task = || {
        serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": "SendMessage", "params": {
            "message": {"messageId": "m-2", "role": "ROLE_USER", "taskId": task.id, "parts": []}
        }})
    };
    let continued = bridge.handle_rpc(continue_task()).await;
    let result = continued.result.expect("the message continues the task");
    assert_eq!(result["task"]["id"], task.id);
    assert_eq!(result["task"]["contextId"], "trip-7");
    assert_eq!(result["task"]["status"]["state"], "TASK_STATE_WORKING");
    let commands: Vec<_> = laser
        .context(conversation)
        .fetch(vec![AgentTopic::Sessions], 10, None)
        .await
        .expect("the conversation reads")
        .into_iter()
        .filter_map(|message| message.envelope)
        .filter(|envelope| envelope.kind == laser_sdk::wire::agent::AgentKind::Command)
        .collect();
    assert_eq!(
        commands.len(),
        2,
        "the follow-up rides the task's conversation"
    );
    assert!(commands.iter().all(|command| {
        command.correlation == Some(correlation)
            && command.target.as_ref().map(|target| target.as_str()) == Some("beta")
    }));
    answer(TaskState::Completed, "booked").await;
    let settled = bridge.task(&task.id).await.expect("the lookup succeeds");
    assert_eq!(settled.status.state, TaskState::Completed);
    assert_eq!(settled.artifacts[0].parts[0].text, "booked");
    let refused = bridge.handle_rpc(continue_task()).await;
    assert_eq!(refused.error.map(|error| error.code), Some(-32004));
}
