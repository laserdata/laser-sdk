use crate::harness;
use async_trait::async_trait;
use laser_sdk::iggy::prelude::{HeaderKey, HeaderValue};
use laser_sdk::prelude::full::*;
use laser_sdk::sign::{KeyRecord, KeyRegistry, SigningKey};
use laser_sdk::wire::agent::{
    AgentDeadLetter, AgentEnvelope, AgentId as WireAgentId, ConversationId as WireConversationId,
    CorrelationId as WireCorrelationId, DeadLetterReason, RecordId as WireRecordId,
    SignatureContext,
};
use laser_sdk::wire::codes::AGENT_OP_VERSION;
use laser_sdk::wire::content::ContentType;
use laser_sdk::wire::framing::encode_named;
use laser_sdk::wire::headers::{AGENT_VERSION, CONTENT_TYPE};
use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

struct Capture {
    principals: Arc<Mutex<Vec<Option<String>>>>,
}

impl AgentHandler for Capture {
    async fn handle(&self, message: &AgentMessage, _ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        self.principals
            .lock()
            .expect("the lock should not be poisoned")
            .push(message.verified_principal.clone());
        Ok(())
    }
}

struct RecordingSink {
    capsules: Arc<Mutex<Vec<AgentDeadLetter>>>,
}

#[async_trait]
impl DeadLetterSink for RecordingSink {
    async fn on_dead_letter(
        &self,
        _message: Option<&AgentMessage>,
        capsule: &AgentDeadLetter,
        _publish_result: &Result<(), LaserError>,
    ) {
        self.capsules
            .lock()
            .expect("the lock should not be poisoned")
            .push(capsule.clone());
    }
}

struct CountingMiddleware {
    before: Arc<AtomicUsize>,
}

#[async_trait]
impl AgentMiddleware for CountingMiddleware {
    async fn before_handle(&self, _message: &AgentMessage) -> Result<(), LaserError> {
        self.before.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

fn now_micros() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("the clock is past the epoch")
            .as_micros(),
    )
    .expect("the epoch time fits in u64 micros")
}

// A signed command encoded exactly as the AGDX producer would publish it, so a
// test can vary the broker headers independently of the signed context.
fn signed_command(key: &SigningKey, conversation: u128, correlation: u128) -> Vec<u8> {
    let mut envelope = AgentEnvelope::command(
        WireRecordId::from_u128(correlation | 0x1000),
        WireConversationId::from_u128(conversation),
        "caller".parse::<WireAgentId>().expect("caller id is valid"),
        WireCorrelationId::from_u128(correlation),
        b"signed-command".to_vec(),
    );
    let signature = key
        .sign_with_context(
            &envelope,
            SignatureContext {
                content_type: Some(ContentType::Cbor.code()),
                agent_version: Some(AGENT_OP_VERSION),
            },
        )
        .expect("signing the command should succeed");
    envelope.signature = Some(signature);
    encode_named(&envelope).expect("the signed envelope should encode")
}

async fn publish_with_headers(
    laser: &Laser,
    bytes: Vec<u8>,
    content_type: Option<u8>,
    agent_version: Option<u32>,
) {
    let mut headers = BTreeMap::new();
    if let Some(version) = agent_version {
        headers.insert(
            HeaderKey::from_str(AGENT_VERSION).expect("the header key is valid"),
            HeaderValue::from(version),
        );
    }
    if let Some(code) = content_type {
        headers.insert(
            HeaderKey::from_str(CONTENT_TYPE).expect("the header key is valid"),
            HeaderValue::from(code),
        );
    }
    laser
        .topic(AgentTopic::Commands.topic_string())
        .send(bytes, headers, None)
        .await
        .expect("the record should publish");
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_verified_consumer_when_broker_headers_are_mutated_then_should_dead_letter_before_dispatch()
 {
    let laser = harness::laser().await;
    let caller_key = SigningKey::from_bytes(&[21u8; 32]);
    let mut registry = KeyRegistry::new();
    registry.enroll("caller", caller_key.verifying_key());

    let principals = Arc::new(Mutex::new(Vec::new()));
    let capsules = Arc::new(Mutex::new(Vec::new()));
    let middleware_calls = Arc::new(AtomicUsize::new(0));
    let mut worker = Agent::builder()
        .id("header-strict".parse().expect("the agent id is valid"))
        .listen_on(AgentTopic::Commands)
        .verifier(Arc::new(registry))
        .handler(Capture {
            principals: principals.clone(),
        })
        .middleware(vec![Arc::new(CountingMiddleware {
            before: middleware_calls.clone(),
        })])
        .on_dead_letter(Arc::new(RecordingSink {
            capsules: capsules.clone(),
        }))
        .build()
        .spawn(laser.clone());
    worker.ready().await.expect("the worker should be ready");

    // The signature binds `agdx.ct = cbor` and the current `agdx.av`. Republish
    // the same signed bytes under a flipped content type, a stripped content
    // type, and a flipped wire version: each must dead-letter before dispatch.
    let conversation = 0x0190_3c1f_aa00_0000_0000_0000_0000_0101u128;
    let bytes = signed_command(&caller_key, conversation, 0x0201);
    publish_with_headers(
        &laser,
        bytes.clone(),
        Some(ContentType::Json.code()),
        Some(AGENT_OP_VERSION),
    )
    .await;
    publish_with_headers(&laser, bytes.clone(), None, Some(AGENT_OP_VERSION)).await;
    publish_with_headers(
        &laser,
        bytes,
        Some(ContentType::Cbor.code()),
        Some(AGENT_OP_VERSION + 1),
    )
    .await;

    harness::eventually(|| {
        let capsules = capsules.clone();
        async move {
            (capsules
                .lock()
                .expect("the lock should not be poisoned")
                .len()
                == 3)
                .then_some(())
        }
    })
    .await;
    assert!(
        principals
            .lock()
            .expect("the lock should not be poisoned")
            .is_empty()
    );
    assert_eq!(middleware_calls.load(Ordering::SeqCst), 0);
    {
        let capsules = capsules.lock().expect("the lock should not be poisoned");
        let rejected = capsules
            .iter()
            .filter(|capsule| {
                capsule.reason == DeadLetterReason::Rejected
                    && capsule.detail.as_deref() == Some("signature verification failed")
            })
            .count();
        // The two header mutations fail context binding; the flipped wire
        // version fails decode before verification.
        assert_eq!(rejected, 2);
        assert_eq!(
            capsules
                .iter()
                .filter(|capsule| capsule.reason == DeadLetterReason::DecodeFailed)
                .count(),
            1
        );
    }

    // The untouched record still verifies: the observed headers match the
    // signed context and the handler sees the enrolled principal.
    let bytes = signed_command(&caller_key, conversation, 0x0202);
    publish_with_headers(
        &laser,
        bytes,
        Some(ContentType::Cbor.code()),
        Some(AGENT_OP_VERSION),
    )
    .await;
    let seen = harness::eventually(|| {
        let principals = principals.clone();
        async move {
            let items = principals
                .lock()
                .expect("the lock should not be poisoned")
                .clone();
            (!items.is_empty()).then_some(items)
        }
    })
    .await;
    assert_eq!(seen, vec![Some("caller".to_owned())]);
    worker.shutdown().await.expect("the worker should drain");
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_lifecycle_bound_keys_when_verified_at_the_broker_timestamp_then_should_gate_by_validity()
 {
    let laser = harness::laser().await;
    let now = now_micros();
    let hour = 3_600_000_000u64;

    let valid_key = SigningKey::from_bytes(&[31u8; 32]);
    let future_key = SigningKey::from_bytes(&[32u8; 32]);
    let expired_key = SigningKey::from_bytes(&[33u8; 32]);
    let revoked_key = SigningKey::from_bytes(&[34u8; 32]);
    let mut registry = KeyRegistry::new();
    registry.enroll_record(KeyRecord::agent("valid", valid_key.verifying_key()));
    registry.enroll_record(
        KeyRecord::agent("future", future_key.verifying_key()).valid_window(now + hour, None),
    );
    registry.enroll_record(
        KeyRecord::agent("expired", expired_key.verifying_key()).valid_window(0, Some(now - hour)),
    );
    registry.enroll_record(KeyRecord::agent("revoked", revoked_key.verifying_key()).revoked());

    let principals = Arc::new(Mutex::new(Vec::new()));
    let capsules = Arc::new(Mutex::new(Vec::new()));
    let mut worker = Agent::builder()
        .id("window-strict".parse().expect("the agent id is valid"))
        .listen_on(AgentTopic::Commands)
        .verifier(Arc::new(registry))
        .handler(Capture {
            principals: principals.clone(),
        })
        .on_dead_letter(Arc::new(RecordingSink {
            capsules: capsules.clone(),
        }))
        .build()
        .spawn(laser.clone());
    worker.ready().await.expect("the worker should be ready");

    // Each key signs an otherwise identical command. The broker stamps the
    // record timestamp on ingest, so only the key whose window covers that
    // stamp may pass; the future, expired, and revoked keys must dead-letter.
    let conversation = WireConversationId::from_u128(0x0190_3c1f_aa00_0000_0000_0000_0000_0102u128);
    for (index, (source, key)) in [
        ("future", &future_key),
        ("expired", &expired_key),
        ("revoked", &revoked_key),
        ("valid", &valid_key),
    ]
    .into_iter()
    .enumerate()
    {
        laser
            .agdx(
                AgentTopic::Commands,
                source.parse::<WireAgentId>().expect("the id is valid"),
                conversation,
            )
            .command(
                WireCorrelationId::from_u128(0x0300 + index as u128),
                b"lifecycle".to_vec(),
            )
            .signed_by(key)
            .send()
            .await
            .expect("the signed command should publish");
    }

    harness::eventually(|| {
        let capsules = capsules.clone();
        let principals = principals.clone();
        async move {
            let rejected = capsules
                .lock()
                .expect("the lock should not be poisoned")
                .len();
            let handled = principals
                .lock()
                .expect("the lock should not be poisoned")
                .len();
            (rejected == 3 && handled == 1).then_some(())
        }
    })
    .await;
    assert_eq!(
        principals
            .lock()
            .expect("the lock should not be poisoned")
            .clone(),
        vec![Some("valid".to_owned())]
    );
    assert!(
        capsules
            .lock()
            .expect("the lock should not be poisoned")
            .iter()
            .all(|capsule| capsule.reason == DeadLetterReason::Rejected)
    );
    worker.shutdown().await.expect("the worker should drain");
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_verifier_when_reply_hub_replies_are_forged_then_should_accept_only_the_signed_target()
 {
    let laser = harness::laser().await;
    let tool_key = SigningKey::from_bytes(&[41u8; 32]);
    let other_key = SigningKey::from_bytes(&[42u8; 32]);
    let mut registry = KeyRegistry::new();
    registry.enroll("tool", tool_key.verifying_key());
    registry.enroll("other", other_key.verifying_key());
    let caller = harness::verified(&laser, Arc::new(registry)).await;

    let conversation = ConversationId::new();
    let correlation = WireCorrelationId::from_u128(0x0190_3c1f_aa00_0000_0000_0000_0000_0401u128);
    let mut provenance = Provenance::builder()
        .conversation_id(conversation)
        .correlation_id(correlation.to_string())
        .build();
    Router::to("tool".parse().expect("tool is a valid agent id")).apply(&mut provenance);

    let requester = caller.clone();
    let pending = tokio::spawn(async move {
        requester
            .request(
                AgentTopic::ToolCalls,
                AgentTopic::ToolResults,
                b"work".to_vec(),
                &provenance,
                Duration::from_secs(10),
            )
            .await
    });
    // Let the reply hub subscribe before the forgeries land.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // A plain unsigned reply, an unsigned AGDX response, and a response signed
    // by a different enrolled key all echo the pending correlation. Under a
    // verifier bound to the target none may resolve the request.
    let mut forged = Provenance::builder()
        .conversation_id(conversation)
        .correlation_id(correlation.to_string())
        .build();
    Router::to("orchestrator".parse().expect("the id is valid")).apply(&mut forged);
    laser
        .send_agent(AgentTopic::ToolResults, b"forged-plain".to_vec(), &forged)
        .await
        .expect("the plain forgery should publish");
    laser
        .agdx(
            AgentTopic::ToolResults,
            "attacker".parse::<WireAgentId>().expect("the id is valid"),
            conversation.into(),
        )
        .respond(correlation, b"forged-unsigned".to_vec())
        .send()
        .await
        .expect("the unsigned forgery should publish");
    laser
        .agdx(
            AgentTopic::ToolResults,
            "other".parse::<WireAgentId>().expect("the id is valid"),
            conversation.into(),
        )
        .respond(correlation, b"forged-wrong-signer".to_vec())
        .signed_by(&other_key)
        .send()
        .await
        .expect("the wrong-signer forgery should publish");
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        !pending.is_finished(),
        "a forged reply resolved the request"
    );

    laser
        .agdx(
            AgentTopic::ToolResults,
            "tool".parse::<WireAgentId>().expect("the id is valid"),
            conversation.into(),
        )
        .respond(correlation, b"honest".to_vec())
        .signed_by(&tool_key)
        .send()
        .await
        .expect("the honest reply should publish");

    let reply = pending
        .await
        .expect("the request task should not panic")
        .expect("the signed reply should resolve the request");
    assert_eq!(reply.body(), b"honest");
    assert_eq!(reply.verified_principal.as_deref(), Some("tool"));
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_verifier_when_input_replies_are_forged_then_should_resume_only_on_the_signed_response()
 {
    let laser = harness::laser().await;
    let approver_key = SigningKey::from_bytes(&[51u8; 32]);
    let mut registry = KeyRegistry::new();
    registry.enroll("approver", approver_key.verifying_key());
    let caller = harness::verified(&laser, Arc::new(registry)).await;

    struct Approve {
        answer: &'static [u8],
    }
    impl AgentHandler for Approve {
        async fn handle(
            &self,
            _message: &AgentMessage,
            ctx: &AgentCtx<'_>,
        ) -> Result<(), LaserError> {
            ctx.respond_input(AgentTopic::Responses, self.answer.to_vec())
                .await
        }
    }

    // An unsigned approver answers every interrupt, but its response cannot
    // verify, so the paused caller must keep waiting and time out.
    let mut faker = Agent::builder()
        .id("faker".parse().expect("faker is a valid agent id"))
        .listen_on(AgentTopic::HumanInput)
        .handler(Approve { answer: b"forged" })
        .build()
        .spawn(laser.clone());
    faker.ready().await.expect("the faker should be ready");

    let orchestrator = caller.agdx(
        AgentTopic::HumanInput,
        "orchestrator"
            .parse::<WireAgentId>()
            .expect("orchestrator is a valid agent id"),
        WireConversationId::from(ConversationId::new()),
    );
    let result = orchestrator
        .request_input(
            AgentTopic::Responses,
            b"approve?".to_vec(),
            Duration::from_secs(2),
        )
        .await;
    assert!(
        matches!(result, Err(LaserError::Timeout(_))),
        "an unsigned forgery must not resume a verified interrupt, got {result:?}",
    );

    // A signing approver resumes the caller: `respond_input` signs with the
    // agent's key, so the verified reader accepts exactly this decision.
    let mut approver = Agent::builder()
        .id("approver".parse().expect("approver is a valid agent id"))
        .listen_on(AgentTopic::HumanInput)
        .signing_key(Arc::new(approver_key))
        .handler(Approve {
            answer: b"approved-signed",
        })
        .build()
        .spawn(laser.clone());
    approver
        .ready()
        .await
        .expect("the approver should be ready");

    let decision = orchestrator
        .request_input(
            AgentTopic::Responses,
            b"approve?".to_vec(),
            Duration::from_secs(10),
        )
        .await
        .expect("the signed response should resume the interrupt");
    assert_eq!(decision.as_slice(), b"approved-signed");

    faker.shutdown().await.expect("the faker should drain");
    approver
        .shutdown()
        .await
        .expect("the approver should drain");
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_an_expired_key_when_replaying_a_record_signed_in_its_window_then_should_accept_it() {
    let laser = harness::laser().await;
    let now = now_micros();
    let key = SigningKey::from_bytes(&[61u8; 32]);
    let mut registry = KeyRegistry::new();
    registry.enroll_record(
        KeyRecord::agent("historic", key.verifying_key())
            .valid_window(now.saturating_sub(3_600_000_000), Some(now + 2_000_000)),
    );

    // The record lands on the log while the key is valid. By the time the
    // consumer replays it the key has expired, so acceptance proves the
    // lifecycle check reads the broker-stamped record time, not the wall clock.
    laser
        .agdx(
            AgentTopic::Commands,
            "historic".parse::<WireAgentId>().expect("the id is valid"),
            WireConversationId::from_u128(0x0190_3c1f_aa00_0000_0000_0000_0000_0103u128),
        )
        .command(WireCorrelationId::from_u128(0x0601), b"replayed".to_vec())
        .signed_by(&key)
        .send()
        .await
        .expect("the signed command should publish");
    tokio::time::sleep(Duration::from_secs(3)).await;

    let principals = Arc::new(Mutex::new(Vec::new()));
    let mut worker = Agent::builder()
        .id("replayer".parse().expect("the agent id is valid"))
        .listen_on(AgentTopic::Commands)
        .verifier(Arc::new(registry))
        .handler(Capture {
            principals: principals.clone(),
        })
        .build()
        .spawn(laser.clone());
    worker.ready().await.expect("the worker should be ready");

    let seen = harness::eventually(|| {
        let principals = principals.clone();
        async move {
            let items = principals
                .lock()
                .expect("the lock should not be poisoned")
                .clone();
            (!items.is_empty()).then_some(items)
        }
    })
    .await;
    assert_eq!(seen, vec![Some("historic".to_owned())]);
    worker.shutdown().await.expect("the worker should drain");
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_a_verifying_registry_when_facts_arrive_then_should_fold_only_operator_signed_ones() {
    let laser = harness::laser().await;
    let operator_key = SigningKey::from_bytes(&[71u8; 32]);
    let agent_key = SigningKey::from_bytes(&[72u8; 32]);
    let mut registry = KeyRegistry::new();
    registry.enroll_operator("operator", operator_key.verifying_key());
    registry.enroll("agent-signer", agent_key.verifying_key());
    let caller = harness::verified(&laser, Arc::new(registry)).await;

    // Three quarantine facts: unsigned, signed by an agent-kind key, and
    // signed by the operator key. A verifying registry folds only the last.
    let operator = "operator".parse().expect("operator is a valid agent id");
    let unsigned_target: AgentId = "q-unsigned".parse().expect("the id is valid");
    let agent_kind_target: AgentId = "q-agent-kind".parse().expect("the id is valid");
    let operator_target: AgentId = "q-operator".parse().expect("the id is valid");
    laser
        .quarantine(operator, &unsigned_target)
        .await
        .expect("the unsigned fact publishes");
    laser
        .quarantine_signed(
            "operator".parse().expect("the id is valid"),
            &agent_kind_target,
            &agent_key,
        )
        .await
        .expect("the agent-kind fact publishes");
    laser
        .quarantine_signed(
            "operator".parse().expect("the id is valid"),
            &operator_target,
            &operator_key,
        )
        .await
        .expect("the operator fact publishes");

    let mut folded = caller
        .agent_registry()
        .expect("the verified caller builds a registry");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while !folded.is_quarantined(&operator_target) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the operator-signed fact should fold within 15s",
        );
        folded
            .refresh(now_micros())
            .await
            .expect("refreshing the registry should succeed");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(!folded.is_quarantined(&unsigned_target));
    assert!(!folded.is_quarantined(&agent_kind_target));
}
