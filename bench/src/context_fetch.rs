use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use laser_sdk::context::{Chain, ContextMessage, ContextPolicy, LastN, RoleFilter, TokenBudget};
use laser_sdk::laser::Laser;
use laser_sdk::provenance::{AgentTopic, Provenance};
use laser_sdk::types::{AgentId, ConversationId};
use serde::{Deserialize, Serialize};
use strum::{Display, EnumString, IntoStaticStr};

use crate::BenchError;
use crate::agdx::{
    AgdxArmEvidence, AgdxArmSummary, AgdxCase, measured_arm, record_payload, seeded_payload,
    validate_case, warmup,
};
use crate::engine::Operation;

#[derive(
    Clone, Copy, Debug, Deserialize, Display, EnumString, IntoStaticStr, Serialize, PartialEq, Eq,
)]
#[serde(rename_all = "snake_case")]
#[strum(
    serialize_all = "snake_case",
    parse_err_ty = BenchError,
    parse_err_fn = invalid_context_policy
)]
pub enum ContextPolicyKind {
    LastN,
    RoleFilter,
    TokenBudget,
}

fn invalid_context_policy(value: &str) -> BenchError {
    BenchError::Invalid(format!("unsupported context-fetch arm `{value}`"))
}

impl ContextPolicyKind {
    #[must_use]
    pub fn label(self) -> &'static str {
        self.into()
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct ContextFetchSummary {
    pub fetch: AgdxArmSummary,
    pub history_messages: u64,
    pub selected_messages: usize,
    pub context_limit: usize,
    pub policy: ContextPolicyKind,
    pub configuration: serde_json::Value,
}

pub struct ContextFetchEvidence {
    pub fetch: AgdxArmEvidence,
    pub history_messages: u64,
    pub selected_messages: usize,
    pub context_limit: usize,
    pub policy: ContextPolicyKind,
    pub configuration: serde_json::Value,
}

impl ContextFetchEvidence {
    #[must_use]
    pub fn summary(&self) -> ContextFetchSummary {
        ContextFetchSummary {
            fetch: self.fetch.summary.clone(),
            history_messages: self.history_messages,
            selected_messages: self.selected_messages,
            context_limit: self.context_limit,
            policy: self.policy,
            configuration: self.configuration.clone(),
        }
    }
}

/// Measure bounded context replay and policy selection without model execution.
///
/// # Errors
///
/// Returns an error for invalid dimensions, setup failure, workload failure, or selected-context mismatch.
pub async fn run_context_fetch_evidence(
    laser: &Laser,
    case: &AgdxCase,
    history_messages: u64,
    context_limit: usize,
    policy: ContextPolicyKind,
    seed: u64,
    monitored_processes: &[(String, u32)],
) -> Result<ContextFetchEvidence, BenchError> {
    validate_case(case)?;
    if history_messages == 0 || context_limit == 0 {
        return Err(BenchError::Invalid(
            "context-fetch requires nonzero history_messages and context_limit".to_owned(),
        ));
    }
    let stream = format!("bench-context-fetch-{seed:016x}");
    laser
        .stream(&stream)
        .topic(AgentTopic::Commands.topic_string())
        .ensure(case.partitions)
        .await
        .map_err(|error| sdk_error(&error))?;
    let scoped = laser.with_default_stream(&stream);
    let conversation = ConversationId::derive(&format!("laser-bench-context-{seed}"));
    let payload = seeded_payload(case.payload_bytes, seed);
    let role_a: AgentId = "laser-bench-role-a"
        .parse()
        .map_err(|error| BenchError::Invalid(format!("invalid context role: {error}")))?;
    let role_b: AgentId = "laser-bench-role-b"
        .parse()
        .map_err(|error| BenchError::Invalid(format!("invalid context role: {error}")))?;
    populate_history(
        &scoped,
        conversation,
        &payload,
        history_messages,
        &role_a,
        &role_b,
    )
    .await?;
    let timeout = Duration::from_millis(case.timeout_millis);
    let warmup_operation = fetch_operation(
        scoped.clone(),
        conversation,
        policy,
        context_limit,
        role_a.clone(),
    );
    warmup(case, timeout, warmup_operation).await?;
    let operation = fetch_operation(scoped.clone(), conversation, policy, context_limit, role_a);
    let mut fetch = measured_arm(
        &format!("context-fetch-{}", policy.label()),
        1,
        case,
        timeout,
        operation,
        monitored_processes,
    )
    .await?;
    let selected = scoped
        .context(conversation)
        .fetch_with(
            vec![AgentTopic::Commands],
            build_policy(
                policy,
                context_limit,
                "laser-bench-role-a".parse().map_err(|error| {
                    BenchError::Invalid(format!("invalid context role: {error}"))
                })?,
            ),
        )
        .await
        .map_err(|error| sdk_error(&error))?;
    let expected = expected_ids(history_messages, case.payload_bytes, context_limit, policy);
    apply_correctness(&mut fetch, &selected, &payload, &expected);
    Ok(ContextFetchEvidence {
        fetch,
        history_messages,
        selected_messages: selected.len(),
        context_limit,
        policy,
        configuration: serde_json::json!({
            "source": "conversation-log",
            "topics": [AgentTopic::Commands.topic_string()],
            "policy": policy.label(),
            "history_messages": history_messages,
            "context_limit": context_limit,
            "model_execution": false,
        }),
    })
}

async fn populate_history(
    laser: &Laser,
    conversation: ConversationId,
    payload: &Bytes,
    history_messages: u64,
    role_a: &AgentId,
    role_b: &AgentId,
) -> Result<(), BenchError> {
    for id in 0..history_messages {
        let mut provenance = Provenance::builder().conversation_id(conversation).build();
        provenance.agent = Some(if id % 2 == 0 {
            role_a.clone()
        } else {
            role_b.clone()
        });
        let body = record_payload(payload, id).map_err(BenchError::Invalid)?;
        laser
            .send_agent(AgentTopic::Commands, body, &provenance)
            .await
            .map_err(|error| sdk_error(&error))?;
    }
    Ok(())
}

fn fetch_operation(
    laser: Laser,
    conversation: ConversationId,
    policy: ContextPolicyKind,
    context_limit: usize,
    role: AgentId,
) -> Operation {
    Arc::new(move |_| {
        let laser = laser.clone();
        let role = role.clone();
        Box::pin(async move {
            laser
                .context(conversation)
                .fetch_with(
                    vec![AgentTopic::Commands],
                    build_policy(policy, context_limit, role),
                )
                .await
                .map(|_| ())
                .map_err(|error| error.to_string())
        })
    })
}

fn build_policy(
    policy: ContextPolicyKind,
    context_limit: usize,
    role: AgentId,
) -> Box<dyn ContextPolicy> {
    match policy {
        ContextPolicyKind::LastN => Box::new(LastN(context_limit)),
        ContextPolicyKind::RoleFilter => Box::new(Chain(vec![
            Box::new(RoleFilter(HashSet::from([role]))),
            Box::new(LastN(context_limit)),
        ])),
        ContextPolicyKind::TokenBudget => Box::new(TokenBudget::new(context_limit)),
    }
}

fn expected_ids(
    history_messages: u64,
    payload_bytes: usize,
    context_limit: usize,
    policy: ContextPolicyKind,
) -> Vec<u64> {
    let all = (0..history_messages).collect::<Vec<_>>();
    match policy {
        ContextPolicyKind::LastN => tail(&all, context_limit),
        ContextPolicyKind::RoleFilter => {
            let roles = all.into_iter().filter(|id| id % 2 == 0).collect::<Vec<_>>();
            tail(&roles, context_limit)
        }
        ContextPolicyKind::TokenBudget => {
            let tokens_per_message = payload_bytes.div_ceil(4);
            let count = context_limit.div_euclid(tokens_per_message).max(1);
            tail(&all, count)
        }
    }
}

fn tail(ids: &[u64], limit: usize) -> Vec<u64> {
    ids[ids.len().saturating_sub(limit)..].to_vec()
}

fn apply_correctness(
    fetch: &mut AgdxArmEvidence,
    selected: &[ContextMessage],
    payload: &Bytes,
    expected: &[u64],
) {
    let mut observed = Vec::with_capacity(selected.len());
    let mut checksum_failures = 0_u64;
    for message in selected {
        match body_id(&message.payload) {
            Some(id) => {
                observed.push(id);
                if record_payload(payload, id).map_or(true, |body| body != message.payload) {
                    checksum_failures = checksum_failures.saturating_add(1);
                }
            }
            None => checksum_failures = checksum_failures.saturating_add(1),
        }
    }
    let mismatches = observed
        .iter()
        .zip(expected)
        .filter(|(left, right)| left != right)
        .count();
    let size_gap = observed.len().abs_diff(expected.len());
    fetch.summary.outcomes.gaps = fetch
        .summary
        .outcomes
        .gaps
        .saturating_add(u64::try_from(mismatches.saturating_add(size_gap)).unwrap_or(u64::MAX));
    fetch.summary.outcomes.checksum_failures = fetch
        .summary
        .outcomes
        .checksum_failures
        .saturating_add(checksum_failures);
}

fn body_id(body: &[u8]) -> Option<u64> {
    let bytes: [u8; size_of::<u64>()] = body.get(..size_of::<u64>())?.try_into().ok()?;
    Some(u64::from_le_bytes(bytes))
}

fn sdk_error(error: &laser_sdk::LaserError) -> BenchError {
    BenchError::Invalid(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_role_filter_when_selecting_ids_then_should_bound_matching_tail() {
        assert_eq!(
            expected_ids(10, 16, 3, ContextPolicyKind::RoleFilter),
            vec![4, 6, 8]
        );
    }

    #[test]
    fn given_small_token_budget_when_selecting_ids_then_should_keep_latest_message() {
        assert_eq!(
            expected_ids(4, 128, 1, ContextPolicyKind::TokenBudget),
            vec![3]
        );
    }
}
