use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Submit a task to an agent or workflow (`AGDX_AGENT_SUBMIT`). `agent_id` names
/// the target, `run_id` lets the caller assign the run id (else the backend
/// mints one), `params` is scalar control, and `input` is the opaque task body.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AgentSubmit {
    pub v: u32,
    pub agent_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub params: BTreeMap<String, String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::encoding::opt_bin_bytes"
    )]
    pub input: Option<Vec<u8>>,
}

/// Cancel a run (`AGDX_AGENT_CANCEL`).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AgentCancel {
    pub v: u32,
    pub run_id: String,
}

/// Read a run's status (`AGDX_AGENT_STATUS`).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AgentStatusReq {
    pub v: u32,
    pub run_id: String,
}

/// List runs (`AGDX_AGENT_LIST`).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AgentList {
    pub v: u32,
}

/// A run's lifecycle state. A pinned snake-case vocabulary, additive.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum AgentRunState {
    #[default]
    Submitted,
    Running,
    Completed,
    Cancelled,
    Failed,
}

/// A run's metadata, returned by submit, status, and list.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRunInfo {
    pub run_id: String,
    pub agent_id: String,
    pub user_id: u32,
    pub state: AgentRunState,
    pub created_at_micros: u64,
}

/// The result of an agent or workflow control op: `Ok` with the outcome, or
/// `Err` with a failure.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub enum AgentReply {
    Ok(AgentOutcome),
    Err(AgentError),
}

/// The successful outcome of an agent or workflow command, shaped per op.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub enum AgentOutcome {
    Submitted(AgentRunInfo),
    Cancelled(bool),
    Status(AgentRunInfo),
    List(Vec<AgentRunInfo>),
}

/// Why an agent or workflow control op failed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[non_exhaustive]
pub enum AgentError {
    #[error("agent ops not supported: {0}")]
    Unsupported(String),
    #[error("run not found: {0}")]
    NotFound(String),
    #[error("invalid agent request: {0}")]
    Invalid(String),
    #[error("agent backend error: {0}")]
    Backend(String),
    #[error("unsupported agent op version (expected {expected}, got {got})")]
    Version { expected: u32, got: u32 },
}

#[cfg(all(test, feature = "cbor"))]
mod tests {
    use super::*;
    use crate::codes::AGENT_WORKFLOW_OP_VERSION;
    use crate::framing::{decode_named, encode_named};

    #[test]
    fn given_a_submit_when_round_tripped_then_should_decode_unchanged() {
        let submit = AgentSubmit {
            v: AGENT_WORKFLOW_OP_VERSION,
            agent_id: "diagnoser".to_owned(),
            run_id: Some("run-7".to_owned()),
            params: BTreeMap::from([("priority".to_owned(), "high".to_owned())]),
            input: Some(br#"{"incident":"INC-7"}"#.to_vec()),
        };
        let bytes = encode_named(&submit).expect("encodes");
        let back: AgentSubmit = decode_named(&bytes).expect("decodes");
        assert_eq!(back.agent_id, submit.agent_id);
        assert_eq!(back.run_id, submit.run_id);
        assert_eq!(back.input, submit.input);
    }

    #[test]
    fn given_a_reply_when_round_tripped_then_should_preserve_the_variant() {
        let reply = AgentReply::Ok(AgentOutcome::Status(AgentRunInfo {
            run_id: "run-7".to_owned(),
            agent_id: "diagnoser".to_owned(),
            user_id: 42,
            state: AgentRunState::Running,
            created_at_micros: 1_717_171_717_000_000,
        }));
        let bytes = encode_named(&reply).expect("encodes");
        let back: AgentReply = decode_named(&bytes).expect("decodes");
        assert!(matches!(
            back,
            AgentReply::Ok(AgentOutcome::Status(info)) if info.run_id == "run-7"
        ));
    }
}
