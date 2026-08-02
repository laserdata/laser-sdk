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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub budget: Option<RunBudget>,
}

/// A per-run resource ceiling carried on submit: caps across independent
/// dimensions, each optional (absent is unbounded on that dimension). A run that
/// crosses any cap is failed with a budget reason.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RunBudget {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_events: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_model_calls: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tool_calls: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_patches: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_depth: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_wall_clock_micros: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_cost_usd: Option<f64>,
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

/// List runs (`AGDX_AGENT_LIST`), filtered and paged. Every field but `v` is
/// optional: an empty request lists the caller's runs from the newest. `limit`
/// is clamped to [`MAX_PAGE_SIZE`](crate::limits::MAX_PAGE_SIZE) and `cursor`
/// is the opaque continuation from the previous page, the kv scan pattern.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct AgentList {
    pub v: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<AgentRunState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::encoding::opt_bin_bytes"
    )]
    pub cursor: Option<Vec<u8>>,
}

/// A run's lifecycle state. A pinned snake-case vocabulary, additive. The
/// strum derives keep the display, parse, and static-str spellings identical
/// to the serde one by construction (`ForkKind` and `ContentType` set the
/// same pattern).
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    strum::Display,
    strum::EnumString,
    strum::IntoStaticStr,
    strum::VariantArray,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
#[non_exhaustive]
pub enum AgentRunState {
    #[default]
    Submitted,
    Running,
    Completed,
    Cancelled,
    Failed,
}

impl AgentRunState {
    /// The pinned snake-case word, the same spelling serde uses, for query
    /// strings and display.
    pub fn as_str(self) -> &'static str {
        self.into()
    }

    /// Whether the run can never leave this state (the fold refuses to move a
    /// terminal run backward).
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            AgentRunState::Completed | AgentRunState::Cancelled | AgentRunState::Failed
        )
    }
}

/// A run's metadata, returned by submit, status, and list. `updated_at_micros`
/// is the time of the last state mark (equal to `created_at_micros` until one
/// lands), and `detail` is the terminal summary (an error message on `failed`,
/// absent when clean), so a console renders a run without a second read.
/// `cancel_requested` is the recorded cancel intent, not a state: the engine
/// observes it at its next step boundary and routes it into its own
/// cancellation path, and the state moves only when the engine reports it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRunInfo {
    pub run_id: String,
    pub agent_id: String,
    pub user_id: u32,
    pub state: AgentRunState,
    pub created_at_micros: u64,
    pub updated_at_micros: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub cancel_requested: bool,
}

/// One page of runs: the rows plus the opaque cursor for the next page, absent
/// on the last one.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RunPage {
    pub runs: Vec<AgentRunInfo>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::encoding::opt_bin_bytes"
    )]
    pub cursor: Option<Vec<u8>>,
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
    Cancelled(AgentRunInfo),
    Status(AgentRunInfo),
    List(RunPage),
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
    /// This plane does not own the mutation partition for the run.
    #[error("not the partition leader for this run")]
    NotLeader,
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
            budget: None,
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
            updated_at_micros: 1_717_171_718_000_000,
            detail: None,
            cancel_requested: false,
        }));
        let bytes = encode_named(&reply).expect("encodes");
        let back: AgentReply = decode_named(&bytes).expect("decodes");
        assert!(matches!(
            back,
            AgentReply::Ok(AgentOutcome::Status(info)) if info.run_id == "run-7"
        ));
    }

    #[test]
    fn given_a_filtered_list_when_round_tripped_then_should_keep_filters_and_cursor() {
        let list = AgentList {
            v: AGENT_WORKFLOW_OP_VERSION,
            agent_id: Some("diagnoser".to_owned()),
            state: Some(AgentRunState::Running),
            limit: Some(25),
            cursor: Some(vec![0x01, 0x02]),
        };
        let bytes = encode_named(&list).expect("encodes");
        let back: AgentList = decode_named(&bytes).expect("decodes");
        assert_eq!(back.agent_id, list.agent_id);
        assert_eq!(back.state, list.state);
        assert_eq!(back.limit, list.limit);
        assert_eq!(back.cursor, list.cursor);
    }

    #[test]
    fn given_run_state_words_when_parsed_then_should_round_trip_through_display() {
        for state in [
            AgentRunState::Submitted,
            AgentRunState::Running,
            AgentRunState::Completed,
            AgentRunState::Cancelled,
            AgentRunState::Failed,
        ] {
            let parsed: AgentRunState = state.as_str().parse().expect("pinned word parses");
            assert_eq!(parsed, state);
        }
        assert!("paused".parse::<AgentRunState>().is_err());
    }

    #[test]
    fn given_an_empty_list_request_when_encoded_then_should_skip_absent_fields() {
        let bare = AgentList {
            v: AGENT_WORKFLOW_OP_VERSION,
            ..AgentList::default()
        };
        let bytes = encode_named(&bare).expect("encodes");
        let back: AgentList = decode_named(&bytes).expect("decodes");
        assert!(back.agent_id.is_none());
        assert!(back.state.is_none());
        assert!(back.limit.is_none());
        assert!(back.cursor.is_none());
    }
}
