use crate::error::LaserError;
use crate::laser::Laser;
use laser_wire::agent_workflow::{
    AgentCancel, AgentList, AgentOutcome, AgentReply, AgentRunInfo, AgentStatusReq, AgentSubmit,
};
use laser_wire::codes::{
    AGDX_AGENT_CANCEL_CODE, AGDX_AGENT_LIST_CODE, AGDX_AGENT_STATUS_CODE, AGDX_AGENT_SUBMIT_CODE,
    AGENT_WORKFLOW_OP_VERSION,
};
use laser_wire::framing::encode_named;
use serde::Serialize;
use std::collections::BTreeMap;

impl Laser {
    /// The managed agent and workflow control surface: submit a run to an agent,
    /// cancel it, read its state, or list runs. Gated on the `agent_workflow`
    /// capability, so a plane that does not serve the band returns
    /// `LaserError::Unsupported`.
    pub fn agent_tasks(&self) -> AgentTasks<'_> {
        AgentTasks { laser: self }
    }
}

/// A handle to the managed agent and workflow control band. Build it with
/// [`Laser::agent_tasks`].
pub struct AgentTasks<'a> {
    laser: &'a Laser,
}

impl AgentTasks<'_> {
    async fn execute(
        &self,
        code: u32,
        request: &impl Serialize,
    ) -> Result<AgentOutcome, LaserError> {
        if !self.laser.capabilities().await.agent_workflow {
            return Err(LaserError::Unsupported(
                "the agent and workflow control band requires a plane that serves it \
                 (agent_workflow capability)"
                    .to_owned(),
            ));
        }
        let payload = bytes::Bytes::from(
            encode_named(request)
                .map_err(|error| LaserError::Codec(format!("encode request: {error}")))?,
        );
        let bytes = self.laser.send_raw_with_response(code, payload).await?;
        match crate::error::decode_managed_reply::<AgentReply>(&bytes)? {
            AgentReply::Ok(outcome) => Ok(outcome),
            AgentReply::Err(error) => Err(error.into()),
            _ => Err(LaserError::Protocol(
                "agent: unknown reply variant".to_owned(),
            )),
        }
    }

    /// Submit `input` to the agent `agent_id`, returning the run's metadata. The
    /// backend assigns the run id.
    pub async fn submit(
        &self,
        agent_id: impl Into<String>,
        input: impl AsRef<[u8]>,
    ) -> Result<AgentRunInfo, LaserError> {
        self.submit_with(agent_id, Some(input.as_ref().to_vec()), BTreeMap::new())
            .await
    }

    /// Submit with explicit `params` and optional `input`, for full control over
    /// the run request.
    pub async fn submit_with(
        &self,
        agent_id: impl Into<String>,
        input: Option<Vec<u8>>,
        params: BTreeMap<String, String>,
    ) -> Result<AgentRunInfo, LaserError> {
        let request = AgentSubmit {
            v: AGENT_WORKFLOW_OP_VERSION,
            agent_id: agent_id.into(),
            run_id: None,
            params,
            input,
        };
        match self.execute(AGDX_AGENT_SUBMIT_CODE, &request).await? {
            AgentOutcome::Submitted(info) => Ok(info),
            other => Err(unexpected("submit", &other)),
        }
    }

    /// Cancel `run_id`. Returns `true` when a live run was cancelled.
    pub async fn cancel(&self, run_id: impl Into<String>) -> Result<bool, LaserError> {
        let request = AgentCancel {
            v: AGENT_WORKFLOW_OP_VERSION,
            run_id: run_id.into(),
        };
        match self.execute(AGDX_AGENT_CANCEL_CODE, &request).await? {
            AgentOutcome::Cancelled(cancelled) => Ok(cancelled),
            other => Err(unexpected("cancel", &other)),
        }
    }

    /// Read the current state of `run_id`.
    pub async fn status(&self, run_id: impl Into<String>) -> Result<AgentRunInfo, LaserError> {
        let request = AgentStatusReq {
            v: AGENT_WORKFLOW_OP_VERSION,
            run_id: run_id.into(),
        };
        match self.execute(AGDX_AGENT_STATUS_CODE, &request).await? {
            AgentOutcome::Status(info) => Ok(info),
            other => Err(unexpected("status", &other)),
        }
    }

    /// List runs.
    pub async fn list(&self) -> Result<Vec<AgentRunInfo>, LaserError> {
        let request = AgentList {
            v: AGENT_WORKFLOW_OP_VERSION,
        };
        match self.execute(AGDX_AGENT_LIST_CODE, &request).await? {
            AgentOutcome::List(runs) => Ok(runs),
            other => Err(unexpected("list", &other)),
        }
    }
}

fn unexpected(op: &str, outcome: &AgentOutcome) -> LaserError {
    LaserError::Protocol(format!("agent {op}: unexpected outcome {outcome:?}"))
}
