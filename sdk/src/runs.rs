use crate::error::LaserError;
use crate::laser::Laser;
use laser_wire::agent_workflow::{
    AgentCancel, AgentList, AgentOutcome, AgentReply, AgentRunInfo, AgentRunState, AgentStatusReq,
    AgentSubmit, RunBudget, RunPage,
};
use laser_wire::codes::{
    AGDX_AGENT_CANCEL_CODE, AGDX_AGENT_LIST_CODE, AGDX_AGENT_STATUS_CODE, AGDX_AGENT_SUBMIT_CODE,
    AGENT_WORKFLOW_OP_VERSION,
};
use laser_wire::control::{ControlCommand, SourceSelector};
use laser_wire::framing::encode_named;
use serde::Serialize;
use std::collections::BTreeMap;

impl Laser {
    /// The managed run registry: submit a run to an agent or workflow, cancel
    /// it, read its state, or list runs. Gated on the `agent_workflow`
    /// capability, so a plane that does not serve the band returns
    /// `LaserError::Unsupported`.
    pub fn runs(&self) -> Runs<'_> {
        Runs { laser: self }
    }
}

/// A handle to the managed run registry. Build it with [`Laser::runs`].
pub struct Runs<'a> {
    laser: &'a Laser,
}

impl<'a> Runs<'a> {
    /// Submit `input` to the agent `agent_id`, returning the run's metadata.
    /// The backend mints the run id by content-addressing the submit identity,
    /// so a retried submit converges on the same run.
    pub async fn submit(
        &self,
        agent_id: impl Into<String>,
        input: impl AsRef<[u8]>,
    ) -> Result<AgentRunInfo, LaserError> {
        self.submit_with(
            agent_id,
            None,
            Some(input.as_ref().to_vec()),
            BTreeMap::new(),
        )
        .await
    }

    /// Submit with a caller-assigned `run_id`, explicit `params`, and optional
    /// `input`, for full control over the run request. An absent `run_id` lets
    /// the backend mint one from the submit identity.
    pub async fn submit_with(
        &self,
        agent_id: impl Into<String>,
        run_id: Option<String>,
        input: Option<Vec<u8>>,
        params: BTreeMap<String, String>,
    ) -> Result<AgentRunInfo, LaserError> {
        let request = AgentSubmit {
            v: AGENT_WORKFLOW_OP_VERSION,
            agent_id: agent_id.into(),
            run_id,
            params,
            input,
            budget: None,
        };
        match self.execute(AGDX_AGENT_SUBMIT_CODE, &request).await? {
            AgentOutcome::Submitted(info) => Ok(info),
            other => Err(unexpected("submit", &other)),
        }
    }

    /// Submit with a multi-dimensional per-run [`RunBudget`]. A run that crosses
    /// any cap is failed by the run governor, surfaced as
    /// [`LaserError::BudgetExceeded`].
    pub async fn submit_budgeted(
        &self,
        agent_id: impl Into<String>,
        input: Option<Vec<u8>>,
        budget: RunBudget,
    ) -> Result<AgentRunInfo, LaserError> {
        let request = AgentSubmit {
            v: AGENT_WORKFLOW_OP_VERSION,
            agent_id: agent_id.into(),
            run_id: None,
            params: BTreeMap::new(),
            input,
            budget: Some(budget),
        };
        match self.execute(AGDX_AGENT_SUBMIT_CODE, &request).await? {
            AgentOutcome::Submitted(info) => Ok(info),
            other => Err(unexpected("submit", &other)),
        }
    }

    /// Record the cancel intent on `run_id` and return the run. The engine
    /// observes the intent at its next step boundary, so the state moves only
    /// when the engine reports it.
    pub async fn cancel(&self, run_id: impl Into<String>) -> Result<AgentRunInfo, LaserError> {
        let request = AgentCancel {
            v: AGENT_WORKFLOW_OP_VERSION,
            run_id: run_id.into(),
        };
        match self.execute(AGDX_AGENT_CANCEL_CODE, &request).await? {
            AgentOutcome::Cancelled(info) => Ok(info),
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

    /// List runs, newest first. Fluent filters narrow the page, `.fetch()`
    /// returns one [`RunPage`] whose `cursor` feeds the next call.
    pub fn list(&self) -> RunListRequest<'a> {
        RunListRequest {
            laser: self.laser,
            request: AgentList {
                v: AGENT_WORKFLOW_OP_VERSION,
                ..AgentList::default()
            },
        }
    }

    /// Register `stream/topic` as a run-status source: LaserData Cloud folds
    /// the run-tagged agent records published there into the run registry.
    /// A control command with 202-accepted semantics, idempotent by source.
    pub async fn register_source(
        &self,
        stream: impl Into<String>,
        topic: impl Into<String>,
    ) -> Result<(), LaserError> {
        self.laser
            .publish_control(ControlCommand::RegisterRunSource(SourceSelector::new(
                stream, topic,
            )))
            .await
    }

    /// Stop folding run-status records from `stream/topic`. Idempotent.
    pub async fn remove_source(
        &self,
        stream: impl Into<String>,
        topic: impl Into<String>,
    ) -> Result<(), LaserError> {
        self.laser
            .publish_control(ControlCommand::RemoveRunSource(SourceSelector::new(
                stream, topic,
            )))
            .await
    }

    async fn execute(
        &self,
        code: u32,
        request: &impl Serialize,
    ) -> Result<AgentOutcome, LaserError> {
        execute(self.laser, code, request).await
    }
}

/// A fluent run-list request. Build it with [`Runs::list`], finish with
/// [`fetch`](Self::fetch).
pub struct RunListRequest<'a> {
    laser: &'a Laser,
    request: AgentList,
}

impl RunListRequest<'_> {
    /// Keep only runs submitted to this agent.
    #[must_use]
    pub fn agent(mut self, agent_id: impl Into<String>) -> Self {
        self.request.agent_id = Some(agent_id.into());
        self
    }

    /// Keep only runs in this state.
    #[must_use]
    pub fn state(mut self, state: AgentRunState) -> Self {
        self.request.state = Some(state);
        self
    }

    /// Page size, clamped server-side to the wire page cap.
    #[must_use]
    pub fn limit(mut self, limit: u32) -> Self {
        self.request.limit = Some(limit);
        self
    }

    /// Resume from a previous page's opaque cursor.
    #[must_use]
    pub fn cursor(mut self, cursor: impl Into<Vec<u8>>) -> Self {
        self.request.cursor = Some(cursor.into());
        self
    }

    /// Run the request, returning one page.
    pub async fn fetch(self) -> Result<RunPage, LaserError> {
        match execute(self.laser, AGDX_AGENT_LIST_CODE, &self.request).await? {
            AgentOutcome::List(page) => Ok(page),
            other => Err(unexpected("list", &other)),
        }
    }
}

async fn execute(
    laser: &Laser,
    code: u32,
    request: &impl Serialize,
) -> Result<AgentOutcome, LaserError> {
    if !laser.capabilities().await.agent_workflow {
        return Err(LaserError::unsupported_feature(
            "runs",
            "agent_workflow",
            "the run registry is not served by this deployment",
        ));
    }
    let payload = encode_named(request)
        .map_err(|error| LaserError::Codec(format!("encode request: {error}")))?;
    let payload = laser.send_raw_with_response(code, payload).await?;
    match crate::error::decode_managed_reply::<AgentReply>(&payload)? {
        AgentReply::Ok(outcome) => Ok(outcome),
        AgentReply::Err(error) => Err(error.into()),
        _ => Err(LaserError::Protocol(
            "agent: unknown reply variant".to_owned(),
        )),
    }
}

fn unexpected(op: &str, outcome: &AgentOutcome) -> LaserError {
    LaserError::Protocol(format!("agent {op}: unexpected outcome {outcome:?}"))
}
