use crate::agent::{Session, Sessions};
use crate::context::LastN;
use crate::error::LaserError;
use crate::provenance::AgentTopic;
use crate::types::ConversationId;
use laser_wire::agent::{
    AgentEnvelope, AgentErrorBody, AgentErrorCode, AgentKind, Budget, OPERATION_SESSION,
    SessionEnd, SessionStart, SessionStatus, TaskState,
};
use laser_wire::framing::decode_named;
use laser_wire::query::Value;
use laser_wire::session::{SessionError, SessionInfo};
use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use tracing::{debug, warn};

/// The `SessionEnd.reason` of a session ended failed for passing its budget.
pub(crate) const BUDGET_END_REASON: &str = "budget";

impl Session {
    /// Whether this session's usage has passed its budget: the summed input
    /// and output tokens of its records over the token ceiling, or their
    /// summed cost over the cost ceiling. A deployment that indexes sessions
    /// answers from its index. On open Apache Iggy, or for a session the
    /// index does not know yet, the retained session lane is folded: the
    /// budget of the first start record against the `usage` of every record.
    /// A session without a budget is never over it. The answer is eventually
    /// consistent, so a budget is a cooperative limit, not a hard spending
    /// cap.
    pub async fn over_budget(&self) -> Result<bool, LaserError> {
        Ok(self.budget_breach().await?.is_some())
    }

    // How this session passed its budget, `None` while it is within it.
    pub(crate) async fn budget_breach(&self) -> Result<Option<BudgetBreach>, LaserError> {
        if self.laser.capabilities().await.sessions {
            match self.status().await {
                Ok(info) => return Ok(BudgetBreach::of_info(&info)),
                Err(LaserError::Session(
                    SessionError::NotFound(_) | SessionError::NotRegistered(_),
                )) => {}
                Err(error) => return Err(error),
            }
        }
        self.lane_budget_breach().await
    }

    // End the session failed with reason `budget`. The terminal latch makes
    // a repeated call on this handle or its clones write the same record.
    pub(crate) async fn fail_over_budget(&self, breach: &BudgetBreach) -> Result<(), LaserError> {
        self.terminate(
            TaskState::Failed,
            SessionEnd {
                reason: Some(BUDGET_END_REASON.to_owned()),
                error: Some(breach.error_body()),
            },
        )
        .await
    }

    async fn lane_budget_breach(&self) -> Result<Option<BudgetBreach>, LaserError> {
        let records = self
            .scope()
            .fetch_with(vec![AgentTopic::Sessions], Box::new(LastN(usize::MAX)))
            .await?;
        let mut usage = LaneUsage::default();
        for envelope in records.iter().filter_map(|record| record.envelope.as_ref()) {
            usage.add(envelope);
        }
        Ok(BudgetBreach::of(
            usage.budget,
            usage.tokens,
            usage.cost_micros,
        ))
    }
}

/// Which ceiling a session passed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BudgetDimension {
    Tokens,
    CostMicros,
}

/// How a session passed its budget.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct BudgetBreach {
    pub(crate) dimension: Option<BudgetDimension>,
    pub(crate) ceiling: u64,
    pub(crate) spent: u64,
}

impl BudgetBreach {
    /// A breach known only by its numbers, such as a workflow's own ceiling.
    pub(crate) fn exceeded(ceiling: u64, spent: u64) -> Self {
        Self {
            dimension: None,
            ceiling,
            spent,
        }
    }

    /// The error a caller stopped by this breach returns.
    pub(crate) fn into_error(self) -> LaserError {
        LaserError::BudgetExceeded {
            ceiling: self.ceiling,
            spent: self.spent,
        }
    }

    // The token ceiling is checked first, then the cost ceiling, the rule
    // the session index applies.
    fn of(budget: Option<Budget>, tokens: u64, cost_micros: u64) -> Option<Self> {
        let budget = budget?;
        if let Some(ceiling) = budget.tokens
            && tokens > ceiling
        {
            return Some(Self {
                dimension: Some(BudgetDimension::Tokens),
                ceiling,
                spent: tokens,
            });
        }
        if let Some(ceiling) = budget.cost_micros
            && cost_micros > ceiling
        {
            return Some(Self {
                dimension: Some(BudgetDimension::CostMicros),
                ceiling,
                spent: cost_micros,
            });
        }
        None
    }

    // The index's flag decides. Its counters name the ceiling that was
    // passed.
    fn of_info(info: &SessionInfo) -> Option<Self> {
        if !info.over_budget {
            return None;
        }
        let tokens = info.tokens_in.saturating_add(info.tokens_out);
        Some(
            Self::of(info.budget, tokens, info.cost_micros)
                .unwrap_or_else(|| Self::exceeded(0, tokens)),
        )
    }

    fn error_body(&self) -> AgentErrorBody {
        let message = match self.dimension {
            Some(BudgetDimension::Tokens) => format!(
                "the session went over its token budget: {} of {} tokens",
                self.spent, self.ceiling
            ),
            Some(BudgetDimension::CostMicros) => format!(
                "the session went over its cost budget: {} of {} micro-units",
                self.spent, self.ceiling
            ),
            None => format!(
                "the session went over its budget: spent {} of ceiling {}",
                self.spent, self.ceiling
            ),
        };
        let mut detail = BTreeMap::from([
            ("ceiling".to_owned(), Value::Uint(self.ceiling)),
            ("spent".to_owned(), Value::Uint(self.spent)),
        ]);
        if let Some(dimension) = self.dimension {
            let name = match dimension {
                BudgetDimension::Tokens => "tokens",
                BudgetDimension::CostMicros => "cost_micros",
            };
            detail.insert("budget".to_owned(), Value::Str(name.to_owned()));
        }
        AgentErrorBody {
            code: AgentErrorCode::Internal,
            message: Some(message),
            retryable: false,
            detail: Some(detail),
        }
    }
}

/// The runtime's budget check before a work record reaches the handler. It
/// reads the session index only, one read per session per poll batch, so a
/// deployment without the index never enforces in the runtime.
#[derive(Default)]
pub(crate) struct BudgetGate {
    // Per partition, the head offset of the poll the verdicts came from and
    // each session's verdict. Records of one poll share their partition's
    // head offset, so a later poll that moved the head reads again.
    batches: Mutex<HashMap<u32, Batch>>,
    // The handles of sessions this runtime fails for their budget, kept
    // until the index shows them ended, so a retried failure repeats the
    // same terminal record and a confirmed one is not written again.
    ending: Mutex<HashMap<ConversationId, Ending>>,
}

impl BudgetGate {
    /// Whether a work record of `session`, delivered from `partition` by a
    /// poll that saw `head`, may reach the handler. A session over its
    /// budget is ended failed with reason `budget` through `lens`, once, and
    /// its record is not handled. A failed index read or terminal write is
    /// logged: the read lets the record through, the write is retried by
    /// the session's next record.
    pub(crate) async fn admit(
        &self,
        sessions: &Sessions,
        session: ConversationId,
        partition: u32,
        head: u64,
        lens: impl FnOnce() -> Session,
    ) -> bool {
        if !sessions.laser().capabilities().await.sessions {
            return true;
        }
        let verdict = match self.cached(partition, head, session) {
            Some(verdict) => verdict,
            None => {
                let verdict = match sessions.get(session).await {
                    Ok(info) => Verdict {
                        breach: BudgetBreach::of_info(&info),
                        ended: matches!(
                            info.status,
                            SessionStatus::Completed
                                | SessionStatus::Failed
                                | SessionStatus::Canceled
                        ),
                    },
                    Err(LaserError::Session(
                        SessionError::NotFound(_) | SessionError::NotRegistered(_),
                    )) => Verdict {
                        breach: None,
                        ended: false,
                    },
                    Err(error) => {
                        warn!(%error, %session, "reading the session budget failed, handling the record");
                        return true;
                    }
                };
                self.store(partition, head, session, verdict);
                verdict
            }
        };
        let Some(breach) = verdict.breach else {
            return true;
        };
        if verdict.ended {
            lock(&self.ending).remove(&session);
            debug!(%session, "skipping work for an ended session over its budget");
            return false;
        }
        let pending = {
            let mut ending = lock(&self.ending);
            match ending.get(&session) {
                Some(entry) if entry.written => None,
                Some(entry) => Some(entry.lens.clone()),
                None => {
                    let lens = lens();
                    ending.insert(
                        session,
                        Ending {
                            lens: lens.clone(),
                            written: false,
                        },
                    );
                    Some(lens)
                }
            }
        };
        if let Some(lens) = pending {
            match lens.fail_over_budget(&breach).await {
                Ok(()) => {
                    if let Some(entry) = lock(&self.ending).get_mut(&session) {
                        entry.written = true;
                    }
                    debug!(%session, "ended a session over its budget");
                }
                Err(error) => {
                    warn!(%error, %session, "failed to end a session over its budget, its next record retries");
                }
            }
        }
        false
    }

    fn cached(&self, partition: u32, head: u64, session: ConversationId) -> Option<Verdict> {
        let batches = lock(&self.batches);
        let batch = batches.get(&partition)?;
        if batch.head != head {
            return None;
        }
        batch.verdicts.get(&session).copied()
    }

    fn store(&self, partition: u32, head: u64, session: ConversationId, verdict: Verdict) {
        let mut batches = lock(&self.batches);
        let batch = batches.entry(partition).or_insert_with(|| Batch {
            head,
            verdicts: HashMap::new(),
        });
        if batch.head != head {
            *batch = Batch {
                head,
                verdicts: HashMap::new(),
            };
        }
        batch.verdicts.insert(session, verdict);
    }
}

struct Batch {
    head: u64,
    verdicts: HashMap<ConversationId, Verdict>,
}

#[derive(Clone, Copy)]
struct Verdict {
    breach: Option<BudgetBreach>,
    ended: bool,
}

struct Ending {
    lens: Session,
    written: bool,
}

// The budget of the first start record on the lane and the usage summed
// over every record, as the session index folds them.
#[derive(Default)]
struct LaneUsage {
    started: bool,
    budget: Option<Budget>,
    tokens: u64,
    cost_micros: u64,
}

impl LaneUsage {
    fn add(&mut self, envelope: &AgentEnvelope) {
        if let Some(usage) = envelope.usage {
            self.tokens = self
                .tokens
                .saturating_add(usage.input_tokens)
                .saturating_add(usage.output_tokens);
            self.cost_micros = self
                .cost_micros
                .saturating_add(usage.cost_micros.unwrap_or(0));
        }
        if !self.started
            && envelope.kind == AgentKind::Status
            && envelope.operation.as_deref() == Some(OPERATION_SESSION)
            && let Ok(start) = decode_named::<SessionStart>(&envelope.body)
        {
            self.started = true;
            self.budget = start.budget;
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn budget(tokens: Option<u64>, cost_micros: Option<u64>) -> Option<Budget> {
        Some(Budget {
            tokens,
            cost_micros,
        })
    }

    #[test]
    fn given_no_budget_when_usage_grows_then_should_never_breach() {
        assert_eq!(BudgetBreach::of(None, u64::MAX, u64::MAX), None);
        assert_eq!(BudgetBreach::of(budget(None, None), 10, 10), None);
    }

    #[test]
    fn given_a_token_ceiling_when_usage_reaches_it_then_should_breach_only_past_it() {
        assert_eq!(BudgetBreach::of(budget(Some(100), None), 100, 0), None);
        assert_eq!(
            BudgetBreach::of(budget(Some(100), None), 101, 0),
            Some(BudgetBreach {
                dimension: Some(BudgetDimension::Tokens),
                ceiling: 100,
                spent: 101,
            })
        );
    }

    #[test]
    fn given_a_cost_ceiling_when_cost_passes_it_then_should_name_the_cost() {
        assert_eq!(
            BudgetBreach::of(budget(Some(1_000), Some(50)), 10, 51),
            Some(BudgetBreach {
                dimension: Some(BudgetDimension::CostMicros),
                ceiling: 50,
                spent: 51,
            })
        );
    }

    #[test]
    fn given_a_breach_when_described_then_should_name_the_budget() {
        let body = BudgetBreach::of(budget(Some(5), None), 9, 0)
            .expect("over the token ceiling")
            .error_body();
        assert!(
            body.message
                .as_deref()
                .is_some_and(|message| message.contains("token budget"))
        );
        let detail = body.detail.expect("the breach carries its numbers");
        assert_eq!(detail.get("budget"), Some(&Value::Str("tokens".to_owned())));
        assert_eq!(detail.get("ceiling"), Some(&Value::Uint(5)));
        assert_eq!(detail.get("spent"), Some(&Value::Uint(9)));
        assert!(!body.retryable);
    }
}
