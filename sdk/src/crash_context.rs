use crate::context::ContextMessage;
use crate::govern::PolicyEvidence;
use laser_wire::agent::AgentDeadLetter;

const MAX_PREVIEW_CHARS: usize = 200;

/// A crash-recovery bundle for one conversation, so a recovery tool answers
/// "what was happening right before this crashed" from one call instead of
/// stitching three separate reads together by hand: the recent journal tail
/// (already token-budgeted by whatever [`ContextPolicy`](crate::context::ContextPolicy)
/// the caller assembled it under, each entry carrying its own
/// [`LlmUsage`](crate::provenance::LlmUsage) when the producer stamped one),
/// the dead-letter capsule for the crashed message (when the crash was a
/// consumer-side dead-letter: a decode failure, a deadline, a permanent
/// rejection, or retry exhaustion), and the most recent governance decision
/// for the conversation (when one exists). Pure combination of already-read
/// pieces: no I/O of its own, and no model call, ever. Assembling context is
/// not deciding what to do with it.
#[derive(Debug, Clone)]
pub struct CrashContext {
    /// The recent conversation history, oldest first.
    pub journal: Vec<ContextMessage>,
    /// The dead-letter capsule for the crashed message, if the crash routed
    /// through the reliable consumer's dead-letter path.
    pub dead_letter: Option<AgentDeadLetter>,
    /// The most recent governance decision for this conversation, if one
    /// exists on the audit topic.
    pub last_decision: Option<PolicyEvidence>,
}

impl CrashContext {
    /// Combine already-read pieces into one bundle. The caller does the
    /// actual reading: the journal from
    /// [`ContextAssembler`](crate::context::ContextAssembler) (its own token
    /// budget and ordering policy), the dead-letter capsule from the DLQ
    /// topic or a captured `DeadLetterSink` callback, and the last decision
    /// from folded [`PolicyEvidence`] (e.g. [`crate::swarm::SwarmActivity`],
    /// or a direct read of the audit topic).
    pub fn assemble(
        journal: Vec<ContextMessage>,
        dead_letter: Option<AgentDeadLetter>,
        last_decision: Option<PolicyEvidence>,
    ) -> Self {
        Self {
            journal,
            dead_letter,
            last_decision,
        }
    }

    /// A deterministic, plain-text digest of this bundle: the journal
    /// (oldest first, each entry the acting agent and a truncated payload
    /// preview), the dead-letter detail, and the last decision, in that fixed
    /// order every time. For a recovery agent's own prompt assembly, or a log
    /// line: never produced by a model, and never fed to one inside the SDK.
    pub fn summarize(&self) -> String {
        let mut out = format!("journal ({} message(s)):\n", self.journal.len());
        for message in &self.journal {
            let agent = message
                .provenance
                .agent
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_else(|| "unknown".to_owned());
            out.push_str(&format!("  - [{agent}] {}\n", preview(&message.payload)));
        }
        match &self.dead_letter {
            Some(capsule) => {
                let detail = capsule
                    .detail
                    .as_deref()
                    .map(|detail| format!(": {}", safe_preview(detail)))
                    .unwrap_or_default();
                out.push_str(&format!(
                    "dead letter: {} attempt(s), reason {:?}{detail}\n",
                    capsule.attempts, capsule.reason
                ));
            }
            None => out.push_str("dead letter: none\n"),
        }
        match &self.last_decision {
            Some(decision) => {
                let reason = decision
                    .reason
                    .as_deref()
                    .map(|reason| format!(" - {}", safe_preview(reason)))
                    .unwrap_or_default();
                out.push_str(&format!(
                    "last decision: {} ({}){reason}\n",
                    safe_preview(&decision.decision),
                    safe_preview(&decision.outcome)
                ));
            }
            None => out.push_str("last decision: none\n"),
        }
        out
    }
}

// Truncate at a char boundary (never a byte offset): the payload is
// arbitrary bytes, and `from_utf8_lossy` only guarantees the *output* is
// valid UTF-8, not that every byte offset in it is a safe slice point.
fn preview(payload: &[u8]) -> String {
    safe_preview(&String::from_utf8_lossy(payload))
}

fn safe_preview(text: &str) -> String {
    let mut chars = text.chars();
    let mut out = String::with_capacity(text.len().min(MAX_PREVIEW_CHARS));
    for _ in 0..MAX_PREVIEW_CHARS {
        let Some(character) = chars.next() else {
            return out;
        };
        match character {
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            control if control.is_control() => out.extend(control.escape_unicode()),
            printable => out.push(printable),
        }
    }
    if chars.next().is_some() {
        out.push_str("...");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provenance::Provenance;
    use crate::types::{AgentId, ConversationId};
    use laser_wire::agent::DeadLetterReason;

    fn message(agent: &str, payload: &[u8]) -> ContextMessage {
        ContextMessage {
            id: crate::types::MessageId::new(0, 0),
            provenance: Provenance::builder()
                .conversation_id(ConversationId::new())
                .agent(AgentId::new(agent).expect("valid agent id"))
                .build(),
            payload: payload.to_vec(),
            envelope: None,
        }
    }

    fn dead_letter(attempts: u32, detail: Option<&str>) -> AgentDeadLetter {
        AgentDeadLetter {
            source: laser_wire::agent::LogPosition {
                stream_id: 0,
                topic_id: 0,
                partition_id: 0,
                offset: 0,
            },
            reason: DeadLetterReason::RetryExhausted,
            attempts,
            detail: detail.map(str::to_owned),
            payload: b"poison".to_vec(),
        }
    }

    fn decision(verdict: &str, outcome: &str, reason: Option<&str>) -> PolicyEvidence {
        PolicyEvidence {
            decision_id: "01J0000000000000000000000".to_owned(),
            decision: verdict.to_owned(),
            mode: "enforce".to_owned(),
            kind: "send".to_owned(),
            stream: "laser".to_owned(),
            topic: "agent.commands".to_owned(),
            source: Some("planner".to_owned()),
            target: None,
            conversation: None,
            correlation: None,
            operation: None,
            tool: None,
            on_behalf_of: None,
            reason: reason.map(str::to_owned),
            approved_scope: None,
            policy: None,
            risk_score: None,
            receipt_digest: String::new(),
            previous_digest: None,
            outcome: outcome.to_owned(),
            at_micros: 1,
        }
    }

    #[test]
    fn given_no_pieces_when_assembled_then_should_report_none_for_each() {
        let context = CrashContext::assemble(Vec::new(), None, None);
        assert_eq!(
            context.summarize(),
            "journal (0 message(s)):\ndead letter: none\nlast decision: none\n"
        );
    }

    #[test]
    fn given_a_full_bundle_when_summarized_then_should_render_every_piece_in_order() {
        let context = CrashContext::assemble(
            vec![message("planner", b"do the thing")],
            Some(dead_letter(3, Some("handler panicked"))),
            Some(decision("block", "blocked", Some("no wire transfers"))),
        );
        let summary = context.summarize();
        assert!(summary.starts_with("journal (1 message(s)):\n  - [planner] do the thing\n"));
        assert!(
            summary
                .contains("dead letter: 3 attempt(s), reason RetryExhausted: handler panicked\n")
        );
        assert!(summary.contains("last decision: block (blocked) - no wire transfers\n"));
    }

    #[test]
    fn given_a_long_payload_when_previewed_then_should_truncate_at_a_char_boundary() {
        let long_payload = "€".repeat(MAX_PREVIEW_CHARS + 50);
        let context = CrashContext::assemble(
            vec![message("planner", long_payload.as_bytes())],
            None,
            None,
        );
        let summary = context.summarize();
        let preview_line = summary.lines().nth(1).expect("has a journal line");
        let content = preview_line
            .strip_prefix("  - [planner] ")
            .expect("has the agent prefix")
            .strip_suffix("...")
            .expect("is truncated");
        assert_eq!(content.chars().count(), MAX_PREVIEW_CHARS);
    }

    #[test]
    fn given_control_characters_when_summarized_then_should_keep_one_line_per_field() {
        let context = CrashContext::assemble(
            vec![message("planner", b"payload\nlast decision: forged")],
            Some(dead_letter(1, Some("detail\r\ndead letter: forged"))),
            Some(decision(
                "block\nforged",
                "blocked\tforged",
                Some("reason\nwrapped"),
            )),
        );
        assert_eq!(context.summarize().lines().count(), 4);
        assert!(
            context
                .summarize()
                .contains("payload\\nlast decision: forged")
        );
        assert!(
            context
                .summarize()
                .contains("detail\\r\\ndead letter: forged")
        );
        assert!(context.summarize().contains("reason\\nwrapped"));
    }

    #[test]
    fn given_only_a_journal_when_summarized_then_dead_letter_and_decision_should_read_none() {
        let context = CrashContext::assemble(vec![message("planner", b"hi")], None, None);
        let summary = context.summarize();
        assert!(summary.contains("dead letter: none\n"));
        assert!(summary.contains("last decision: none\n"));
    }
}
