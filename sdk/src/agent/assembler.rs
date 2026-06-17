use laser_wire::agent::{AgentEnvelope, AgentKind, TokenUsage};

// Synthetic finish reasons, reader-local: never published. The log keeps the
// raw truth, the live view diverges from it in this one direction only.
/// Finish reason synthesized when the stream's deadline passed with no chunk.
pub const FINISH_REASON_ABANDONED: &str = "abandoned";
/// Finish reason synthesized when a `sequence` was skipped (the in-order log
/// proves the producer never published it).
pub const FINISH_REASON_GAP: &str = "gap";

/// One reassembled stream occurrence, in order.
#[derive(Debug, Clone, PartialEq)]
pub enum StreamEvent {
    /// The next body bytes of the stream.
    Body { sequence: u64, bytes: Vec<u8> },
    /// The stream ended. `synthetic` marks reader-local endings (gap,
    /// abandonment) that never reach the log.
    Finished {
        finish_reason: Option<String>,
        usage: Option<TokenUsage>,
        synthetic: bool,
    },
    /// The stream ended on a `kind = error` terminal. `body` is the encoded
    /// `AgentErrorBody`.
    Failed { body: Vec<u8> },
}

/// Per-channel reassembly state machine, pure and clock-free (the consumer
/// wires the deadline timer and calls [`abandon`](Self::abandon)).
#[derive(Debug, Default)]
pub struct ChunkAssembler {
    next_sequence: u64,
    finished: bool,
    duplicates_dropped: u64,
    late_dropped: u64,
}

// Reassembly is mechanical: chunks apply in `sequence` order from 0, each once.
// A duplicate drops and counts. A gap ends the stream with a synthetic `gap`
// terminal. Everything after a terminal drops and counts, first terminal wins.
// A `kind = error` carrying the channel is the failure terminal.
impl ChunkAssembler {
    /// A fresh assembler for one channel.
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply one envelope of this channel. Returns the events it produces
    /// (zero, one, or body-plus-terminal for a non-empty terminal chunk).
    pub fn feed(&mut self, envelope: &AgentEnvelope) -> Vec<StreamEvent> {
        match envelope.kind {
            AgentKind::Chunk => self.feed_chunk(envelope),
            AgentKind::Error => {
                if self.finished {
                    self.late_dropped += 1;
                    return Vec::new();
                }
                self.finished = true;
                vec![StreamEvent::Failed {
                    body: envelope.body.clone(),
                }]
            }
            AgentKind::Command | AgentKind::Response | AgentKind::Event | AgentKind::Status => {
                Vec::new()
            }
        }
    }

    /// Synthesize the reader-local abandonment terminal (the deadline passed
    /// with no chunk). `None` when the stream already ended.
    pub fn abandon(&mut self) -> Option<StreamEvent> {
        if self.finished {
            return None;
        }
        self.finished = true;
        Some(StreamEvent::Finished {
            finish_reason: Some(FINISH_REASON_ABANDONED.to_owned()),
            usage: None,
            synthetic: true,
        })
    }

    /// Whether a terminal (real or synthetic) has been seen.
    pub fn is_finished(&self) -> bool {
        self.finished
    }

    /// Redelivered chunks dropped (consumer at-least-once).
    pub fn duplicates_dropped(&self) -> u64 {
        self.duplicates_dropped
    }

    /// Chunks and terminals dropped after the stream ended.
    pub fn late_dropped(&self) -> u64 {
        self.late_dropped
    }

    fn feed_chunk(&mut self, envelope: &AgentEnvelope) -> Vec<StreamEvent> {
        let Some(sequence) = envelope.sequence else {
            // validate() rejects this upstream, and a malformed chunk never
            // advances the stream.
            return Vec::new();
        };
        if self.finished {
            self.late_dropped += 1;
            return Vec::new();
        }
        if sequence < self.next_sequence {
            self.duplicates_dropped += 1;
            return Vec::new();
        }
        if sequence > self.next_sequence {
            self.finished = true;
            return vec![StreamEvent::Finished {
                finish_reason: Some(FINISH_REASON_GAP.to_owned()),
                usage: None,
                synthetic: true,
            }];
        }
        self.next_sequence += 1;
        let mut events = Vec::new();
        if !envelope.body.is_empty() {
            events.push(StreamEvent::Body {
                sequence,
                bytes: envelope.body.clone(),
            });
        }
        if envelope.last {
            self.finished = true;
            events.push(StreamEvent::Finished {
                finish_reason: envelope.finish_reason.clone(),
                usage: envelope.usage,
                synthetic: false,
            });
        }
        events
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use laser_wire::agent::{
        AgentErrorBody, AgentErrorCode, ChannelId, ConversationId, CorrelationId, OPERATION_CHAT,
        RecordId,
    };
    use laser_wire::framing::encode_named;

    #[test]
    fn given_an_ordered_stream_when_fed_then_should_emit_bodies_and_the_terminal() {
        let mut assembler = ChunkAssembler::new();
        assert_eq!(
            assembler.feed(&chunk(0, b"he")),
            vec![StreamEvent::Body {
                sequence: 0,
                bytes: b"he".to_vec()
            }]
        );
        assert_eq!(
            assembler.feed(&chunk(1, b"llo")),
            vec![StreamEvent::Body {
                sequence: 1,
                bytes: b"llo".to_vec()
            }]
        );
        assert_eq!(
            assembler.feed(&terminal(2)),
            vec![StreamEvent::Finished {
                finish_reason: Some("stop".to_owned()),
                usage: None,
                synthetic: false
            }]
        );
        assert!(assembler.is_finished());
        assert_eq!(assembler.duplicates_dropped(), 0);
        assert_eq!(assembler.late_dropped(), 0);
    }

    #[test]
    fn given_a_redelivered_chunk_when_fed_then_should_drop_and_count_it() {
        let mut assembler = ChunkAssembler::new();
        assembler.feed(&chunk(0, b"a"));
        assembler.feed(&chunk(1, b"b"));
        assert!(assembler.feed(&chunk(0, b"a")).is_empty());
        assert!(assembler.feed(&chunk(1, b"b")).is_empty());
        assert_eq!(assembler.duplicates_dropped(), 2);
        assert!(!assembler.is_finished());
        // The stream continues exactly where it was.
        assert_eq!(
            assembler.feed(&chunk(2, b"c")),
            vec![StreamEvent::Body {
                sequence: 2,
                bytes: b"c".to_vec()
            }]
        );
    }

    #[test]
    fn given_a_sequence_gap_when_fed_then_should_synthesize_the_gap_terminal() {
        let mut assembler = ChunkAssembler::new();
        assembler.feed(&chunk(0, b"a"));
        let events = assembler.feed(&chunk(2, b"c"));
        assert_eq!(
            events,
            vec![StreamEvent::Finished {
                finish_reason: Some(FINISH_REASON_GAP.to_owned()),
                usage: None,
                synthetic: true
            }]
        );
        assert!(assembler.is_finished());
        // Whatever arrives after the synthetic ending is late.
        assert!(assembler.feed(&chunk(1, b"b")).is_empty());
        assert_eq!(assembler.late_dropped(), 1);
    }

    #[test]
    fn given_chunks_after_the_terminal_when_fed_then_first_terminal_should_win() {
        let mut assembler = ChunkAssembler::new();
        assembler.feed(&chunk(0, b"a"));
        assembler.feed(&terminal(1));
        // A late body and a second terminal both drop.
        assert!(assembler.feed(&chunk(2, b"x")).is_empty());
        assert!(assembler.feed(&terminal(3)).is_empty());
        assert_eq!(assembler.late_dropped(), 2);
    }

    #[test]
    fn given_a_non_empty_terminal_chunk_when_fed_then_should_emit_body_then_finished() {
        let mut assembler = ChunkAssembler::new();
        let mut closing = chunk(0, b"tail");
        closing.last = true;
        closing.finish_reason = Some("stop".to_owned());
        let events = assembler.feed(&closing);
        assert_eq!(events.len(), 2);
        assert!(matches!(events[0], StreamEvent::Body { sequence: 0, .. }));
        assert!(matches!(
            events[1],
            StreamEvent::Finished {
                synthetic: false,
                ..
            }
        ));
    }

    #[test]
    fn given_an_error_terminal_when_fed_then_should_fail_the_stream() {
        let error_body = encode_named(&AgentErrorBody {
            code: AgentErrorCode::ToolFailure,
            message: Some("boom".to_owned()),
            retryable: false,
            detail: None,
        })
        .expect("error body encodes");
        let mut error = AgentEnvelope::error(
            RecordId::from_u128(9),
            ConversationId::from_u128(1),
            "source-agent".parse().expect("valid agent id"),
            CorrelationId::from_u128(3),
            error_body.clone(),
        );
        error.channel = Some(ChannelId::from_u128(4));
        error.sequence = Some(1);

        let mut assembler = ChunkAssembler::new();
        assembler.feed(&chunk(0, b"a"));
        assert_eq!(
            assembler.feed(&error),
            vec![StreamEvent::Failed { body: error_body }]
        );
        assert!(assembler.is_finished());
        // A second failure after the first is late.
        assert!(assembler.feed(&error.clone()).is_empty());
        assert_eq!(assembler.late_dropped(), 1);
    }

    #[test]
    fn given_an_idle_stream_when_abandoned_then_should_synthesize_once() {
        let mut assembler = ChunkAssembler::new();
        assembler.feed(&chunk(0, b"a"));
        let ending = assembler.abandon().expect("first abandonment synthesizes");
        assert_eq!(
            ending,
            StreamEvent::Finished {
                finish_reason: Some(FINISH_REASON_ABANDONED.to_owned()),
                usage: None,
                synthetic: true
            }
        );
        assert!(assembler.abandon().is_none());
        // Late chunks after local abandonment drop while the log keeps them.
        assert!(assembler.feed(&chunk(1, b"b")).is_empty());
        assert_eq!(assembler.late_dropped(), 1);
    }

    fn chunk(sequence: u64, body: &[u8]) -> AgentEnvelope {
        let envelope = AgentEnvelope::chunk(
            ConversationId::from_u128(1),
            "source-agent".parse().expect("valid agent id"),
            CorrelationId::from_u128(3),
            ChannelId::from_u128(4),
            sequence,
            body.to_vec(),
        );
        if sequence == 0 {
            envelope.with_operation(OPERATION_CHAT)
        } else {
            envelope
        }
    }

    fn terminal(sequence: u64) -> AgentEnvelope {
        let mut envelope = chunk(sequence, b"");
        envelope.last = true;
        envelope.finish_reason = Some("stop".to_owned());
        envelope
    }
}
