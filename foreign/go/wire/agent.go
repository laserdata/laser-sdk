package wire

import (
	"fmt"
	"math"
	"strconv"

	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
)

// AgentKind is the AGDX message kind, riding the wire as its snake_case name.
type AgentKind string

const (
	// KindCommand expects a reply or effect, so it requires a correlation.
	// Fire-and-forget commands do not exist by definition, since those are
	// events.
	KindCommand AgentKind = "command"
	// KindResponse is the paired answer to a command.
	KindResponse AgentKind = "response"
	// KindEvent expects nothing.
	KindEvent AgentKind = "event"
	// KindChunk is one piece of a stream, ordered by sequence within a channel.
	KindChunk AgentKind = "chunk"
	// KindStatus is a lifecycle signal, discriminated by operation.
	KindStatus AgentKind = "status"
	// KindError is a terminal failure carrying a structured AgentErrorBody.
	KindError AgentKind = "error"
)

// IsKnown reports whether the kind is one this build names.
func (kind AgentKind) IsKnown() bool {
	switch kind {
	case KindCommand, KindResponse, KindEvent, KindChunk, KindStatus, KindError:
		return true
	default:
		return false
	}
}

// MarshalCBOR writes the snake_case name.
func (kind AgentKind) MarshalCBOR() (cborform.Value, error) {
	if !kind.IsKnown() {
		return nil, fmt.Errorf("wire: unknown agent kind %q", string(kind))
	}
	return cborform.Text(string(kind)), nil
}

// UnmarshalCBOR reads the snake_case name.
func (kind *AgentKind) UnmarshalCBOR(item cborform.Value) error {
	text, ok := item.(cborform.Text)
	if !ok {
		return fmt.Errorf("wire: cannot decode %T as an agent kind", item)
	}
	candidate := AgentKind(text)
	if !candidate.IsKnown() {
		return fmt.Errorf("wire: unknown agent kind %q", string(text))
	}
	*kind = candidate
	return nil
}

// TaskState is A2A's task lifecycle with LaserData's Paused extension, riding
// the wire as a u8 code.
//
// A future A2A state takes the next free code and flows through older consumers
// as an opaque non-terminal value instead of forcing a version bump. Codes are
// permanent and never renumbered.
type TaskState uint8

const (
	TaskSubmitted     TaskState = 1
	TaskWorking       TaskState = 2
	TaskInputRequired TaskState = 3
	TaskCompleted     TaskState = 4
	TaskCanceled      TaskState = 5
	TaskFailed        TaskState = 6
	TaskRejected      TaskState = 7
	TaskAuthRequired  TaskState = 8
	TaskUnknown       TaskState = 9
	// TaskPaused is LaserData's paused session state.
	TaskPaused TaskState = 10
)

// Code is the pinned wire code.
func (state TaskState) Code() uint8 { return uint8(state) }

// IsRecognized reports whether this build names the code. An unrecognized code
// is still carried and re-emitted unchanged.
func (state TaskState) IsRecognized() bool {
	return state >= TaskSubmitted && state <= TaskPaused
}

// IsTerminal reports whether the state ends the task, A2A's terminal set.
// Unrecognized codes are non-terminal by rule.
func (state TaskState) IsTerminal() bool {
	switch state {
	case TaskCompleted, TaskCanceled, TaskFailed, TaskRejected:
		return true
	default:
		return false
	}
}

// String is the A2A kebab-case name, for bridges and consoles.
func (state TaskState) String() string {
	switch state {
	case TaskSubmitted:
		return "submitted"
	case TaskWorking:
		return "working"
	case TaskInputRequired:
		return "input-required"
	case TaskCompleted:
		return "completed"
	case TaskCanceled:
		return "canceled"
	case TaskFailed:
		return "failed"
	case TaskRejected:
		return "rejected"
	case TaskAuthRequired:
		return "auth-required"
	case TaskUnknown:
		return "unknown"
	case TaskPaused:
		return "paused"
	default:
		return "unrecognized-" + strconv.Itoa(int(state))
	}
}

// ParseTaskState reads an A2A kebab-case name. The unrecognized passthrough
// form never parses from text, matching the Rust FromStr.
func ParseTaskState(text string) (TaskState, error) {
	switch text {
	case "submitted":
		return TaskSubmitted, nil
	case "working":
		return TaskWorking, nil
	case "input-required":
		return TaskInputRequired, nil
	case "completed":
		return TaskCompleted, nil
	case "canceled":
		return TaskCanceled, nil
	case "failed":
		return TaskFailed, nil
	case "rejected":
		return TaskRejected, nil
	case "auth-required":
		return TaskAuthRequired, nil
	case "unknown":
		return TaskUnknown, nil
	case "paused":
		return TaskPaused, nil
	default:
		return 0, &InvalidError{Reason: fmt.Sprintf("unknown task state %q", text)}
	}
}

// MarshalCBOR writes the u8 code.
func (state TaskState) MarshalCBOR() (cborform.Value, error) {
	return cborform.Uint(uint64(state)), nil
}

// UnmarshalCBOR reads the u8 code, carrying unknown codes through.
func (state *TaskState) UnmarshalCBOR(item cborform.Value) error {
	number, ok := item.(cborform.Uint)
	if !ok {
		return fmt.Errorf("wire: cannot decode %T as a task state", item)
	}
	if number > math.MaxUint8 {
		return fmt.Errorf("wire: task state code %d exceeds a u8", number)
	}
	*state = TaskState(number)
	return nil
}

// AgentErrorCode says why an agent operation failed, as a pinned u8 dictionary
// following the TaskState pattern: unknown codes decode and pass through.
type AgentErrorCode uint8

const (
	ErrorInvalidRequest   AgentErrorCode = 1
	ErrorUnauthorized     AgentErrorCode = 2
	ErrorUnsupported      AgentErrorCode = 3
	ErrorDeadlineExceeded AgentErrorCode = 4
	ErrorCancelled        AgentErrorCode = 5
	ErrorToolFailure      AgentErrorCode = 6
	ErrorInternal         AgentErrorCode = 7
)

// Code is the pinned wire code.
func (code AgentErrorCode) Code() uint8 { return uint8(code) }

// IsRecognized reports whether this build names the code.
func (code AgentErrorCode) IsRecognized() bool {
	return code >= ErrorInvalidRequest && code <= ErrorInternal
}

// MarshalCBOR writes the u8 code.
func (code AgentErrorCode) MarshalCBOR() (cborform.Value, error) {
	return cborform.Uint(uint64(code)), nil
}

// UnmarshalCBOR reads the u8 code, carrying unknown codes through.
func (code *AgentErrorCode) UnmarshalCBOR(item cborform.Value) error {
	number, ok := item.(cborform.Uint)
	if !ok {
		return fmt.Errorf("wire: cannot decode %T as an agent error code", item)
	}
	if number > math.MaxUint8 {
		return fmt.Errorf("wire: agent error code %d exceeds a u8", number)
	}
	*code = AgentErrorCode(number)
	return nil
}

// TokenUsage is the OTel token accounting an agent message may carry.
type TokenUsage struct {
	InputTokens              uint64  `cbor:"input_tokens"`
	OutputTokens             uint64  `cbor:"output_tokens"`
	ReasoningOutputTokens    *uint64 `cbor:"reasoning_output_tokens,omitempty"`
	CacheReadInputTokens     *uint64 `cbor:"cache_read_input_tokens,omitempty"`
	CacheCreationInputTokens *uint64 `cbor:"cache_creation_input_tokens,omitempty"`
	// CostMicros is cost in integer micro-units of the deployment's currency.
	CostMicros *uint64 `cbor:"cost_micros,omitempty"`
}

// SignatureSchemeEd25519 is the Ed25519 signing scheme code.
const SignatureSchemeEd25519 uint8 = 1

// SignatureDomain is the domain separator prefixed to the canonical envelope
// encoding before signing, so an AGDX signature can never be replayed into
// another protocol.
var SignatureDomain = []byte("agdx.signature.v1")

const (
	ed25519KeyIDBytes     = 8
	ed25519SignatureBytes = 64
)

// SignatureContext carries the interpretation-bearing attributes a Signature
// binds beyond the body: the content-type code and wire version a consumer
// reads to select its codec. Riding unsigned headers, these would let an
// intermediary reinterpret a signed record.
type SignatureContext struct {
	// ContentType is the agdx.ct code the body is encoded with.
	ContentType *uint8 `cbor:"content_type,omitempty"`
	// AgentVersion is the agdx.av wire version the envelope is stamped with.
	AgentVersion *uint32 `cbor:"agent_version,omitempty"`
}

// Signature is a detached signature over the canonical encoding of an envelope
// with the signature field absent, domain-separated by SignatureDomain.
//
// Verification is SDK-side against a per-agent key registry, so the wire layer
// stays crypto-free.
type Signature struct {
	// Scheme is the signing scheme code. Codes are permanent, and unknown
	// codes pass through like every open dictionary.
	Scheme uint8 `cbor:"scheme"`
	// KeyID says which of the agent's registered keys signed.
	KeyID []byte `cbor:"key_id"`
	// Bytes is the signature over the canonical envelope encoding.
	Bytes []byte `cbor:"bytes"`
	// Context is folded into the signed preimage when present.
	Context *SignatureContext `cbor:"context,omitempty"`
}

// AgentEnvelope is the AGDX envelope: one CBOR named-field decode unit per
// agent message.
//
// Field semantics, the per-kind validity matrix and the caps are enforced by
// Validate, and the per-kind constructors stamp the required shape. Routing
// fields are also stamped as typed headers so projections and plain Iggy
// consumers work without decoding bodies.
//
// Metadata is the AGDX-native extension slot, distinct from headers (the
// substrate and observability dictionary) and body (the content). Foreign
// metadata never maps into it. It tunnels whole inside the body, which keeps
// bridge round trips byte-identical.
//
// Field order is the wire order: serde writes struct fields in declaration
// order, so reordering these changes the bytes.
type AgentEnvelope struct {
	Kind AgentKind `cbor:"kind"`
	// Record is the producer-assigned identity, required on every kind except
	// chunk, which is identified by channel and sequence.
	Record *RecordID `cbor:"record,omitempty"`
	// Conversation is also the partition key.
	Conversation ConversationID `cbor:"conversation"`
	// Parent is the session this conversation was spawned from.
	Parent *ConversationID `cbor:"parent,omitempty"`
	// Root is the first conversation in this session tree.
	Root *ConversationID `cbor:"root,omitempty"`
	// Source is the agent-authorship claim.
	Source AgentID `cbor:"source"`
	// Target refines routing within a shared topic. The topic itself is the
	// primary address and the only confidentiality boundary.
	Target *AgentID `cbor:"target,omitempty"`
	// Cause is the causal parent's record id, stable across replication.
	Cause *RecordID `cbor:"cause,omitempty"`
	// CauseAt is the causal parent's log position, deployment-local.
	CauseAt *LogPosition `cbor:"cause_at,omitempty"`
	// Correlation pairs a request with its reply.
	Correlation *CorrelationID `cbor:"correlation,omitempty"`
	// Channel groups chunks when many streams run under one correlation.
	Channel        *ChannelID      `cbor:"channel,omitempty"`
	IdempotencyKey *IdempotencyKey `cbor:"idempotency_key,omitempty"`
	// DeadlineMicros is the drop-dead time in epoch micros. On a stream-opening
	// message it is also the reader-local abandonment bound.
	DeadlineMicros *uint64 `cbor:"deadline_micros,omitempty"`
	// Sequence orders chunks within a channel.
	Sequence *uint64 `cbor:"sequence,omitempty"`
	// Last is the terminal flag. False is equivalent to absence and is skipped
	// on encode, so only true has protocol meaning.
	Last bool `cbor:"last,omitempty"`
	// FinishReason uses the OTel finish-reason vocabulary, deliberately a
	// string because that vocabulary belongs to OTel and the providers.
	FinishReason *string    `cbor:"finish_reason,omitempty"`
	TaskState    *TaskState `cbor:"task_state,omitempty"`
	// Operation is the OTel gen_ai.operation.name value. On status it is the
	// required discriminator.
	Operation *string `cbor:"operation,omitempty"`
	// Tool is the OTel gen_ai.tool.name, for tool commands and results.
	Tool  *string     `cbor:"tool,omitempty"`
	Usage *TokenUsage `cbor:"usage,omitempty"`
	// Metadata is AGDX-native scalar extension context, never foreign metadata.
	Metadata map[string]Value `cbor:"metadata,omitempty"`
	// MustUnderstand is a bitset of feature bits a receiver must understand to
	// process this message correctly. A receiver that sees a set bit it does
	// not implement must reject or dead-letter the message rather than
	// mis-handle it. Zero means "ignore anything you don't understand", the
	// open-world default, and is skipped on the wire so pre-marker records
	// stay byte-identical.
	MustUnderstand uint64 `cbor:"must_understand,omitempty"`
	// Body is the content, codec per the agdx.ct header.
	Body []byte `cbor:"body,omitempty"`
	// Signature is absent on an unsigned record, which keeps such a record
	// byte-identical to a pre-signature one.
	Signature *Signature `cbor:"signature,omitempty"`
}

// FeatureNone is the empty must-understand set. No feature bits are defined
// yet, so a current build understands none and only ever sees zero.
const FeatureNone uint64 = 0

// UnmetRequirements is the subset of this message's must-understand bits absent
// from the receiver's understood set. Non-zero means the receiver cannot safely
// process the message.
func (envelope *AgentEnvelope) UnmetRequirements(understood uint64) uint64 {
	return envelope.MustUnderstand &^ understood
}
