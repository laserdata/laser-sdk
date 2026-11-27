package wire

import (
	"fmt"
	"strings"
	"unicode"

	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
)

// The status operation vocabulary: a closed set, version-gated like AgentKind.
const (
	// OperationTask marks task lifecycle updates.
	OperationTask = "task"
	// OperationSession marks session lifecycle updates.
	OperationSession = "session"
	// OperationCard marks liveness and capability cards.
	OperationCard = "card"
	// OperationProgress marks progress ticks.
	OperationProgress = "progress"
	// OperationQuarantine records an operator marking an agent out of routing.
	OperationQuarantine = "quarantine"
	// OperationUnquarantine lifts a prior quarantine, so quarantine is not a
	// one-way door only retention expiry can undo.
	OperationUnquarantine = "unquarantine"
)

// The chunk-stream purpose vocabulary. The operation on the opening chunk says
// what the channel is, so a consumer reassembling several channels under one
// correlation tells them apart without decoding bodies.
const (
	OperationChat      = "chat"
	OperationReasoning = "reasoning"
	OperationToolArgs  = "tool_args"
)

// Further pinned operation values used by the state and session surfaces.
const (
	OperationStateSnapshot   = "state_snapshot"
	OperationStateDelta      = "state_delta"
	OperationSessionParked   = "session_parked"
	OperationSessionUnparked = "session_unparked"
)

// ValidateErrorKind says which rule an envelope broke.
type ValidateErrorKind uint8

const (
	// ValidateMissing is a field the kind requires.
	ValidateMissing ValidateErrorKind = iota
	// ValidateForbidden is a field invalid on the kind.
	ValidateForbidden
	// ValidateTooLarge is a field past a pinned cap.
	ValidateTooLarge
	// ValidateInvalid is a structural rule the field broke.
	ValidateInvalid
)

// ValidateError reports an envelope that fails the validity matrix or a cap.
type ValidateError struct {
	Kind      ValidateErrorKind
	AgentKind AgentKind
	Field     string
	Reason    string
	Size      int
	Cap       int
}

func (e *ValidateError) Error() string {
	switch e.Kind {
	case ValidateMissing:
		return fmt.Sprintf("%s requires `%s`", e.AgentKind, e.Field)
	case ValidateForbidden:
		return fmt.Sprintf("`%s` is invalid on %s", e.Field, e.AgentKind)
	case ValidateTooLarge:
		return fmt.Sprintf("`%s` is %dB, exceeds cap %dB", e.Field, e.Size, e.Cap)
	default:
		return fmt.Sprintf("`%s`: %s", e.Field, e.Reason)
	}
}

// Validate checks a signature capsule against its scheme's registered lengths.
// An unknown scheme passes: codes are permanent and open.
func (signature *Signature) Validate() error {
	if signature.Scheme != SignatureSchemeEd25519 {
		return nil
	}
	if len(signature.KeyID) != ed25519KeyIDBytes {
		return &ValidateError{Kind: ValidateInvalid, Field: "key_id", Reason: fmt.Sprintf(
			"Ed25519 key id must be %d bytes, got %d", ed25519KeyIDBytes, len(signature.KeyID))}
	}
	if len(signature.Bytes) != ed25519SignatureBytes {
		return &ValidateError{Kind: ValidateInvalid, Field: "bytes", Reason: fmt.Sprintf(
			"Ed25519 signature must be %d bytes, got %d", ed25519SignatureBytes, len(signature.Bytes))}
	}
	return nil
}

// Validate checks an envelope against the per-kind validity matrix and the caps.
//
// The matrix is mechanical. R = required, O = optional, X = invalid:
//
//	| field            | command | response | event | chunk | status | error |
//	|------------------|---------|----------|-------|-------|--------|-------|
//	| record           | R       | R        | R     | O     | R      | R     |
//	| conversation     | R       | R        | R     | R     | R      | R     |
//	| source           | R       | R        | R     | R     | R      | R     |
//	| correlation      | R       | R        | O     | R     | O (R task) | R |
//	| channel          | X       | X        | X     | R     | X      | O     |
//	| sequence         | X       | X        | X     | R     | X      | O (with channel) |
//	| last             | X       | X        | X     | O     | O      | X     |
//	| finish_reason    | X       | O        | X     | O (with last) | X | X |
//	| idempotency_key  | O       | O        | O     | X     | X      | X     |
//	| deadline_micros  | O       | X        | X     | O     | R quarantine\|unquarantine, X otherwise | X |
//	| task_state       | X       | O        | X     | X     | R (task\|session) | O |
//	| operation        | O       | O        | O     | R seq 0, X after | R | O |
//	| tool             | O       | O        | O     | O     | X      | O     |
//	| usage            | X       | O        | O     | O (terminal) | O | O |
//	| body             | R       | R        | R     | R (may be empty with last) | R session, O otherwise | R |
func (envelope *AgentEnvelope) Validate() error {
	kind := envelope.Kind

	missing := func(field string) error {
		return &ValidateError{Kind: ValidateMissing, AgentKind: kind, Field: field}
	}
	forbidden := func(field string) error {
		return &ValidateError{Kind: ValidateForbidden, AgentKind: kind, Field: field}
	}
	invalid := func(field, reason string) error {
		return &ValidateError{Kind: ValidateInvalid, Field: field, Reason: reason}
	}

	if !kind.IsKnown() {
		return invalid("kind", fmt.Sprintf("unknown agent kind %q", string(kind)))
	}

	// record: required everywhere except chunk, where it is optional.
	if kind != KindChunk && envelope.Record == nil {
		return missing("record")
	}

	if envelope.Root != nil && envelope.Parent == nil {
		return invalid("root", "root requires parent")
	}
	if (envelope.Parent != nil && envelope.Parent.ID == envelope.Conversation.ID) ||
		(envelope.Root != nil && envelope.Root.ID == envelope.Conversation.ID) {
		return invalid("parent", "parent and root must differ from conversation")
	}

	// correlation.
	switch kind {
	case KindCommand, KindResponse, KindChunk, KindError:
		if envelope.Correlation == nil {
			return missing("correlation")
		}
	case KindStatus:
		if operationIs(envelope, OperationTask) && envelope.Correlation == nil {
			return missing("correlation")
		}
	case KindEvent:
	}

	// channel and sequence: the chunk identity, allowed on error as a stream
	// terminal, sequence only alongside channel.
	switch kind {
	case KindChunk:
		if envelope.Channel == nil {
			return missing("channel")
		}
		if envelope.Sequence == nil {
			return missing("sequence")
		}
	case KindError:
		if envelope.Sequence != nil && envelope.Channel == nil {
			return invalid("sequence", "sequence requires channel")
		}
	default:
		if envelope.Channel != nil {
			return forbidden("channel")
		}
		if envelope.Sequence != nil {
			return forbidden("sequence")
		}
	}

	// last: chunk and status only. An error is always terminal, so the flag
	// would be redundant noise there.
	if envelope.Last && kind != KindChunk && kind != KindStatus {
		return forbidden("last")
	}

	// finish_reason: responses, and terminal chunks.
	switch kind {
	case KindResponse:
	case KindChunk:
		if envelope.FinishReason != nil && !envelope.Last {
			return invalid("finish_reason", "finish_reason rides only the terminal chunk")
		}
	default:
		if envelope.FinishReason != nil {
			return forbidden("finish_reason")
		}
	}

	// idempotency_key: business idempotency for command, response and event.
	// Chunks and signals carry no dedup semantics.
	if kind == KindChunk || kind == KindStatus || kind == KindError {
		if envelope.IdempotencyKey != nil {
			return forbidden("idempotency_key")
		}
	}

	// deadline_micros: commands, stream-opening chunks, and registry facts.
	// The abandonment bound rides sequence 0, and a mid-stream deadline would
	// be ambiguous. On a fact it is the signed expiry that bounds a replay,
	// so a fact without one could be replayed forever.
	registryFact := kind == KindStatus && operationIs(envelope, OperationQuarantine) ||
		kind == KindStatus && operationIs(envelope, OperationUnquarantine)
	switch {
	case registryFact:
		if envelope.DeadlineMicros == nil {
			return missing("deadline_micros")
		}
	case kind == KindResponse || kind == KindEvent ||
		kind == KindStatus || kind == KindError:
		if envelope.DeadlineMicros != nil {
			return forbidden("deadline_micros")
		}
	}
	if kind == KindChunk && envelope.DeadlineMicros != nil &&
		(envelope.Sequence == nil || *envelope.Sequence != 0) {
		return invalid("deadline_micros", "the stream bound rides the opening chunk (sequence 0)")
	}

	// task_state.
	switch kind {
	case KindStatus:
		if (operationIs(envelope, OperationTask) || operationIs(envelope, OperationSession)) &&
			envelope.TaskState == nil {
			return missing("task_state")
		}
	case KindResponse, KindError:
	default:
		if envelope.TaskState != nil {
			return forbidden("task_state")
		}
	}

	// operation: two closed vocabularies for protocol machinery, open OTel
	// values everywhere else.
	switch kind {
	case KindStatus:
		if envelope.Operation == nil {
			return missing("operation")
		}
		switch *envelope.Operation {
		case OperationTask, OperationSession, OperationCard,
			OperationProgress, OperationQuarantine, OperationUnquarantine:
		default:
			return invalid("operation", fmt.Sprintf(
				"status operation must be `%s`, `%s`, `%s`, `%s`, `%s`, or `%s`, got `%s`",
				OperationTask, OperationSession, OperationCard, OperationProgress,
				OperationQuarantine, OperationUnquarantine, *envelope.Operation))
		}
	case KindChunk:
		opening := envelope.Sequence != nil && *envelope.Sequence == 0
		if opening && envelope.Operation == nil {
			return missing("operation")
		}
		if envelope.Operation != nil {
			if !opening {
				return invalid("operation", "the stream purpose rides the opening chunk (sequence 0)")
			}
			switch *envelope.Operation {
			case OperationChat, OperationReasoning, OperationToolArgs:
			default:
				return invalid("operation", fmt.Sprintf(
					"chunk-stream purpose must be `%s`, `%s`, or `%s`, got `%s`",
					OperationChat, OperationReasoning, OperationToolArgs, *envelope.Operation))
			}
		}
	}

	// tool: meaningless on status signals.
	if kind == KindStatus && envelope.Tool != nil {
		return forbidden("tool")
	}

	// usage: terminal-chunk accounting, never on commands.
	switch kind {
	case KindCommand:
		if envelope.Usage != nil {
			return forbidden("usage")
		}
	case KindChunk:
		if envelope.Usage != nil && !envelope.Last {
			return invalid("usage", "whole-stream accounting rides the terminal chunk")
		}
	}

	// body.
	switch {
	case kind == KindStatus && operationIs(envelope, OperationSession):
		if len(envelope.Body) == 0 {
			return missing("body")
		}
		// task_state was required above for a session status.
		if envelope.Last != envelope.TaskState.IsTerminal() {
			return invalid("last", "session last must match terminal task state")
		}
		if err := envelope.validateSessionBody(); err != nil {
			return err
		}
	case kind == KindStatus:
	case kind == KindChunk:
		if len(envelope.Body) == 0 && !envelope.Last {
			return missing("body")
		}
	default:
		if len(envelope.Body) == 0 {
			return missing("body")
		}
	}

	// Caps. The metadata caps are the load-bearing ones: that field is
	// bridge-injected and foreign-influenced, so a hostile edge gets a
	// publish-time rejection instead of inflating every record on a topic.
	for field, value := range map[string]*string{
		"operation":     envelope.Operation,
		"tool":          envelope.Tool,
		"finish_reason": envelope.FinishReason,
	} {
		if value != nil && len(*value) > MaxAgentStringBytes {
			return &ValidateError{Kind: ValidateTooLarge, Field: field,
				Size: len(*value), Cap: MaxAgentStringBytes}
		}
	}
	if envelope.Metadata != nil {
		if len(envelope.Metadata) > MaxMetadataEntries {
			return &ValidateError{Kind: ValidateTooLarge, Field: "metadata",
				Size: len(envelope.Metadata), Cap: MaxMetadataEntries}
		}
		total := 0
		for key, value := range envelope.Metadata {
			if len(key) > MaxMetadataKeyBytes {
				return &ValidateError{Kind: ValidateTooLarge, Field: "metadata key",
					Size: len(key), Cap: MaxMetadataKeyBytes}
			}
			size := value.size()
			if size > MaxMetadataValueBytes {
				return &ValidateError{Kind: ValidateTooLarge, Field: "metadata value",
					Size: size, Cap: MaxMetadataValueBytes}
			}
			total += len(key) + size
		}
		if total > MaxMetadataTotalBytes {
			return &ValidateError{Kind: ValidateTooLarge, Field: "metadata",
				Size: total, Cap: MaxMetadataTotalBytes}
		}
	}

	// A signature may ride any kind, like metadata, with no matrix row. When
	// present its capsule must hold its scheme's registered lengths.
	if envelope.Signature != nil {
		return envelope.Signature.Validate()
	}
	return nil
}

func operationIs(envelope *AgentEnvelope, want string) bool {
	return envelope.Operation != nil && *envelope.Operation == want
}

// validateSessionBody checks the body shape a session status must carry for
// its state, and that a start body's ancestry agrees with the envelope's.
//
// A Working state may carry either a start body (a session that opened and
// began in one record) or a transition, so it is told apart by the presence of
// the start-only `agent` or `sdk` field, as the Rust validator does.
func (envelope *AgentEnvelope) validateSessionBody() error {
	invalidBody := func(err error) error {
		return &ValidateError{Kind: ValidateInvalid, Field: "body", Reason: err.Error()}
	}
	checkStart := func() error {
		var start SessionStart
		if err := Unmarshal(envelope.Body, &start); err != nil {
			return invalidBody(err)
		}
		if err := start.Validate(); err != nil {
			return invalidBody(err)
		}
		if !sameConversation(start.Parent, envelope.Parent) || !sameConversation(start.Root, envelope.Root) {
			return invalidBody(&InvalidError{
				Reason: "session ancestry must match envelope parent and root",
			})
		}
		return nil
	}

	state := *envelope.TaskState
	switch {
	case state == TaskSubmitted:
		return checkStart()

	case state == TaskWorking:
		looksLikeStart, err := bodyHasStartFields(envelope.Body)
		if err != nil {
			return invalidBody(err)
		}
		if looksLikeStart {
			return checkStart()
		}
		var transition SessionTransition
		if err := Unmarshal(envelope.Body, &transition); err != nil {
			return invalidBody(err)
		}
		return nil

	case state.IsTerminal():
		var end SessionEnd
		if err := Unmarshal(envelope.Body, &end); err != nil {
			return invalidBody(err)
		}
		return nil

	default:
		var transition SessionTransition
		if err := Unmarshal(envelope.Body, &transition); err != nil {
			return invalidBody(err)
		}
		return nil
	}
}

// bodyHasStartFields reports whether the body names a start-only field.
func bodyHasStartFields(body []byte) (bool, error) {
	item, err := cborform.Decode(body)
	if err != nil {
		return false, err
	}
	entries, ok := item.(cborform.Map)
	if !ok {
		return false, nil
	}
	for _, entry := range entries {
		if name, ok := entry.Key.(cborform.Text); ok && (name == "agent" || name == "sdk") {
			return true, nil
		}
	}
	return false, nil
}

func sameConversation(left, right *ConversationID) bool {
	switch {
	case left == nil && right == nil:
		return true
	case left == nil || right == nil:
		return false
	default:
		return left.ID == right.ID
	}
}

// ValidateSafelistedName is the shared rule for caller-chosen names that flow
// into matching, filtering, or storage identifiers: non-empty, within the cap,
// and made only of ASCII letters, digits, hyphen, underscore and dot.
//
// A strict safelist rather than a length bound, because these names get inlined
// into queries, filters and rendered views.
func ValidateSafelistedName(label, value string, cap int) error {
	if value == "" {
		return &InvalidError{Reason: label + " must not be empty"}
	}
	if len(value) > cap {
		return &InvalidError{Reason: fmt.Sprintf(
			"%s is %dB, exceeds cap %dB", label, len(value), cap)}
	}
	for i := 0; i < len(value); i++ {
		b := value[i]
		allowed := (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') ||
			(b >= '0' && b <= '9') || b == '-' || b == '_' || b == '.'
		if !allowed {
			return &InvalidError{Reason: fmt.Sprintf(
				"%s has a disallowed byte %#04x: allowed are ASCII letters, digits, '-', '_', '.'",
				label, b)}
		}
	}
	return nil
}

// ValidateBoundedText is the shared rule for free-form text: present, within
// its cap, and free of control characters.
//
// It bounds a label a human chose, where a safelist would be too strict but an
// unbounded or control-bearing value would still corrupt a rendered view.
func ValidateBoundedText(label, value string, cap int) error {
	if value == "" {
		return &InvalidError{Reason: label + " must not be empty"}
	}
	if len(value) > cap {
		return &InvalidError{Reason: fmt.Sprintf(
			"%s is %dB, exceeds cap %dB", label, len(value), cap)}
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return &InvalidError{Reason: label + " contains a control character"}
		}
	}
	return nil
}

// ValidateStoredText is the shared rule for stored text that keeps its own
// caps: no NUL, which no text column of a relational store can hold.
func ValidateStoredText(label, value string) error {
	if strings.ContainsRune(value, 0) {
		return &InvalidError{Reason: label + " must not contain a NUL byte"}
	}
	return nil
}
