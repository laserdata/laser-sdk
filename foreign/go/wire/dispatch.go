package wire

import "github.com/laserdata/laser-sdk/foreign/go/internal/cborform"

// The session control operations, honoured only on the control topic.
const (
	// OperationSessionPause asks a session to pause.
	OperationSessionPause = "session_pause"
	// OperationSessionResume asks a paused session to resume.
	OperationSessionResume = "session_resume"
	// OperationSessionCancel asks a session to cancel.
	OperationSessionCancel = "session_cancel"
	// OperationForceCancel ends a session as canceled without its agent.
	OperationForceCancel = "force_cancel"
)

// OperationInputRequest asks a person or a named approver for input. Only a
// handler that lists it among its operations receives it as work.
const OperationInputRequest = "input_request"

// ControlOperations are the session control operations.
var ControlOperations = [4]string{
	OperationSessionPause,
	OperationSessionResume,
	OperationSessionCancel,
	OperationForceCancel,
}

// The OTel model and tool operations.
const (
	OperationTextCompletion  = "text_completion"
	OperationGenerateContent = "generate_content"
	OperationExecuteTool     = "execute_tool"
	OperationInvokeAgent     = "invoke_agent"
)

// The event operations a session timeline records.
const (
	OperationContextAssembled = "context_assembled"
	OperationContextCompacted = "context_compacted"
	OperationContextRetrieved = "context_retrieved"
	OperationPolicyDecision   = "policy_decision"
)

// MetadataRole is the metadata key naming the author's role.
const MetadataRole = "role"

// MetadataRecorded marks a record a session wrote to its own timeline rather
// than a live request, so a handler never acts on it a second time.
const MetadataRecorded = "recorded"

// The advisory metadata keys. Each is a claim the producer makes, binding
// only when the envelope is signed, because an unsigned record's metadata is
// whatever the writer put there.
const (
	// MetadataSubmitted marks a submitted session. The receiving agent marks
	// the session working when it picks the command up.
	MetadataSubmitted = "submitted"
	// MetadataDurationMicros is a call's duration, measured by the
	// application around the provider or tool call.
	MetadataDurationMicros = "duration_micros"
	// MetadataBridgeHops counts the bridges a record crossed, so a loop
	// across deployments is bounded.
	MetadataBridgeHops = "bridge_hops"
	// MetadataDelegatedBy names whom an intermediary acted for. It rides in
	// the metadata rather than a header so it falls inside the signed span,
	// and a signer cannot forge whom it claims to act for.
	MetadataDelegatedBy = "on_behalf_of"
	// MetadataPurpose is the declared purpose of the operation, a stable
	// input for a policy engine at the effect boundary.
	MetadataPurpose = "purpose"
	// MetadataDataClassification is the declared classification of the data
	// the operation touches.
	MetadataDataClassification = "data_classification"
	// MetadataTaskContext is the task this operation serves.
	MetadataTaskContext = "task_context"
	// MetadataSessionIntent is the session's declared intent.
	MetadataSessionIntent = "session_intent"
)

// AddresseeFilterName is the name an agent's consumer group policy is saved
// under. Each group owns its policy, so one name serves every agent.
const AddresseeFilterName = "agent-addressee"

// DisplayType is how a session timeline shows one record. It is derived from
// the record, never stored as a separate field.
type DisplayType string

const (
	DisplaySessionSubmitted DisplayType = "session.submitted"
	DisplaySessionStarted   DisplayType = "session.started"
	DisplaySessionPaused    DisplayType = "session.paused"
	DisplaySessionResumed   DisplayType = "session.resumed"
	DisplaySessionCompleted DisplayType = "session.completed"
	DisplaySessionFailed    DisplayType = "session.failed"
	DisplaySessionCanceled  DisplayType = "session.canceled"
	DisplaySessionHeartbeat DisplayType = "session.heartbeat"
	// DisplaySessionParked is a work record a paused session's agent held.
	DisplaySessionParked DisplayType = "session.parked"
	// DisplaySessionUnparked is a held work record handled after the resume.
	DisplaySessionUnparked DisplayType = "session.unparked"
	DisplaySessionControl  DisplayType = "session.control"
	DisplayUserMessage     DisplayType = "user.message"
	DisplayModelRequest    DisplayType = "model.request"
	DisplayModelResponse   DisplayType = "model.response"
	DisplayModelStream     DisplayType = "model.stream"
	DisplayToolCall        DisplayType = "tool.call"
	DisplayToolResult      DisplayType = "tool.result"
	DisplayAgentHandoff    DisplayType = "agent.handoff"
	// DisplayAgentMessage is an agent record matching no more specific row.
	DisplayAgentMessage     DisplayType = "agent.message"
	DisplayStateUpdated     DisplayType = "state.updated"
	DisplayContextAssembled DisplayType = "context.assembled"
	DisplayContextCompacted DisplayType = "context.compacted"
	DisplayContextRetrieved DisplayType = "context.retrieved"
	DisplayMemoryCreated    DisplayType = "memory.created"
	DisplayMemoryForgotten  DisplayType = "memory.forgotten"
	DisplayMemoryFeedback   DisplayType = "memory.feedback"
	DisplayTaskStatus       DisplayType = "task.status"
	DisplayWorkflowStep     DisplayType = "workflow.step"
	DisplayPolicyDecision   DisplayType = "policy.decision"
	DisplayError            DisplayType = "error"
	DisplayDeadLetter       DisplayType = "dead_letter"
	DisplayUndecodable      DisplayType = "undecodable"
	// DisplayInvalid is a record whose header and body name different
	// identities.
	DisplayInvalid             DisplayType = "invalid"
	DisplayKvSet               DisplayType = "kv.set"
	DisplayGraphUpsert         DisplayType = "graph.upsert"
	DisplayUnauthorizedControl DisplayType = "unauthorized_control"
)

// SessionRecordKind discriminates one record a session timeline can show.
type SessionRecordKind uint8

const (
	RecordEnvelope SessionRecordKind = iota
	RecordMemory
	RecordDeadLetter
	RecordJournal
	RecordKvMutation
	RecordGraphMutation
	// RecordUndecodable is a record whose body does not decode, with its kind
	// when the header names one.
	RecordUndecodable
)

// SessionRecord is one record a session timeline can show.
type SessionRecord struct {
	Kind     SessionRecordKind
	Envelope *AgentEnvelope
	Memory   *MemoryRecord
	// AgentKind is the header-named kind of an undecodable record, when there
	// is one.
	AgentKind *AgentKind
}

// The session record constructors.
func EnvelopeRecord(envelope *AgentEnvelope) SessionRecord {
	return SessionRecord{Kind: RecordEnvelope, Envelope: envelope}
}
func MemorySessionRecord(record *MemoryRecord) SessionRecord {
	return SessionRecord{Kind: RecordMemory, Memory: record}
}
func DeadLetterRecord() SessionRecord    { return SessionRecord{Kind: RecordDeadLetter} }
func JournalRecord() SessionRecord       { return SessionRecord{Kind: RecordJournal} }
func KvMutationRecord() SessionRecord    { return SessionRecord{Kind: RecordKvMutation} }
func GraphMutationRecord() SessionRecord { return SessionRecord{Kind: RecordGraphMutation} }
func UndecodableRecord(kind *AgentKind) SessionRecord {
	return SessionRecord{Kind: RecordUndecodable, AgentKind: kind}
}

// DisplayTypeOf is how a session timeline shows a record read from a topic.
func DisplayTypeOf(record SessionRecord, topic string) DisplayType {
	switch record.Kind {
	case RecordMemory:
		if record.Memory == nil {
			return DisplayAgentMessage
		}
		switch record.Memory.Kind {
		case MemoryItem:
			return DisplayMemoryCreated
		case MemoryForget:
			return DisplayMemoryForgotten
		default:
			return DisplayMemoryFeedback
		}
	case RecordDeadLetter:
		return DisplayDeadLetter
	case RecordJournal:
		return DisplayWorkflowStep
	case RecordKvMutation:
		return DisplayKvSet
	case RecordGraphMutation:
		return DisplayGraphUpsert
	case RecordUndecodable:
		return DisplayUndecodable
	}

	envelope := record.Envelope
	if envelope == nil {
		return DisplayAgentMessage
	}
	operation := ""
	if envelope.Operation != nil {
		operation = *envelope.Operation
	}

	switch envelope.Kind {
	case KindStatus:
		return statusDisplay(envelope, operation, topic)

	case KindCommand:
		switch {
		case isControlOperation(operation):
			// A control operation is honoured only on the control topic;
			// anywhere else it is reported, never acted on.
			if topic == AgentControl {
				return DisplaySessionControl
			}
			return DisplayUnauthorizedControl
		case isUserAuthored(envelope):
			return DisplayUserMessage
		case isModelOperation(operation):
			return DisplayModelRequest
		case operation == OperationExecuteTool:
			return DisplayToolCall
		case operation == OperationInvokeAgent:
			return DisplayAgentHandoff
		}

	case KindEvent:
		if isUserAuthored(envelope) {
			return DisplayUserMessage
		}
		switch operation {
		case OperationStateDelta, OperationStateSnapshot:
			return DisplayStateUpdated
		case OperationContextAssembled:
			return DisplayContextAssembled
		case OperationContextCompacted:
			return DisplayContextCompacted
		case OperationContextRetrieved:
			return DisplayContextRetrieved
		case OperationPolicyDecision:
			return DisplayPolicyDecision
		case OperationSessionParked:
			return DisplaySessionParked
		case OperationSessionUnparked:
			return DisplaySessionUnparked
		}
		return DisplayAgentMessage

	case KindResponse:
		if isModelOperation(operation) {
			return DisplayModelResponse
		}
		if operation == OperationExecuteTool {
			return DisplayToolResult
		}

	case KindError:
		if operation == OperationExecuteTool {
			return DisplayToolResult
		}
		return DisplayError

	case KindChunk:
		if operation == OperationChat || operation == OperationReasoning {
			return DisplayModelStream
		}
	}
	return DisplayAgentMessage
}

func statusDisplay(envelope *AgentEnvelope, operation, topic string) DisplayType {
	if operation == OperationProgress && topic == AgentHeartbeats {
		return DisplaySessionHeartbeat
	}
	if operation != OperationSession {
		if operation == OperationTask {
			return DisplayTaskStatus
		}
		return DisplayAgentMessage
	}
	if envelope.TaskState == nil {
		return DisplayTaskStatus
	}
	switch *envelope.TaskState {
	case TaskSubmitted:
		return DisplaySessionSubmitted
	case TaskWorking:
		// A start body names the agent and SDK. A transition body names
		// neither, which is what tells a first start from a resume.
		if looksLikeSessionStart(envelope.Body) {
			return DisplaySessionStarted
		}
		return DisplaySessionResumed
	case TaskPaused:
		return DisplaySessionPaused
	case TaskCompleted:
		return DisplaySessionCompleted
	case TaskFailed, TaskRejected:
		return DisplaySessionFailed
	case TaskCanceled:
		return DisplaySessionCanceled
	default:
		return DisplayTaskStatus
	}
}

// looksLikeSessionStart reports whether a body names a start-only field.
func looksLikeSessionStart(body []byte) bool {
	start, err := bodyHasStartFields(body)
	return err == nil && start
}

func isControlOperation(operation string) bool {
	for _, candidate := range ControlOperations {
		if candidate == operation {
			return true
		}
	}
	return false
}

func isModelOperation(operation string) bool {
	switch operation {
	case OperationChat, OperationTextCompletion, OperationGenerateContent:
		return true
	default:
		return false
	}
}

// isRecorded reports a record a session wrote to its own timeline rather than
// a live request. A recorded model or tool call informs the timeline and is
// never handled again.
func isRecorded(envelope *AgentEnvelope) bool {
	recorded, present := envelope.Metadata[MetadataRecorded]
	return present && recorded.Kind == ValueBool && recorded.Bool
}

func isUserAuthored(envelope *AgentEnvelope) bool {
	role, present := envelope.Metadata[MetadataRole]
	return present && role.Kind == ValueStr && role.Str == "user"
}

// Dispatch is what a reliable consumer does with one record it read.
type Dispatch string

const (
	// DispatchWork is a command addressed to this agent for an operation its
	// handler serves.
	DispatchWork Dispatch = "work"
	// DispatchObservational is a record that informs the timeline and is
	// never handled: events, model and tool records for operations the
	// handler does not serve, model and tool calls a session recorded, input
	// requests for a handler that did not list them, and control operations
	// found outside the control topic.
	DispatchObservational Dispatch = "observational"
	// DispatchReply is a response, error or chunk. Reply waiters read it,
	// handlers never do.
	DispatchReply Dispatch = "reply"
	// DispatchLifecycle is a status record.
	DispatchLifecycle Dispatch = "lifecycle"
	// DispatchControl is a control operation on the control topic, for the
	// control follower.
	DispatchControl Dispatch = "control"
	// DispatchForeign is a record addressed to another agent.
	DispatchForeign Dispatch = "foreign"
)

// HandledOperations is the command operations a handler serves.
type HandledOperations struct {
	// Any serves every command operation.
	Any bool
	// Only serves the listed operations.
	Only []string
}

// AnyOperation serves every command operation.
func AnyOperation() HandledOperations { return HandledOperations{Any: true} }

// OnlyOperations serves the listed operations.
func OnlyOperations(operations ...string) HandledOperations {
	return HandledOperations{Only: operations}
}

// handles reports whether this handler serves the operation.
//
// An input request reaches only an approver that opted in to it, so serving
// every operation still excludes it.
func (handled HandledOperations) handles(operation string) bool {
	if handled.Any {
		return operation != OperationInputRequest
	}
	for _, candidate := range handled.Only {
		if candidate == operation {
			return true
		}
	}
	return false
}

// Classify is what an agent does with an envelope read from a topic.
//
// The author is never a discriminator, so an agent may send work to itself.
func Classify(envelope *AgentEnvelope, topic string, me AgentID, handled HandledOperations) Dispatch {
	operation := ""
	if envelope.Operation != nil {
		operation = *envelope.Operation
	}

	switch envelope.Kind {
	case KindStatus:
		return DispatchLifecycle
	case KindResponse, KindError, KindChunk:
		return DispatchReply
	case KindEvent:
		return DispatchObservational
	case KindCommand:
		if envelope.Target != nil && envelope.Target.String() != me.String() {
			return DispatchForeign
		}
		control := isControlOperation(operation)
		onControlTopic := topic == AgentControl
		switch {
		case control && onControlTopic:
			return DispatchControl
		case control:
			return DispatchObservational
		case onControlTopic:
			// A non-control command has no business on the control topic.
			return DispatchForeign
		case isRecorded(envelope):
			return DispatchObservational
		case handled.handles(operation):
			return DispatchWork
		default:
			return DispatchObservational
		}
	default:
		return DispatchObservational
	}
}

// ClassifyGeneric is what an agent does with a generic record, one without an
// envelope, read from a topic.
//
// Such a record has no kind, so a record naming both its causal parent and a
// correlation is a reply, and any other record is work unless it is addressed
// to another agent.
func ClassifyGeneric(addressee *string, hasCause, hasCorrelation bool, topic string, me AgentID) Dispatch {
	if addressee != nil && *addressee != me.String() && *addressee != Broadcast {
		return DispatchForeign
	}
	if topic == AgentControl {
		return DispatchForeign
	}
	if hasCause && hasCorrelation {
		return DispatchReply
	}
	return DispatchWork
}

// HeadersOnlyFilter builds a filter that never decodes the payload, so only
// header predicates are allowed and the payload may be in any format.
func HeadersOnlyFilter(expr FilterExpr) ConsumerFilter {
	return NewConsumerFilter(expr, CodecHeadersOnly)
}

// AddresseeFilter is the headers-only group filter selecting records addressed
// to one agent or to every agent. Its digest differs per agent identity.
func AddresseeFilter(me AgentID) ConsumerFilter {
	return HeadersOnlyFilter(ExprHeaderPredicate(HeaderTargetAgentID, CmpIn,
		ListTyped(StringTyped(me.String()), StringTyped(Broadcast))))
}

// BroadcastFilter is the headers-only group filter selecting broadcast records
// only.
func BroadcastFilter() ConsumerFilter {
	return HeadersOnlyFilter(ExprHeaderPredicate(HeaderTargetAgentID, CmpEq,
		StringTyped(Broadcast)))
}

// MarshalCBOR writes the display word.
func (display DisplayType) MarshalCBOR() (cborform.Value, error) {
	return cborform.Text(string(display)), nil
}

// MarshalCBOR writes the dispatch word.
func (dispatch Dispatch) MarshalCBOR() (cborform.Value, error) {
	return cborform.Text(string(dispatch)), nil
}
