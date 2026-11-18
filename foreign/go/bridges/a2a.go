// Package bridges maps external agent interfaces onto AGDX.
//
// A bridge is a translation and nothing more: it turns a foreign protocol's
// request into an AGDX envelope and an AGDX answer back into that protocol's
// reply. The log stays the source of truth, so a task survives a bridge
// restart and remains replayable.
package bridges

import (
	"encoding/json"
	"strings"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// A2AMethod is a JSON-RPC method the A2A edge serves.
type A2AMethod string

const (
	// A2AMessageSend submits work and answers with the task it created.
	A2AMessageSend A2AMethod = "message/send"
	// A2ATasksGet looks a task up by id.
	A2ATasksGet A2AMethod = "tasks/get"
	// A2ATasksCancel asks for a task to stop.
	A2ATasksCancel A2AMethod = "tasks/cancel"
)

// A2APart is one piece of a message or artifact, carrying text.
type A2APart struct {
	Text string `json:"text"`
}

// A2AArtifact is something a task produced.
type A2AArtifact struct {
	ArtifactID string    `json:"artifactId"`
	Parts      []A2APart `json:"parts"`
}

// A2ATaskStatus is where a task stands.
type A2ATaskStatus struct {
	State string `json:"state"`
}

// A2ATask is the A2A view of one exchange.
type A2ATask struct {
	ID        string        `json:"id"`
	ContextID string        `json:"contextId"`
	Status    A2ATaskStatus `json:"status"`
	Artifacts []A2AArtifact `json:"artifacts,omitempty"`
}

// The A2A task-state vocabulary. It is the protocol's own spelling, which is
// why it is written out rather than derived from the contract's names: the two
// belong to different owners and must be free to differ.
const (
	a2aSubmitted     = "TASK_STATE_SUBMITTED"
	a2aWorking       = "TASK_STATE_WORKING"
	a2aInputRequired = "TASK_STATE_INPUT_REQUIRED"
	a2aCompleted     = "TASK_STATE_COMPLETED"
	a2aCanceled      = "TASK_STATE_CANCELED"
	a2aFailed        = "TASK_STATE_FAILED"
	a2aRejected      = "TASK_STATE_REJECTED"
	a2aAuthRequired  = "TASK_STATE_AUTH_REQUIRED"
	a2aUnspecified   = "TASK_STATE_UNSPECIFIED"
)

// A2AState renders a contract task state in A2A's vocabulary.
//
// A paused session reads as working, because A2A has no paused state and a
// paused task has not stopped: reporting it as anything terminal would tell a
// caller the work ended when it has not.
func A2AState(state wire.TaskState) string {
	switch state {
	case wire.TaskSubmitted:
		return a2aSubmitted
	case wire.TaskWorking, wire.TaskPaused:
		return a2aWorking
	case wire.TaskInputRequired:
		return a2aInputRequired
	case wire.TaskCompleted:
		return a2aCompleted
	case wire.TaskCanceled:
		return a2aCanceled
	case wire.TaskFailed:
		return a2aFailed
	case wire.TaskRejected:
		return a2aRejected
	case wire.TaskAuthRequired:
		return a2aAuthRequired
	default:
		// An unrecognised state is unspecified rather than guessed: a bridge
		// that invented a state would report something the deployment never
		// said.
		return a2aUnspecified
	}
}

// TaskStateFromA2A reads an A2A state back into the contract's.
//
// A word outside the vocabulary is refused rather than mapped to unknown: an
// edge sending one is speaking a protocol this bridge does not implement, and
// guessing would act on work it did not understand.
func TaskStateFromA2A(word string) (wire.TaskState, error) {
	switch word {
	case a2aSubmitted:
		return wire.TaskSubmitted, nil
	case a2aWorking:
		return wire.TaskWorking, nil
	case a2aInputRequired:
		return wire.TaskInputRequired, nil
	case a2aCompleted:
		return wire.TaskCompleted, nil
	case a2aCanceled:
		return wire.TaskCanceled, nil
	case a2aFailed:
		return wire.TaskFailed, nil
	case a2aRejected:
		return wire.TaskRejected, nil
	case a2aAuthRequired:
		return wire.TaskAuthRequired, nil
	case a2aUnspecified:
		return wire.TaskUnknown, nil
	default:
		return 0, &BridgeError{Reason: "unknown A2A task state " + word}
	}
}

// AnswerState is the state an answering envelope reports.
//
// An envelope that names its state is believed. One that does not is read from
// its kind, because an answer that arrived is either the work finishing or the
// work failing, and a bridge must say which.
func AnswerState(envelope *wire.AgentEnvelope) wire.TaskState {
	if envelope.TaskState != nil {
		return *envelope.TaskState
	}
	if envelope.Kind == wire.KindError {
		return wire.TaskFailed
	}
	return wire.TaskCompleted
}

// TaskFromEnvelope is the outbound view: the envelope answering a task becomes
// the A2A task.
//
// A non-empty body becomes the one text artifact, named by the answer's record
// so a caller can tie the artifact back to the record that produced it.
func TaskFromEnvelope(taskID, contextID string, envelope *wire.AgentEnvelope) A2ATask {
	task := A2ATask{
		ID:        taskID,
		ContextID: contextID,
		Status:    A2ATaskStatus{State: A2AState(AnswerState(envelope))},
	}
	if len(envelope.Body) == 0 {
		return task
	}
	artifactID := taskID + "-artifact"
	if envelope.Record != nil {
		artifactID = envelope.Record.String()
	}
	task.Artifacts = []A2AArtifact{{
		ArtifactID: artifactID,
		Parts:      []A2APart{{Text: string(envelope.Body)}},
	}}
	return task
}

// ContextIDOf is the context a task belongs to: the one its params named, or
// the task's own conversation when they named none.
//
// A caller that groups several tasks under one context expects them to stay
// grouped, so a named context wins over the conversation.
func ContextIDOf(paramsJSON []byte, task wire.ConversationID) string {
	var params struct {
		Message struct {
			ContextID string `json:"contextId"`
		} `json:"message"`
	}
	if err := json.Unmarshal(paramsJSON, &params); err == nil &&
		params.Message.ContextID != "" {
		return params.Message.ContextID
	}
	return task.String()
}

// TextFromParams is the text an A2A message carries, joined from its parts.
//
// Parts are joined rather than only the first taken, because a caller that
// split a message across parts meant all of it.
func TextFromParams(paramsJSON []byte) string {
	var params struct {
		Message struct {
			Parts []A2APart `json:"parts"`
		} `json:"message"`
	}
	if err := json.Unmarshal(paramsJSON, &params); err != nil {
		return ""
	}
	texts := make([]string, 0, len(params.Message.Parts))
	for _, part := range params.Message.Parts {
		if part.Text != "" {
			texts = append(texts, part.Text)
		}
	}
	return strings.Join(texts, "")
}

// BridgeError reports a request a bridge cannot translate.
//
// It is kept distinct from a transport failure because the remedy differs:
// nothing is wrong with the connection, the edge sent something this bridge
// does not implement.
type BridgeError struct{ Reason string }

func (e *BridgeError) Error() string { return e.Reason }
