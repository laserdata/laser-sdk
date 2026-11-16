package wire

import (
	"fmt"

	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
)

// SessionOutcomeKind discriminates a successful session read.
type SessionOutcomeKind uint8

const (
	SessionInfoOutcome SessionOutcomeKind = iota
	SessionPageOutcome
	SessionEventsOutcome
	SessionStateOutcome
	SessionLinksOutcome
	SessionSourcesOutcome
	SessionChangesOutcome
)

// SessionOutcome is the successful outcome of a session read, shaped per
// request.
type SessionOutcome struct {
	Kind    SessionOutcomeKind
	Info    *SessionInfo
	Page    *SessionPage
	Events  *SessionEventsPage
	State   *SessionStateView
	Links   *SessionLinksView
	Sources *SessionSources
	Changes *SessionChanges
}

// The externally tagged session outcome names.
const (
	sessionInfoVariant    = "Info"
	sessionPageVariant    = "Page"
	sessionEventsVariant  = "Events"
	sessionStateVariant   = "State"
	sessionLinksVariant   = "Links"
	sessionSourcesVariant = "Sources"
	sessionChangesVariant = "Changes"
)

// MarshalCBOR writes the externally tagged outcome.
func (outcome SessionOutcome) MarshalCBOR() (cborform.Value, error) {
	switch outcome.Kind {
	case SessionInfoOutcome:
		if outcome.Info == nil {
			return nil, fmt.Errorf("wire: an info outcome carries no session")
		}
		return marshalVariantStruct(sessionInfoVariant, *outcome.Info)
	case SessionPageOutcome:
		if outcome.Page == nil {
			return nil, fmt.Errorf("wire: a page outcome carries no page")
		}
		return marshalVariantStruct(sessionPageVariant, *outcome.Page)
	case SessionEventsOutcome:
		if outcome.Events == nil {
			return nil, fmt.Errorf("wire: an events outcome carries no page")
		}
		return marshalVariantStruct(sessionEventsVariant, *outcome.Events)
	case SessionStateOutcome:
		if outcome.State == nil {
			return nil, fmt.Errorf("wire: a state outcome carries no document")
		}
		return marshalVariantStruct(sessionStateVariant, *outcome.State)
	case SessionLinksOutcome:
		if outcome.Links == nil {
			return nil, fmt.Errorf("wire: a links outcome carries no links")
		}
		return marshalVariantStruct(sessionLinksVariant, *outcome.Links)
	case SessionSourcesOutcome:
		if outcome.Sources == nil {
			return nil, fmt.Errorf("wire: a sources outcome carries no sources")
		}
		return marshalVariantStruct(sessionSourcesVariant, *outcome.Sources)
	case SessionChangesOutcome:
		if outcome.Changes == nil {
			return nil, fmt.Errorf("wire: a changes outcome carries no rows")
		}
		return marshalVariantStruct(sessionChangesVariant, *outcome.Changes)
	default:
		return nil, fmt.Errorf("wire: unknown session outcome %d", outcome.Kind)
	}
}

// UnmarshalCBOR reads the externally tagged outcome.
func (outcome *SessionOutcome) UnmarshalCBOR(item cborform.Value) error {
	name, payload, err := readVariant(item)
	if err != nil {
		return err
	}
	if err := requirePayload("SessionOutcome", name, payload); err != nil {
		return err
	}
	switch name {
	case sessionInfoVariant:
		var info SessionInfo
		if err := fromValueInto(payload, &info); err != nil {
			return err
		}
		*outcome = SessionOutcome{Kind: SessionInfoOutcome, Info: &info}
	case sessionPageVariant:
		var page SessionPage
		if err := fromValueInto(payload, &page); err != nil {
			return err
		}
		*outcome = SessionOutcome{Kind: SessionPageOutcome, Page: &page}
	case sessionEventsVariant:
		var events SessionEventsPage
		if err := fromValueInto(payload, &events); err != nil {
			return err
		}
		*outcome = SessionOutcome{Kind: SessionEventsOutcome, Events: &events}
	case sessionStateVariant:
		var state SessionStateView
		if err := fromValueInto(payload, &state); err != nil {
			return err
		}
		*outcome = SessionOutcome{Kind: SessionStateOutcome, State: &state}
	case sessionLinksVariant:
		var links SessionLinksView
		if err := fromValueInto(payload, &links); err != nil {
			return err
		}
		*outcome = SessionOutcome{Kind: SessionLinksOutcome, Links: &links}
	case sessionSourcesVariant:
		var sources SessionSources
		if err := fromValueInto(payload, &sources); err != nil {
			return err
		}
		*outcome = SessionOutcome{Kind: SessionSourcesOutcome, Sources: &sources}
	case sessionChangesVariant:
		var changes SessionChanges
		if err := fromValueInto(payload, &changes); err != nil {
			return err
		}
		*outcome = SessionOutcome{Kind: SessionChangesOutcome, Changes: &changes}
	default:
		return unknownVariant("SessionOutcome", name)
	}
	return nil
}

// SessionErrorKind discriminates a session read failure.
type SessionErrorKind uint8

const (
	SessionUnsupported SessionErrorKind = iota
	SessionNotFound
	SessionNotRegistered
	SessionInvalid
	SessionUnauthorized
	// SessionStale means the request named a stream or topic generation the
	// index does not hold.
	SessionStale
	SessionBackend
	// SessionUnavailable means the identical request may succeed later.
	SessionUnavailable
)

// SessionError is why a session read failed.
type SessionError struct {
	Kind    SessionErrorKind
	Message string
}

// The externally tagged session error names.
const (
	sessionUnsupportedVariant   = "Unsupported"
	sessionNotFoundVariant      = "NotFound"
	sessionNotRegisteredVariant = "NotRegistered"
	sessionInvalidVariant       = "Invalid"
	sessionUnauthorizedVariant  = "Unauthorized"
	sessionStaleVariant         = "Stale"
	sessionBackendVariant       = "Backend"
	sessionUnavailableVariant   = "Unavailable"
)

var sessionErrorNames = map[SessionErrorKind]string{
	SessionUnsupported:   sessionUnsupportedVariant,
	SessionNotFound:      sessionNotFoundVariant,
	SessionNotRegistered: sessionNotRegisteredVariant,
	SessionInvalid:       sessionInvalidVariant,
	SessionUnauthorized:  sessionUnauthorizedVariant,
	SessionStale:         sessionStaleVariant,
	SessionBackend:       sessionBackendVariant,
	SessionUnavailable:   sessionUnavailableVariant,
}

// MarshalCBOR writes the externally tagged error.
func (sessionError SessionError) MarshalCBOR() (cborform.Value, error) {
	name, known := sessionErrorNames[sessionError.Kind]
	if !known {
		return nil, fmt.Errorf("wire: unknown session error %d", sessionError.Kind)
	}
	return taggedVariant(name, cborform.Text(sessionError.Message)), nil
}

// UnmarshalCBOR reads the externally tagged error.
func (sessionError *SessionError) UnmarshalCBOR(item cborform.Value) error {
	name, payload, err := readVariant(item)
	if err != nil {
		return err
	}
	if err := requirePayload("SessionError", name, payload); err != nil {
		return err
	}
	message, ok := payload.(cborform.Text)
	if !ok {
		return fmt.Errorf("wire: %q carries a message, got %T", name, payload)
	}
	for kind, candidate := range sessionErrorNames {
		if candidate == name {
			*sessionError = SessionError{Kind: kind, Message: string(message)}
			return nil
		}
	}
	return unknownVariant("SessionError", name)
}

// Error renders the failure, matching the Rust messages.
func (sessionError SessionError) Error() string {
	switch sessionError.Kind {
	case SessionUnsupported:
		return "sessions not supported: " + sessionError.Message
	case SessionNotFound:
		return "session not found: " + sessionError.Message
	case SessionNotRegistered:
		return "stream is not registered for sessions: " + sessionError.Message
	case SessionInvalid:
		return "invalid session request: " + sessionError.Message
	case SessionUnauthorized:
		return "session read not authorized: " + sessionError.Message
	case SessionStale:
		return "stale session generation: " + sessionError.Message
	case SessionBackend:
		return "session backend error: " + sessionError.Message
	case SessionUnavailable:
		return "temporarily unavailable: " + sessionError.Message
	default:
		return "unknown session error"
	}
}

// ResultCode projects the failure onto the shared outcome dictionary.
func (sessionError SessionError) ResultCode() ResultCode {
	switch sessionError.Kind {
	case SessionUnsupported:
		return ResultUnsupported
	case SessionNotFound, SessionNotRegistered:
		return ResultNotFound
	case SessionInvalid:
		return ResultInvalidArgument
	case SessionUnauthorized:
		return ResultForbidden
	case SessionStale:
		return ResultStaleGeneration
	case SessionBackend:
		return ResultBackend
	case SessionUnavailable:
		return ResultUnavailable
	default:
		return ResultBackend
	}
}

// SessionReply is the reply to every session read.
type SessionReply struct {
	Err     *SessionError
	Outcome *SessionOutcome
}

// IsOk reports a successful reply.
func (reply SessionReply) IsOk() bool { return reply.Err == nil && reply.Outcome != nil }

// MarshalCBOR writes the externally tagged reply.
func (reply SessionReply) MarshalCBOR() (cborform.Value, error) {
	switch {
	case reply.Err != nil && reply.Outcome != nil:
		return nil, fmt.Errorf("wire: a session reply is either an outcome or an error")
	case reply.Err != nil:
		payload, err := reply.Err.MarshalCBOR()
		if err != nil {
			return nil, err
		}
		return taggedVariant(kvErrVariant, payload), nil
	case reply.Outcome != nil:
		payload, err := reply.Outcome.MarshalCBOR()
		if err != nil {
			return nil, err
		}
		return taggedVariant(kvOkVariant, payload), nil
	default:
		return nil, fmt.Errorf("wire: an empty session reply")
	}
}

// UnmarshalCBOR reads the externally tagged reply.
func (reply *SessionReply) UnmarshalCBOR(item cborform.Value) error {
	name, payload, err := readVariant(item)
	if err != nil {
		return err
	}
	if err := requirePayload("SessionReply", name, payload); err != nil {
		return err
	}
	switch name {
	case kvOkVariant:
		var outcome SessionOutcome
		if err := outcome.UnmarshalCBOR(payload); err != nil {
			return err
		}
		*reply = SessionReply{Outcome: &outcome}
		return nil
	case kvErrVariant:
		var sessionError SessionError
		if err := sessionError.UnmarshalCBOR(payload); err != nil {
			return err
		}
		*reply = SessionReply{Err: &sessionError}
		return nil
	default:
		return unknownVariant("SessionReply", name)
	}
}
