package wire

import (
	"fmt"
	"unicode"
)

// SdkInfo names the client that opened a session.
type SdkInfo struct {
	Language string `cbor:"language"`
	Version  string `cbor:"version"`
}

// Budget is the optional token and cost ceiling requested for a session.
type Budget struct {
	Tokens     *uint64 `cbor:"tokens,omitempty"`
	CostMicros *uint64 `cbor:"cost_micros,omitempty"`
}

// SessionStart is the first status body of a session.
type SessionStart struct {
	Label             *string         `cbor:"label,omitempty"`
	Namespace         *string         `cbor:"namespace,omitempty"`
	Agent             AgentID         `cbor:"agent"`
	SDK               SdkInfo         `cbor:"sdk"`
	Parent            *ConversationID `cbor:"parent,omitempty"`
	Root              *ConversationID `cbor:"root,omitempty"`
	IdleTimeoutMicros *uint64         `cbor:"idle_timeout_micros,omitempty"`
	Budget            *Budget         `cbor:"budget,omitempty"`
	Tags              []string        `cbor:"tags,omitempty"`
}

// Validate checks a session start against its caps and ancestry rule.
func (start *SessionStart) Validate() error {
	if start.Label != nil {
		if len(*start.Label) > MaxSessionLabelBytes || hasControl(*start.Label) {
			return &InvalidError{Reason: "session label exceeds its cap or contains a control character"}
		}
	}
	if start.Namespace != nil {
		if err := ValidateNamespace(*start.Namespace); err != nil {
			return err
		}
	}
	if start.Root != nil && start.Parent == nil {
		return &InvalidError{Reason: "session root requires a parent"}
	}
	if len(start.Tags) > MaxMemoryTags {
		return &InvalidError{Reason: "session has too many tags"}
	}
	for _, tag := range start.Tags {
		if len(tag) > MaxMemoryTagBytes {
			return &InvalidError{Reason: "session tag exceeds its byte cap"}
		}
	}
	return nil
}

func hasControl(value string) bool {
	for _, character := range value {
		if unicode.IsControl(character) {
			return true
		}
	}
	return false
}

// ValidateNamespace applies the shared namespace rule: non-empty, within the
// cap, and free of ASCII control characters.
func ValidateNamespace(namespace string) error {
	if namespace == "" {
		return &InvalidError{Reason: "namespace must not be empty"}
	}
	if len(namespace) > MaxNamespaceBytes {
		return &InvalidError{Reason: fmt.Sprintf(
			"namespace is %dB, exceeds cap %dB", len(namespace), MaxNamespaceBytes)}
	}
	for i := 0; i < len(namespace); i++ {
		if namespace[i] < 0x20 || namespace[i] == 0x7f {
			return &InvalidError{Reason: "namespace must not contain ASCII control characters"}
		}
	}
	return nil
}

// SessionTransition is a session status update that can acknowledge a control
// record.
type SessionTransition struct {
	Actor        *AgentID     `cbor:"actor,omitempty"`
	Acknowledges *LogPosition `cbor:"acknowledges,omitempty"`
}

// SessionEnd is the body of a terminal session status.
type SessionEnd struct {
	Reason *string         `cbor:"reason,omitempty"`
	Error  *AgentErrorBody `cbor:"error,omitempty"`
}

// SessionPauseRequest is the body of a session_pause request on agent.control:
// the agents whose acknowledgments complete the pause, frozen when the request
// is written. An empty set names no agent.
type SessionPauseRequest struct {
	Participants []AgentID `cbor:"participants,omitempty"`
}

// AgentErrorBody is the structured body of an error record.
type AgentErrorBody struct {
	Code    AgentErrorCode `cbor:"code"`
	Message *string        `cbor:"message,omitempty"`
	// Retryable carries no skip rule, so false is written explicitly.
	Retryable bool             `cbor:"retryable"`
	Detail    map[string]Value `cbor:"detail,omitempty"`
}

// EstimateTokens is the estimated token count of a byte length: one token per
// four bytes, rounded up. Every SDK and the session fold use this one estimate.
func EstimateTokens(bytes int) uint64 {
	if bytes <= 0 {
		return 0
	}
	return uint64((bytes + 3) / 4)
}

// The metadata keys the contract pins for model-call attribution.
const (
	MetadataRequestModel  = "gen_ai.request.model"
	MetadataResponseModel = "gen_ai.response.model"
	MetadataProviderName  = "gen_ai.provider.name"
)
