package bridges

import (
	"encoding/json"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// A2AProtocolVersion is the A2A protocol version this bridge's card declares.
const A2AProtocolVersion = "1.0"

// A2AJSONRPCBinding is the JSON-RPC binding label of a v1.0 AgentInterface.
const A2AJSONRPCBinding = "JSONRPC"

// AgentCard is the bridge's A2A discovery document, the v1.0 AgentCard shape.
//
// The endpoint and its protocol version live in SupportedInterfaces: v1.0
// dropped the top-level protocolVersion, url and preferredTransport. It is
// not wire.AgentCard, which is the contract's own presence record: the two
// describe the same agent to different audiences and are free to differ.
type AgentCard struct {
	Name                string                `json:"name"`
	Description         string                `json:"description"`
	Version             string                `json:"version"`
	SupportedInterfaces []AgentInterface      `json:"supportedInterfaces"`
	Capabilities        AgentCardCapabilities `json:"capabilities"`
	DefaultInputModes   []string              `json:"defaultInputModes"`
	DefaultOutputModes  []string              `json:"defaultOutputModes"`
	Skills              []AgentSkill          `json:"skills"`
	// Signatures are detached JWS signatures over the canonicalized card,
	// omitted when the card is unsigned.
	Signatures []AgentCardSignature `json:"signatures,omitempty"`
}

// AgentInterface is one v1.0 AgentInterface: where the agent serves and over
// what binding.
type AgentInterface struct {
	URL             string `json:"url"`
	ProtocolBinding string `json:"protocolBinding"`
	ProtocolVersion string `json:"protocolVersion"`
}

// AgentCardSignature is one RFC 7515 JWS over the card, detached: the payload
// is the canonicalized card without its signatures, which the verifier
// rebuilds.
type AgentCardSignature = laser.AgentCardSignature

// SignedCard returns the discovery card with a detached Ed25519 signature.
func (bridge *A2ABridge) SignedCard(key *laser.SigningKey) (*AgentCard, error) {
	card, err := bridge.Card()
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(card)
	if err != nil {
		return nil, err
	}
	signature, err := laser.SignCardValue(key, raw)
	if err != nil {
		return nil, err
	}
	card.Signatures = []AgentCardSignature{*signature}
	return card, nil
}

// AgentCardCapabilities are the A2A capability flags of a card.
type AgentCardCapabilities struct {
	// Streaming is never set: AGDX chunk channels ride the log and are read
	// over Apache Iggy's own transport rather than over SSE.
	Streaming              bool `json:"streaming"`
	PushNotifications      bool `json:"pushNotifications"`
	StateTransitionHistory bool `json:"stateTransitionHistory"`
	// ExtendedAgentCard is never set: the well-known card is the whole truth
	// this bridge serves.
	ExtendedAgentCard bool `json:"extendedAgentCard"`
}

// AgentSkill is one skill advertised on the card.
type AgentSkill struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Tags        []string `json:"tags"`
	// InputModes and OutputModes are the content modes the skill accepts and
	// produces, omitted when the capability named none.
	InputModes  []string `json:"inputModes,omitempty"`
	OutputModes []string `json:"outputModes,omitempty"`
}

// ContentRefMode projects a capability's content reference onto an A2A
// content mode string: a content-type code onto its media type, a registered
// schema id onto an AGDX schema media type.
//
// An unknown code falls back to opaque bytes rather than being dropped,
// because a skill that declared a mode this client cannot name still accepts
// something, and saying nothing would read as accepting anything.
func ContentRefMode(reference wire.ContentRef) string {
	if reference.Kind == wire.ContentRefSchemaID {
		return "application/x-agdx-schema;id=" + reference.SchemaID
	}
	switch reference.ContentType {
	case wire.ContentJSON:
		return "application/json"
	case wire.ContentMsgpack:
		return "application/msgpack"
	case wire.ContentCBOR:
		return "application/cbor"
	case wire.ContentBSON:
		return "application/bson"
	case wire.ContentAvro:
		return "application/avro"
	case wire.ContentProtobuf:
		return "application/protobuf"
	case wire.ContentArrow:
		return "application/vnd.apache.arrow.stream"
	case wire.ContentBodyRef:
		return "application/x-agdx-ref"
	case wire.ContentAny:
		return "*/*"
	default:
		return "application/octet-stream"
	}
}
