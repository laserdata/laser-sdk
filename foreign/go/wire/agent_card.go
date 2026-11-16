package wire

import (
	"fmt"

	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
)

// Health is an advertised skill's health, a pinned code dictionary.
//
// A code this build does not know passes through rather than failing the card,
// so a newer peer's health never makes a whole capability undecodable.
type Health uint8

const (
	HealthHealthy     Health = 1
	HealthDegraded    Health = 2
	HealthUnavailable Health = 3
)

// IsNamed reports whether this build names the health code.
func (health Health) IsNamed() bool {
	return health >= HealthHealthy && health <= HealthUnavailable
}

// ContentRefKind discriminates how a capability names its content shape.
type ContentRefKind string

const (
	// ContentRefContentType names one of the first-party codecs.
	ContentRefContentType ContentRefKind = "content_type"
	// ContentRefSchemaID names a registered schema.
	ContentRefSchemaID ContentRefKind = "schema_id"
)

// ContentRef is the content shape a capability takes or produces.
//
// The tag separates a codec name from a schema id: without it, a name like
// "json" would be ambiguous between the two, and a content-type name from a
// newer peer would read as a schema id.
type ContentRef struct {
	Kind        ContentRefKind
	ContentType ContentType
	SchemaID    string
}

// The content reference constructors.
func ContentTypeRef(contentType ContentType) ContentRef {
	return ContentRef{Kind: ContentRefContentType, ContentType: contentType}
}

func SchemaIDRef(id string) ContentRef {
	return ContentRef{Kind: ContentRefSchemaID, SchemaID: id}
}

// MarshalCBOR writes the externally tagged reference.
func (reference ContentRef) MarshalCBOR() (cborform.Value, error) {
	switch reference.Kind {
	case ContentRefContentType:
		payload, err := toValueOf(reference.ContentType)
		if err != nil {
			return nil, err
		}
		return taggedVariant(string(ContentRefContentType), payload), nil
	case ContentRefSchemaID:
		return taggedVariant(string(ContentRefSchemaID),
			cborform.Text(reference.SchemaID)), nil
	default:
		return nil, fmt.Errorf("wire: unknown content reference %q", reference.Kind)
	}
}

// UnmarshalCBOR reads the externally tagged reference.
func (reference *ContentRef) UnmarshalCBOR(item cborform.Value) error {
	name, payload, err := readVariant(item)
	if err != nil {
		return err
	}
	if err := requirePayload("ContentRef", name, payload); err != nil {
		return err
	}
	switch ContentRefKind(name) {
	case ContentRefContentType:
		var contentType ContentType
		if err := fromValueInto(payload, &contentType); err != nil {
			return err
		}
		*reference = ContentTypeRef(contentType)
		return nil
	case ContentRefSchemaID:
		text, ok := payload.(cborform.Text)
		if !ok {
			return fmt.Errorf("wire: a schema id reference is text, got %T", payload)
		}
		*reference = SchemaIDRef(string(text))
		return nil
	default:
		return unknownVariant("ContentRef", name)
	}
}

// CapabilityDescriptor is one structured capability on an agent card: which
// skill, its content shapes, and the advisory classes a router picks by.
//
// The cost and latency classes are opaque to the protocol: lower is cheaper
// and faster, and what the numbers mean is the deployment's business.
type CapabilityDescriptor struct {
	SkillID        string      `cbor:"skill_id"`
	Input          *ContentRef `cbor:"input,omitempty"`
	Output         *ContentRef `cbor:"output,omitempty"`
	CostClass      *uint8      `cbor:"cost_class,omitempty"`
	LatencyClass   *uint8      `cbor:"latency_class,omitempty"`
	MaxConcurrency *uint32     `cbor:"max_concurrency,omitempty"`
	Health         *Health     `cbor:"health,omitempty"`
	// Load is the current load, per mille of advertised capacity.
	Load *uint16 `cbor:"load,omitempty"`
}

// AgentCard is the pinned minimal body of a liveness or capability card.
//
// Without a pinned shape every bridge and viewer invents its own card variant.
// The publishing agent rides the envelope's source, so the card carries only
// what discovery needs. Anything richer is application data in the metadata
// or a follow-up body, never new card fields by convention.
type AgentCard struct {
	Name         *string                `cbor:"name,omitempty"`
	Version      *string                `cbor:"version,omitempty"`
	Capabilities []CapabilityDescriptor `cbor:"capabilities,omitempty"`
	// TTLMicros is how long the card stays fresh. A card older than its
	// lifetime means a dead agent, which is what makes a card liveness and not
	// only discovery.
	TTLMicros *uint64 `cbor:"ttl_micros,omitempty"`
	// Inbox is the topic the agent consumes its work on, the durable inbox
	// a route resolves to when no live presence advertises one.
	Inbox *string `cbor:"inbox,omitempty"`
}

// Validate checks a decoded card against the caps.
func (card AgentCard) Validate() error {
	if err := capString(card.Name, "name"); err != nil {
		return err
	}
	if err := capString(card.Version, "version"); err != nil {
		return err
	}
	if len(card.Capabilities) > MaxCardCapabilities {
		return &InvalidError{Reason: fmt.Sprintf(
			"a card advertises %d capabilities, the cap is %d",
			len(card.Capabilities), MaxCardCapabilities)}
	}
	for _, capability := range card.Capabilities {
		skill := capability.SkillID
		if err := capString(&skill, "capability skill_id"); err != nil {
			return err
		}
	}
	return nil
}

// capString bounds one optional vocabulary string.
func capString(value *string, field string) error {
	if value == nil {
		return nil
	}
	if len(*value) == 0 || len(*value) > MaxAgentStringBytes {
		return &InvalidError{Reason: fmt.Sprintf(
			"%s must contain 1..=%d bytes", field, MaxAgentStringBytes)}
	}
	return nil
}

// AgentPresence is the live presence an agent advertises in its connection
// metadata, which the discovery read surfaces per connection.
//
// It is deliberately distinct from an agent card. A card is durable,
// replayable capability that outlives a brief disconnect. Presence is live
// transport truth: it vanishes on disconnect and answers where to send this
// agent work right now, through the inbox topic it is currently consuming,
// which may be a topic it created for one workflow and drops when done.
//
// The body rides the opaque connection-metadata bytes with no envelope header,
// so it carries its own version.
type AgentPresence struct {
	V     uint32  `cbor:"v"`
	Agent AgentID `cbor:"agent"`
	// Inbox is the topic this agent consumes its work on, within the stream
	// its connection is scoped to. Absent means liveness-only presence with no
	// declared inbox, so routing falls through to another signal.
	Inbox *string `cbor:"inbox,omitempty"`
}

// NewAgentPresence builds presence at the current version.
func NewAgentPresence(agent AgentID) AgentPresence {
	return AgentPresence{V: PresenceOpVersion, Agent: agent}
}

// Validate checks the presence version and its inbox name.
func (presence AgentPresence) Validate() error {
	if presence.V != PresenceOpVersion {
		return &InvalidError{Reason: fmt.Sprintf(
			"presence version must be %d, got %d", PresenceOpVersion, presence.V)}
	}
	if presence.Agent.IsZero() {
		return &InvalidError{Reason: "presence must name its agent"}
	}
	if presence.Inbox != nil {
		return ValidateSafelistedName("inbox", *presence.Inbox, MaxAgentStringBytes)
	}
	return nil
}

// DeadLetterReason is why a record could not be handled, a pinned code
// dictionary. An unknown code passes through rather than failing the record.
type DeadLetterReason uint8

const (
	DeadLetterRetryExhausted   DeadLetterReason = 1
	DeadLetterRejected         DeadLetterReason = 2
	DeadLetterDecodeFailed     DeadLetterReason = 3
	DeadLetterDeadlineExceeded DeadLetterReason = 4
)

// IsNamed reports whether this build names the reason code.
func (reason DeadLetterReason) IsNamed() bool {
	return reason >= DeadLetterRetryExhausted && reason <= DeadLetterDeadlineExceeded
}

// AgentDeadLetter is one record a handler gave up on, with the payload kept so
// it can be replayed once the cause is fixed.
type AgentDeadLetter struct {
	Source   LogPosition      `cbor:"source"`
	Reason   DeadLetterReason `cbor:"reason"`
	Attempts uint32           `cbor:"attempts"`
	Detail   *string          `cbor:"detail,omitempty"`
	Payload  []byte           `cbor:"payload"`
}

// SessionParking is one record a session set aside, with the role that must
// act before it resumes.
type SessionParking struct {
	Source  SourceRef   `cbor:"source"`
	Role    AgentID     `cbor:"role"`
	Request LogPosition `cbor:"request"`
}

// Validate checks that the parked record names a log record and its topic
// generation, which is what makes it resumable after a topic is recreated.
func (parking SessionParking) Validate() error {
	if parking.Source.Kind != SourceMessage || parking.Source.Message == nil {
		return &InvalidError{Reason: "a parked record must point at a log record"}
	}
	if parking.Source.Message.Generation == nil {
		return &InvalidError{
			Reason: "a parked record must name its source topic generation"}
	}
	return nil
}

// BodyRef is a capsule pointing at content stored outside the record, the
// claim-check form for a body too large to ride inline.
type BodyRef struct {
	// Reference is where the bytes live: a URI, an object key, or a key-value
	// key.
	Reference string `cbor:"reference"`
	SizeBytes uint64 `cbor:"size_bytes"`
	// SHA256 is the digest of the externalized content, so a reader can tell
	// it got the bytes the producer meant.
	SHA256 []byte `cbor:"sha256"`
	// Encryption names the scheme the content is sealed with. Absent means
	// plaintext.
	Encryption *uint8 `cbor:"encryption,omitempty"`
}

// NewBodyRef is a plaintext reference to externalized content.
func NewBodyRef(reference string, sizeBytes uint64, sha256 [DigestBytes]byte) BodyRef {
	return BodyRef{
		Reference: reference, SizeBytes: sizeBytes, SHA256: append([]byte{}, sha256[:]...),
	}
}

// Validate checks the reference and its digest.
func (reference BodyRef) Validate() error {
	if len(reference.Reference) == 0 ||
		len(reference.Reference) > MaxBodyReferenceBytes {
		return &InvalidError{Reason: fmt.Sprintf(
			"a body reference must contain 1..=%d bytes", MaxBodyReferenceBytes)}
	}
	if len(reference.SHA256) != DigestBytes {
		return &InvalidError{Reason: fmt.Sprintf(
			"a body reference digest is %d bytes, must be %d",
			len(reference.SHA256), DigestBytes)}
	}
	return nil
}
