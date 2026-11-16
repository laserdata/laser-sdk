package wire

import (
	"encoding/binary"
	"math"
	"sort"
	"strconv"
)

// Header conventions. Format framing rides the agdx.* keys and index values
// ride the agdx.idx. namespace as small scalars.
//
// The reserved keys are deliberately short and their values typed, because
// these headers ride every message: at millions of messages the key and value
// bytes are real bandwidth. The byte layout also matches the planned native
// Iggy reserved-block carve, so that migration is a dispatch-key swap.
const (
	// HeaderContentType carries the wire codec tag as a u8 code.
	HeaderContentType = "agdx.ct"
	// HeaderSchemaID carries the u32 writer-schema id for a schema-first
	// codec such as Avro or Protobuf.
	HeaderSchemaID = "agdx.sid"
	// HeaderLogicalSchemaFingerprint carries the 32-byte logical schema
	// fingerprint for an Arrow IPC stream.
	HeaderLogicalSchemaFingerprint = "agdx.sfp"
	// HeaderIdxPrefix marks an indexed-field header, agdx.idx.<name>.
	HeaderIdxPrefix = "agdx.idx."
	// HeaderInlinePayload is the per-record inline-payload override, a bool.
	HeaderInlinePayload = "agdx.inline"
	// HeaderProjectionRef says which projection a record routes to. An opaque
	// string by design, distinct from agdx.sid, which selects a codec's
	// writer schema rather than a materialization rule.
	HeaderProjectionRef = "agdx.ref"
	// HeaderCorrelationID is the generic request/reply correlation id, kept
	// independent of any agentic header so the substrate works without
	// provenance.
	HeaderCorrelationID = "agdx.corr"
)

// Well-known projected field names, the agdx.idx.* value after the prefix.
const (
	FieldMessageType = "message_type"
	FieldTS          = "ts"
	// VectorField is the default field and pointer for the embedding vector.
	VectorField = "embedding"
	// WindowStart carries a tumbling window's lower edge in epoch micros on a
	// result row, byte-identical to the Cloud's window_start column.
	WindowStart = "window_start"
)

// On-wire header caps, at the Iggy level rather than provenance-specific.
const (
	// HeaderSoftCap is the soft cap on total header bytes per record.
	HeaderSoftCap = 1024
	// HeaderFramingBytes is the per-header wire overhead counted toward the
	// soft cap: key length, value kind and value length.
	HeaderFramingBytes = 9
	// HeaderValueMax is the most bytes one header value may carry, since Iggy
	// serializes the length as a u8.
	HeaderValueMax = 255
)

// The provenance dictionary. The OTel gen_ai.* keys stay spec-exact so traces
// correlate across tooling. The custom keys are short because they ride every
// agentic message.
const (
	// HeaderConversationID is OTel gen_ai.conversation.id.
	HeaderConversationID = "gen_ai.conversation.id"
	// ConversationField is the projected-column name a deployment
	// materializes the conversation header under, so every projection row is
	// filterable by its conversation without a producer stamping an index
	// header.
	ConversationField = "conversation_id"

	HeaderAgentID            = "gen_ai.agent.id"
	HeaderRequestModel       = "gen_ai.request.model"
	HeaderResponseModel      = "gen_ai.response.model"
	HeaderProviderName       = "gen_ai.provider.name"
	HeaderUsageInputTokens   = "gen_ai.usage.input_tokens"
	HeaderUsageOutputTokens  = "gen_ai.usage.output_tokens"
	HeaderCausalParent       = "agdx.cause"
	HeaderParentConversation = "agdx.parent_conv"
	HeaderRootConversation   = "agdx.root_conv"
	HeaderTargetAgentID      = "agdx.to"
	// HeaderDelegatedBy is a reserved alias for an on-behalf-of subject.
	// Signed delegation uses the envelope metadata key instead.
	HeaderDelegatedBy    = "agdx.on_behalf_of"
	HeaderIdempotencyKey = "agdx.idem"
	HeaderDeadline       = "agdx.deadline"
	HeaderCostUSD        = "agdx.cost"
	// HeaderFence is the strictly-monotonic per-task fence the producer held.
	// A consumer rejects a log-resident effect whose fence is below the
	// highest it has accepted for the task, so a stale-holder replay cannot
	// re-fire.
	HeaderFence = "agdx.fence"
	// HeaderAgentVersion is the envelope's wire version, riding outside the
	// CBOR body so projections, viewers and rolling-upgrade consumers pick a
	// decoder without decoding the body.
	HeaderAgentVersion = "agdx.av"
)

// Memory scope, carried as headers so the fold keys the read view on the scope
// rather than the topic, letting one shared topic resolve every context.
const (
	HeaderMemoryNamespace = "agdx.mem.ns"
	HeaderMemoryUser      = "agdx.mem.user"
	HeaderMemoryApp       = "agdx.mem.app"
)

// HeaderFieldKind discriminates a typed header value.
type HeaderFieldKind uint8

// The typed header value kinds a record carries.
const (
	HeaderText HeaderFieldKind = iota
	HeaderUint8
	HeaderUint32
	HeaderUint64
	HeaderFloat64
)

// HeaderField is one typed user-header value as a record carries it.
type HeaderField struct {
	Kind    HeaderFieldKind
	Text    string
	Uint8   uint8
	Uint32  uint32
	Uint64  uint64
	Float64 float64
}

// The HeaderField constructors.
func TextHeader(value string) HeaderField   { return HeaderField{Kind: HeaderText, Text: value} }
func Uint8Header(value uint8) HeaderField   { return HeaderField{Kind: HeaderUint8, Uint8: value} }
func Uint32Header(value uint32) HeaderField { return HeaderField{Kind: HeaderUint32, Uint32: value} }
func Uint64Header(value uint64) HeaderField { return HeaderField{Kind: HeaderUint64, Uint64: value} }
func Float64Header(value float64) HeaderField {
	return HeaderField{Kind: HeaderFloat64, Float64: value}
}

// String renders a header value for display and for the text-valued keys.
func (field HeaderField) String() string {
	switch field.Kind {
	case HeaderText:
		return field.Text
	case HeaderUint8:
		return strconv.FormatUint(uint64(field.Uint8), 10)
	case HeaderUint32:
		return strconv.FormatUint(uint64(field.Uint32), 10)
	case HeaderUint64:
		return strconv.FormatUint(field.Uint64, 10)
	default:
		return strconv.FormatFloat(field.Float64, 'g', -1, 64)
	}
}

// Broadcast is the agdx.to value that addresses every agent.
const Broadcast = "*"

// Addressee is who a record is addressed to. Broadcast rides agdx.to as the
// literal star and is never read as an agent id.
type Addressee struct {
	Agent       AgentID
	IsBroadcast bool
}

// AgentAddressee addresses one named agent.
func AgentAddressee(agent AgentID) Addressee { return Addressee{Agent: agent} }

// BroadcastAddressee addresses every agent.
func BroadcastAddressee() Addressee { return Addressee{IsBroadcast: true} }

// String is the agdx.to text.
func (addressee Addressee) String() string {
	if addressee.IsBroadcast {
		return Broadcast
	}
	return addressee.Agent.String()
}

// ParseAddressee reads an agdx.to value. A star is broadcast, and anything
// else must be an agent id.
func ParseAddressee(text string) (Addressee, error) {
	if text == Broadcast {
		return BroadcastAddressee(), nil
	}
	agent, err := NewAgentID(text)
	if err != nil {
		return Addressee{}, &InvalidError{Reason: "agdx.to is not an agent id or `*`"}
	}
	return AgentAddressee(agent), nil
}

// EnvelopeStamp is the version and codec an envelope record stamps on its
// headers. Generic records carry neither.
type EnvelopeStamp struct {
	Version     uint32
	ContentType ContentType
}

// RecordHeaders is the routing and provenance header block of one record.
//
// Ids ride as canonical strings and numbers ride typed, so a plain Iggy
// consumer and a projection can both route on them without decoding the body.
type RecordHeaders struct {
	Envelope       *EnvelopeStamp
	Conversation   ConversationID
	Parent         *ConversationID
	Root           *ConversationID
	Agent          *AgentID
	Addressee      *Addressee
	CausalParent   *string
	IdempotencyKey *string
	Correlation    *string
	Fence          *uint64
	DeadlineMicros *uint64
	InputTokens    *uint64
	OutputTokens   *uint64
	CostUSD        *float64
}

// NewRecordHeaders is the header block of a generic record in a conversation.
func NewRecordHeaders(conversation ConversationID) RecordHeaders {
	return RecordHeaders{Conversation: conversation}
}

// HeadersForEnvelope is the header block of an envelope record: version,
// content type, conversation, ancestry, author and addressee.
//
// A record without a target is addressed to every agent when broadcast is set,
// as every record on a shared session topic must be.
func HeadersForEnvelope(envelope *AgentEnvelope, contentType ContentType, broadcast bool) RecordHeaders {
	headers := NewRecordHeaders(envelope.Conversation)
	headers.Envelope = &EnvelopeStamp{Version: AgentOpVersion, ContentType: contentType}
	headers.Parent = envelope.Parent
	headers.Root = envelope.Root
	source := envelope.Source
	headers.Agent = &source
	switch {
	case envelope.Target != nil:
		addressee := AgentAddressee(*envelope.Target)
		headers.Addressee = &addressee
	case broadcast:
		addressee := BroadcastAddressee()
		headers.Addressee = &addressee
	}
	return headers
}

// HeaderEntry is one key and value of an encoded header block.
type HeaderEntry struct {
	Key   string
	Value HeaderField
}

// Encode renders the header block, sorted by key.
func (headers RecordHeaders) Encode() []HeaderEntry {
	block := make([]HeaderEntry, 0, 16)
	if headers.Envelope != nil {
		code, _ := headers.Envelope.ContentType.Code()
		block = append(block,
			HeaderEntry{HeaderAgentVersion, Uint32Header(headers.Envelope.Version)},
			HeaderEntry{HeaderContentType, Uint8Header(code)},
		)
	}
	block = append(block, HeaderEntry{HeaderConversationID, TextHeader(headers.Conversation.String())})

	if headers.Parent != nil {
		block = append(block, HeaderEntry{HeaderParentConversation, TextHeader(headers.Parent.String())})
	}
	if headers.Root != nil {
		block = append(block, HeaderEntry{HeaderRootConversation, TextHeader(headers.Root.String())})
	}
	if headers.Agent != nil {
		block = append(block, HeaderEntry{HeaderAgentID, TextHeader(headers.Agent.String())})
	}
	if headers.Addressee != nil {
		block = append(block, HeaderEntry{HeaderTargetAgentID, TextHeader(headers.Addressee.String())})
	}
	if headers.CausalParent != nil {
		block = append(block, HeaderEntry{HeaderCausalParent, TextHeader(*headers.CausalParent)})
	}
	if headers.IdempotencyKey != nil {
		block = append(block, HeaderEntry{HeaderIdempotencyKey, TextHeader(*headers.IdempotencyKey)})
	}
	if headers.Correlation != nil {
		block = append(block, HeaderEntry{HeaderCorrelationID, TextHeader(*headers.Correlation)})
	}
	if headers.Fence != nil {
		block = append(block, HeaderEntry{HeaderFence, Uint64Header(*headers.Fence)})
	}
	if headers.DeadlineMicros != nil {
		block = append(block, HeaderEntry{HeaderDeadline, Uint64Header(*headers.DeadlineMicros)})
	}
	if headers.InputTokens != nil {
		block = append(block, HeaderEntry{HeaderUsageInputTokens, Uint64Header(*headers.InputTokens)})
	}
	if headers.OutputTokens != nil {
		block = append(block, HeaderEntry{HeaderUsageOutputTokens, Uint64Header(*headers.OutputTokens)})
	}
	if headers.CostUSD != nil {
		block = append(block, HeaderEntry{HeaderCostUSD, Float64Header(*headers.CostUSD)})
	}

	sort.Slice(block, func(a, b int) bool { return block[a].Key < block[b].Key })
	return block
}

// IggyBytes is the header value as Iggy carries it on the wire: text as UTF-8,
// and the numeric kinds as fixed-width little-endian payloads.
//
// The width and byte order are the substrate's, not the contract's, so a
// consumer reading a record with a plain Iggy client sees the same bytes every
// LaserData client writes.
func (field HeaderField) IggyBytes() []byte {
	switch field.Kind {
	case HeaderText:
		return []byte(field.Text)
	case HeaderUint8:
		return []byte{field.Uint8}
	case HeaderUint32:
		return binary.LittleEndian.AppendUint32(nil, field.Uint32)
	case HeaderUint64:
		return binary.LittleEndian.AppendUint64(nil, field.Uint64)
	default:
		return binary.LittleEndian.AppendUint64(nil, math.Float64bits(field.Float64))
	}
}

// WireSize is the header's contribution to the soft cap: the key, the value
// payload, and the per-header framing overhead.
func (entry HeaderEntry) WireSize() int {
	return len(entry.Key) + len(entry.Value.IggyBytes()) + HeaderFramingBytes
}

// WithinSoftCap reports whether an encoded block stays inside the soft cap on
// total header bytes per record.
func WithinSoftCap(block []HeaderEntry) bool {
	total := 0
	for _, entry := range block {
		if len(entry.Value.IggyBytes()) > HeaderValueMax {
			return false
		}
		total += entry.WireSize()
	}
	return total <= HeaderSoftCap
}
