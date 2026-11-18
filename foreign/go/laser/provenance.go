package laser

import (
	"encoding/binary"
	"math"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// LlmUsage is the token and cost usage of one model call, carried on
// provenance so a conversation's spend can be rolled up from the log alone.
type LlmUsage struct {
	InputTokens  *uint64
	OutputTokens *uint64
	CostUSD      *float64
}

// Provenance is the agentic message spine: conversation, causality, routing
// and usage. It rides as record headers, so every client reads the same fields
// off the same keys.
//
// The conversation is required, because it is the partition key: without it a
// record has no ordering guarantee and no conversation to belong to.
type Provenance struct {
	// Conversation is the conversation this record belongs to, and the
	// partition key that keeps one conversation ordered.
	Conversation wire.ConversationID
	// CausalParent is the record this one answers, if any.
	CausalParent *wire.RecordID
	// ParentConversation and RootConversation place a sub-conversation in its
	// tree, so a reply can be traced back to the exchange that spawned it.
	ParentConversation *wire.ConversationID
	RootConversation   *wire.ConversationID
	// Agent is the agent that produced this record.
	Agent *wire.AgentID
	// TargetAgent is the agent this record is addressed to. A broadcast
	// addresses every agent and is never an agent id, so it decodes as no
	// target rather than as an agent named "*".
	TargetAgent *wire.AgentID
	Usage       *LlmUsage
	// DeadlineAt is the drop-dead time. A consumer past it dead-letters the
	// record instead of working on something nobody is waiting for.
	DeadlineAt *time.Time
	// IdempotencyKey suppresses a duplicate of the same logical operation. It
	// is kept apart from CorrelationID so a business dedup key and a reply
	// correlation never overload one slot: setting a real dedup key and
	// retrying would otherwise cross-match replies.
	IdempotencyKey *string
	// CorrelationID matches a reply to the request that minted it.
	CorrelationID *string
	// FenceToken is the strictly monotonic per-task fence the producer held.
	// A consumer drops a record whose fence is below the highest it has
	// accepted for that task, which is how a stale holder's replay is ignored.
	FenceToken *uint64
}

// PartitionKey is the conversation id, which is what keeps one conversation on
// one partition and therefore in order.
func (provenance *Provenance) PartitionKey() string {
	return provenance.Conversation.String()
}

// RecordHeaders renders the provenance as the contract's header block.
func (provenance *Provenance) RecordHeaders() wire.RecordHeaders {
	headers := wire.NewRecordHeaders(provenance.Conversation)
	headers.Parent = provenance.ParentConversation
	headers.Root = provenance.RootConversation
	headers.Agent = provenance.Agent
	if provenance.TargetAgent != nil {
		addressee := wire.AgentAddressee(*provenance.TargetAgent)
		headers.Addressee = &addressee
	}
	if provenance.CausalParent != nil {
		text := provenance.CausalParent.String()
		headers.CausalParent = &text
	}
	headers.IdempotencyKey = provenance.IdempotencyKey
	headers.Correlation = provenance.CorrelationID
	headers.Fence = provenance.FenceToken
	if provenance.DeadlineAt != nil {
		micros := uint64(provenance.DeadlineAt.UnixMicro())
		headers.DeadlineMicros = &micros
	}
	if provenance.Usage != nil {
		headers.InputTokens = provenance.Usage.InputTokens
		headers.OutputTokens = provenance.Usage.OutputTokens
		headers.CostUSD = provenance.Usage.CostUSD
	}
	return headers
}

// Headers renders the provenance as the Iggy user headers a record carries.
//
// The block is checked against the value and soft size caps here rather than
// at the server, because a record refused after the round trip has already
// cost the caller the publish.
func (provenance *Provenance) Headers() ([]iggcon.HeaderEntry, error) {
	return EncodeHeaderBlock(provenance.RecordHeaders())
}

// EncodeHeaderBlock lowers a contract header block onto Iggy's user headers.
func EncodeHeaderBlock(block wire.RecordHeaders) ([]iggcon.HeaderEntry, error) {
	encoded := block.Encode()
	if !wire.WithinSoftCap(encoded) {
		return nil, &InvalidError{Reason: oversizedHeaderReason(encoded)}
	}
	entries := make([]iggcon.HeaderEntry, 0, len(encoded))
	for _, entry := range encoded {
		value, err := headerValue(entry.Key, entry.Value)
		if err != nil {
			return nil, err
		}
		key, err := iggcon.NewHeaderKeyString(entry.Key)
		if err != nil {
			return nil, &InvalidError{Reason: "header name " + entry.Key + ": " + err.Error()}
		}
		entries = append(entries, iggcon.HeaderEntry{Key: key, Value: value})
	}
	return entries, nil
}

// headerValue carries a typed contract field onto the wire in its own kind, so
// a number is a number rather than its decimal text.
//
// The bytes come from the contract's own encoder rather than being written
// again here: two encoders of one format drift, and this one would drift
// silently, since a wrong byte order still decodes into a plausible number.
func headerValue(key string, field wire.HeaderField) (iggcon.HeaderValue, error) {
	var kind iggcon.HeaderKind
	switch field.Kind {
	case wire.HeaderText:
		// The value length is the contract's rule, enforced by WithinSoftCap
		// above, so it is not checked again here: two copies of one rule drift.
		// An empty value has no such rule, so it is checked here.
		if field.Text == "" {
			return iggcon.HeaderValue{}, &InvalidError{
				Reason: "header " + key + " must not be empty"}
		}
		kind = iggcon.String
	case wire.HeaderUint8:
		kind = iggcon.Uint8
	case wire.HeaderUint32:
		kind = iggcon.Uint32
	case wire.HeaderUint64:
		kind = iggcon.Uint64
	case wire.HeaderFloat64:
		// A non-finite cost has no agreed spelling, so it is refused rather
		// than written as a value a reader would roll up into a total.
		if math.IsNaN(field.Float64) || math.IsInf(field.Float64, 0) {
			return iggcon.HeaderValue{}, &InvalidError{
				Reason: "header " + key + " must be a finite number"}
		}
		kind = iggcon.Double
	default:
		return iggcon.HeaderValue{}, &InvalidError{
			Reason: "header " + key + " has an unknown value kind"}
	}
	return iggcon.HeaderValue{Kind: kind, Value: field.IggyBytes()}, nil
}

// ProvenanceFrom reads the spine back off a record's user headers.
//
// A header outside the provenance dictionary is foreign and ignored, so an
// application's own keys and the AGDX envelope stamps pass through untouched.
// A key this dictionary does know, carrying a value of the wrong kind, is
// corruption rather than something to drop, and is reported.
func ProvenanceFrom(message *iggcon.IggyMessage) (*Provenance, error) {
	entries, err := iggcon.DeserializeHeaders(message.UserHeaders)
	if err != nil {
		return nil, &CodecError{Reason: "read the record's headers", Cause: err}
	}
	return ProvenanceFromHeaders(entries)
}

// ProvenanceFromHeaders reads the spine from an already-parsed header block,
// so a caller holding one decodes it once rather than parsing it twice.
func ProvenanceFromHeaders(entries []iggcon.HeaderEntry) (*Provenance, error) {
	provenance := &Provenance{}
	usage := LlmUsage{}
	hasConversation, hasUsage := false, false

	for _, entry := range entries {
		key := string(entry.Key.Value)
		switch key {
		case wire.HeaderConversationID:
			id, err := conversationValue(entry.Value, key)
			if err != nil {
				return nil, err
			}
			provenance.Conversation, hasConversation = id, true
		case wire.HeaderCausalParent:
			text, err := textValue(entry.Value, key)
			if err != nil {
				return nil, err
			}
			id, err := wire.ParseID(text)
			if err != nil {
				return nil, invalidHeader(key)
			}
			provenance.CausalParent = &wire.RecordID{ID: id}
		case wire.HeaderParentConversation:
			id, err := conversationText(entry.Value, key)
			if err != nil {
				return nil, err
			}
			provenance.ParentConversation = id
		case wire.HeaderRootConversation:
			id, err := conversationText(entry.Value, key)
			if err != nil {
				return nil, err
			}
			provenance.RootConversation = id
		case wire.HeaderAgentID:
			agent, err := agentValue(entry.Value, key)
			if err != nil {
				return nil, err
			}
			provenance.Agent = agent
		case wire.HeaderTargetAgentID:
			text, err := textValue(entry.Value, key)
			if err != nil {
				return nil, err
			}
			// Broadcast addresses every agent and is never an agent id, so it
			// reads as no target rather than as an agent named "*".
			if text == wire.Broadcast {
				continue
			}
			agent, err := wire.NewAgentID(text)
			if err != nil {
				return nil, invalidHeader(key)
			}
			provenance.TargetAgent = &agent
		case wire.HeaderIdempotencyKey:
			text, err := textValue(entry.Value, key)
			if err != nil {
				return nil, err
			}
			provenance.IdempotencyKey = &text
		case wire.HeaderCorrelationID:
			text, err := textValue(entry.Value, key)
			if err != nil {
				return nil, err
			}
			provenance.CorrelationID = &text
		case wire.HeaderFence:
			// A malformed fence is a decode error, never dropped to nothing:
			// silently losing it would skip the gate and let a stale holder's
			// replay through as if it were unfenced.
			value, err := uint64Value(entry.Value, key)
			if err != nil {
				return nil, err
			}
			provenance.FenceToken = &value
		case wire.HeaderDeadline:
			value, err := uint64Value(entry.Value, key)
			if err != nil {
				return nil, err
			}
			deadline := time.UnixMicro(int64(value))
			provenance.DeadlineAt = &deadline
		case wire.HeaderUsageInputTokens:
			value, err := uint64Value(entry.Value, key)
			if err != nil {
				return nil, err
			}
			usage.InputTokens, hasUsage = &value, true
		case wire.HeaderUsageOutputTokens:
			value, err := uint64Value(entry.Value, key)
			if err != nil {
				return nil, err
			}
			usage.OutputTokens, hasUsage = &value, true
		case wire.HeaderCostUSD:
			value, err := float64Value(entry.Value, key)
			if err != nil {
				return nil, err
			}
			usage.CostUSD, hasUsage = &value, true
		}
	}

	if !hasConversation {
		return nil, &NoProvenanceError{}
	}
	if hasUsage {
		provenance.Usage = &usage
	}
	return provenance, nil
}

// oversizedHeaderReason names which of the two size rules a block broke, so a
// caller shortens the right thing.
func oversizedHeaderReason(block []wire.HeaderEntry) string {
	total := 0
	for _, entry := range block {
		if len(entry.Value.IggyBytes()) > wire.HeaderValueMax {
			return "header " + entry.Key + " is longer than the " +
				itoa(wire.HeaderValueMax) + " byte value cap"
		}
		total += entry.WireSize()
	}
	return "the record's headers are " + itoa(total) + " bytes, over the " +
		itoa(wire.HeaderSoftCap) + " byte cap"
}

func invalidHeader(key string) error {
	return &InvalidError{Reason: "header " + key + " carries an unusable value"}
}

func textValue(value iggcon.HeaderValue, key string) (string, error) {
	if value.Kind != iggcon.String && value.Kind != iggcon.Raw {
		return "", invalidHeader(key)
	}
	return string(value.Value), nil
}

func conversationText(value iggcon.HeaderValue, key string) (*wire.ConversationID, error) {
	text, err := textValue(value, key)
	if err != nil {
		return nil, err
	}
	id, err := wire.ParseID(text)
	if err != nil {
		return nil, invalidHeader(key)
	}
	return &wire.ConversationID{ID: id}, nil
}

// conversationValue reads the conversation, which rides as text from this
// client and may arrive as a 128 bit number from a peer that writes it
// numerically.
func conversationValue(value iggcon.HeaderValue, key string) (wire.ConversationID, error) {
	if value.Kind == iggcon.Uint128 {
		if len(value.Value) != 16 {
			return wire.ConversationID{}, invalidHeader(key)
		}
		var raw [16]byte
		copy(raw[:], value.Value)
		return wire.ConversationID{ID: wire.IDFromBytes(raw)}, nil
	}
	id, err := conversationText(value, key)
	if err != nil {
		return wire.ConversationID{}, err
	}
	return *id, nil
}

func agentValue(value iggcon.HeaderValue, key string) (*wire.AgentID, error) {
	text, err := textValue(value, key)
	if err != nil {
		return nil, err
	}
	agent, err := wire.NewAgentID(text)
	if err != nil {
		return nil, invalidHeader(key)
	}
	return &agent, nil
}

func uint64Value(value iggcon.HeaderValue, key string) (uint64, error) {
	switch value.Kind {
	case iggcon.Uint64:
		if len(value.Value) != 8 {
			return 0, invalidHeader(key)
		}
		return binary.LittleEndian.Uint64(value.Value), nil
	case iggcon.Uint32:
		if len(value.Value) != 4 {
			return 0, invalidHeader(key)
		}
		return uint64(binary.LittleEndian.Uint32(value.Value)), nil
	default:
		return 0, invalidHeader(key)
	}
}

func float64Value(value iggcon.HeaderValue, key string) (float64, error) {
	if value.Kind != iggcon.Double || len(value.Value) != 8 {
		return 0, invalidHeader(key)
	}
	decoded := math.Float64frombits(binary.LittleEndian.Uint64(value.Value))
	if math.IsNaN(decoded) || math.IsInf(decoded, 0) {
		return 0, &InvalidError{Reason: "header " + key + " must be a finite number"}
	}
	return decoded, nil
}
