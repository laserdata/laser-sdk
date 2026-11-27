package wire

import (
	"fmt"

	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
)

// SourceRefKind discriminates where a graph or memory element was observed.
type SourceRefKind uint8

const (
	// SourceMessage points at a record on the message log.
	SourceMessage SourceRefKind = iota
	// SourceKv points at a key in the managed key-value store.
	SourceKv
	// SourceMemory points at a managed memory item by its id.
	SourceMemory
)

// MessageSource addresses a record by numeric stream id, topic id, partition
// and offset. Ids rather than names, so the pointer is route-ready and
// survives a rename.
type MessageSource struct {
	Stream    uint32 `cbor:"stream"`
	Topic     uint32 `cbor:"topic"`
	Partition uint32 `cbor:"partition"`
	Offset    uint64 `cbor:"offset"`
	// Generation is the topic creation timestamp, proving the numeric topic
	// id's generation.
	Generation *uint64 `cbor:"generation,omitempty"`
	// Conversation is the record's conversation id, kept for the conversation
	// lens but excluded from any content-addressed id, so the same element
	// re-observed from another conversation keeps its identity.
	Conversation *string `cbor:"conversation,omitempty"`
}

// KvSource addresses a key in the managed key-value store.
type KvSource struct {
	Namespace string `cbor:"namespace"`
	Key       string `cbor:"key"`
}

// MemorySource addresses a managed memory item.
type MemorySource struct {
	ID string `cbor:"id"`
}

// SourceRef is where a graph or memory element was last observed, so a reader
// can navigate back to its origin.
//
// On an edge this is the record that asserted the relationship, kept
// last-writer. On a node it is the first record the entity was seen in, so a
// re-observed node's stored bytes stay stable. It is excluded from the
// content-addressed id, and never affects identity or idempotent upsert.
type SourceRef struct {
	Kind    SourceRefKind
	Message *MessageSource
	Kv      *KvSource
	Memory  *MemorySource
}

// NewMessageSource builds a log-record reference.
func NewMessageSource(source MessageSource) SourceRef {
	return SourceRef{Kind: SourceMessage, Message: &source}
}

// NewKvSource builds a key-value reference.
func NewKvSource(namespace, key string) SourceRef {
	return SourceRef{Kind: SourceKv, Kv: &KvSource{Namespace: namespace, Key: key}}
}

// NewMemorySource builds a memory-item reference.
func NewMemorySource(id string) SourceRef {
	return SourceRef{Kind: SourceMemory, Memory: &MemorySource{ID: id}}
}

// The externally tagged variant names.
const (
	sourceMessageVariant = "Message"
	sourceKvVariant      = "Kv"
	sourceMemoryVariant  = "Memory"
)

// MarshalCBOR writes the externally tagged variant.
func (source SourceRef) MarshalCBOR() (cborform.Value, error) {
	switch source.Kind {
	case SourceMessage:
		if source.Message == nil {
			return nil, fmt.Errorf("wire: a message source ref carries no record")
		}
		return marshalVariantStruct(sourceMessageVariant, *source.Message)
	case SourceKv:
		if source.Kv == nil {
			return nil, fmt.Errorf("wire: a key-value source ref carries no key")
		}
		return marshalVariantStruct(sourceKvVariant, *source.Kv)
	case SourceMemory:
		if source.Memory == nil {
			return nil, fmt.Errorf("wire: a memory source ref carries no id")
		}
		return marshalVariantStruct(sourceMemoryVariant, *source.Memory)
	default:
		return nil, fmt.Errorf("wire: unknown source ref kind %d", source.Kind)
	}
}

// UnmarshalCBOR reads the externally tagged variant.
func (source *SourceRef) UnmarshalCBOR(item cborform.Value) error {
	name, payload, err := readVariant(item)
	if err != nil {
		return err
	}
	if err := requirePayload("SourceRef", name, payload); err != nil {
		return err
	}
	switch name {
	case sourceMessageVariant:
		var message MessageSource
		if err := fromValueInto(payload, &message); err != nil {
			return err
		}
		*source = NewMessageSource(message)
		return nil
	case sourceKvVariant:
		var kv KvSource
		if err := fromValueInto(payload, &kv); err != nil {
			return err
		}
		*source = SourceRef{Kind: SourceKv, Kv: &kv}
		return nil
	case sourceMemoryVariant:
		var memory MemorySource
		if err := fromValueInto(payload, &memory); err != nil {
			return err
		}
		*source = SourceRef{Kind: SourceMemory, Memory: &memory}
		return nil
	default:
		return unknownVariant("SourceRef", name)
	}
}

// SessionRef is one session addressed within a stream by name.
type SessionRef struct {
	Stream  string         `cbor:"stream"`
	Session ConversationID `cbor:"session"`
}
