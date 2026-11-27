package wire

import (
	"fmt"

	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
)

// MemoryRecordKind discriminates a memory record.
type MemoryRecordKind uint8

const (
	// MemoryItem is a remembered item.
	MemoryItem MemoryRecordKind = iota
	// MemoryForget is a tombstone removing an item from recall.
	MemoryForget
	// MemoryFeedback reweights an item's recall rank.
	MemoryFeedback
)

type memoryItemFields struct {
	ID   string `cbor:"id"`
	Kind string `cbor:"kind"`
	// Body is a bare byte vector in the contract, so it rides as an array of
	// integers rather than a byte string.
	Body     ByteList      `cbor:"body"`
	Origin   *SourceRef    `cbor:"origin,omitempty"`
	Producer *ProducerInfo `cbor:"producer,omitempty"`
}

type memoryForgetFields struct {
	Target       string  `cbor:"target"`
	Conversation *string `cbor:"conversation,omitempty"`
}

type memoryFeedbackFields struct {
	Target       string  `cbor:"target"`
	Weight       float32 `cbor:"weight"`
	Conversation *string `cbor:"conversation,omitempty"`
}

// MemoryRecord is a memory record as it rides the memory topic: the on-log
// audit of one scope's changes.
//
// This is the shape the SDK's memory facade writes and the shape a deployment
// folds into the versioned key-value read view, so both sides agree on the
// bytes without the reader importing the SDK.
type MemoryRecord struct {
	Kind MemoryRecordKind
	// Item.
	ID       string
	ItemKind string
	// Body rides the wire as an array of integers, matching the contract's
	// bare byte vector.
	Body     ByteList
	Origin   *SourceRef
	Producer *ProducerInfo
	// Forget and Feedback.
	Target string
	Weight float32
	// Conversation scopes a tombstone or a feedback signal to the
	// conversation the item was remembered in.
	Conversation *string
}

// RememberItem records a remembered item.
func RememberItem(id, kind string, body []byte) MemoryRecord {
	return MemoryRecord{Kind: MemoryItem, ID: id, ItemKind: kind, Body: ByteList(body)}
}

// ForgetItem removes an item from recall.
func ForgetItem(target string) MemoryRecord {
	return MemoryRecord{Kind: MemoryForget, Target: target}
}

// FeedbackForItem reweights an item's recall rank.
func FeedbackForItem(target string, weight float32) MemoryRecord {
	return MemoryRecord{Kind: MemoryFeedback, Target: target, Weight: weight}
}

// The externally tagged memory record variant names.
const (
	memoryItemVariant     = "Item"
	memoryForgetVariant   = "Forget"
	memoryFeedbackVariant = "Feedback"
)

// MarshalCBOR writes the externally tagged record.
func (record MemoryRecord) MarshalCBOR() (cborform.Value, error) {
	switch record.Kind {
	case MemoryItem:
		return marshalVariantStruct(memoryItemVariant, memoryItemFields{
			ID: record.ID, Kind: record.ItemKind, Body: record.Body,
			Origin: record.Origin, Producer: record.Producer,
		})
	case MemoryForget:
		return marshalVariantStruct(memoryForgetVariant, memoryForgetFields{
			Target: record.Target, Conversation: record.Conversation,
		})
	case MemoryFeedback:
		return marshalVariantStruct(memoryFeedbackVariant, memoryFeedbackFields{
			Target: record.Target, Weight: record.Weight, Conversation: record.Conversation,
		})
	default:
		return nil, fmt.Errorf("wire: unknown memory record kind %d", record.Kind)
	}
}

// UnmarshalCBOR reads the externally tagged record.
func (record *MemoryRecord) UnmarshalCBOR(item cborform.Value) error {
	name, payload, err := readVariant(item)
	if err != nil {
		return err
	}
	if err := requirePayload("MemoryRecord", name, payload); err != nil {
		return err
	}
	switch name {
	case memoryItemVariant:
		var fields memoryItemFields
		if err := fromValueInto(payload, &fields); err != nil {
			return err
		}
		*record = MemoryRecord{
			Kind: MemoryItem, ID: fields.ID, ItemKind: fields.Kind, Body: fields.Body,
			Origin: fields.Origin, Producer: fields.Producer,
		}
		return nil
	case memoryForgetVariant:
		var fields memoryForgetFields
		if err := fromValueInto(payload, &fields); err != nil {
			return err
		}
		*record = MemoryRecord{
			Kind: MemoryForget, Target: fields.Target, Conversation: fields.Conversation,
		}
		return nil
	case memoryFeedbackVariant:
		var fields memoryFeedbackFields
		if err := fromValueInto(payload, &fields); err != nil {
			return err
		}
		*record = MemoryRecord{
			Kind: MemoryFeedback, Target: fields.Target,
			Weight: fields.Weight, Conversation: fields.Conversation,
		}
		return nil
	default:
		return unknownVariant("MemoryRecord", name)
	}
}
