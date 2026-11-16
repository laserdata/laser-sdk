package wire

import (
	"fmt"

	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
)

// SnapshotOffset is one source partition and its last folded offset.
//
// It rides the wire as a four-element array, the form a Rust tuple encodes to,
// rather than a named-field map: a snapshot carries one of these per partition,
// so the field names would dominate its size.
type SnapshotOffset struct {
	TopicID uint32
	// TopicCreatedAtMicros identifies the topic incarnation. Offsets of
	// different incarnations do not compare.
	TopicCreatedAtMicros uint64
	PartitionID          uint32
	Offset               uint64
}

// NewSnapshotOffset names one partition's folded position.
func NewSnapshotOffset(topicID uint32, createdAtMicros uint64, partitionID uint32, offset uint64) SnapshotOffset {
	return SnapshotOffset{
		TopicID:              topicID,
		TopicCreatedAtMicros: createdAtMicros,
		PartitionID:          partitionID,
		Offset:               offset,
	}
}

// precedes orders two offsets by their source identity, which is how a
// snapshot's entries are sorted and searched.
func (offset SnapshotOffset) precedes(other SnapshotOffset) bool {
	if offset.TopicID != other.TopicID {
		return offset.TopicID < other.TopicID
	}
	if offset.TopicCreatedAtMicros != other.TopicCreatedAtMicros {
		return offset.TopicCreatedAtMicros < other.TopicCreatedAtMicros
	}
	return offset.PartitionID < other.PartitionID
}

// sameSource reports whether two offsets name the same partition incarnation.
func (offset SnapshotOffset) sameSource(other SnapshotOffset) bool {
	return offset.TopicID == other.TopicID &&
		offset.TopicCreatedAtMicros == other.TopicCreatedAtMicros &&
		offset.PartitionID == other.PartitionID
}

// MarshalCBOR writes the four-element array.
func (offset SnapshotOffset) MarshalCBOR() (cborform.Value, error) {
	return cborform.Array{
		cborform.Uint(uint64(offset.TopicID)),
		cborform.Uint(offset.TopicCreatedAtMicros),
		cborform.Uint(uint64(offset.PartitionID)),
		cborform.Uint(offset.Offset),
	}, nil
}

// UnmarshalCBOR reads the four-element array.
func (offset *SnapshotOffset) UnmarshalCBOR(item cborform.Value) error {
	fields, ok := item.(cborform.Array)
	if !ok || len(fields) != 4 {
		return fmt.Errorf("wire: a snapshot offset is a four-element array, got %T", item)
	}
	numbers := make([]uint64, 4)
	for i, field := range fields {
		number, ok := field.(cborform.Uint)
		if !ok {
			return fmt.Errorf("wire: snapshot offset element %d is not an integer", i)
		}
		numbers[i] = uint64(number)
	}
	*offset = NewSnapshotOffset(
		uint32(numbers[0]), numbers[1], uint32(numbers[2]), numbers[3])
	return nil
}

// FoldSnapshot is a fold state and the exact source positions included in it.
type FoldSnapshot struct {
	Stream                string         `cbor:"stream"`
	StreamID              uint32         `cbor:"stream_id"`
	StreamCreatedAtMicros uint64         `cbor:"stream_created_at_micros"`
	Conversation          ConversationID `cbor:"conversation"`
	Fold                  string         `cbor:"fold"`
	// AsOf is sorted by topic id, topic creation time and partition id.
	AsOf []SnapshotOffset `cbor:"as_of"`
	// State is opaque: the application chooses the codec for these bytes.
	State []byte `cbor:"state"`
}

// ResumeOffset is the next offset to read for one exact source partition.
//
// A partition the snapshot does not name resumes at zero, since nothing of it
// is included in the state.
func (snapshot FoldSnapshot) ResumeOffset(topicID uint32, createdAtMicros uint64, partitionID uint32) uint64 {
	wanted := NewSnapshotOffset(topicID, createdAtMicros, partitionID, 0)
	for _, entry := range snapshot.AsOf {
		if entry.sameSource(wanted) {
			if entry.Offset == ^uint64(0) {
				return entry.Offset // saturating, as the Rust side does
			}
			return entry.Offset + 1
		}
	}
	return 0
}

// Validate checks the snapshot names its stream and fold, and that its offsets
// are sorted with no duplicate source.
//
// Duplicates would make the resume position ambiguous, and an unsorted list
// would break the ordered search a fold relies on.
func (snapshot FoldSnapshot) Validate() error {
	if snapshot.Stream == "" || snapshot.Fold == "" {
		return &InvalidError{Reason: "snapshot stream and fold must be non-empty"}
	}
	for i := 1; i < len(snapshot.AsOf); i++ {
		if !snapshot.AsOf[i-1].precedes(snapshot.AsOf[i]) {
			return &InvalidError{
				Reason: "snapshot offsets must be sorted with no duplicate source",
			}
		}
	}
	return nil
}
