package wire

import (
	"fmt"

	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
)

// FragmentKind discriminates one input in an assembled model context.
type FragmentKind string

const (
	FragmentMessage FragmentKind = "Message"
	FragmentMemory  FragmentKind = "Memory"
	FragmentKv      FragmentKind = "Kv"
	FragmentState   FragmentKind = "State"
	FragmentSummary FragmentKind = "Summary"
)

// Fragment is one input the model received, with what it cost.
//
// The token and byte counts are what make a manifest auditable: a reader can
// see what filled the window without re-fetching any of it.
type Fragment struct {
	Kind FragmentKind

	// At is the log record a message or summary fragment came from.
	At *SourceRef
	// ID is the memory item a memory fragment came from.
	ID string
	// Namespace and Key address a key-value fragment, and Key alone addresses
	// a state fragment.
	Namespace string
	Key       string
	Version   *uint64
	Digest    *Digest32
	Revision  uint64

	Tokens uint32
	Bytes  uint32
}

type (
	messageFragmentFields struct {
		At     SourceRef `cbor:"at"`
		Tokens uint32    `cbor:"tokens"`
		Bytes  uint32    `cbor:"bytes"`
	}
	memoryFragmentFields struct {
		ID      string    `cbor:"id"`
		Version *uint64   `cbor:"version,omitempty"`
		Digest  *Digest32 `cbor:"digest,omitempty"`
		Tokens  uint32    `cbor:"tokens"`
		Bytes   uint32    `cbor:"bytes"`
	}
	kvFragmentFields struct {
		Namespace string    `cbor:"namespace"`
		Key       string    `cbor:"key"`
		Version   *uint64   `cbor:"version,omitempty"`
		Digest    *Digest32 `cbor:"digest,omitempty"`
		Tokens    uint32    `cbor:"tokens"`
		Bytes     uint32    `cbor:"bytes"`
	}
	stateFragmentFields struct {
		Key      string `cbor:"key"`
		Revision uint64 `cbor:"revision"`
		Tokens   uint32 `cbor:"tokens"`
		Bytes    uint32 `cbor:"bytes"`
	}
)

// The fragment constructors.
func MessageFragment(at SourceRef, tokens, bytes uint32) Fragment {
	return Fragment{Kind: FragmentMessage, At: &at, Tokens: tokens, Bytes: bytes}
}

func SummaryFragment(at SourceRef, tokens, bytes uint32) Fragment {
	return Fragment{Kind: FragmentSummary, At: &at, Tokens: tokens, Bytes: bytes}
}

func MemoryFragment(id string, tokens, bytes uint32) Fragment {
	return Fragment{Kind: FragmentMemory, ID: id, Tokens: tokens, Bytes: bytes}
}

func KvFragment(namespace, key string, tokens, bytes uint32) Fragment {
	return Fragment{
		Kind: FragmentKv, Namespace: namespace, Key: key, Tokens: tokens, Bytes: bytes,
	}
}

func StateFragment(key string, revision uint64, tokens, bytes uint32) Fragment {
	return Fragment{
		Kind: FragmentState, Key: key, Revision: revision, Tokens: tokens, Bytes: bytes,
	}
}

// MarshalCBOR writes the externally tagged fragment.
func (fragment Fragment) MarshalCBOR() (cborform.Value, error) {
	switch fragment.Kind {
	case FragmentMessage, FragmentSummary:
		if fragment.At == nil {
			return nil, fmt.Errorf("wire: a %s fragment names no record", fragment.Kind)
		}
		return marshalVariantStruct(string(fragment.Kind), messageFragmentFields{
			At: *fragment.At, Tokens: fragment.Tokens, Bytes: fragment.Bytes,
		})
	case FragmentMemory:
		return marshalVariantStruct(string(fragment.Kind), memoryFragmentFields{
			ID: fragment.ID, Version: fragment.Version, Digest: fragment.Digest,
			Tokens: fragment.Tokens, Bytes: fragment.Bytes,
		})
	case FragmentKv:
		return marshalVariantStruct(string(fragment.Kind), kvFragmentFields{
			Namespace: fragment.Namespace, Key: fragment.Key,
			Version: fragment.Version, Digest: fragment.Digest,
			Tokens: fragment.Tokens, Bytes: fragment.Bytes,
		})
	case FragmentState:
		return marshalVariantStruct(string(fragment.Kind), stateFragmentFields{
			Key: fragment.Key, Revision: fragment.Revision,
			Tokens: fragment.Tokens, Bytes: fragment.Bytes,
		})
	default:
		return nil, fmt.Errorf("wire: unknown context fragment %q", fragment.Kind)
	}
}

// UnmarshalCBOR reads the externally tagged fragment.
func (fragment *Fragment) UnmarshalCBOR(item cborform.Value) error {
	name, payload, err := readVariant(item)
	if err != nil {
		return err
	}
	if err := requirePayload("Fragment", name, payload); err != nil {
		return err
	}
	kind := FragmentKind(name)
	*fragment = Fragment{Kind: kind}

	switch kind {
	case FragmentMessage, FragmentSummary:
		var fields messageFragmentFields
		if err := fromValueInto(payload, &fields); err != nil {
			return err
		}
		fragment.At, fragment.Tokens, fragment.Bytes =
			&fields.At, fields.Tokens, fields.Bytes
	case FragmentMemory:
		var fields memoryFragmentFields
		if err := fromValueInto(payload, &fields); err != nil {
			return err
		}
		fragment.ID, fragment.Version, fragment.Digest = fields.ID, fields.Version, fields.Digest
		fragment.Tokens, fragment.Bytes = fields.Tokens, fields.Bytes
	case FragmentKv:
		var fields kvFragmentFields
		if err := fromValueInto(payload, &fields); err != nil {
			return err
		}
		fragment.Namespace, fragment.Key = fields.Namespace, fields.Key
		fragment.Version, fragment.Digest = fields.Version, fields.Digest
		fragment.Tokens, fragment.Bytes = fields.Tokens, fields.Bytes
	case FragmentState:
		var fields stateFragmentFields
		if err := fromValueInto(payload, &fields); err != nil {
			return err
		}
		fragment.Key, fragment.Revision = fields.Key, fields.Revision
		fragment.Tokens, fragment.Bytes = fields.Tokens, fields.Bytes
	default:
		return unknownVariant("Fragment", name)
	}
	return nil
}

// FrontierPosition is one source partition and the offset a manifest read
// through, as the triple it rides as.
type FrontierPosition struct {
	Topic     uint32
	Partition uint32
	Offset    uint64
}

// MarshalCBOR writes the position as the three-element tuple the contract
// carries.
func (position FrontierPosition) MarshalCBOR() (cborform.Value, error) {
	return cborform.Array{
		cborform.Uint(position.Topic),
		cborform.Uint(position.Partition),
		cborform.Uint(position.Offset),
	}, nil
}

// UnmarshalCBOR reads the three-element tuple.
func (position *FrontierPosition) UnmarshalCBOR(item cborform.Value) error {
	var fields [3]uint64
	if err := fromValueInto(item, &fields); err != nil {
		return err
	}
	*position = FrontierPosition{
		Topic: uint32(fields[0]), Partition: uint32(fields[1]), Offset: fields[2],
	}
	return nil
}

// ContextManifest is the inputs and source positions used to assemble one
// model context.
//
// It is what makes a model call reproducible: the policy that selected the
// inputs, the inputs themselves, and the exact source positions they were read
// at.
type ContextManifest struct {
	Policy        string             `cbor:"policy"`
	PolicyVersion string             `cbor:"policy_version"`
	Fragments     []Fragment         `cbor:"fragments"`
	Tokens        uint64             `cbor:"tokens"`
	Bytes         uint64             `cbor:"bytes"`
	Frontier      []FrontierPosition `cbor:"frontier"`
	Correlation   *CorrelationID     `cbor:"correlation,omitempty"`
}

// Validate checks the manifest's fragment count and each fragment's digest.
func (manifest ContextManifest) Validate() error {
	if len(manifest.Fragments) > MaxManifestFragments {
		return &InvalidError{Reason: "context manifest has too many fragments"}
	}
	for _, fragment := range manifest.Fragments {
		if fragment.Digest == nil {
			continue
		}
		if err := fragment.Digest.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// CoveredRange is one source partition and the half-open offset span a
// compaction summarized, as the quadruple it rides as.
type CoveredRange struct {
	Topic        uint32
	Partition    uint32
	Start        uint64
	EndExclusive uint64
}

// MarshalCBOR writes the four-element tuple the contract carries.
func (covered CoveredRange) MarshalCBOR() (cborform.Value, error) {
	return cborform.Array{
		cborform.Uint(covered.Topic),
		cborform.Uint(covered.Partition),
		cborform.Uint(covered.Start),
		cborform.Uint(covered.EndExclusive),
	}, nil
}

// UnmarshalCBOR reads the four-element tuple.
func (covered *CoveredRange) UnmarshalCBOR(item cborform.Value) error {
	var fields [4]uint64
	if err := fromValueInto(item, &fields); err != nil {
		return err
	}
	*covered = CoveredRange{
		Topic: uint32(fields[0]), Partition: uint32(fields[1]),
		Start: fields[2], EndExclusive: fields[3],
	}
	return nil
}

// ContextCompaction is a summary that replaces a range of context records, so
// a long conversation stays within a window without losing what it decided.
type ContextCompaction struct {
	SummaryAt  SourceRef      `cbor:"summary_at"`
	Covered    []CoveredRange `cbor:"covered"`
	Summarizer ProducerInfo   `cbor:"summarizer"`
}

// RetrievedItem is one item a retrieval selected, with its score.
type RetrievedItem struct {
	ID    string
	Score float32
}

// MarshalCBOR writes the pair the contract carries.
func (item RetrievedItem) MarshalCBOR() (cborform.Value, error) {
	score, err := toValueOf(item.Score)
	if err != nil {
		return nil, err
	}
	return cborform.Array{cborform.Text(item.ID), score}, nil
}

// UnmarshalCBOR reads the pair.
func (item *RetrievedItem) UnmarshalCBOR(value cborform.Value) error {
	entries, ok := value.(cborform.Array)
	if !ok || len(entries) != 2 {
		return fmt.Errorf("wire: a retrieved item is a pair, got %T", value)
	}
	id, ok := entries[0].(cborform.Text)
	if !ok {
		return fmt.Errorf("wire: a retrieved item names a string id")
	}
	var score float32
	if err := fromValueInto(entries[1], &score); err != nil {
		return err
	}
	*item = RetrievedItem{ID: string(id), Score: score}
	return nil
}

// ContextRetrieval is the items selected by one context retrieval, with the
// query that selected them when there was one.
type ContextRetrieval struct {
	Query *string         `cbor:"query,omitempty"`
	Items []RetrievedItem `cbor:"items"`
}

// StateDelta is one revision-guarded state patch.
//
// The base revision makes the patch conditional, so two writers racing on one
// document cannot both apply, and the operation id makes a retry idempotent.
type StateDelta struct {
	BaseRevision uint64           `cbor:"base_revision"`
	Patch        []PatchOperation `cbor:"patch"`
	OpID         string           `cbor:"op_id"`
}

// Validate checks the patch length against the cap.
func (delta StateDelta) Validate() error {
	if len(delta.Patch) == 0 || len(delta.Patch) > MaxStatePatchOps {
		return &InvalidError{Reason: fmt.Sprintf(
			"a state patch must contain 1..=%d operations", MaxStatePatchOps)}
	}
	for _, operation := range delta.Patch {
		if err := operation.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// StateSnapshot is a complete state document at the revision it replaces.
type StateSnapshot struct {
	BaseRevision uint64   `cbor:"base_revision"`
	Document     DynValue `cbor:"document"`
}
