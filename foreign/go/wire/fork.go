package wire

import (
	"fmt"

	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
)

// ForkKind is how a fork sees the trunk.
type ForkKind string

const (
	// ForkSevered is a frozen snapshot: it sees trunk rows only up to the
	// offsets captured at creation, plus the fork's own rows. Later trunk
	// appends are invisible.
	ForkSevered ForkKind = "severed"
	// ForkContinuous is a live branch: it sees the trunk as it grows, with
	// the fork's own rows overlaid. The default.
	ForkContinuous ForkKind = "continuous"
)

// ForkStatus is a fork's lifecycle state.
type ForkStatus string

const (
	ForkOpen     ForkStatus = "open"
	ForkPromoted ForkStatus = "promoted"
	ForkSquashed ForkStatus = "squashed"
)

// ForkInfo is a fork's metadata, returned by create and list.
type ForkInfo struct {
	ForkID          string     `cbor:"fork_id"`
	Parent          *string    `cbor:"parent,omitempty"`
	Kind            ForkKind   `cbor:"kind"`
	UserID          uint32     `cbor:"user_id"`
	Status          ForkStatus `cbor:"status"`
	CreatedAtMicros uint64     `cbor:"created_at_micros"`
	RowCount        uint64     `cbor:"row_count"`
}

// ForkCreate is the create request.
type ForkCreate struct {
	V      uint32   `cbor:"v"`
	ForkID string   `cbor:"fork_id"`
	Parent *string  `cbor:"parent,omitempty"`
	Kind   ForkKind `cbor:"kind"`
	Tables []string `cbor:"tables,omitempty"`
	// Points are the trunk positions a severed fork is frozen at, stamped by
	// the server that logs the request so every replica folds the same fork.
	// A client leaves it unset.
	Points []ForkPoint `cbor:"points,omitempty"`
}

// ForkPoint is one trunk position a severed fork is frozen at.
type ForkPoint struct {
	Table       string `cbor:"table"`
	PartitionID uint32 `cbor:"partition_id"`
	Offset      uint64 `cbor:"offset"`
	// SourceIncarnation is the creation time of the source topic incarnation
	// the offset belongs to, set by the server that logs the create.
	SourceIncarnation *uint64 `cbor:"source_incarnation,omitempty"`
}

// ForkDelete is the squash request.
type ForkDelete struct {
	V      uint32 `cbor:"v"`
	ForkID string `cbor:"fork_id"`
}

// ForkPromote is the promote request.
type ForkPromote struct {
	V      uint32 `cbor:"v"`
	ForkID string `cbor:"fork_id"`
}

// ForkList is the list request.
type ForkList struct {
	V uint32 `cbor:"v"`
	// Stream is the stream whose resources the listing covers. It is required
	// in stream tenancy, where the server authorizes the stream and lists
	// only its resources.
	Stream *string `cbor:"stream,omitempty"`
}

// ForkPut writes one speculative row into a fork.
type ForkPut struct {
	V                 uint32            `cbor:"v"`
	ForkID            string            `cbor:"fork_id"`
	Table             string            `cbor:"table"`
	PartitionID       uint32            `cbor:"partition_id"`
	Offset            uint64            `cbor:"offset"`
	ProjectionID      string            `cbor:"projection_id"`
	ProjectionVersion uint32            `cbor:"projection_version"`
	Fields            map[string]string `cbor:"fields,omitempty"`
	Metadata          map[string]string `cbor:"metadata,omitempty"`
	Payload           []byte            `cbor:"payload,omitempty"`
	Embedding         *string           `cbor:"embedding,omitempty"`
	Tombstone         bool              `cbor:"tombstone"`
}

// ForkOutcomeKind discriminates a successful fork outcome.
type ForkOutcomeKind uint8

const (
	ForkCreated ForkOutcomeKind = iota
	ForkDeleted
	ForkPromotedRows
	ForkListed
	ForkWritten
)

type forkPromotedFields struct {
	Rows uint64 `cbor:"rows"`
}

// ForkOutcome is the successful outcome of a fork command, shaped per op.
type ForkOutcome struct {
	Kind    ForkOutcomeKind
	Info    *ForkInfo
	Deleted bool
	Rows    uint64
	List    []ForkInfo
}

// The externally tagged fork outcome names.
const (
	forkCreatedVariant  = "Created"
	forkDeletedVariant  = "Deleted"
	forkPromotedVariant = "Promoted"
	forkListVariant     = "List"
	forkWrittenVariant  = "Written"
)

// MarshalCBOR writes the externally tagged outcome.
func (outcome ForkOutcome) MarshalCBOR() (cborform.Value, error) {
	switch outcome.Kind {
	case ForkCreated:
		if outcome.Info == nil {
			return nil, fmt.Errorf("wire: a created outcome carries no fork")
		}
		return marshalVariantStruct(forkCreatedVariant, *outcome.Info)
	case ForkDeleted:
		return taggedVariant(forkDeletedVariant, cborform.Bool(outcome.Deleted)), nil
	case ForkPromotedRows:
		return marshalVariantStruct(forkPromotedVariant, forkPromotedFields{Rows: outcome.Rows})
	case ForkListed:
		payload, err := toValueOf(outcome.List)
		if err != nil {
			return nil, err
		}
		return taggedVariant(forkListVariant, payload), nil
	case ForkWritten:
		return unitVariant(forkWrittenVariant), nil
	default:
		return nil, fmt.Errorf("wire: unknown fork outcome %d", outcome.Kind)
	}
}

// UnmarshalCBOR reads the externally tagged outcome.
func (outcome *ForkOutcome) UnmarshalCBOR(item cborform.Value) error {
	name, payload, err := readVariant(item)
	if err != nil {
		return err
	}
	switch name {
	case forkCreatedVariant:
		if err := requirePayload("ForkOutcome", name, payload); err != nil {
			return err
		}
		var info ForkInfo
		if err := fromValueInto(payload, &info); err != nil {
			return err
		}
		*outcome = ForkOutcome{Kind: ForkCreated, Info: &info}
		return nil
	case forkDeletedVariant:
		if err := requirePayload("ForkOutcome", name, payload); err != nil {
			return err
		}
		flag, ok := payload.(cborform.Bool)
		if !ok {
			return fmt.Errorf("wire: a deleted outcome carries a boolean, got %T", payload)
		}
		*outcome = ForkOutcome{Kind: ForkDeleted, Deleted: bool(flag)}
		return nil
	case forkPromotedVariant:
		if err := requirePayload("ForkOutcome", name, payload); err != nil {
			return err
		}
		var fields forkPromotedFields
		if err := fromValueInto(payload, &fields); err != nil {
			return err
		}
		*outcome = ForkOutcome{Kind: ForkPromotedRows, Rows: fields.Rows}
		return nil
	case forkListVariant:
		if err := requirePayload("ForkOutcome", name, payload); err != nil {
			return err
		}
		var list []ForkInfo
		if err := fromValueInto(payload, &list); err != nil {
			return err
		}
		*outcome = ForkOutcome{Kind: ForkListed, List: list}
		return nil
	case forkWrittenVariant:
		*outcome = ForkOutcome{Kind: ForkWritten}
		return nil
	default:
		return unknownVariant("ForkOutcome", name)
	}
}

// ForkErrorKind discriminates a fork failure.
type ForkErrorKind uint8

const (
	ForkUnsupported ForkErrorKind = iota
	ForkNotFound
	ForkInvalid
	ForkConflict
	ForkBackend
	// ForkUnavailable means the identical request may succeed later.
	ForkUnavailable
	ForkVersionMismatch
	// ForkNotLeader means this backend does not own the mutation partition
	// for the fork.
	ForkNotLeader
)

// ForkError is why a fork operation failed.
type ForkError struct {
	Kind     ForkErrorKind
	Message  string
	Expected uint32
	Got      uint32
}

// The externally tagged fork error names.
const (
	forkUnsupportedVariant = "Unsupported"
	forkNotFoundVariant    = "NotFound"
	forkInvalidVariant     = "InvalidFork"
	forkConflictVariant    = "Conflict"
	forkBackendVariant     = "Backend"
	forkUnavailableVariant = "Unavailable"
	forkVersionVariant     = "Version"
	forkNotLeaderVariant   = "NotLeader"
)

// MarshalCBOR writes the externally tagged error.
func (forkError ForkError) MarshalCBOR() (cborform.Value, error) {
	text := func(name string) (cborform.Value, error) {
		return taggedVariant(name, cborform.Text(forkError.Message)), nil
	}
	switch forkError.Kind {
	case ForkUnsupported:
		return text(forkUnsupportedVariant)
	case ForkNotFound:
		return text(forkNotFoundVariant)
	case ForkInvalid:
		return text(forkInvalidVariant)
	case ForkConflict:
		return text(forkConflictVariant)
	case ForkBackend:
		return text(forkBackendVariant)
	case ForkUnavailable:
		return text(forkUnavailableVariant)
	case ForkVersionMismatch:
		return marshalVariantStruct(forkVersionVariant, kvVersionPayloadPair{
			Expected: forkError.Expected, Got: forkError.Got,
		})
	case ForkNotLeader:
		return unitVariant(forkNotLeaderVariant), nil
	default:
		return nil, fmt.Errorf("wire: unknown fork error %d", forkError.Kind)
	}
}

// UnmarshalCBOR reads the externally tagged error.
func (forkError *ForkError) UnmarshalCBOR(item cborform.Value) error {
	name, payload, err := readVariant(item)
	if err != nil {
		return err
	}

	text := func(kind ForkErrorKind) error {
		if err := requirePayload("ForkError", name, payload); err != nil {
			return err
		}
		message, ok := payload.(cborform.Text)
		if !ok {
			return fmt.Errorf("wire: %q carries a message, got %T", name, payload)
		}
		*forkError = ForkError{Kind: kind, Message: string(message)}
		return nil
	}

	switch name {
	case forkUnsupportedVariant:
		return text(ForkUnsupported)
	case forkNotFoundVariant:
		return text(ForkNotFound)
	case forkInvalidVariant:
		return text(ForkInvalid)
	case forkConflictVariant:
		return text(ForkConflict)
	case forkBackendVariant:
		return text(ForkBackend)
	case forkUnavailableVariant:
		return text(ForkUnavailable)
	case forkVersionVariant:
		if err := requirePayload("ForkError", name, payload); err != nil {
			return err
		}
		var detail kvVersionPayloadPair
		if err := fromValueInto(payload, &detail); err != nil {
			return err
		}
		*forkError = ForkError{
			Kind: ForkVersionMismatch, Expected: detail.Expected, Got: detail.Got,
		}
		return nil
	case forkNotLeaderVariant:
		*forkError = ForkError{Kind: ForkNotLeader}
		return nil
	default:
		return unknownVariant("ForkError", name)
	}
}

// Error renders the failure, matching the Rust messages.
func (forkError ForkError) Error() string {
	switch forkError.Kind {
	case ForkUnsupported:
		return "forks not supported: " + forkError.Message
	case ForkNotFound:
		return "fork not found: " + forkError.Message
	case ForkInvalid:
		return "invalid fork: " + forkError.Message
	case ForkConflict:
		return "fork conflict: " + forkError.Message
	case ForkBackend:
		return "fork backend error: " + forkError.Message
	case ForkUnavailable:
		return "temporarily unavailable: " + forkError.Message
	case ForkVersionMismatch:
		return fmt.Sprintf("unsupported fork op version (expected %d, got %d)",
			forkError.Expected, forkError.Got)
	case ForkNotLeader:
		return "not the partition leader for this fork"
	default:
		return "unknown fork error"
	}
}

// ResultCode projects the failure onto the shared outcome dictionary.
func (forkError ForkError) ResultCode() ResultCode {
	switch forkError.Kind {
	case ForkUnsupported:
		return ResultUnsupported
	case ForkNotFound:
		return ResultNotFound
	case ForkInvalid:
		return ResultInvalidArgument
	case ForkConflict:
		return ResultConflict
	case ForkBackend:
		return ResultBackend
	case ForkUnavailable, ForkNotLeader:
		return ResultUnavailable
	case ForkVersionMismatch:
		return ResultVersionSkew
	default:
		return ResultBackend
	}
}

// ForkReply is the result of a fork op.
type ForkReply struct {
	Err     *ForkError
	Outcome *ForkOutcome
}

// IsOk reports a successful reply.
func (reply ForkReply) IsOk() bool { return reply.Err == nil && reply.Outcome != nil }

// MarshalCBOR writes the externally tagged reply.
func (reply ForkReply) MarshalCBOR() (cborform.Value, error) {
	switch {
	case reply.Err != nil && reply.Outcome != nil:
		return nil, fmt.Errorf("wire: a fork reply is either an outcome or an error")
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
		return nil, fmt.Errorf("wire: an empty fork reply")
	}
}

// UnmarshalCBOR reads the externally tagged reply.
func (reply *ForkReply) UnmarshalCBOR(item cborform.Value) error {
	name, payload, err := readVariant(item)
	if err != nil {
		return err
	}
	if err := requirePayload("ForkReply", name, payload); err != nil {
		return err
	}
	switch name {
	case kvOkVariant:
		var outcome ForkOutcome
		if err := outcome.UnmarshalCBOR(payload); err != nil {
			return err
		}
		*reply = ForkReply{Outcome: &outcome}
		return nil
	case kvErrVariant:
		var forkError ForkError
		if err := forkError.UnmarshalCBOR(payload); err != nil {
			return err
		}
		*reply = ForkReply{Err: &forkError}
		return nil
	default:
		return unknownVariant("ForkReply", name)
	}
}

// ValidateForkID is the canonical fork-id safelist, shared by every
// fork-serving backend.
//
// A fork id is caller-chosen and a backend that overlays a fork inlines it
// into a copy-on-write query as a quoted identifier, so the charset is a
// strict safelist rather than a length bound. A stream-scoped id validates
// both halves.
func ValidateForkID(forkID string) error {
	stream, local, scoped := SplitScopedResource(forkID)
	if !scoped {
		return ValidateSafelistedName("fork id", forkID, MaxForkIDBytes)
	}
	if len(forkID) > MaxForkIDBytes {
		return &InvalidError{Reason: fmt.Sprintf(
			"fork id is %dB, exceeds cap %dB", len(forkID), MaxForkIDBytes)}
	}
	if err := ValidateSafelistedName("fork id stream", stream, MaxForkIDBytes); err != nil {
		return err
	}
	return ValidateSafelistedName("fork id", local, MaxForkIDBytes)
}

// UnmarshalCBOR reads the value, refusing one outside the closed vocabulary.
func (value *ForkKind) UnmarshalCBOR(item cborform.Value) error {
	return decodeClosedEnum(item, value, "fork kind", ForkSevered, ForkContinuous)
}

// UnmarshalCBOR reads the value, refusing one outside the closed vocabulary.
func (value *ForkStatus) UnmarshalCBOR(item cborform.Value) error {
	return decodeClosedEnum(item, value, "fork status", ForkOpen, ForkPromoted, ForkSquashed)
}
