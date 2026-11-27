package wire

import "github.com/laserdata/laser-sdk/foreign/go/internal/cborform"

// ManagedRequestVersion is the version every managed envelope stamps.
const ManagedRequestVersion uint32 = 1

// MutationPosition is the position of one applied mutation on a surface's
// mutation topic: the fold coordinate a barriered read waits for.
//
// Positions compare only within one topic generation. A generation mismatch is
// fail-closed and never an implicit reset.
type MutationPosition struct {
	TopicGeneration uint64 `cbor:"topic_generation"`
	Partition       uint32 `cbor:"partition"`
	Offset          uint64 `cbor:"offset"`
}

// Precedes reports whether this position is strictly earlier than other within
// the same topic generation. Positions of different generations do not compare,
// so the second result is false there.
func (position MutationPosition) Precedes(other MutationPosition) (bool, bool) {
	if position.TopicGeneration != other.TopicGeneration {
		return false, false
	}
	if position.Partition != other.Partition {
		return position.Partition < other.Partition, true
	}
	return position.Offset < other.Offset, true
}

// ManagedRequestEnvelope wraps a managed request that changes state, giving it
// a stable operation identity across transport retries.
type ManagedRequestEnvelope struct {
	V           uint32 `cbor:"v"`
	OperationID U128   `cbor:"operation_id"`
	Payload     []byte `cbor:"payload"`
}

// Validate checks the version and that the operation identity is set.
func (envelope *ManagedRequestEnvelope) Validate() error {
	if envelope.V != ManagedRequestVersion {
		return &InvalidError{Reason: "unsupported managed request version"}
	}
	if envelope.OperationID.IsZero() {
		return &InvalidError{Reason: "managed request operation id must not be zero"}
	}
	return nil
}

// PositionSurface is a managed surface with its own mutation log and fold.
type PositionSurface string

const (
	SurfaceKv    PositionSurface = "kv"
	SurfaceFork  PositionSurface = "fork"
	SurfaceGraph PositionSurface = "graph"
)

// PositionSurfaceOfCode is the surface a managed read or mutation acts on. The
// second result is false for a command outside the three mutation logs.
func PositionSurfaceOfCode(code uint32) (PositionSurface, bool) {
	switch code {
	case AgdxKvGetCode, AgdxKvScanCode, AgdxKvNamespacesCode, AgdxKvExistsCode,
		AgdxKvSetCode, AgdxKvCasCode, AgdxKvDeleteCode, AgdxKvDeleteManyCode,
		AgdxKvExpireCode, AgdxKvPatchCode, AgdxKvCopyCode, AgdxKvMoveCode,
		AgdxKvCasFencedCode, AgdxKvLeaseCode, AgdxKvLeaseRenewCode, AgdxKvReleaseCode:
		return SurfaceKv, true
	case AgdxForkListCode, AgdxForkCreateCode, AgdxForkDeleteCode,
		AgdxForkPromoteCode, AgdxForkPutCode:
		return SurfaceFork, true
	case AgdxGraphQueryCode, AgdxGraphNeighborsCode, AgdxGraphUpsertCode:
		return SurfaceGraph, true
	default:
		return "", false
	}
}

// SessionPosition is how far a node's session fold has applied one partition of
// a session source. Positions compare only within one stream and topic
// generation.
type SessionPosition struct {
	StreamID uint32 `cbor:"stream_id"`
	// StreamGeneration is the creation time of the stream incarnation, in
	// microseconds.
	StreamGeneration uint64 `cbor:"stream_generation"`
	Topic            string `cbor:"topic"`
	// TopicGeneration is the creation time of the topic incarnation.
	TopicGeneration uint64 `cbor:"topic_generation"`
	PartitionID     uint32 `cbor:"partition_id"`
	// Offset is the last offset the fold applied.
	Offset uint64 `cbor:"offset"`
}

// DeniesUnknownFields refuses unknown fields on a session position.
func (*SessionPosition) DeniesUnknownFields() bool { return true }

// sameLane reports whether two session positions address the same partition of
// the same topic incarnation, the only case in which their offsets compare.
func (position SessionPosition) sameLane(other SessionPosition) bool {
	return position.StreamID == other.StreamID &&
		position.StreamGeneration == other.StreamGeneration &&
		position.Topic == other.Topic &&
		position.TopicGeneration == other.TopicGeneration &&
		position.PartitionID == other.PartitionID
}

// ManagedReadEnvelope is a managed read sent with a consistency requirement.
//
// With MinPosition, the answering node serves the read only once its fold for
// the read's surface has applied that position, so a read never misses its own
// earlier writes whichever node answers. With LogHead, the node first waits
// until its fold has applied every record the surface's log held when the read
// arrived, which a client asks for after its requests moved to another node. A
// node that cannot get there within its wait bound answers stale rather than
// older state.
type ManagedReadEnvelope struct {
	V uint32 `cbor:"v"`
	// Read is the request the client would otherwise send on its own.
	Read        []byte            `cbor:"read"`
	MinPosition *MutationPosition `cbor:"min_position,omitempty"`
	LogHead     bool              `cbor:"log_head,omitempty"`
	// MinSessions names the session-fold positions a session read requires.
	// Positions of other streams are ignored.
	MinSessions []SessionPosition `cbor:"min_sessions,omitempty"`
}

// DeniesUnknownFields keeps a plain read request from decoding as a barrier
// frame, so a wrapped and an unwrapped request can never be confused.
func (*ManagedReadEnvelope) DeniesUnknownFields() bool { return true }

// AppliedFrontier is the positions a node's folds had applied when it answered,
// per surface. A surface whose fold has applied nothing yet is absent.
// Sessions covers the stream of a session read and is empty for every other
// reply.
type AppliedFrontier struct {
	Kv       *MutationPosition `cbor:"kv,omitempty"`
	Fork     *MutationPosition `cbor:"fork,omitempty"`
	Graph    *MutationPosition `cbor:"graph,omitempty"`
	Sessions []SessionPosition `cbor:"sessions,omitempty"`
}

// DeniesUnknownFields refuses unknown fields on a frontier.
func (*AppliedFrontier) DeniesUnknownFields() bool { return true }

// Position reads one surface's applied position.
func (frontier AppliedFrontier) Position(surface PositionSurface) *MutationPosition {
	switch surface {
	case SurfaceKv:
		return frontier.Kv
	case SurfaceFork:
		return frontier.Fork
	case SurfaceGraph:
		return frontier.Graph
	default:
		return nil
	}
}

// MergeFrontier folds an answering node's frontier into the client's running
// one, keeping the highest position per surface.
//
// A position from another topic generation does not compare, so it replaces
// the held one rather than being silently dropped: the surface was rebuilt and
// the old coordinate no longer means anything.
func MergeFrontier(held *AppliedFrontier, seen AppliedFrontier) {
	mergeOne := func(current **MutationPosition, next *MutationPosition) {
		if next == nil {
			return
		}
		if *current == nil {
			copied := *next
			*current = &copied
			return
		}
		earlier, comparable := (**current).Precedes(*next)
		if !comparable || earlier {
			copied := *next
			*current = &copied
		}
	}
	mergeOne(&held.Kv, seen.Kv)
	mergeOne(&held.Fork, seen.Fork)
	mergeOne(&held.Graph, seen.Graph)

	for _, position := range seen.Sessions {
		replaced := false
		for i, existing := range held.Sessions {
			if existing.sameLane(position) {
				if position.Offset > existing.Offset {
					held.Sessions[i] = position
				}
				replaced = true
				break
			}
		}
		if !replaced {
			held.Sessions = append(held.Sessions, position)
		}
	}
}

// ManagedReplyEnvelope is the reply to a managed request: the surface reply,
// plus the frontier the answering node had applied once it computed that reply.
//
// After a mutation the frontier covers the mutation itself, and after a read it
// covers every write the read could see. The client keeps the highest position
// per surface and sends it back as a read's MinPosition.
type ManagedReplyEnvelope struct {
	V        uint32          `cbor:"v"`
	Frontier AppliedFrontier `cbor:"frontier"`
	Reply    []byte          `cbor:"reply"`
}

// DeniesUnknownFields keeps a surface reply from decoding as this frame.
func (*ManagedReplyEnvelope) DeniesUnknownFields() bool { return true }

// MutationCommandEnvelope is one managed command stored on a mutation topic.
//
// Payload is the canonical typed request encoded with the normal wire framing
// for CommandCode, so append and fold share one request schema instead of
// maintaining a second mutation representation.
type MutationCommandEnvelope struct {
	V               uint32 `cbor:"v"`
	OperationID     U128   `cbor:"operation_id"`
	DeploymentID    uint64 `cbor:"deployment_id"`
	TimestampMicros uint64 `cbor:"timestamp_micros"`
	CommandCode     uint32 `cbor:"command_code"`
	// Scope is the trusted stream identity carried through durable replay.
	Scope   *ForwardedScope `cbor:"scope,omitempty"`
	Payload []byte          `cbor:"payload"`
	// Caller is the client that asked for the command, stamped before it
	// enters the log and the only actor it names. Absent on a command the
	// backend issues on its own.
	Caller *Caller `cbor:"caller,omitempty"`
}

// Validate checks the version and operation identity.
func (envelope *MutationCommandEnvelope) Validate() error {
	if envelope.V == 0 {
		return &InvalidError{Reason: "mutation command version must not be zero"}
	}
	if envelope.OperationID.IsZero() {
		return &InvalidError{Reason: "mutation command operation id must not be zero"}
	}
	return nil
}

// UnmarshalCBOR reads the value, refusing one outside the closed vocabulary.
func (value *PositionSurface) UnmarshalCBOR(item cborform.Value) error {
	return decodeClosedEnum(item, value, "position surface", SurfaceKv, SurfaceFork, SurfaceGraph)
}
