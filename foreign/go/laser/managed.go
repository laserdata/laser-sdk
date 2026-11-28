package laser

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"slices"
	"strings"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// ResourceNaming decides whether a managed name is scoped to the default
// stream before it is sent.
type ResourceNaming int

const (
	// ResourceNamingStream sends stream:<default stream>/<name> for key-value,
	// memory, lease and fence namespaces, the key registry, graph names,
	// projection and index ids, query indexes, fork ids, and the watch index
	// filter. The default, and what every other client does.
	ResourceNamingStream ResourceNaming = iota
	// ResourceNamingBare sends the caller's name unchanged, for a deployment
	// that does not scope managed names per stream.
	ResourceNamingBare
)

// resourceStream is the stream managed names are scoped to, empty when this
// handle does not scope them.
func (laser *Laser) resourceStream() string {
	if laser.resourceNaming == ResourceNamingBare {
		return ""
	}
	return laser.defaultStream
}

// ResourceName is the name to send for a caller's managed name. A handle that
// does not scope, or that pinned no default stream, sends it unchanged.
func (laser *Laser) ResourceName(name string) string {
	stream := laser.resourceStream()
	if stream == "" {
		return name
	}
	return wire.ScopedResource(stream, name)
}

// LocalResourceName is the caller's name for one a listing returned: the local
// part when it sits under this handle's own prefix. The second result is false
// when the name belongs to another stream or, while scoping is active, to no
// stream, so a listing never hands back a name this handle cannot address.
func (laser *Laser) LocalResourceName(name string) (string, bool) {
	stream := laser.resourceStream()
	if stream == "" {
		return name, true
	}
	owner, local, scoped := wire.SplitScopedResource(name)
	if !scoped || owner != stream {
		return "", false
	}
	return local, true
}

// ensureScopable refuses a managed call this handle cannot name resources for.
//
// A stream whose name contains a slash cannot be scoped, because the first
// slash is what ends the stream in a scoped name, so the result would address
// a different resource than the caller meant. The probe is exempt: it names no
// resource.
func (laser *Laser) ensureScopable(code uint32) error {
	stream := laser.resourceStream()
	if stream == "" || code == wire.AgdxHelloCode || !strings.Contains(stream, "/") {
		return nil
	}
	return &InvalidError{Reason: "default stream `" + stream +
		"` contains `/`, so managed names cannot be scoped to it, use ResourceNamingBare"}
}

// SeenPositions is the highest mutation position this client has seen per
// managed surface. Every managed reply raises it, and a later read of that
// surface sends it, so whichever node answers has applied at least what this
// client wrote or read before.
func (laser *Laser) SeenPositions() wire.AppliedFrontier {
	state := laser.state()
	state.mutex.RLock()
	defer state.mutex.RUnlock()
	snapshot := state.frontier
	snapshot.Kv = copyPosition(snapshot.Kv)
	snapshot.Fork = copyPosition(snapshot.Fork)
	snapshot.Graph = copyPosition(snapshot.Graph)
	snapshot.Sessions = slices.Clone(snapshot.Sessions)
	return snapshot
}

func copyPosition(position *wire.MutationPosition) *wire.MutationPosition {
	if position == nil {
		return nil
	}
	snapshot := *position
	return &snapshot
}

// SendManaged sends a managed command and returns the surface reply.
//
// A command that changes state is wrapped in a managed envelope carrying a
// fresh operation identity, so a transport retry of the same call is applied
// once rather than twice. A read is sent as given.
//
// A read every node refused for want of its managed backend waits on the
// managed backoff and walks the roster again until it is served or
// ManagedDeadline passes, then returns the same refusal it would have
// returned at once. A mutation already repeated under its identity. A read
// has none, and on a single node, or on a cluster that restarted all at
// once, the first refusal is one the deployment served a moment later.
//
// This is the raw band. Whether a deployment serves a given surface is the
// surface's own question, because they do not share one answer: the key-value
// store and query need a managed backend, while the authorization verbs are
// served by the streaming server itself. Each typed surface gates its own
// calls, so a refusal names the surface rather than the band.
func (laser *Laser) SendManaged(
	ctx context.Context, code uint32, payload []byte,
) ([]byte, error) {
	return laser.sendManagedWalking(ctx, code, payload, walkingUntilServed())
}

// sendManagedWalking is SendManaged with the walk a read takes while every
// node refuses it. A mutation repeats under its own operation identity and
// ignores the walk.
func (laser *Laser) sendManagedWalking(
	ctx context.Context, code uint32, payload []byte, w walk,
) ([]byte, error) {
	if wire.IsBarrierRead(code) {
		shared := laser.state()
		shared.mutex.RLock()
		read := wire.ManagedReadEnvelope{V: wire.ManagedRequestVersion, Read: payload}
		if surface, ok := wire.PositionSurfaceOfCode(code); ok {
			read.MinPosition = shared.frontier.Position(surface)
		} else {
			read.MinSessions = slices.Clone(shared.frontier.Sessions)
		}
		shared.mutex.RUnlock()
		frame, err := wire.Marshal(read)
		if err != nil {
			return nil, &CodecError{Reason: "encode managed read", Cause: err}
		}
		return laser.sendManagedFrame(ctx, code, frame, w)
	}
	if !wire.IsIdempotentManagedRequest(code) {
		return laser.sendManagedFrame(ctx, code, payload, w)
	}
	return laser.SendManagedWithOperation(ctx, code, payload, NewOperationID())
}

// SendManagedWithOperation is SendManaged with a caller-chosen operation
// identity, so a retry that outlives the process still applies once.
//
// The identity is also what lets this client repeat the mutation itself
// while its outcome is unknown, up to ManagedDeadline. A reply that never
// came back is not a mutation that did not happen, so the frame is sent
// again rather than reported as a failure the deployment may have carried
// out. An outcome still unknown at the deadline, or a refusal after an
// attempt whose outcome was never known, is an AmbiguousMutationError naming
// the identity to recover under.
func (laser *Laser) SendManagedWithOperation(
	ctx context.Context, code uint32, payload []byte, operationID wire.U128,
) ([]byte, error) {
	if operationID.IsZero() {
		return nil, &InvalidError{Reason: "operation id must be nonzero"}
	}
	framed, err := wire.Marshal(wire.ManagedRequestEnvelope{
		V:           wire.ManagedRequestVersion,
		OperationID: operationID,
		Payload:     payload,
	})
	if err != nil {
		return nil, &CodecError{Reason: "encode managed request", Cause: err}
	}
	return laser.sendManagedMutation(ctx, code, operationID, framed)
}

func (laser *Laser) sendManagedFrame(
	ctx context.Context, code uint32, frame []byte, w walk,
) ([]byte, error) {
	if err := laser.ensureScopable(code); err != nil {
		return nil, err
	}
	shared := laser.state()
	shared.mutex.RLock()
	client := shared.client
	shared.mutex.RUnlock()
	if client == nil {
		return nil, &TransportError{Reason: "the client is closed"}
	}
	// On a single node the roster has one entry and this is the plain call it
	// always was. On a cluster it keeps this client to one node, picked by a
	// key of its own, and moves off a node whose link fails or that cannot
	// reach its own backend.
	routed, err := laser.nodes().send(ctx, client, code, frame, targetOfCode(code), w)
	laser.reportLink(err)
	if err != nil {
		return nil, &TransportError{Reason: "managed command", Cause: err}
	}
	if routed.frontier != nil {
		shared.mutex.Lock()
		wire.MergeFrontier(&shared.frontier, *routed.frontier)
		shared.mutex.Unlock()
	}
	return routed.reply, nil
}

// nodes is this connection's node router, opened on first use.
//
// One router per connection is what makes the caller's node stable: a router
// per handle would spread one client's requests over the cluster rather than
// keeping it to the node it picked.
func (laser *Laser) nodes() *nodeRouter {
	shared := laser.state()
	shared.mutex.Lock()
	defer shared.mutex.Unlock()
	if shared.router == nil {
		shared.router = newNodeRouter()
	}
	return shared.router
}

func decodeManagedReply(raw []byte, reply any, reason string) error {
	err := wire.Unmarshal(raw, reply)
	if err == nil {
		return nil
	}
	var failure wire.CommandError
	if wire.Unmarshal(raw, &failure) == nil && failure.Code != wire.ResultOk {
		return &CommandFailure{Failure: failure}
	}
	return &CodecError{Reason: reason, Cause: err}
}

// NewOperationID mints the identity a managed write is applied under.
//
// The layout is a ULID's: a 48-bit millisecond timestamp then 80 random bits,
// so ids from one client sort by the order they were minted in, and two
// clients minting in the same millisecond still differ.
func NewOperationID() wire.U128 {
	var value wire.U128
	binary.BigEndian.PutUint64(value[:8], uint64(time.Now().UnixMilli())<<16)
	var random [10]byte
	if _, err := rand.Read(random[:]); err != nil {
		// crypto/rand does not fail on any supported platform, and a zero
		// identity would be refused rather than silently reused.
		panic("laser: no entropy to mint an operation id: " + err.Error())
	}
	value[6], value[7] = random[0], random[1]
	copy(value[8:], random[2:])
	return value
}
