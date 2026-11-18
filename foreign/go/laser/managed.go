package laser

import (
	"context"
	"crypto/rand"
	"encoding/binary"
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
	laser.mutex.RLock()
	defer laser.mutex.RUnlock()
	return laser.frontier
}

// SendManaged sends a managed command and returns the surface reply.
//
// A command that changes state is wrapped in a managed envelope carrying a
// fresh operation identity, so a transport retry of the same call is applied
// once rather than twice. A read is sent as given.
//
// This is the raw band. Whether a deployment serves a given surface is the
// surface's own question, because they do not share one answer: the key-value
// store and query need a managed backend, while the authorization verbs are
// served by the streaming server itself. Each typed surface gates its own
// calls, so a refusal names the surface rather than the band.
func (laser *Laser) SendManaged(
	ctx context.Context, code uint32, payload []byte,
) ([]byte, error) {
	if !wire.IsIdempotentManagedRequest(code) {
		return laser.sendManagedFrame(ctx, code, payload)
	}
	return laser.SendManagedWithOperation(ctx, code, payload, NewOperationID())
}

// SendManagedWithOperation is SendManaged with a caller-chosen operation
// identity, so a retry that outlives the process still applies once.
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
	return laser.sendManagedFrame(ctx, code, framed)
}

func (laser *Laser) sendManagedFrame(
	ctx context.Context, code uint32, frame []byte,
) ([]byte, error) {
	if err := laser.ensureScopable(code); err != nil {
		return nil, err
	}
	laser.mutex.RLock()
	client := laser.client
	laser.mutex.RUnlock()
	if client == nil {
		return nil, &TransportError{Reason: "the client is closed"}
	}
	reply, err := client.SendBinaryRequest(ctx, code, frame)
	if err != nil {
		return nil, &TransportError{Reason: "managed command", Cause: err}
	}
	return laser.openPositioned(reply), nil
}

// openPositioned unwraps the envelope a managed node may answer in, folding
// the frontier it carries into this client's running one.
//
// A surface reply that is not positioned is returned as it arrived. The
// envelope refuses unknown fields, so a plain reply cannot be mistaken for one.
func (laser *Laser) openPositioned(reply []byte) []byte {
	var envelope wire.ManagedReplyEnvelope
	if err := wire.Unmarshal(reply, &envelope); err != nil {
		return reply
	}
	laser.mutex.Lock()
	wire.MergeFrontier(&laser.frontier, envelope.Frontier)
	laser.mutex.Unlock()
	return envelope.Reply
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
