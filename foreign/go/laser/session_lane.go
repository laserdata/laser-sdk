package laser

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	iggcon "github.com/apache/iggy/foreign/go/contracts"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// laneIdentity is one incarnation of a session lane.
//
// The numeric ids alone do not identify it: a deleted stream or topic that is
// created again takes the same name and can take the same id, so a handle
// retained across the gap would address the replacement as if nothing had
// happened. The creation timestamps are what separate the incarnations, and
// the partition count is here because a record's partition is chosen by
// hashing over it -- change the count and the same conversation's records
// land somewhere else, which splits one session's history across two
// partitions just as surely as a recreated topic does.
//
// Ported from LaneIdentity in sdk/src/agent/session.rs.
type laneIdentity struct {
	streamID         uint32
	streamGeneration uint64
	topicID          uint32
	topicGeneration  uint64
	partitions       uint32
}

// laneGuard pins the lane identity a set of session handles first observed,
// and refuses them once the lane underneath is no longer that one.
//
// One guard is shared by a Sessions and every handle derived from it, so the
// pin is per-lane rather than per-handle: two handles on the same lane must
// not disagree about which incarnation they are writing to.
//
// Ported from LaneGuard in sdk/src/agent/session.rs.
type laneGuard struct {
	mutex    sync.Mutex
	identity *laneIdentity
	// registered latches the one-time check that the deployment's registered
	// session source names this same lane. Atomic rather than under mutex
	// because it is read on every write and written once.
	registered atomic.Bool
}

// observe reads the lane's current identity, pinning it the first time and
// refusing afterwards if it has changed.
//
// The first observation is the pin: a handle is allowed to start on whatever
// incarnation is there when it starts, and is held to that one from then on.
func (guard *laneGuard) observe(
	ctx context.Context, laser *Laser, stream, topic string,
) (laneIdentity, error) {
	current, err := readLaneIdentity(ctx, laser, stream, topic)
	if err != nil {
		return laneIdentity{}, err
	}
	// No partitions means nothing can be written and nothing read back, so
	// this is a lane in name only. Refused as stale rather than left to fail
	// later as an opaque publish error.
	if current.partitions == 0 {
		return laneIdentity{}, &StaleLaneError{
			Reason: topic + " has no partitions"}
	}
	guard.mutex.Lock()
	defer guard.mutex.Unlock()
	if guard.identity == nil {
		pinned := current
		guard.identity = &pinned
		return current, nil
	}
	if *guard.identity != current {
		return laneIdentity{}, &StaleLaneError{
			Reason: "the session lane identity or partition count changed"}
	}
	return *guard.identity, nil
}

// check is observe plus the one-time agreement between the lane this handle
// writes to and the session source the deployment has registered.
//
// A deployment that folds sessions reads them through the registered source.
// A lane that is healthy in itself but is not the registered one would accept
// writes that no aggregate read would ever return, so the write is refused
// instead. Checked once and latched: it cannot become true again without a
// re-registration, which mints a new identity and fails the pin above first.
func (guard *laneGuard) check(
	ctx context.Context, laser *Laser, stream, topic string,
	conversation wire.ConversationID,
) (laneIdentity, error) {
	identity, err := guard.observe(ctx, laser, stream, topic)
	if err != nil {
		return laneIdentity{}, err
	}
	if guard.registered.Load() {
		return identity, nil
	}
	// A deployment that does not fold sessions has no registration to read,
	// so there is nothing to disagree with and the pin above is the whole
	// check.
	if !laser.Capabilities().Sessions {
		return identity, nil
	}
	sources, err := laser.SessionsIn(stream).LaneSources(ctx, conversation)
	if err != nil {
		// A session the deployment holds no registration for is not a
		// mismatch: nothing has claimed the lane yet. Any other failure is
		// the caller's to see -- including a session the index does not
		// know, which Rust also propagates rather than waving through.
		var refusal *SessionError
		if errors.As(err, &refusal) &&
			refusal.Failure.Kind == wire.SessionNotRegistered {
			return identity, nil
		}
		return laneIdentity{}, err
	}
	if sources == nil || sources.Lane == nil {
		return identity, nil
	}
	lane := *sources.Lane
	if lane.TopicID != identity.topicID ||
		lane.Generation != identity.topicGeneration ||
		lane.Partitions != identity.partitions {
		return laneIdentity{}, &StaleLaneError{
			Reason: "the registered session lane does not match the current source"}
	}
	guard.registered.Store(true)
	return identity, nil
}

// readLaneIdentity reads the stream and its session topic in one call, so
// both generations describe the same moment. Reading them separately could
// straddle a recreation and compose an identity that never existed.
func readLaneIdentity(
	ctx context.Context, laser *Laser, stream, topic string,
) (laneIdentity, error) {
	if stream == "" {
		return laneIdentity{}, errNoStream()
	}
	client, err := laser.connected()
	if err != nil {
		return laneIdentity{}, err
	}
	streamID, err := iggcon.NewIdentifier(stream)
	if err != nil {
		return laneIdentity{}, &InvalidError{Reason: "stream name: " + err.Error()}
	}
	details, err := client.GetStream(ctx, streamID)
	if err != nil {
		return laneIdentity{}, &TransportError{Reason: "read the session stream", Cause: err}
	}
	if details == nil {
		return laneIdentity{}, missingLane("the session stream does not exist")
	}
	for index := range details.Topics {
		if details.Topics[index].Name != topic {
			continue
		}
		return laneIdentity{
			streamID:         details.Id,
			streamGeneration: details.CreatedAt,
			topicID:          details.Topics[index].Id,
			topicGeneration:  details.Topics[index].CreatedAt,
			partitions:       details.Topics[index].PartitionsCount,
		}, nil
	}
	return laneIdentity{}, missingLane(topic + " does not exist")
}

// missingLane names the call that creates what is missing, so the refusal is
// actionable rather than only a diagnosis.
func missingLane(missing string) *StaleLaneError {
	return &StaleLaneError{
		Reason: missing + ": create it with Sessions.Bootstrap before writing"}
}
