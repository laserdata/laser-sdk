package laser

import (
	"context"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// presenceAttempt bounds one advertise or clear against the replica pinned
// for this connection's presence, so a pinned replica that stops answering
// fails fast rather than hanging on the managed backoff.
const presenceAttempt = 5 * time.Second

// AdvertisePresence advertises this connection's live presence
// (AGDX_SET_CLIENT_METADATA): the agent id this connection is and the inbox
// topic it currently consumes, so a fused registry can route work to it
// without any shared topic-name convention. Mutable mid-session
// (re-advertise to move the inbox to a per-workflow topic) and
// garbage-collected for free on disconnect. Use ClearPresence to withdraw it.
//
// Served by Iggy, so it returns a transport error against a deployment that
// lacks the command.
//
// The presence lives on the replica that received it. The first advertise
// pins that replica, and every later advertise or clear goes there, so a
// client whose managed requests moved to another node never leaves a stale
// presence behind or advertises a second one elsewhere. While the pinned
// replica does not answer within presenceAttempt, the call fails and keeps
// the pin.
func (laser *Laser) AdvertisePresence(ctx context.Context, presence wire.AgentPresence) error {
	if presence.Agent.IsZero() {
		return &InvalidError{Reason: "presence must name its agent"}
	}
	shared := laser.state()
	shared.presenceMutex.Lock()
	defer shared.presenceMutex.Unlock()
	if err := claimPresence(shared, presence.Agent); err != nil {
		return err
	}
	payload, err := wire.Marshal(presence)
	if err != nil {
		return &CodecError{Reason: "encode presence", Cause: err}
	}
	replica, err := laser.sendPresence(ctx, payload, shared.presencePin)
	if err != nil {
		return err
	}
	if replica != nil {
		shared.presencePin = replica
	}
	return nil
}

// claimPresence binds shared's connection to requested, refusing a second
// agent once one is claimed. One physical connection advertises one logical
// agent: a second would overwrite the first's connection metadata and steal
// its route. The caller holds shared.presenceMutex.
func claimPresence(shared *connection, requested wire.AgentID) error {
	if shared.advertisedAgent != nil && *shared.advertisedAgent != requested {
		return &PresenceConflictError{Advertised: *shared.advertisedAgent, Requested: requested}
	}
	shared.advertisedAgent = &requested
	return nil
}

// ClearPresence withdraws this connection's advertised presence (an empty
// AGDX_SET_CLIENT_METADATA clears it). Disconnecting clears it too; this is
// the explicit early withdrawal. The clear goes to the replica that holds the
// presence, and while that replica does not answer the call fails and keeps
// the pin, never reporting a presence cleared that is still advertised.
func (laser *Laser) ClearPresence(ctx context.Context) error {
	shared := laser.state()
	shared.presenceMutex.Lock()
	defer shared.presenceMutex.Unlock()
	if _, err := laser.sendPresence(ctx, nil, shared.presencePin); err != nil {
		return err
	}
	shared.presencePin = nil
	shared.advertisedAgent = nil
	return nil
}

// sendPresence sends one presence frame (nil payload clears it) to the pinned
// replica, or to the caller's own node when nothing is pinned yet, naming
// whichever replica answered so a caller can pin it.
func (laser *Laser) sendPresence(
	ctx context.Context, payload []byte, pinned *uint8,
) (*uint8, error) {
	client, err := laser.connected()
	if err != nil {
		return nil, err
	}
	attempt, cancel := context.WithTimeout(ctx, presenceAttempt)
	defer cancel()
	target := callerTarget()
	if pinned != nil {
		target = replicaTarget(*pinned)
	}
	routed, err := laser.nodes().send(
		attempt, client, wire.AgdxSetClientMetadataCode, payload, target, walkingOnce())
	laser.reportLink(err)
	if err != nil {
		return nil, &TransportError{Reason: "advertise presence", Cause: err}
	}
	return routed.replica, nil
}

// PublishCard publishes source's capability card to the registry topic, the
// write side of the log card registry: a status record with operation "card"
// carrying the encoded card. A registry read model folds the latest card per
// agent, so capability routing can resolve source. Re-publish on an interval
// shorter than the card's TTLMicros to keep it fresh, since a card older than
// its ttl reads as a dead agent.
func (laser *Laser) PublishCard(ctx context.Context, source wire.AgentID, card wire.AgentCard) error {
	stream := laser.defaultStream
	if stream == "" {
		return errNoStream()
	}
	body, err := wire.Marshal(card)
	if err != nil {
		return &CodecError{Reason: "encode card", Cause: err}
	}
	if err := laser.Stream(stream).Topic(wire.AgentRegistry).Ensure(ctx, 1, nil); err != nil {
		return err
	}
	_, err = laser.AgdxOn(stream, wire.AgentRegistry, source, NewConversationID()).
		Status(wire.OperationCard).
		Body(body).
		ContentType(wire.ContentCBOR).
		Send(ctx)
	return err
}
