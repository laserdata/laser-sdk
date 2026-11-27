package laser

import (
	"context"
	"errors"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// agentAdvertisement is a spawned agent's capability card, kept fresh on its
// own ticker until withdraw stops it: the registry card and the inbox's live
// presence that let capability routing and fan-out find this agent, so
// SpawnAgent needs no caller wiring to advertise one.
type agentAdvertisement struct {
	laser *Laser
	id    wire.AgentID
	card  wire.AgentCard
	stop  chan struct{}
	done  chan struct{}
}

// startAdvertisement publishes id's card (capabilities, inbox, a ttl a few
// refreshes long) and its live presence, then refreshes the card every
// refresh (DefaultCardRefresh when zero) until withdraw. Returns nil when
// capabilities is empty: a private agent that does not advertise.
//
// Both the card and the presence are best-effort: most failures here do not
// stop the agent, since a missing advertisement only degrades discovery. The
// one exception is a *PresenceConflictError: this connection already
// advertises a different agent's presence, which is a caller error (two
// agents sharing one connection) rather than a degraded deployment, so it
// fails the spawn instead of starting an agent whose presence was silently
// never advertised.
func startAdvertisement(
	ctx context.Context, laser *Laser, id wire.AgentID, inbox string,
	capabilities []wire.CapabilityDescriptor, refresh time.Duration,
) (*agentAdvertisement, error) {
	if len(capabilities) == 0 {
		return nil, nil
	}
	if refresh <= 0 {
		refresh = DefaultCardRefresh
	}
	ttl := uint64(refresh.Microseconds()) * uint64(cardTTLHeartbeats)
	card := wire.AgentCard{
		Capabilities: append([]wire.CapabilityDescriptor(nil), capabilities...),
		TTLMicros:    &ttl,
		Inbox:        &inbox,
	}
	_ = laser.PublishCard(ctx, id, card)
	presence := wire.NewAgentPresence(id)
	presence.Inbox = &inbox
	if err := laser.AdvertisePresence(ctx, presence); err != nil {
		var conflict *PresenceConflictError
		if errors.As(err, &conflict) {
			return nil, err
		}
		// Expected against a deployment without the presence command, where
		// routing falls back to the card's inbox. Not an error.
	}
	advertisement := &agentAdvertisement{
		laser: laser, id: id, card: card, stop: make(chan struct{}), done: make(chan struct{}),
	}
	go advertisement.refreshLoop(refresh)
	return advertisement, nil
}

// refreshLoop republishes the card every tick while the agent runs, so it
// stays fresh within its ttl. A failed refresh is ignored; the next tick
// tries again.
func (advertisement *agentAdvertisement) refreshLoop(refresh time.Duration) {
	defer close(advertisement.done)
	ticker := time.NewTicker(refresh)
	defer ticker.Stop()
	for {
		select {
		case <-advertisement.stop:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), refresh)
			_ = advertisement.laser.PublishCard(ctx, advertisement.id, advertisement.card)
			cancel()
		}
	}
}

// withdraw stops the refresh, marks every capability unavailable, republishes
// the card once more, and clears the live presence, so routing stops
// choosing this agent before the card's ttl runs out. Best-effort: a failed
// withdrawal is not reported, since the agent is already stopping.
func (advertisement *agentAdvertisement) withdraw() {
	close(advertisement.stop)
	<-advertisement.done
	unavailable := wire.HealthUnavailable
	for index := range advertisement.card.Capabilities {
		advertisement.card.Capabilities[index].Health = &unavailable
	}
	ctx, cancel := context.WithTimeout(context.Background(), presenceAttempt)
	defer cancel()
	_ = advertisement.laser.PublishCard(ctx, advertisement.id, advertisement.card)
	_ = advertisement.laser.ClearPresence(ctx)
}
