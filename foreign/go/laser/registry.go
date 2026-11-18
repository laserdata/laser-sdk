package laser

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// RegisteredCard is one agent's card as the registry observed it.
type RegisteredCard struct {
	Agent wire.AgentID
	Card  wire.AgentCard
	// ObservedAt is when the registry saw this card. It is the registry's own
	// fold time rather than the agent's clock, so a card refreshed faster than
	// its lifetime keeps a healthy agent fresh without the two clocks having
	// to agree.
	ObservedAt time.Time
}

// Fresh reports whether the card still describes a live agent.
//
// A card with no lifetime never expires, which is a discovery-only card. One
// with a lifetime is fresh while it has not run out, which is what makes a
// card liveness rather than only discovery.
func (card RegisteredCard) Fresh(now time.Time) bool {
	if card.Card.TTLMicros == nil {
		return true
	}
	lifetime := time.Duration(*card.Card.TTLMicros) * time.Microsecond
	return !card.ObservedAt.Add(lifetime).Before(now)
}

// Serves reports whether the card advertises a skill at all.
func (card RegisteredCard) Serves(skill string) bool {
	return card.Capability(skill) != nil
}

// Capability is the card's descriptor for a skill, nil when it advertises
// none.
func (card RegisteredCard) Capability(skill string) *wire.CapabilityDescriptor {
	for index := range card.Card.Capabilities {
		if card.Card.Capabilities[index].SkillID == skill {
			return &card.Card.Capabilities[index]
		}
	}
	return nil
}

// AvailableFor reports whether the agent's advertised health permits routing
// this skill to it.
//
// An agent that declared itself unavailable is skipped. Degraded, healthy and
// unspecified all stay routable, because an agent that is struggling is still
// better than no agent, and an agent that said nothing has not opted out.
func (card RegisteredCard) AvailableFor(skill string) bool {
	capability := card.Capability(skill)
	if capability == nil {
		return false
	}
	return capability.Health == nil || *capability.Health != wire.HealthUnavailable
}

// AgentRegistry is what the deployment knows about its agents: the cards they
// published and the ones an operator took out of rotation.
//
// It is safe for concurrent use, because a router resolves against it from
// whichever goroutine is sending.
type AgentRegistry struct {
	mutex       sync.RWMutex
	cards       map[wire.AgentID]RegisteredCard
	quarantined map[wire.AgentID]struct{}
}

// NewAgentRegistry opens an empty registry.
func NewAgentRegistry() *AgentRegistry {
	return &AgentRegistry{
		cards:       map[wire.AgentID]RegisteredCard{},
		quarantined: map[wire.AgentID]struct{}{},
	}
}

// Observe records an agent's card, replacing whatever was held for it.
func (registry *AgentRegistry) Observe(
	agent wire.AgentID, card wire.AgentCard, at time.Time,
) {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	registry.cards[agent] = RegisteredCard{Agent: agent, Card: card, ObservedAt: at}
}

// Quarantine takes an agent out of rotation. A quarantined agent is excluded
// from capability resolution and refused by a route that names it, so an
// operator can stop work reaching it without stopping the agent.
func (registry *AgentRegistry) Quarantine(agent wire.AgentID) {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	registry.quarantined[agent] = struct{}{}
}

// Release lifts a quarantine.
func (registry *AgentRegistry) Release(agent wire.AgentID) {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	delete(registry.quarantined, agent)
}

// Quarantined reports whether an agent is out of rotation.
func (registry *AgentRegistry) Quarantined(agent wire.AgentID) bool {
	registry.mutex.RLock()
	defer registry.mutex.RUnlock()
	_, held := registry.quarantined[agent]
	return held
}

// Card is one agent's card, and whether the registry holds a fresh one.
func (registry *AgentRegistry) Card(
	agent wire.AgentID, now time.Time,
) (RegisteredCard, bool) {
	registry.mutex.RLock()
	defer registry.mutex.RUnlock()
	card, held := registry.cards[agent]
	if !held || !card.Fresh(now) {
		return RegisteredCard{}, false
	}
	return card, true
}

// Capable lists the agents that can be handed a skill right now: fresh card,
// advertising the skill, not declaring itself unavailable, not quarantined.
//
// The result is sorted by agent name, so two resolutions over one registry
// pick the same agent rather than whichever the map iterated first.
func (registry *AgentRegistry) Capable(skill string, now time.Time) []RegisteredCard {
	registry.mutex.RLock()
	defer registry.mutex.RUnlock()

	var capable []RegisteredCard
	for agent, card := range registry.cards {
		if _, out := registry.quarantined[agent]; out {
			continue
		}
		if !card.Fresh(now) || !card.AvailableFor(skill) {
			continue
		}
		capable = append(capable, card)
	}
	sort.Slice(capable, func(first, second int) bool {
		return capable[first].Agent.String() < capable[second].Agent.String()
	})
	return capable
}

// Refresh folds the registry topic into the registry.
//
// The cards are on the log rather than in a store, so a registry is rebuilt by
// reading it: a client that starts after the agents did still learns what they
// advertise. Reading from the cursor's position rather than from the start is
// what lets a long-running client refresh cheaply.
//
// A record that is not a card is left alone, because the topic also carries
// the quarantine facts an operator writes.
func (registry *AgentRegistry) Refresh(ctx context.Context, cursor *Cursor) error {
	records, err := cursor.Poll(ctx)
	if err != nil {
		return err
	}
	for _, record := range records {
		message, err := OpenAgdx(record)
		if err != nil {
			// A record that is not an agent message is another writer's
			// business, not a reason to stop folding.
			continue
		}
		registry.fold(message, record)
	}
	return nil
}

// fold applies one registry record.
//
// A card names the agent that published it through the envelope's source, so
// the registry never has to trust a card to say who it belongs to.
func (registry *AgentRegistry) fold(message *AgdxMessage, record *Record) {
	envelope := message.Envelope
	if envelope.Operation == nil {
		return
	}
	observed := time.UnixMicro(int64(record.Message.Header.Timestamp))
	switch *envelope.Operation {
	case wire.OperationCard:
		var card wire.AgentCard
		if err := wire.Unmarshal(envelope.Body, &card); err != nil {
			return
		}
		if err := card.Validate(); err != nil {
			// A card that fails the contract would advertise a capability the
			// deployment never agreed to, so it is not folded.
			return
		}
		registry.Observe(envelope.Source, card, observed)
	case wire.OperationQuarantine:
		// A quarantine is bounded, so one that has lapsed is not applied: an
		// operator who set it and moved on does not exclude an agent forever.
		if lapsed(envelope, observed) {
			return
		}
		registry.Quarantine(envelope.Source)
	case wire.OperationUnquarantine:
		registry.Release(envelope.Source)
	}
}

// lapsed reports whether a bounded fact has passed its expiry.
//
// The fold reads the record's own append time rather than the local clock, so
// replaying a log settles to the same registry however long afterwards it is
// read.
func lapsed(envelope wire.AgentEnvelope, observed time.Time) bool {
	if envelope.DeadlineMicros == nil {
		return false
	}
	return observed.After(time.UnixMicro(int64(*envelope.DeadlineMicros)))
}

// Agents lists every agent holding a fresh card, quarantined or not, sorted by
// name. An operator listing the deployment wants to see the quarantined ones
// too.
func (registry *AgentRegistry) Agents(now time.Time) []RegisteredCard {
	registry.mutex.RLock()
	defer registry.mutex.RUnlock()

	var live []RegisteredCard
	for _, card := range registry.cards {
		if card.Fresh(now) {
			live = append(live, card)
		}
	}
	sort.Slice(live, func(first, second int) bool {
		return live[first].Agent.String() < live[second].Agent.String()
	})
	return live
}
