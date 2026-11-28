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
	// presence is live connection truth, keyed by the agent id each
	// connection claims, seeded by RefreshPresence. It is deliberately
	// separate from cards: a card is durable, replayable capability that
	// outlives a brief disconnect, where presence vanishes with the
	// connection and answers where to send work right now.
	presence map[wire.AgentID]presenceEntry
	// presenceReadAt is when the connection table was last paged, so a
	// refresh within PresenceTTL reuses what is held.
	presenceReadAt time.Time
	// appliedFacts are the ids of the quarantine facts already folded, oldest first in appliedFactOrder.
	appliedFacts     map[wire.RecordID]struct{}
	appliedFactOrder []appliedFact
	// verifier, when set, folds a quarantine fact only under an enrolled operator key.
	verifier *KeyRegistry
}

// NewAgentRegistry opens an empty registry.
func NewAgentRegistry() *AgentRegistry {
	return &AgentRegistry{
		cards:       map[wire.AgentID]RegisteredCard{},
		quarantined: map[wire.AgentID]struct{}{},
		presence:    map[wire.AgentID]presenceEntry{},
	}
}

// Verifier makes the registry fold a quarantine or unquarantine fact only when an enrolled operator key signed it.
func (registry *AgentRegistry) Verifier(verifier *KeyRegistry) *AgentRegistry {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	registry.verifier = verifier
	return registry
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
	_, err := registry.refresh(ctx, cursor)
	return err
}

// refresh is Refresh, also reporting how many records the poll read.
func (registry *AgentRegistry) refresh(ctx context.Context, cursor *Cursor) (int, error) {
	records, err := cursor.Poll(ctx)
	if err != nil {
		return 0, err
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
	return len(records), nil
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
	case wire.OperationQuarantine, wire.OperationUnquarantine:
		// A fact is about the agent its body names. Its source is the operator that wrote it.
		agent, err := wire.NewAgentID(string(envelope.Body))
		if err != nil || !registry.factAuthorized(envelope, record) ||
			!registry.admitFact(envelope, record.Message.Header.Timestamp) {
			return
		}
		if *envelope.Operation == wire.OperationQuarantine {
			registry.Quarantine(agent)
		} else {
			registry.Release(agent)
		}
	}
}

// factAuthorized reports whether a fact may fold. Without a verifier the registry topic's write access is the gate.
func (registry *AgentRegistry) factAuthorized(envelope wire.AgentEnvelope, record *Record) bool {
	registry.mutex.RLock()
	verifier := registry.verifier
	registry.mutex.RUnlock()
	if verifier == nil {
		return true
	}
	stamp, err := envelopeStamp(record)
	if err != nil || !stamp.present {
		return false
	}
	observed := wire.SignatureContext{AgentVersion: &stamp.version}
	if code, named := stamp.contentType.Code(); named {
		observed.ContentType = &code
	}
	verified, err := verifier.VerifyObservedAt(envelope, observed, record.Message.Header.Timestamp)
	return err == nil && verified.Kind == wire.KeyOperator
}

// appliedFactCapacity bounds the fact ids remembered to catch a verbatim replay.
const appliedFactCapacity = 16384

// appliedFact is one folded fact id with the append time it was folded at.
type appliedFact struct {
	record         wire.RecordID
	observedMicros uint64
}

// admitFact reports whether a fact folds: appended inside its bounded validity by the record's own append time, and no replay of a folded one.
func (registry *AgentRegistry) admitFact(envelope wire.AgentEnvelope, observedMicros uint64) bool {
	if envelope.DeadlineMicros == nil {
		return false
	}
	deadline := *envelope.DeadlineMicros
	if observedMicros >= deadline || deadline-observedMicros > wire.MaxRegistryFactTTLMicros {
		return false
	}
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	if envelope.Record != nil {
		if _, replayed := registry.appliedFacts[*envelope.Record]; replayed {
			return false
		}
		if registry.appliedFacts == nil {
			registry.appliedFacts = map[wire.RecordID]struct{}{}
		}
		registry.appliedFacts[*envelope.Record] = struct{}{}
		registry.appliedFactOrder = append(registry.appliedFactOrder,
			appliedFact{record: *envelope.Record, observedMicros: observedMicros})
	}
	// An id is kept for as long as its fact could still be inside its validity.
	cutoff := uint64(0)
	if observedMicros > wire.MaxRegistryFactTTLMicros {
		cutoff = observedMicros - wire.MaxRegistryFactTTLMicros
	}
	for len(registry.appliedFactOrder) > 0 && (len(registry.appliedFactOrder) > appliedFactCapacity ||
		registry.appliedFactOrder[0].observedMicros < cutoff) {
		delete(registry.appliedFacts, registry.appliedFactOrder[0].record)
		registry.appliedFactOrder = registry.appliedFactOrder[1:]
	}
	return true
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

// Quarantine appends a quarantine fact to the registry topic as operator, so every registry that folds it stops routing to agent.
func (laser *Laser) Quarantine(ctx context.Context, operator, agent wire.AgentID) error {
	return laser.publishRegistryFact(ctx, wire.OperationQuarantine, operator, agent, nil)
}

// Unquarantine appends the fact that lifts a quarantine on agent and returns it to routing.
func (laser *Laser) Unquarantine(ctx context.Context, operator, agent wire.AgentID) error {
	return laser.publishRegistryFact(ctx, wire.OperationUnquarantine, operator, agent, nil)
}

// QuarantineSigned is Quarantine with the fact signed by the operator's key, for a registry that verifies operators.
func (laser *Laser) QuarantineSigned(ctx context.Context, operator, agent wire.AgentID, key *SigningKey) error {
	return laser.publishRegistryFact(ctx, wire.OperationQuarantine, operator, agent, key)
}

// UnquarantineSigned is Unquarantine with the fact signed by the operator's key.
func (laser *Laser) UnquarantineSigned(ctx context.Context, operator, agent wire.AgentID, key *SigningKey) error {
	return laser.publishRegistryFact(ctx, wire.OperationUnquarantine, operator, agent, key)
}

func (laser *Laser) publishRegistryFact(
	ctx context.Context, operation string, operator, agent wire.AgentID, key *SigningKey,
) error {
	stream := laser.defaultStream
	if stream == "" {
		return errNoStream()
	}
	if agent.IsZero() {
		return &InvalidError{Reason: "a registry fact must name the agent it is about"}
	}
	if err := laser.Stream(stream).Topic(wire.AgentRegistry).Ensure(ctx, 1, nil); err != nil {
		return err
	}
	until := time.Now().Add(time.Duration(wire.RegistryFactTTLMicros) * time.Microsecond)
	producer := laser.AgdxOn(stream, wire.AgentRegistry, operator, NewConversationID())
	send := producer.Quarantine(until, []byte(agent.String()))
	if operation == wire.OperationUnquarantine {
		send = producer.Unquarantine(until, []byte(agent.String()))
	}
	if key != nil {
		send = send.Signed(key)
	}
	_, err := send.Send(ctx)
	return err
}
