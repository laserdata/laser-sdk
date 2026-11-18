package laser_test

import (
	"errors"
	"testing"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func uint8Pointer(value uint8) *uint8    { return &value }
func uint16Pointer(value uint16) *uint16 { return &value }

// cardFor builds a card advertising one skill, with the advisory classes a
// policy ranks on.
func cardFor(skill string, cost, latency *uint8, load *uint16, health *wire.Health) wire.AgentCard {
	return wire.AgentCard{
		Capabilities: []wire.CapabilityDescriptor{{
			SkillID:      skill,
			CostClass:    cost,
			LatencyClass: latency,
			Load:         load,
			Health:       health,
		}},
	}
}

func registryWith(t *testing.T, now time.Time, agents map[string]wire.AgentCard) *laser.AgentRegistry {
	t.Helper()
	registry := laser.NewAgentRegistry()
	for name, card := range agents {
		registry.Observe(agentID(t, name), card, now)
	}
	return registry
}

// A card with a lifetime stops describing a live agent once it runs out,
// which is what makes a card liveness rather than only discovery.
func TestGivenAnExpiredCardWhenResolvedThenShouldNotBeCapable(t *testing.T) {
	now := time.Now()
	lifetime := uint64(60_000_000)
	card := cardFor("summarise", nil, nil, nil, nil)
	card.TTLMicros = &lifetime
	registry := laser.NewAgentRegistry()
	registry.Observe(agentID(t, "planner"), card, now.Add(-2*time.Minute))

	if len(registry.Capable("summarise", now)) != 0 {
		t.Error("a card past its lifetime describes a dead agent")
	}
	// The same card, observed recently, is live.
	registry.Observe(agentID(t, "planner"), card, now)
	if len(registry.Capable("summarise", now)) != 1 {
		t.Error("a freshly observed card describes a live agent")
	}
}

// A card with no lifetime is discovery only and never expires.
func TestGivenACardWithNoLifetimeWhenResolvedThenShouldStayCapable(t *testing.T) {
	now := time.Now()
	registry := laser.NewAgentRegistry()
	registry.Observe(agentID(t, "planner"),
		cardFor("summarise", nil, nil, nil, nil), now.Add(-100*time.Hour))

	if len(registry.Capable("summarise", now)) != 1 {
		t.Error("a card with no lifetime never expires")
	}
}

// An agent that declared itself unavailable must not be handed work, while one
// that is merely struggling still can be: a degraded agent beats no agent.
func TestGivenAnUnavailableSkillWhenResolvedThenShouldBeSkipped(t *testing.T) {
	now := time.Now()
	unavailable := wire.HealthUnavailable
	degraded := wire.HealthDegraded
	registry := registryWith(t, now, map[string]wire.AgentCard{
		"down":       cardFor("summarise", nil, nil, nil, &unavailable),
		"struggling": cardFor("summarise", nil, nil, nil, &degraded),
	})

	capable := registry.Capable("summarise", now)
	if len(capable) != 1 || capable[0].Agent != agentID(t, "struggling") {
		t.Errorf("want only the struggling agent, got %+v", capable)
	}
}

// An operator takes an agent out of rotation without stopping it, so work must
// stop reaching it both by capability and by name.
func TestGivenAQuarantinedAgentWhenRoutedThenShouldBeRefusedBothWays(t *testing.T) {
	now := time.Now()
	registry := registryWith(t, now, map[string]wire.AgentCard{
		"planner": cardFor("summarise", nil, nil, nil, nil),
	})
	registry.Quarantine(agentID(t, "planner"))

	if len(registry.Capable("summarise", now)) != 0 {
		t.Error("a quarantined agent must not be a candidate")
	}
	_, err := laser.To(agentID(t, "planner")).ResolveTargets(registry, now)
	if err == nil {
		t.Error("a route naming a quarantined agent must be refused")
	}

	// Lifting it puts the agent back in rotation.
	registry.Release(agentID(t, "planner"))
	if len(registry.Capable("summarise", now)) != 1 {
		t.Error("lifting a quarantine must restore the agent")
	}
}

// The caller asked for a skill, so delivering the work to whoever happens to
// be listening is not that: it is an error.
func TestGivenNoCapableAgentWhenRoutedThenShouldFailRatherThanBroadcast(t *testing.T) {
	now := time.Now()
	registry := laser.NewAgentRegistry()

	_, err := laser.ToCapable("summarise", laser.RouteAny).ResolveTargets(registry, now)

	var absent *laser.NoCapableAgentError
	if !errors.As(err, &absent) {
		t.Fatalf("want a NoCapableAgentError, got %T: %v", err, err)
	}
	if absent.Skill != "summarise" {
		t.Errorf("want the skill named, got %q", absent.Skill)
	}
}

func TestGivenABroadcastWhenResolvedThenShouldNameNobody(t *testing.T) {
	targets, err := laser.Broadcast().ResolveTargets(laser.NewAgentRegistry(), time.Now())
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if len(targets) != 0 {
		t.Errorf("a broadcast names nobody, got %v", targets)
	}
}

// A fan-out reaches every capable agent, which is what a verifier panel wants.
func TestGivenAFanOutWhenResolvedThenShouldNameEveryCapableAgent(t *testing.T) {
	now := time.Now()
	registry := registryWith(t, now, map[string]wire.AgentCard{
		"first":  cardFor("verify", nil, nil, nil, nil),
		"second": cardFor("verify", nil, nil, nil, nil),
		"other":  cardFor("summarise", nil, nil, nil, nil),
	})

	targets, err := laser.AllCapable("verify", laser.RouteAny).ResolveTargets(registry, now)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if len(targets) != 2 {
		t.Errorf("want both verifiers, got %v", targets)
	}
}

// The advisory classes are what a policy ranks on, so each must pick the agent
// it names.
func TestGivenAdvertisedClassesWhenRankedThenShouldPickByThePolicy(t *testing.T) {
	now := time.Now()
	registry := registryWith(t, now, map[string]wire.AgentCard{
		"cheap": cardFor("summarise",
			uint8Pointer(1), uint8Pointer(9), uint16Pointer(900), nil),
		"quick": cardFor("summarise",
			uint8Pointer(9), uint8Pointer(1), uint16Pointer(500), nil),
		"idle": cardFor("summarise",
			uint8Pointer(5), uint8Pointer(5), uint16Pointer(10), nil),
	})

	for name, expected := range map[string]struct {
		policy laser.RoutePolicy
		agent  string
	}{
		"cheapest":     {laser.RouteCheapest, "cheap"},
		"fastest":      {laser.RouteFastest, "quick"},
		"least loaded": {laser.RouteLeastLoaded, "idle"},
	} {
		t.Run(name, func(t *testing.T) {
			targets, err := laser.ToCapable("summarise", expected.policy).
				ResolveTargets(registry, now)
			if err != nil {
				t.Fatalf("resolve: %v", err)
			}
			if len(targets) != 1 || targets[0] != agentID(t, expected.agent) {
				t.Errorf("want %s, got %v", expected.agent, targets)
			}
		})
	}
}

// A deployment that publishes no classes must still route, so an agent that
// advertised nothing is chosen when nobody advertised anything.
func TestGivenNoAdvertisedClassesWhenRankedThenShouldStillRoute(t *testing.T) {
	now := time.Now()
	registry := registryWith(t, now, map[string]wire.AgentCard{
		"planner": cardFor("summarise", nil, nil, nil, nil),
	})

	targets, err := laser.ToCapable("summarise", laser.RouteCheapest).
		ResolveTargets(registry, now)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if len(targets) != 1 {
		t.Errorf("want the unranked agent routed to, got %v", targets)
	}
}

// Pinning a conversation to one agent is a preference, not a requirement, so a
// sticky route falls back rather than failing when its agent is gone.
func TestGivenAStickyRouteWhenThePreferredAgentIsGoneThenShouldFallBack(t *testing.T) {
	now := time.Now()
	withPreferred := registryWith(t, now, map[string]wire.AgentCard{
		"preferred": cardFor("summarise", nil, nil, nil, nil),
		"other":     cardFor("summarise", nil, nil, nil, nil),
	})

	targets, err := laser.Sticky("summarise", agentID(t, "preferred")).
		ResolveTargets(withPreferred, now)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if len(targets) != 1 || targets[0] != agentID(t, "preferred") {
		t.Errorf("want the preferred agent, got %v", targets)
	}

	withoutPreferred := registryWith(t, now, map[string]wire.AgentCard{
		"other": cardFor("summarise", nil, nil, nil, nil),
	})
	targets, err = laser.Sticky("summarise", agentID(t, "preferred")).
		ResolveTargets(withoutPreferred, now)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if len(targets) != 1 || targets[0] != agentID(t, "other") {
		t.Errorf("want the fallback agent, got %v", targets)
	}
}

// Two resolutions over one registry must pick the same agent, or a sticky
// conversation drifts and a retry lands somewhere else.
func TestGivenSeveralCandidatesWhenResolvedTwiceThenShouldPickTheSameAgent(t *testing.T) {
	now := time.Now()
	registry := registryWith(t, now, map[string]wire.AgentCard{
		"a": cardFor("summarise", nil, nil, nil, nil),
		"b": cardFor("summarise", nil, nil, nil, nil),
		"c": cardFor("summarise", nil, nil, nil, nil),
	})
	route := laser.ToCapable("summarise", laser.RouteAny)

	first, err := route.ResolveTargets(registry, now)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	for range 20 {
		again, err := route.ResolveTargets(registry, now)
		if err != nil {
			t.Fatalf("resolve: %v", err)
		}
		if again[0] != first[0] {
			t.Fatalf("resolution drifted: %v then %v", first, again)
		}
	}
}

// A scorer that refuses every candidate must route nowhere, rather than
// falling back to one it declined.
func TestGivenAScorerThatRefusesWhenResolvedThenShouldRouteNowhere(t *testing.T) {
	now := time.Now()
	registry := registryWith(t, now, map[string]wire.AgentCard{
		"planner": cardFor("summarise", nil, nil, nil, nil),
	})

	_, err := laser.WithScorer("summarise", refusingScorer{}).ResolveTargets(registry, now)

	var absent *laser.NoCapableAgentError
	if !errors.As(err, &absent) {
		t.Fatalf("want a NoCapableAgentError, got %T: %v", err, err)
	}
}

// A scorer sees the same candidates the built-in policies do, so a custom
// ranking can never be handed less than a shipped one.
func TestGivenAScorerWhenResolvedThenShouldSeeEveryCandidate(t *testing.T) {
	now := time.Now()
	registry := registryWith(t, now, map[string]wire.AgentCard{
		"a": cardFor("summarise", nil, nil, nil, nil),
		"b": cardFor("summarise", nil, nil, nil, nil),
	})
	scorer := &lastScorer{}

	targets, err := laser.WithScorer("summarise", scorer).ResolveTargets(registry, now)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if scorer.seen != 2 {
		t.Errorf("want both candidates offered, got %d", scorer.seen)
	}
	if scorer.skill != "summarise" {
		t.Errorf("want the skill named, got %q", scorer.skill)
	}
	if len(targets) != 1 || targets[0] != agentID(t, "b") {
		t.Errorf("want the scorer's pick, got %v", targets)
	}
}

type refusingScorer struct{}

func (refusingScorer) Select(string, []laser.RegisteredCard) int { return -1 }

type lastScorer struct {
	seen  int
	skill string
}

func (scorer *lastScorer) Select(skill string, candidates []laser.RegisteredCard) int {
	scorer.seen, scorer.skill = len(candidates), skill
	return len(candidates) - 1
}

// A capability route is resolved against a registry, so stamping a target
// directly would address the wrong agent. Leaving it unset broadcasts, which
// is safe.
func TestGivenACapabilityRouteWhenAppliedThenShouldLeaveTheTargetUnset(t *testing.T) {
	provenance := &laser.Provenance{Conversation: conversation(1)}

	laser.To(agentID(t, "executor")).Apply(provenance)
	if provenance.TargetAgent == nil || *provenance.TargetAgent != agentID(t, "executor") {
		t.Errorf("a named route stamps its target, got %v", provenance.TargetAgent)
	}

	laser.ToCapable("summarise", laser.RouteAny).Apply(provenance)
	if provenance.TargetAgent != nil {
		t.Errorf("a capability route must not guess a target, got %v", provenance.TargetAgent)
	}

	laser.To(agentID(t, "executor")).Apply(provenance)
	laser.Broadcast().Apply(provenance)
	if provenance.TargetAgent != nil {
		t.Errorf("a broadcast clears the target, got %v", provenance.TargetAgent)
	}
}

// A capability route with nothing to resolve against is a mistake worth
// naming, rather than a silent broadcast.
func TestGivenNoRegistryWhenResolvingACapabilityRouteThenShouldRefuseIt(t *testing.T) {
	_, err := laser.ToCapable("summarise", laser.RouteAny).ResolveTargets(nil, time.Now())

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want an InvalidError, got %T: %v", err, err)
	}
}
