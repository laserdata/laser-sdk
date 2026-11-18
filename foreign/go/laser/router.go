package laser

import (
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// RouteKind is how a router picks its targets.
type RouteKind int

const (
	// RouteTo names one agent.
	RouteTo RouteKind = iota
	// RouteBroadcast names nobody, so every member of a consumer group may
	// handle the message.
	RouteBroadcast
	// RouteToCapable names the one agent a policy picks from those
	// advertising a skill.
	RouteToCapable
	// RouteAllCapable names every agent advertising a skill, for a fan-out
	// such as a verifier panel.
	RouteAllCapable
)

// RoutePolicy ranks the agents advertising a skill.
type RoutePolicy int

const (
	// RouteAny takes the first candidate. The default, and deterministic
	// because the registry returns candidates in a stable order.
	RouteAny RoutePolicy = iota
	// RouteCheapest takes the lowest advertised cost class.
	RouteCheapest
	// RouteFastest takes the lowest advertised latency class.
	RouteFastest
	// RouteLeastLoaded takes the lowest advertised load.
	RouteLeastLoaded
	// RouteSticky prefers one named agent when it is capable, and otherwise
	// falls back to RouteAny. It keeps a conversation pinned to one agent
	// without failing when that agent is gone.
	RouteSticky
	// RouteCustom defers to a caller-supplied ranking.
	RouteCustom
)

// RouteScorer picks among the candidates for a skill, returning the index of
// its choice.
//
// Returning an index outside the slice means no pick, which routes nowhere
// exactly as an empty candidate list does, rather than falling back to an
// agent the scorer refused.
type RouteScorer interface {
	Select(skill string, candidates []RegisteredCard) int
}

// Router decides which agents a message is addressed to.
type Router struct {
	Kind RouteKind
	// Agent is the named target for RouteTo, and the preferred agent for
	// RouteSticky.
	Agent wire.AgentID
	// Skill is what the capability routes resolve against.
	Skill  string
	Policy RoutePolicy
	Scorer RouteScorer
}

// To routes to one agent.
func To(agent wire.AgentID) Router {
	return Router{Kind: RouteTo, Agent: agent}
}

// Broadcast clears the target, so every member of a consumer group may handle
// the message.
func Broadcast() Router { return Router{Kind: RouteBroadcast} }

// ToCapable routes to the one agent advertising a skill that the policy picks.
func ToCapable(skill string, policy RoutePolicy) Router {
	return Router{Kind: RouteToCapable, Skill: skill, Policy: policy}
}

// AllCapable routes to every agent advertising a skill.
func AllCapable(skill string, policy RoutePolicy) Router {
	return Router{Kind: RouteAllCapable, Skill: skill, Policy: policy}
}

// Sticky prefers one agent for a skill, falling back to any other capable one
// when it is gone.
func Sticky(skill string, agent wire.AgentID) Router {
	return Router{Kind: RouteToCapable, Skill: skill, Policy: RouteSticky, Agent: agent}
}

// WithScorer routes by a caller's own ranking.
func WithScorer(skill string, scorer RouteScorer) Router {
	return Router{
		Kind: RouteToCapable, Skill: skill, Policy: RouteCustom, Scorer: scorer,
	}
}

// Apply stamps the target on a provenance for the routes that name one
// directly.
//
// A capability route leaves the target unset rather than guessing, because it
// is resolved against a registry by ResolveTargets. Leaving it unset
// broadcasts, which is safe, where guessing would address the wrong agent.
func (router Router) Apply(provenance *Provenance) {
	switch router.Kind {
	case RouteTo:
		agent := router.Agent
		provenance.TargetAgent = &agent
	default:
		provenance.TargetAgent = nil
	}
}

// ResolveTargets names the concrete agents this route reaches.
//
// A broadcast reaches everyone, so it names nobody and returns an empty list.
// A capability route that matches no live agent is an error rather than a
// silent broadcast: the caller asked for a skill, and delivering the work to
// whoever happens to be listening is not that.
func (router Router) ResolveTargets(
	registry *AgentRegistry, now time.Time,
) ([]wire.AgentID, error) {
	switch router.Kind {
	case RouteTo:
		if registry != nil && registry.Quarantined(router.Agent) {
			return nil, &InvalidError{Reason: "agent " + router.Agent.String() +
				" is quarantined, so work must not be routed to it"}
		}
		return []wire.AgentID{router.Agent}, nil

	case RouteBroadcast:
		return nil, nil

	case RouteToCapable, RouteAllCapable:
		if registry == nil {
			return nil, &InvalidError{
				Reason: "a capability route needs a registry to resolve against"}
		}
		candidates := registry.Capable(router.Skill, now)
		if len(candidates) == 0 {
			return nil, &NoCapableAgentError{Skill: router.Skill}
		}
		if router.Kind == RouteAllCapable {
			agents := make([]wire.AgentID, 0, len(candidates))
			for _, candidate := range candidates {
				agents = append(agents, candidate.Agent)
			}
			return agents, nil
		}
		chosen, picked := router.pick(candidates)
		if !picked {
			return nil, &NoCapableAgentError{Skill: router.Skill}
		}
		return []wire.AgentID{chosen.Agent}, nil

	default:
		return nil, &InvalidError{Reason: "unknown route kind"}
	}
}

// pick ranks the candidates by the policy.
func (router Router) pick(candidates []RegisteredCard) (RegisteredCard, bool) {
	switch router.Policy {
	case RouteSticky:
		for _, candidate := range candidates {
			if candidate.Agent == router.Agent {
				return candidate, true
			}
		}
		// The preferred agent is gone, so the route falls back rather than
		// failing: pinning a conversation is a preference, not a requirement.
		return candidates[0], true

	case RouteCheapest:
		return lowest(candidates, router.Skill, func(
			capability *wire.CapabilityDescriptor,
		) (uint32, bool) {
			if capability == nil || capability.CostClass == nil {
				return 0, false
			}
			return uint32(*capability.CostClass), true
		})

	case RouteFastest:
		return lowest(candidates, router.Skill, func(
			capability *wire.CapabilityDescriptor,
		) (uint32, bool) {
			if capability == nil || capability.LatencyClass == nil {
				return 0, false
			}
			return uint32(*capability.LatencyClass), true
		})

	case RouteLeastLoaded:
		return lowest(candidates, router.Skill, func(
			capability *wire.CapabilityDescriptor,
		) (uint32, bool) {
			if capability == nil || capability.Load == nil {
				return 0, false
			}
			return uint32(*capability.Load), true
		})

	case RouteCustom:
		if router.Scorer == nil {
			return RegisteredCard{}, false
		}
		index := router.Scorer.Select(router.Skill, candidates)
		// An index outside the slice is a refusal, which routes nowhere rather
		// than falling back to a candidate the scorer declined.
		if index < 0 || index >= len(candidates) {
			return RegisteredCard{}, false
		}
		return candidates[index], true

	default:
		return candidates[0], true
	}
}

// lowest picks the candidate advertising the smallest value of a class.
//
// A candidate that advertises nothing is not ranked last: it is simply unknown,
// and an unranked agent is only chosen when nobody advertised anything, so a
// deployment that publishes no classes still routes.
func lowest(
	candidates []RegisteredCard, skill string,
	value func(*wire.CapabilityDescriptor) (uint32, bool),
) (RegisteredCard, bool) {
	best, found := RegisteredCard{}, false
	var bestValue uint32
	for _, candidate := range candidates {
		advertised, known := value(candidate.Capability(skill))
		if !known {
			continue
		}
		if !found || advertised < bestValue {
			best, bestValue, found = candidate, advertised, true
		}
	}
	if found {
		return best, true
	}
	return candidates[0], true
}

// NoCapableAgentError reports a capability route that matched no live agent.
//
// It is kept distinct from a transport failure because the remedy is
// different: nothing is wrong with the connection, there is simply nobody
// advertising the skill right now.
type NoCapableAgentError struct{ Skill string }

func (e *NoCapableAgentError) Error() string {
	return "no live agent advertises the skill " + e.Skill
}
