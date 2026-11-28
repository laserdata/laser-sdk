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
	// RouteToPrincipal names one agent, and reaches it only while its live
	// connection authenticated as a required principal.
	RouteToPrincipal
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
	// Principal, when set, requires the agent this route reaches to have
	// live presence under that authenticated identity. It is what
	// RouteToPrincipal carries, and what WithPrincipal binds to a capability
	// route. Nil keeps claim-only discovery, where an agent id is taken at
	// its word.
	Principal *PrincipalID
}

// To routes to one agent.
func To(agent wire.AgentID) Router {
	return Router{Kind: RouteTo, Agent: agent}
}

// ToPrincipal routes to one agent, but only while its live presence is bound
// to principal.
//
// An agent id is a claim any connection can make. A principal is what the
// deployment authenticated. Binding the two is what stops a connection
// advertising somebody else's agent id from being handed that agent's work,
// so this is the directed route to use wherever the target's identity
// matters rather than merely its name. It needs live presence, which means a
// registry refreshed with RefreshPresence: against a deployment that serves
// no connection metadata there is nothing to bind against and the route
// refuses rather than falling back to the name alone.
func ToPrincipal(agent wire.AgentID, principal PrincipalID) Router {
	return Router{Kind: RouteToPrincipal, Agent: agent, Principal: &principal}
}

// WithPrincipal requires every agent a capability route picks to have live
// presence under principal, so discovery by skill cannot hand work to an
// agent that merely claims the name.
//
// On a directed route it is the same requirement, which is what ToPrincipal
// builds. On a broadcast it is meaningless and ignored, since a broadcast
// names no agent to bind.
func (router Router) WithPrincipal(principal PrincipalID) Router {
	router.Principal = &principal
	if router.Kind == RouteTo {
		router.Kind = RouteToPrincipal
	}
	return router
}

// needsRegistry reports whether resolving this route reads the agent registry.
func (router Router) needsRegistry() bool {
	switch router.Kind {
	case RouteToPrincipal, RouteToCapable, RouteAllCapable:
		return true
	default:
		return false
	}
}

// RequiresPresence reports whether resolving this route needs live,
// principal-bearing presence, so a caller can refresh it first.
func (router Router) RequiresPresence() bool {
	if router.Principal == nil {
		return false
	}
	switch router.Kind {
	case RouteToPrincipal, RouteToCapable, RouteAllCapable:
		return true
	default:
		return false
	}
}

// RequiredPrincipal is the authenticated principal this route requires, or
// nil when it requires none.
//
// Reply verification uses the same identity, which is what joins discovery
// and authorship: the agent a route insisted on is the agent whose signature
// an answer must carry.
func (router Router) RequiredPrincipal() *PrincipalID {
	if !router.RequiresPresence() {
		return nil
	}
	return router.Principal
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
	case RouteTo, RouteToPrincipal:
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
//
// avoid, when given, is a capability route's preference, not a requirement: a
// candidate outside avoid is picked when one exists, and a candidate inside it
// is still picked rather than refusing when no other agent is capable (a
// workflow reassignment reusing the agent that just timed out is better than
// no agent at all). Only the workflow engine's own exclusive-step
// reassignment passes it. Every other caller omits it.
func (router Router) ResolveTargets(
	registry *AgentRegistry, now time.Time, avoid ...wire.AgentID,
) ([]wire.AgentID, error) {
	return router.resolveRoutable(registry, nil, now, avoid)
}

// ResolveRoutable resolves like ResolveTargets, keeping only the capability
// candidates inbox can actually address, so a capable agent with nowhere to
// receive work never wins a pick over one that has somewhere.
//
// A directed route is not filtered: the caller named that agent, so an
// unreachable one is reported when its inbox is resolved rather than quietly
// becoming "no capable agent".
func (router Router) ResolveRoutable(
	registry *AgentRegistry, inbox InboxRoute, now time.Time, avoid ...wire.AgentID,
) ([]wire.AgentID, error) {
	return router.resolveRoutable(registry, &inbox, now, avoid)
}

func (router Router) resolveRoutable(
	registry *AgentRegistry, inbox *InboxRoute, now time.Time, avoid []wire.AgentID,
) ([]wire.AgentID, error) {
	switch router.Kind {
	case RouteTo, RouteToPrincipal:
		if registry != nil && registry.Quarantined(router.Agent) {
			return nil, &QuarantinedError{Agent: router.Agent}
		}
		if router.Kind == RouteToPrincipal {
			if registry == nil {
				return nil, &InvalidError{
					Reason: "a principal-bound route needs a registry to resolve against"}
			}
			if err := requirePrincipal(registry, router.Agent, router.Principal); err != nil {
				return nil, err
			}
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
		candidates, err := trustedCandidates(registry, candidates, router.Principal)
		if err != nil {
			return nil, err
		}
		if inbox != nil {
			candidates = reachableCandidates(registry, candidates, *inbox)
			if len(candidates) == 0 {
				return nil, &NoCapableAgentError{Skill: router.Skill}
			}
		}
		if router.Kind == RouteAllCapable {
			agents := make([]wire.AgentID, 0, len(candidates))
			for _, candidate := range candidates {
				agents = append(agents, candidate.Agent)
			}
			return agents, nil
		}
		chosen, picked := router.pick(candidates, avoid)
		if !picked {
			return nil, &NoCapableAgentError{Skill: router.Skill}
		}
		return []wire.AgentID{chosen.Agent}, nil

	default:
		return nil, &InvalidError{Reason: "unknown route kind"}
	}
}

// requirePrincipal refuses an agent whose live presence is not bound to the
// principal the route named.
//
// A nil principal on a principal-bound route is a caller mistake rather than
// a permissive wildcard: the whole point of the route is the requirement, so
// a route built without one must not quietly behave like a plain To.
func requirePrincipal(
	registry *AgentRegistry, agent wire.AgentID, principal *PrincipalID,
) error {
	if principal == nil {
		return &InvalidError{
			Reason: "a principal-bound route must name the principal it requires"}
	}
	actual := registry.PrincipalFor(agent)
	if actual != nil && *actual == *principal {
		return nil
	}
	return &RoutePrincipalMismatchError{
		Agent: agent, Expected: *principal, Actual: actual}
}

// trustedCandidates keeps only the capability candidates whose live presence
// is bound to the required principal, and passes every candidate through
// untouched when the route requires none.
//
// A route that required a principal and found capable agents, none of them
// bound to it, reports the mismatch rather than "no capable agent": the
// skill is served, just not by anyone the caller is willing to trust, and
// telling those two apart is the difference between provisioning an agent
// and fixing an enrollment.
func trustedCandidates(
	registry *AgentRegistry, candidates []RegisteredCard, principal *PrincipalID,
) ([]RegisteredCard, error) {
	if principal == nil {
		return candidates, nil
	}
	trusted := make([]RegisteredCard, 0, len(candidates))
	for _, candidate := range candidates {
		if actual := registry.PrincipalFor(candidate.Agent); actual != nil &&
			*actual == *principal {
			trusted = append(trusted, candidate)
		}
	}
	if len(trusted) == 0 {
		return nil, &RoutePrincipalMismatchError{
			Agent:    candidates[0].Agent,
			Expected: *principal,
			Actual:   registry.PrincipalFor(candidates[0].Agent),
		}
	}
	return trusted, nil
}

// reachableCandidates keeps the candidates the inbox route can address.
//
// An agent advertising no inbox is capable but unreachable, and picking it
// would turn a routing problem into a send failure after the pick is already
// committed.
func reachableCandidates(
	registry *AgentRegistry, candidates []RegisteredCard, inbox InboxRoute,
) []RegisteredCard {
	reachable := make([]RegisteredCard, 0, len(candidates))
	for _, candidate := range candidates {
		if inbox.resolvableFor(candidate.Agent, registry.InboxFor(candidate.Agent)) {
			reachable = append(reachable, candidate)
		}
	}
	return reachable
}

// pick ranks the candidates by the policy, preferring one outside avoid and
// falling back to the full candidate list only when that picks nobody (so a
// reassignment reuses the agent that just timed out only when no other agent
// is capable).
func (router Router) pick(candidates []RegisteredCard, avoid []wire.AgentID) (RegisteredCard, bool) {
	if len(avoid) > 0 {
		preferred := make([]RegisteredCard, 0, len(candidates))
		for _, candidate := range candidates {
			if !containsAgent(avoid, candidate.Agent) {
				preferred = append(preferred, candidate)
			}
		}
		if len(preferred) > 0 {
			if chosen, picked := router.pickAmong(preferred); picked {
				return chosen, true
			}
		}
	}
	return router.pickAmong(candidates)
}

// pickAmong ranks the candidates by the policy.
func (router Router) pickAmong(candidates []RegisteredCard) (RegisteredCard, bool) {
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
