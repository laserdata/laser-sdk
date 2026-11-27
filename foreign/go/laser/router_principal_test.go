package laser_test

import (
	"errors"
	"testing"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// boundRegistry is a registry holding both halves a principal-bound route
// needs: the cards that advertise a skill, and the live presence that says
// which principal each claimed agent is actually connected as.
func boundRegistry(
	t *testing.T, now time.Time, skill string, bindings map[string]*uint32,
) *laser.AgentRegistry {
	t.Helper()
	clients := make([]wire.ClientMetadata, 0, len(bindings))
	names := make([]string, 0, len(bindings))
	for name := range bindings {
		names = append(names, name)
	}
	// Sorted, so the fold order is the test's rather than the map's.
	for index := 0; index < len(names); index++ {
		for other := index + 1; other < len(names); other++ {
			if names[other] < names[index] {
				names[index], names[other] = names[other], names[index]
			}
		}
	}
	for _, name := range names {
		clients = append(clients, advertised(t, name, name+"-inbox", bindings[name]))
	}
	registry, _ := seedPresence(t, clients)
	for _, name := range names {
		registry.Observe(agentID(t, name), cardFor(skill, nil, nil, nil, nil), now)
	}
	return registry
}

// The whole point of the route: an agent id is a claim, and the route only
// reaches it while the deployment authenticated that connection as the
// principal the caller required.
func TestGivenABoundAgentWhenRoutedToItsPrincipalThenShouldResolve(t *testing.T) {
	now := time.Now()
	registry := boundRegistry(t, now, "summarise", map[string]*uint32{
		"planner": principalOf(7),
	})

	targets, err := laser.ToPrincipal(agentID(t, "planner"), laser.PrincipalID(7)).
		ResolveTargets(registry, now)
	if err != nil {
		t.Fatalf("resolve a bound agent: %v", err)
	}
	if len(targets) != 1 || targets[0] != agentID(t, "planner") {
		t.Fatalf("expected the named agent, got %v", targets)
	}
}

// An agent connected as somebody else is the attack this route exists to
// stop, and it must read as a mismatch naming both principals rather than as
// a missing agent.
func TestGivenAnAgentBoundToAnotherPrincipalWhenRoutedThenShouldRefuseWithBoth(t *testing.T) {
	now := time.Now()
	registry := boundRegistry(t, now, "summarise", map[string]*uint32{
		"planner": principalOf(9),
	})

	_, err := laser.ToPrincipal(agentID(t, "planner"), laser.PrincipalID(7)).
		ResolveTargets(registry, now)
	var mismatch *laser.RoutePrincipalMismatchError
	if err == nil {
		t.Fatal("expected a route to an agent under another principal to be refused")
	}
	if !errors.As(err, &mismatch) {
		t.Fatalf("expected *RoutePrincipalMismatchError, got %T: %v", err, err)
	}
	if mismatch.Expected != laser.PrincipalID(7) {
		t.Errorf("expected principal 7 named, got %d", mismatch.Expected)
	}
	if mismatch.Actual == nil || *mismatch.Actual != laser.PrincipalID(9) {
		t.Errorf("expected the actual principal 9 named, got %v", mismatch.Actual)
	}
}

// No presence at all is a different fact from presence under the wrong
// principal: one agent is not there, the other is there as somebody else.
func TestGivenAnAgentWithNoPresenceWhenRoutedToAPrincipalThenShouldReportAbsent(t *testing.T) {
	now := time.Now()
	registry := laser.NewAgentRegistry()
	registry.Observe(agentID(t, "planner"), cardFor("summarise", nil, nil, nil, nil), now)

	_, err := laser.ToPrincipal(agentID(t, "planner"), laser.PrincipalID(7)).
		ResolveTargets(registry, now)
	var mismatch *laser.RoutePrincipalMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("expected *RoutePrincipalMismatchError, got %T: %v", err, err)
	}
	if mismatch.Actual != nil {
		t.Fatalf("expected no actual principal for an agent with no presence, got %d",
			*mismatch.Actual)
	}
}

// Discovery by skill must be bindable too, or an attacker only has to
// advertise the skill instead of the name.
func TestGivenACapabilityRouteWithAPrincipalWhenResolvedThenShouldPickOnlyTheBoundAgent(t *testing.T) {
	now := time.Now()
	registry := boundRegistry(t, now, "summarise", map[string]*uint32{
		"planner": principalOf(7),
		"auditor": principalOf(9),
	})

	targets, err := laser.ToCapable("summarise", laser.RouteAny).
		WithPrincipal(laser.PrincipalID(9)).
		ResolveTargets(registry, now)
	if err != nil {
		t.Fatalf("resolve a bound capability route: %v", err)
	}
	if len(targets) != 1 || targets[0] != agentID(t, "auditor") {
		t.Fatalf("expected only the agent bound to principal 9, got %v", targets)
	}
}

// A fan-out binds the same way, so a verifier panel cannot be joined by an
// agent that merely claims the skill.
func TestGivenAFanOutWithAPrincipalWhenResolvedThenShouldNameOnlyBoundAgents(t *testing.T) {
	now := time.Now()
	registry := boundRegistry(t, now, "verify", map[string]*uint32{
		"planner": principalOf(7),
		"auditor": principalOf(7),
		"stray":   principalOf(9),
	})

	targets, err := laser.AllCapable("verify", laser.RouteAny).
		WithPrincipal(laser.PrincipalID(7)).
		ResolveTargets(registry, now)
	if err != nil {
		t.Fatalf("resolve a bound fan-out: %v", err)
	}
	if len(targets) != 2 {
		t.Fatalf("expected the two bound agents, got %v", targets)
	}
	for _, target := range targets {
		if target == agentID(t, "stray") {
			t.Fatal("an agent under another principal joined the fan-out")
		}
	}
}

// The skill being served by nobody the caller trusts is a different problem
// from the skill being served by nobody, and the error has to say which:
// one is fixed by provisioning an agent, the other by fixing an enrollment.
func TestGivenNoBoundAgentWhenResolvedThenShouldReportMismatchNotNoCapableAgent(t *testing.T) {
	now := time.Now()
	registry := boundRegistry(t, now, "summarise", map[string]*uint32{
		"planner": principalOf(9),
	})

	_, err := laser.ToCapable("summarise", laser.RouteAny).
		WithPrincipal(laser.PrincipalID(7)).
		ResolveTargets(registry, now)
	var mismatch *laser.RoutePrincipalMismatchError
	var missing *laser.NoCapableAgentError
	if errors.As(err, &missing) {
		t.Fatal("a skill served by the wrong principal is not an unserved skill")
	}
	if !errors.As(err, &mismatch) {
		t.Fatalf("expected *RoutePrincipalMismatchError, got %T: %v", err, err)
	}
}

// A quarantine is an operator's decision and outranks the principal check,
// so it is refused as quarantined rather than as a mismatch.
func TestGivenAQuarantinedBoundAgentWhenRoutedThenShouldRefuseAsQuarantined(t *testing.T) {
	now := time.Now()
	registry := boundRegistry(t, now, "summarise", map[string]*uint32{
		"planner": principalOf(7),
	})
	registry.Quarantine(agentID(t, "planner"))

	_, err := laser.ToPrincipal(agentID(t, "planner"), laser.PrincipalID(7)).
		ResolveTargets(registry, now)
	var mismatch *laser.RoutePrincipalMismatchError
	if err == nil {
		t.Fatal("expected a quarantined agent to be refused")
	}
	if errors.As(err, &mismatch) {
		t.Fatal("a quarantined agent should be refused as quarantined, not as a mismatch")
	}
}

// A principal-bound route still addresses its agent on the wire, exactly as
// the plain directed route does.
func TestGivenAPrincipalBoundRouteWhenAppliedThenShouldStampTheTarget(t *testing.T) {
	provenance := laser.Provenance{Conversation: laser.NewConversationID()}
	laser.ToPrincipal(agentID(t, "planner"), laser.PrincipalID(7)).Apply(&provenance)

	if provenance.TargetAgent == nil || *provenance.TargetAgent != agentID(t, "planner") {
		t.Fatalf("expected the target stamped, got %v", provenance.TargetAgent)
	}
}

// A caller has to be able to tell which routes need presence refreshed
// before they resolve, and which principal an answer must be signed by.
func TestGivenRoutesWhenAskedForTheirPrincipalThenShouldReportOnlyTheBoundOnes(t *testing.T) {
	planner := agentID(t, "planner")
	bound := laser.ToPrincipal(planner, laser.PrincipalID(7))
	if !bound.RequiresPresence() {
		t.Error("a principal-bound route needs live presence to resolve")
	}
	if principal := bound.RequiredPrincipal(); principal == nil ||
		*principal != laser.PrincipalID(7) {
		t.Errorf("expected principal 7, got %v", principal)
	}

	for name, route := range map[string]laser.Router{
		"to":        laser.To(planner),
		"broadcast": laser.Broadcast(),
		"capable":   laser.ToCapable("summarise", laser.RouteAny),
	} {
		if route.RequiresPresence() {
			t.Errorf("%s route should not require presence", name)
		}
		if route.RequiredPrincipal() != nil {
			t.Errorf("%s route should require no principal", name)
		}
	}

	// A broadcast names no agent to bind, so binding one is meaningless
	// rather than a requirement it could silently fail to meet.
	if laser.Broadcast().WithPrincipal(laser.PrincipalID(7)).RequiresPresence() {
		t.Error("a broadcast binds nobody, so it requires no presence")
	}
}

// WithPrincipal on a plain directed route is the same requirement
// ToPrincipal builds, so it has to become that route rather than a To
// carrying an ignored field.
func TestGivenADirectedRouteWhenGivenAPrincipalThenShouldBecomeBound(t *testing.T) {
	now := time.Now()
	registry := boundRegistry(t, now, "summarise", map[string]*uint32{
		"planner": principalOf(9),
	})

	_, err := laser.To(agentID(t, "planner")).
		WithPrincipal(laser.PrincipalID(7)).
		ResolveTargets(registry, now)
	var mismatch *laser.RoutePrincipalMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("expected the bound route to be enforced, got %T: %v", err, err)
	}
}

// A principal-bound route cannot resolve against nothing: falling back to
// the name alone would drop the requirement exactly when it matters.
func TestGivenNoRegistryWhenRoutingToAPrincipalThenShouldRefuse(t *testing.T) {
	_, err := laser.ToPrincipal(agentID(t, "planner"), laser.PrincipalID(7)).
		ResolveTargets(nil, time.Now())
	if err == nil {
		t.Fatal("expected a principal-bound route with no registry to be refused")
	}
}
