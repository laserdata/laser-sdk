package laser

import (
	"errors"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// isolatedLaser is an unconnected handle with its own shared connection,
// rather than the package's shared zero-value one, so a test that claims
// presence on it never leaks that claim into another test.
func isolatedLaser() *Laser {
	return &Laser{shared: &connection{}}
}

func presenceAgentID(t *testing.T, value string) wire.AgentID {
	t.Helper()
	id, err := wire.NewAgentID(value)
	if err != nil {
		t.Fatalf("agent id %q: %v", value, err)
	}
	return id
}

func TestGivenOneConnectionWhenTwoAgentsClaimPresenceThenShouldRejectTheSecond(t *testing.T) {
	shared := &connection{}
	risk := presenceAgentID(t, "risk")
	audit := presenceAgentID(t, "audit")

	if err := claimPresence(shared, risk); err != nil {
		t.Fatalf("first claim: %v", err)
	}
	// Re-claiming the same agent is idempotent: a re-advertise moving its
	// inbox must not read as a second agent on this connection.
	if err := claimPresence(shared, risk); err != nil {
		t.Fatalf("re-claim by the same agent: %v", err)
	}

	err := claimPresence(shared, audit)
	var conflict *PresenceConflictError
	if err == nil {
		t.Fatal("expected a conflict claiming a second agent, got nil")
	}
	if !errors.As(err, &conflict) {
		t.Fatalf("expected *PresenceConflictError, got %T: %v", err, err)
	}
	if conflict.Advertised != risk || conflict.Requested != audit {
		t.Fatalf("conflict named %q wants %q, got advertised=%q requested=%q",
			risk, audit, conflict.Advertised, conflict.Requested)
	}
}

func TestGivenNoCapabilitiesWhenStartingAnAdvertisementThenShouldAdvertiseNothing(t *testing.T) {
	laser := isolatedLaser()
	advertisement, err := startAdvertisement(
		t.Context(), laser, presenceAgentID(t, "worker"), "worker.work", nil, 0)
	if err != nil {
		t.Fatalf("expected no error with no capabilities, got %v", err)
	}
	if advertisement != nil {
		t.Fatalf("expected no advertisement with no capabilities, got %+v", advertisement)
	}
}

func TestGivenCapabilitiesWhenStartingAnAdvertisementThenShouldOwnACopyOfThem(t *testing.T) {
	laser := isolatedLaser()
	capabilities := []wire.CapabilityDescriptor{{SkillID: "plan"}}
	advertisement, err := startAdvertisement(
		t.Context(), laser, presenceAgentID(t, "worker"), "worker.work", capabilities, 0)
	if err != nil {
		t.Fatalf("start the advertisement: %v", err)
	}
	if advertisement == nil {
		t.Fatal("expected an advertisement with capabilities set")
	}
	defer advertisement.withdraw()

	capabilities[0].SkillID = "mutated"
	if advertisement.card.Capabilities[0].SkillID != "plan" {
		t.Fatalf("advertisement aliased the caller's slice: got %q",
			advertisement.card.Capabilities[0].SkillID)
	}
}

// A second advertising agent on one connection must fail fast, not silently
// run with no presence advertised: the shared BDD scenario
// (bdd/scenarios/agent.feature, "A second advertising agent on one
// connection reports the presence conflict as it starts") expects
// SpawnAgent to refuse it, which only holds if startAdvertisement does not
// swallow this one error the way it swallows every other AdvertisePresence
// failure (a deployment without the presence command, a transport hiccup).
func TestGivenASecondAgentOnOneConnectionWhenStartingAnAdvertisementThenShouldFailWithPresenceConflict(
	t *testing.T,
) {
	shared := &connection{}
	laser := &Laser{shared: shared}
	first := presenceAgentID(t, "first-worker")
	if err := claimPresence(shared, first); err != nil {
		t.Fatalf("claim the first agent's presence: %v", err)
	}

	second := presenceAgentID(t, "second-worker")
	capabilities := []wire.CapabilityDescriptor{{SkillID: "second"}}
	advertisement, err := startAdvertisement(
		t.Context(), laser, second, "second.work", capabilities, 0)
	if advertisement != nil {
		t.Fatalf("expected no advertisement on conflict, got %+v", advertisement)
	}
	var conflict *PresenceConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("expected *PresenceConflictError, got %T: %v", err, err)
	}
	if conflict.Advertised != first || conflict.Requested != second {
		t.Fatalf("conflict named advertised=%q requested=%q, want %q/%q",
			conflict.Advertised, conflict.Requested, first, second)
	}
}
