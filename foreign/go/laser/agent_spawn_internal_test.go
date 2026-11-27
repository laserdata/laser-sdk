package laser

import (
	"context"
	"testing"
	"time"
)

// noopGovernor never governs anything in these tests: only the wiring that
// picks it (or doesn't) is under test here, never its own Decide.
type noopGovernor struct{}

func (noopGovernor) Decide(context.Context, *GovernedAction) (*ActionDecision, error) {
	return &ActionDecision{}, nil
}

func TestGivenNoGovernorWhenSpawningThenShouldLeaveTheHandleUngoverned(t *testing.T) {
	laser := isolatedLaser()
	scoped := spawnGoverned(laser, SpawnOptions{})
	if scoped != laser {
		t.Fatal("expected the same handle back when no governor is set")
	}
	if _, governed := scoped.Governor(); governed {
		t.Fatal("expected no governor on an ungoverned spawn")
	}
}

func TestGivenAGovernorWhenSpawningThenShouldScopeTheHandleUnderIt(t *testing.T) {
	laser := isolatedLaser()
	scoped := spawnGoverned(laser, SpawnOptions{Governor: noopGovernor{}, GovernorMode: GovernorEnforce})
	if scoped == laser {
		t.Fatal("expected a distinct, governed handle")
	}
	mode, governed := scoped.Governor()
	if !governed || mode != GovernorEnforce {
		t.Fatalf("want governed under GovernorEnforce, got governed=%v mode=%v", governed, mode)
	}
	if _, governed := laser.Governor(); governed {
		t.Fatal("the original handle must stay ungoverned")
	}
}

func TestGivenNoRetentionWhenSpawningGovernedThenShouldDefaultIt(t *testing.T) {
	laser := isolatedLaser()
	scoped := spawnGoverned(laser, SpawnOptions{Governor: noopGovernor{}})
	if got, want := scoped.governor.retention, DefaultGovernorRetention(); got != want {
		t.Fatalf("want the default retention %+v, got %+v", want, got)
	}
}

func TestGivenAnExplicitRetentionWhenSpawningGovernedThenShouldUseIt(t *testing.T) {
	laser := isolatedLaser()
	retention := GovernorRetention{Capacity: 7, IdleTTL: time.Minute}
	scoped := spawnGoverned(laser, SpawnOptions{Governor: noopGovernor{}, GovernorRetention: &retention})
	if scoped.governor.retention != retention {
		t.Fatalf("want the caller's retention %+v, got %+v", retention, scoped.governor.retention)
	}
}
