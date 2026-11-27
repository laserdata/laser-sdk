package laser_test

import (
	"context"
	"errors"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
)

// A swap only changes which policy the next Decide runs under: an effect
// governed before the swap is judged by the old policy, one governed after by
// the new one, with no restart and no new Laser handle.
func TestGivenASwappableGovernorWhenSwappedThenShouldChangeOnlyTheNextDecision(t *testing.T) {
	allow := &scriptedGovernor{decision: laser.Allow()}
	block := &scriptedGovernor{decision: laser.Block("no longer allowed")}
	swappable := laser.NewSwappableGovernor(allow)
	server := &publishServer{}
	handle := governedHandle(server).WithGovernor(swappable, laser.GovernorEnforce)

	if _, err := handle.Topic("events").Publish().
		Payload([]byte("first")).Key("k").Send(context.Background()); err != nil {
		t.Fatalf("publish under the initial policy: %v", err)
	}

	previous := swappable.Swap(block)
	if previous != allow {
		t.Errorf("want Swap to return the policy it replaced")
	}
	if swappable.Current() != block {
		t.Errorf("want Current to report the newly installed policy")
	}

	_, err := handle.Topic("events").Publish().
		Payload([]byte("second")).Key("k").Send(context.Background())
	var blocked *laser.PolicyBlockedError
	if !errors.As(err, &blocked) {
		t.Fatalf("want the swapped-in policy to block, got %T: %v", err, err)
	}

	if len(server.published(t)) != 1 {
		t.Error("want only the first, pre-swap publish to have reached the log")
	}
	if len(allow.asked()) != 1 || len(block.asked()) != 1 {
		t.Errorf("want each policy asked exactly once, got allow=%d block=%d",
			len(allow.asked()), len(block.asked()))
	}
}
