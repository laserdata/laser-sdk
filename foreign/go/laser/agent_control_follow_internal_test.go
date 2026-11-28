package laser

import (
	"testing"
	"time"
)

func TestGivenAMissingControlTopicWhenLookedForAgainThenShouldDoubleTheWaitUpToThirtySeconds(t *testing.T) {
	wait := controlDiscoveryStart
	var waits []time.Duration
	for range 10 {
		wait = nextControlDiscovery(wait)
		waits = append(waits, wait)
	}

	if waits[0] != 200*time.Millisecond || waits[1] != 400*time.Millisecond {
		t.Errorf("want the wait doubled from 100 ms, got %v", waits[:2])
	}
	if last := waits[len(waits)-1]; last != 30*time.Second {
		t.Errorf("want the wait capped at 30 s, got %v", last)
	}
}
