package bdd_test

import (
	"testing"
	"time"
)

// TestGivenTheSharedProvenanceScenariosWhenRunThenShouldHold runs the
// provenance feature, which exercises the spine this client writes and reads.
func TestGivenTheSharedProvenanceScenariosWhenRunThenShouldHold(t *testing.T) {
	runFeature(t, "provenance.feature", time.Minute, platformAgentStep)
}
