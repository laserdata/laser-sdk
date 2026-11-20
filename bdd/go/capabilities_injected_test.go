package bdd_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	bdd "github.com/laserdata/laser-sdk/bdd/go"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// TestGivenTheSharedInjectedCapabilityScenariosWhenRunThenShouldHold runs the
// read-consistency pre-gate.
//
// A read-your-writes query against a surface that advertises only the weakest
// consistency must fail before any round trip: a server that did not
// understand the ask would drop the additive field and answer a wider
// question than the caller posed, and the caller would never know.
func TestGivenTheSharedInjectedCapabilityScenariosWhenRunThenShouldHold(t *testing.T) {
	runFeature(t, "capabilities_injected.feature", time.Minute, injectedStep)
}

func injectedStep(ctx context.Context, state *world, step bdd.Step) error {
	handled, err := background(ctx, state, step)
	if handled {
		return err
	}
	text := step.Text
	arguments := argumentsOf(step)

	switch {
	case text == "a managed-query connection that does not advertise read-your-writes":
		return injectWeakQuery(state)

	case strings.HasPrefix(text, "I run a read-your-writes query against topic"):
		return runQuery(ctx, state, arguments, true)

	case text == "the call fails as unsupported":
		return capabilityStep(ctx, state, step)

	case text == "the unified result code is unsupported":
		return unifiedCode(state)

	case strings.HasPrefix(text, "a prepared release receives"),
		strings.HasPrefix(text, "the command error keeps code"),
		text == "the release fails as an ambiguous mutation":
		return blocked("the Go client has no fenced-lease client and no " +
			"prepared mutation: there is no prepareRelease to hand a managed " +
			"result code to, so neither the preserved command error nor the " +
			"ambiguous unresolved attempt has anything to exercise")

	default:
		return fmt.Errorf("this runner does not implement the step")
	}
}

// injectWeakQuery hands the client a capability set the live server never
// reported: a managed query surface serving only eventual consistency.
//
// The point is to reach the local pre-gate on a deployment that has no
// managed backend at all, so the gate is proved rather than the server's
// refusal.
func injectWeakQuery(state *world) error {
	capabilities := state.client.Capabilities()
	capabilities.Managed = true
	capabilities.Query.Available = true
	capabilities.Query.Consistency = wire.ConsistencyEventual
	state.client = state.client.WithCapabilities(capabilities)
	if state.client.Capabilities().ServesConsistency(wire.ConsistencyReadYourWrites) {
		return fmt.Errorf("the injected surface still serves read-your-writes")
	}
	return nil
}
