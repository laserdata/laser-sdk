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

// TestGivenTheSharedAgentScenariosWhenRunThenShouldHold runs the AGDX agent
// envelope feature, which is the threading rule: messages sharing a
// conversation replay in the order they were appended.
func TestGivenTheSharedAgentScenariosWhenRunThenShouldHold(t *testing.T) {
	runFeature(t, "agent.feature", time.Minute, agentStep)
}

func agentStep(ctx context.Context, state *world, step bdd.Step) error {
	// The presence scenario is the one step of this feature the client cannot
	// serve, so it is named here rather than left to fail as an unimplemented
	// step, which would read as a gap in the runner instead of in the client.
	if strings.Contains(step.Text, "advertising the skill") ||
		strings.Contains(step.Text, "presence conflict") ||
		strings.Contains(step.Text, "starts on the same connection") {
		return blocked("the Go client advertises no agent presence: there is " +
			"no capability-card advertise and no ready signal to conflict on")
	}
	return platformAgentStep(ctx, state, step)
}

// TestGivenTheSharedMustUnderstandScenariosWhenRunThenShouldHold runs the
// must-understand feature.
//
// It builds and inspects an envelope in process, so it reaches no deployment
// and runs wherever the suite runs: making it skip for want of a server would
// hide a receiver-rule regression behind an unrelated absence.
func TestGivenTheSharedMustUnderstandScenariosWhenRunThenShouldHold(t *testing.T) {
	runLocalFeature(t, "agent_must_understand.feature", time.Minute, mustUnderstandStep)
}

func mustUnderstandStep(_ context.Context, state *world, step bdd.Step) error {
	switch step.Text {
	case "I build an agent event requiring feature bits the receiver lacks":
		envelope := plainEvent()
		// Bit 8 stands for a feature this build does not implement, and the
		// receiver understands nothing, so the requirement is unmet.
		envelope.MustUnderstand = 1 << 8
		state.put("understood", envelope.UnmetRequirements(0) == 0)
		return nil

	case "I build a plain agent event":
		envelope := plainEvent()
		state.put("understood", envelope.UnmetRequirements(0) == 0)
		return nil

	case "the receiver rejects it as not understood":
		return checkUnderstood(state, false)

	case "the receiver understands it":
		return checkUnderstood(state, true)

	default:
		return fmt.Errorf("this runner does not implement the step")
	}
}

// plainEvent is the envelope both scenarios start from, so the only thing
// that differs between them is the marker under test.
func plainEvent() wire.AgentEnvelope {
	record := wire.RecordID{ID: wire.IDFromBytes(smallID(1))}
	source, err := wire.NewAgentID("sender")
	if err != nil {
		panic(err)
	}
	return wire.AgentEnvelope{
		Kind:         wire.KindEvent,
		Record:       &record,
		Conversation: wire.ConversationID{ID: wire.IDFromBytes(smallID(2))},
		Source:       source,
		Body:         []byte("event"),
	}
}

// smallID is a wire id holding one small number, which is how the other
// runners spell the fixed ids of an in-process envelope.
func smallID(value byte) [16]byte {
	var id [16]byte
	id[15] = value
	return id
}

func checkUnderstood(state *world, want bool) error {
	got, found := state.get("understood")
	if !found {
		return fmt.Errorf("no envelope was built")
	}
	if got != want {
		return fmt.Errorf("want understood %v, got %v", want, got)
	}
	return nil
}
