package bdd_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	bdd "github.com/laserdata/laser-sdk/bdd/go"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// TestGivenTheSharedAgentScenariosWhenRunThenShouldHold runs the AGDX agent
// envelope feature, which is the threading rule: messages sharing a
// conversation replay in the order they were appended.
func TestGivenTheSharedAgentScenariosWhenRunThenShouldHold(t *testing.T) {
	runFeature(t, "agent.feature", time.Minute, agentStep)
}

func agentStep(ctx context.Context, state *world, step bdd.Step) error {
	text := step.Text
	switch {
	case strings.Contains(text, "is ready on this connection"):
		return spawnAdvertisingAgent(ctx, state, argumentsOf(step), true)
	case strings.Contains(text, "starts on the same connection"):
		return spawnAdvertisingAgent(ctx, state, argumentsOf(step), false)
	case text == "waiting for the second agent to be ready fails with a presence conflict":
		return checkSecondAgentPresenceConflict(ctx, state)
	}
	return platformAgentStep(ctx, state, step)
}

// spawnAdvertisingAgent spawns an agent advertising one skill on state's
// shared connection: SpawnAgent publishes its capability card and advertises
// its inbox as live presence (laser/presence.go, laser/agent_advertisement.go).
//
// For the first agent (waitReady), this is a Given step: any failure to spawn
// or become ready fails the scenario outright. For the second (sharing the
// connection the first already claimed), this is the When step: SpawnAgent
// itself now fails fast on the presence conflict (laser/agent_spawn.go
// advertises before joining the consumer group precisely so this does not
// need a live handle to observe), so whichever of SpawnAgent's error or a
// later Ready() reports it is stashed for the Then step to assert on.
func spawnAdvertisingAgent(
	ctx context.Context, state *world, arguments []string, waitReady bool,
) error {
	if len(arguments) < 2 {
		return fmt.Errorf("want an agent name and a skill, got %v", arguments)
	}
	name, skill := arguments[0], arguments[1]
	identity, err := bridgeAgent(name)
	if err != nil {
		return err
	}
	handler := laser.HandlerFunc(func(context.Context, *laser.AgentMessage, *laser.AgentCtx) error {
		return nil
	})
	handle, spawnErr := state.client.SpawnAgent(ctx, identity, laser.SpawnOptions{
		RuntimeOptions: laser.RuntimeOptions{Topic: wire.AgentSessions},
		Capabilities:   []wire.CapabilityDescriptor{{SkillID: skill}},
	}, handler)
	if !waitReady {
		state.put("secondAgentHandle", handle)
		state.put("secondAgentSpawnErr", spawnErr)
		return nil
	}
	if spawnErr != nil {
		return spawnErr
	}
	state.t.Cleanup(func() { _ = handle.Shutdown(context.Background()) })
	readyCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return handle.Ready(readyCtx)
}

// checkSecondAgentPresenceConflict asserts that the second agent spawned in
// spawnAdvertisingAgent could not start for exactly one reason: this
// connection already advertises the first agent's presence.
func checkSecondAgentPresenceConflict(ctx context.Context, state *world) error {
	if spawnErr, _ := state.get("secondAgentSpawnErr"); spawnErr != nil {
		return expectPresenceConflict(spawnErr.(error))
	}
	handleValue, found := state.get("secondAgentHandle")
	handle, _ := handleValue.(*laser.AgentHandle)
	if !found || handle == nil {
		return errors.New("the second agent neither failed to spawn nor produced a handle to wait on")
	}
	state.t.Cleanup(func() { _ = handle.Shutdown(context.Background()) })
	readyCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return expectPresenceConflict(handle.Ready(readyCtx))
}

// expectPresenceConflict reports whether err is a *laser.PresenceConflictError,
// as a Then step's own pass/fail rather than the raw error: this scenario's
// whole point is that the failure happens, so only its absence, or the wrong
// kind of failure, is what fails the step.
func expectPresenceConflict(err error) error {
	var conflict *laser.PresenceConflictError
	if !errors.As(err, &conflict) {
		return fmt.Errorf("want a presence conflict, got %v", err)
	}
	return nil
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
