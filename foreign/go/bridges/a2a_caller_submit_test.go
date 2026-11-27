package bridges_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/bridges"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// A caller's SubmitTo addresses one task to one agent, the same as the
// bridge's own SubmitTo, but owned by the caller (bound task ids, when the
// bridge binds them, still scope to this principal).
func TestGivenACallerWhenSubmittedToAnAgentThenShouldAddressOnlyThatAgent(t *testing.T) {
	log := &bridgeLog{}
	bridge := bridgeOver(t, log)
	caller := bridge.Caller("tenant-a")
	target := agent(t, "worker")

	if _, err := caller.SubmitTo(context.Background(), target, []byte(sendParams)); err != nil {
		t.Fatalf("submit to: %v", err)
	}

	stored := log.stored()
	var command wire.AgentEnvelope
	if err := wire.Unmarshal(stored[len(stored)-1].Payload, &command); err != nil {
		t.Fatalf("decode the command: %v", err)
	}
	if command.Target == nil || *command.Target != target {
		t.Errorf("want the command addressed to the target, got %v", command.Target)
	}
}

// A caller's SubmitWithHops carries the inbound hop path with this bridge's
// own hop appended, the same as the bridge's own SubmitWithHops.
func TestGivenACallerWhenSubmittedWithHopsThenShouldCarryTheExtendedPath(t *testing.T) {
	log := &bridgeLog{}
	bridge := bridgeOver(t, log)
	caller := bridge.Caller("tenant-a")

	if _, err := caller.SubmitWithHops(
		context.Background(), []string{"upstream-bridge"}, nil, []byte(sendParams),
	); err != nil {
		t.Fatalf("submit with hops: %v", err)
	}

	stored := log.stored()
	var command wire.AgentEnvelope
	if err := wire.Unmarshal(stored[len(stored)-1].Payload, &command); err != nil {
		t.Fatalf("decode the command: %v", err)
	}
	got, carried := command.Metadata[bridges.BridgeHopsMetaKey]
	if !carried {
		t.Fatal("want the hop path carried on the command")
	}
	want := bridges.HopsMetadata([]string{"upstream-bridge", "edge"})
	if !reflect.DeepEqual(got, want) {
		t.Errorf("want the extended hop path %+v, got %+v", want, got)
	}
}

// A caller's SubmitIn writes the child session's submitted start, with the
// ancestry, before the command -- the same as the bridge's own SubmitIn.
func TestGivenACallerWhenSubmittedInAChildThenShouldWriteTheStartBeforeTheCommand(t *testing.T) {
	log := &bridgeLog{}
	bridge := bridgeOver(t, log)
	caller := bridge.Caller("tenant-a")
	parent, root := laser.NewConversationID(), laser.NewConversationID()

	task, err := caller.SubmitIn(context.Background(), parent, root, []byte(sendParams))
	if err != nil {
		t.Fatalf("submit in: %v", err)
	}

	stored := log.stored()
	if len(stored) != 2 {
		t.Fatalf("want a session start and a command, got %d", len(stored))
	}
	var start wire.AgentEnvelope
	if err := wire.Unmarshal(stored[0].Payload, &start); err != nil {
		t.Fatalf("decode the start: %v", err)
	}
	if start.Parent == nil || *start.Parent != parent ||
		start.Root == nil || *start.Root != root {
		t.Errorf("want the start to carry the ancestry, got parent=%v root=%v",
			start.Parent, start.Root)
	}
	var command wire.AgentEnvelope
	if err := wire.Unmarshal(stored[1].Payload, &command); err != nil {
		t.Fatalf("decode the command: %v", err)
	}
	if command.Parent == nil || *command.Parent != parent ||
		command.Root == nil || *command.Root != root {
		t.Errorf("want the command to carry the ancestry, got parent=%v root=%v",
			command.Parent, command.Root)
	}
	if task.Status.State != "TASK_STATE_SUBMITTED" {
		t.Errorf("want a submitted task, got %q", task.Status.State)
	}
}

// A caller's SubmitInTo is SubmitIn addressed to one agent -- the same
// composition the bridge's own SubmitInTo gives.
func TestGivenACallerWhenSubmittedInToAChildThenShouldAddressOnlyThatAgent(t *testing.T) {
	log := &bridgeLog{}
	bridge := bridgeOver(t, log)
	caller := bridge.Caller("tenant-a")
	parent, root := laser.NewConversationID(), laser.NewConversationID()
	target := agent(t, "worker")

	if _, err := caller.SubmitInTo(
		context.Background(), target, parent, root, []byte(sendParams),
	); err != nil {
		t.Fatalf("submit in to: %v", err)
	}

	stored := log.stored()
	var command wire.AgentEnvelope
	if err := wire.Unmarshal(stored[len(stored)-1].Payload, &command); err != nil {
		t.Fatalf("decode the command: %v", err)
	}
	if command.Target == nil || *command.Target != target {
		t.Errorf("want the command addressed to the target, got %v", command.Target)
	}
	if command.Parent == nil || *command.Parent != parent ||
		command.Root == nil || *command.Root != root {
		t.Errorf("want the command to carry the ancestry, got parent=%v root=%v",
			command.Parent, command.Root)
	}
}

// A bound bridge's new submit variants scope their task the same as Submit
// does: the owning caller reads it, another caller does not.
func TestGivenABoundBridgeWhenSubmittedToThenShouldStillScopeToTheCaller(t *testing.T) {
	log := &bridgeLog{}
	bridge := bridgeOver(t, log).WithTaskBinding([32]byte{1: 7}, time.Hour)
	owner := bridge.Caller("tenant-a")
	target := agent(t, "worker")

	task, err := owner.SubmitTo(context.Background(), target, []byte(sendParams))
	if err != nil {
		t.Fatalf("submit to: %v", err)
	}

	if _, err := owner.Task(context.Background(), task.ID); err != nil {
		t.Fatalf("the owner must read its own task: %v", err)
	}
	if _, err := bridge.Caller("tenant-b").Task(context.Background(), task.ID); err == nil {
		t.Fatal("a bound task must not open for another caller")
	}
}
