package laser_test

import (
	"context"
	"errors"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// rememberedOn reads back what was written to the memory topic.
func rememberedOn(t *testing.T, server *runtimeServer) []string {
	t.Helper()
	var bodies []string
	// A memory record rides as itself rather than inside an agent envelope,
	// which is what makes the topic readable by a plain fold.
	for _, message := range server.on(wire.AgentMemory) {
		var record wire.MemoryRecord
		if err := wire.Unmarshal(message.Payload, &record); err != nil {
			continue
		}
		bodies = append(bodies, string(record.Body))
	}
	return bodies
}

// Wrapping is the point: the handler keeps doing its own job and the turn is
// remembered around it.
func TestGivenAutoRememberWhenAHandlerSucceedsThenShouldRememberTheTurn(t *testing.T) {
	server := newRuntimeServer()
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	handled := 0
	inner := laser.HandlerFunc(func(
		context.Context, *laser.AgentMessage, *laser.AgentCtx,
	) error {
		handled++
		return nil
	})
	handler := laser.WithMemory(inner, handle.Memory("agent-mem")).
		AutoRemember(laser.MemoryMessage)
	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{})
	source := sourceOf(workRecord(t, 41, "a turn"))

	if err := runUntilDrained(t, runtime, source, handler); err != nil {
		t.Fatalf("run: %v", err)
	}
	if handled != 1 {
		t.Errorf("want the wrapped handler still run, got %d", handled)
	}
	remembered := rememberedOn(t, server)
	if len(remembered) != 1 || remembered[0] != "a turn" {
		t.Errorf("want the turn remembered, got %v", remembered)
	}
}

// Wrapping alone changes nothing: remembering is off until a kind names what
// to store the turn as.
func TestGivenNoKindWhenAHandlerSucceedsThenShouldRememberNothing(t *testing.T) {
	server := newRuntimeServer()
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	handler := laser.WithMemory(laser.HandlerFunc(func(
		context.Context, *laser.AgentMessage, *laser.AgentCtx,
	) error {
		return nil
	}), handle.Memory("agent-mem"))
	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{})

	if err := runUntilDrained(
		t, runtime, sourceOf(workRecord(t, 41, "a turn")), handler); err != nil {
		t.Fatalf("run: %v", err)
	}
	if remembered := rememberedOn(t, server); len(remembered) != 0 {
		t.Errorf("want nothing remembered without a kind, got %v", remembered)
	}
}

// A turn that failed was not a turn: remembering it would record work that
// never happened, and the failure is the handler's to report.
func TestGivenAFailedHandlerWhenWrappedThenShouldNotRememberAndShouldReportIt(t *testing.T) {
	server := newRuntimeServer()
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	failure := errors.New("handler failed")
	handler := laser.WithMemory(laser.HandlerFunc(func(
		context.Context, *laser.AgentMessage, *laser.AgentCtx,
	) error {
		return failure
	}), handle.Memory("agent-mem")).AutoRemember(laser.MemoryMessage)

	err := handler.Handle(context.Background(), &laser.AgentMessage{}, nil)
	if !errors.Is(err, failure) {
		t.Fatalf("want the handler's failure reported, got %v", err)
	}
	if remembered := rememberedOn(t, server); len(remembered) != 0 {
		t.Errorf("want a failed turn not remembered, got %v", remembered)
	}
}

// A memory handler wraps a handler, so one without is a caller mistake
// rather than a silent no-op.
func TestGivenNoInnerHandlerWhenHandledThenShouldRefuse(t *testing.T) {
	server := newRuntimeServer()
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	handler := laser.WithMemory(nil, handle.Memory("agent-mem"))

	if err := handler.Handle(context.Background(), &laser.AgentMessage{}, nil); err == nil {
		t.Fatal("want a memory handler with nothing to wrap refused")
	}
}
