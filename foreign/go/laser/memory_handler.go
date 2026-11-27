package laser

import (
	"context"
)

// Lifetime is how long a remembered fact lives.
type Lifetime string

const (
	// LifetimeSession is conversation-scoped and prunable. The default.
	LifetimeSession Lifetime = "session"
	// LifetimeDurable is shared across conversations and feeds the
	// knowledge graph.
	LifetimeDurable Lifetime = "durable"
)

// MemoryHandler wraps a handler so each turn it handles is remembered.
//
// The wrapping is the point: a handler that writes its own memory has to
// remember to, and has to get the scope right every time. This does it once,
// around any handler, and leaves the handler to its own job.
type MemoryHandler struct {
	inner    AgentHandler
	memory   *Memory
	kind     *MemoryKind
	lifetime Lifetime
}

// WithMemory wraps inner, writing to memory. Remembering is off until
// AutoRemember names a kind, so wrapping alone changes nothing.
func WithMemory(inner AgentHandler, memory *Memory) *MemoryHandler {
	return &MemoryHandler{inner: inner, memory: memory, lifetime: LifetimeSession}
}

// AutoRemember remembers each successfully handled message under its own
// conversation, stored as kind.
func (handler *MemoryHandler) AutoRemember(kind MemoryKind) *MemoryHandler {
	derived := *handler
	derived.kind = &kind
	return &derived
}

// Lifetime sets how long what this handler remembers lives.
func (handler *MemoryHandler) Lifetime(lifetime Lifetime) *MemoryHandler {
	derived := *handler
	derived.lifetime = lifetime
	return &derived
}

// Handle runs the wrapped handler and, when it succeeded, remembers the turn.
//
// A failed write does not fail the turn. The message was already handled, and
// a retry would repeat the handler's own effects, so a memory that could not
// be written is dropped rather than turned into a redelivery.
func (handler *MemoryHandler) Handle(
	ctx context.Context, message *AgentMessage, agent *AgentCtx,
) error {
	if handler.inner == nil {
		return &InvalidError{Reason: "a memory handler wraps a handler, so it needs one"}
	}
	if err := handler.inner.Handle(ctx, message, agent); err != nil {
		return err
	}
	if handler.kind == nil || handler.memory == nil {
		return nil
	}
	scope := handler.memory.Scope()
	if conversation, named := sessionOf(message); named {
		scope.Conversation = &conversation
	}
	if agent != nil {
		identity := agent.Identity()
		scope.Agent = &identity
	}
	// Dropped, not reported: see Handle's own note on why a failed write
	// must not become a redelivery.
	_, _ = handler.memory.For(scope).Remember(ctx, *handler.kind, message.Body())
	return nil
}
