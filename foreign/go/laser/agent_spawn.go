package laser

import (
	"context"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// SpawnOptions configures a spawned agent: the identity gates (AgentOptions),
// the reliable-consumer behavior (RuntimeOptions), and how it joins the
// topic.
//
// This is the Go shape of what Rust and TypeScript build with
// Agent::builder()/spawn(): one call that joins a consumer group and starts
// the reliable consumer on its own goroutine, rather than a caller wiring
// Laser.Agent, Laser.AgentRuntime and Topic.ConsumerGroup together by hand
// every time. A plain struct literal is the idiom the rest of this package
// already uses for configuration (AgentOptions, RuntimeOptions, ConsumerOptions),
// so SpawnOptions embeds both rather than introducing a parallel chained
// builder type.
type SpawnOptions struct {
	AgentOptions
	RuntimeOptions
	// Consumer configures the underlying poll: wait, batch size, start
	// position, auto-commit, encryption. The zero value takes every
	// ConsumerOptions default.
	Consumer ConsumerOptions
}

// AgentHandle owns a spawned agent's background consumer.
//
// Call Shutdown or Join to observe how it stopped. A handle that is simply
// dropped leaves its goroutine running: Go has no destructor to catch that
// the way Rust's Drop does, so discarding a handle without stopping it first
// leaks the consumer for as long as the process runs.
type AgentHandle struct {
	identity wire.AgentID
	cancel   context.CancelFunc
	ready    chan struct{}
	done     chan struct{}
	result   error
}

// Identity is the agent this handle runs.
func (handle *AgentHandle) Identity() wire.AgentID { return handle.identity }

// Ready waits until the agent's consume loop has started running, or reports
// why it already stopped if it never got that far.
//
// SpawnAgent joins the consumer group before it returns a handle at all, so
// by the time a caller holds one, group membership has already landed
// server-side: a publish after SpawnAgent returns is never racing the join.
// What Ready still waits out is the goroutine scheduling gap between the
// handle being returned and its loop actually running, and it is idempotent:
// a later call after the first sees the same outcome.
//
// Ready cannot distinguish "the loop started" from "the loop started and
// then failed on its very first read": both close the same instant from a
// caller's view, and AgentRuntime.Run offers no earlier hook to signal
// readiness from inside the loop itself. A source that fails immediately
// is still reported correctly, just possibly by Join rather than by Ready.
func (handle *AgentHandle) Ready(ctx context.Context) error {
	// Checked separately and first, rather than left to select's random pick
	// among ready cases: once the run has actually finished, its real result
	// is what a caller must see, not whichever of the two closed channels
	// select happens to choose.
	select {
	case <-handle.done:
		return handle.result
	default:
	}
	select {
	case <-handle.ready:
		return nil
	case <-handle.done:
		return handle.result
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Join waits for the agent to stop on its own, its consumer ending or
// erroring, or for ctx to end first, and reports why it stopped.
func (handle *AgentHandle) Join(ctx context.Context) error {
	select {
	case <-handle.done:
		return handle.result
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Shutdown asks the agent to stop and waits for it.
//
// AgentRuntime.Run finishes the record already in its hands within its own
// shutdown grace before the loop actually exits, so this can take as long as
// that grace even though the signal is sent at once.
func (handle *AgentHandle) Shutdown(ctx context.Context) error {
	handle.cancel()
	return handle.Join(ctx)
}

// Abort signals the agent to stop without waiting for it.
//
// Unlike Rust's task abort, Go has no way to forcibly kill a running
// goroutine: this cancels the same context Shutdown does, and the consumer
// loop still runs its own shutdown grace before exiting. A caller that needs
// to know when the agent is actually gone should call Join (with its own
// bound, if one matters) rather than treat Abort alone as proof of that.
func (handle *AgentHandle) Abort() {
	handle.cancel()
}

// startAgentHandle runs handler over source under runtime on its own
// goroutine and returns a handle to observe or stop it.
//
// source is expected to already be open (a consumer group member that has
// joined, or any other RecordSource ready to read), so this carries no
// network call of its own: it is the lifecycle wiring alone, which is what
// keeps it testable against a fake source without a server.
func startAgentHandle(
	identity wire.AgentID, runtime *AgentRuntime, source RecordSource, handler AgentHandler,
) *AgentHandle {
	ctx, cancel := context.WithCancel(context.Background())
	handle := &AgentHandle{
		identity: identity,
		cancel:   cancel,
		ready:    make(chan struct{}),
		done:     make(chan struct{}),
	}
	go func() {
		close(handle.ready)
		handle.result = runtime.Run(ctx, source, handler)
		close(handle.done)
	}()
	return handle
}

// SpawnAgent opens identity's reliable consumer on its own goroutine, joining
// a consumer group on options.Topic (named after identity unless
// options.Group says otherwise), and returns a handle to observe or stop it.
//
// The group join happens here, before this returns, rather than being
// deferred to the handle's Ready call: Topic.ConsumerGroup already performs
// it synchronously, so there is no cheaper point at which to fail fast on a
// bad topic or a deployment that refused the join.
func (laser *Laser) SpawnAgent(
	ctx context.Context, identity wire.AgentID, options SpawnOptions, handler AgentHandler,
) (*AgentHandle, error) {
	if identity.IsZero() {
		return nil, &InvalidError{Reason: "a spawned agent needs an identity"}
	}
	if handler == nil {
		return nil, &InvalidError{Reason: "a spawned agent needs a handler"}
	}
	if options.Topic == "" {
		return nil, &InvalidError{Reason: "a spawned agent needs the topic to listen on"}
	}
	agent := laser.Agent(identity, options.AgentOptions)
	runtime, err := laser.AgentRuntime(agent, options.RuntimeOptions)
	if err != nil {
		return nil, err
	}
	stream := options.RuntimeOptions.Stream
	if stream == "" {
		stream = laser.defaultStream
	}
	if stream == "" {
		return nil, errNoStream()
	}
	group := options.Group
	if group == "" {
		group = identity.String()
	}
	source, err := laser.Stream(stream).Topic(options.Topic).
		ConsumerGroup(ctx, group, options.Consumer)
	if err != nil {
		return nil, err
	}
	return startAgentHandle(identity, runtime, source, handler), nil
}
