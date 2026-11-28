package laser

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// DefaultCardRefresh is how often a spawned agent's capability card is
// republished and its live presence re-advertised when SpawnOptions.
// CardRefresh is zero. Matches Rust's Agent::builder default session
// heartbeat, which the capability card's ttl is also derived from.
const DefaultCardRefresh = 60 * time.Second

// cardTTLHeartbeats is how many refreshes a spawned agent's card stays fresh
// for, so one missed refresh does not read a live agent as dead.
const cardTTLHeartbeats = 3

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
	// Capabilities is what this agent serves. When non-empty, SpawnAgent
	// publishes a capability card to the registry (so capability routing can
	// discover it) and advertises options.Topic as this agent's live presence
	// (so a fan-out resolves where to send it work), refreshing both every
	// CardRefresh until the agent stops, when it withdraws both. Empty means
	// a private agent that does not advertise, the prior behavior.
	Capabilities []wire.CapabilityDescriptor
	// CardRefresh is how often the card and presence are refreshed, and
	// (times cardTTLHeartbeats) how long the card stays fresh. Zero takes
	// DefaultCardRefresh. Only read when Capabilities is non-empty.
	CardRefresh time.Duration
	// Governor, when set, re-scopes this agent's Laser with Governor and
	// GovernorMode before anything else in SpawnAgent runs, so every effect
	// the spawned handler's AgentCtx sends (publishes, AGDX verbs, memory
	// writes) runs the governor first. Equivalent to calling
	// Laser.WithGovernor yourself before SpawnAgent, but scoped to this one
	// agent rather than replacing the governor on the handle the caller
	// keeps.
	Governor ActionGovernor
	// GovernorMode is read only when Governor is set.
	GovernorMode GovernorMode
	// GovernorRetention overrides the governed handle's chain-head budget.
	// Nil takes DefaultGovernorRetention. Only read when Governor is set.
	GovernorRetention *GovernorRetention
	// ConsolidateEvery and Consolidator run Consolidator.Consolidate every
	// *ConsolidateEvery, off the handler loop (its own goroutine, stopped
	// with the agent), scoped to just this agent's own memory. Both must be
	// given for the tick to exist: either nil means no background
	// consolidation, ever. A zero *ConsolidateEvery fails SpawnAgent with an
	// InvalidError instead of starting, regardless of whether Consolidator
	// is set.
	ConsolidateEvery *time.Duration
	Consolidator     Consolidator
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
	// unready records a Ready that saw the run end before it was polling.
	unready atomic.Bool
}

// errStoppedUnready is what a run that ended cleanly before it was polling reports.
const errStoppedUnready = "agent stopped before it became ready"

// Identity is the agent this handle runs.
func (handle *AgentHandle) Identity() wire.AgentID { return handle.identity }

// Ready waits until the agent is polling its topic, so a publish after it is
// delivered rather than racing the start. It is idempotent.
//
// SpawnAgent joins the consumer group before it returns a handle, and the
// agent counts as ready at its first read, after its startup reads. A run that
// failed before that makes Ready return the failure. A run that ended without
// one before that makes Ready, and a Join or Shutdown after it, return a
// ConfigError saying the agent stopped before it became ready.
func (handle *AgentHandle) Ready(ctx context.Context) error {
	select {
	case <-handle.ready:
		return nil
	case <-handle.done:
		// Both may be closed, and an agent that was polling was ready.
		select {
		case <-handle.ready:
			return nil
		default:
		}
		if handle.result != nil {
			return handle.result
		}
		handle.unready.Store(true)
		return &ConfigError{Reason: errStoppedUnready}
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Join waits for the agent to stop on its own, its consumer ending or
// erroring, or for ctx to end first, and reports why it stopped.
func (handle *AgentHandle) Join(ctx context.Context) error {
	select {
	case <-handle.done:
		if handle.result == nil && handle.unready.Load() {
			return &ConfigError{Reason: errStoppedUnready}
		}
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
// keeps it testable against a fake source without a server. A source that
// closes is closed once the loop ends, which is what leaves its consumer
// group. advertisement and consolidation, when non-nil, are stopped then too,
// before the handle reports done, so Join and Shutdown never return before
// the cleanup they promise.
func startAgentHandle(
	identity wire.AgentID, runtime *AgentRuntime, source RecordSource, handler AgentHandler,
	advertisement *agentAdvertisement, consolidation *agentConsolidation,
) *AgentHandle {
	ctx, cancel := context.WithCancel(context.Background())
	handle := &AgentHandle{
		identity: identity,
		cancel:   cancel,
		ready:    make(chan struct{}),
		done:     make(chan struct{}),
	}
	polling := sync.OnceFunc(func() { close(handle.ready) })
	reading := RecordSource(readySource{RecordSource: source, polling: polling})
	if assigned, lists := source.(interface{ Partitions() []uint32 }); lists {
		reading = assignedReadySource{readySource: reading.(readySource), assigned: assigned}
	}
	go func() {
		handle.result = runtime.Run(ctx, reading, handler)
		// A stop that was asked for is honoured only after readiness is reported.
		if handle.result == nil && ctx.Err() != nil {
			polling()
		}
		// A source left open keeps its group membership, and the partitions with it.
		if closer, closes := source.(interface{ Close(context.Context) error }); closes {
			closing, release := context.WithTimeout(context.Background(), DefaultShutdownGrace)
			if err := closer.Close(closing); err != nil {
				runtime.failed(nil, err)
			}
			release()
		}
		if consolidation != nil {
			consolidation.abort()
		}
		if advertisement != nil {
			advertisement.withdraw()
		}
		close(handle.done)
	}()
	return handle
}

// readySource reports a source's first read, which is when its agent is polling.
type readySource struct {
	RecordSource
	polling func()
}

// Next reports the read before it is made.
func (source readySource) Next(ctx context.Context) (*Record, error) {
	source.polling()
	return source.RecordSource.Next(ctx)
}

// assignedReadySource keeps the partition listing of a source that has one.
type assignedReadySource struct {
	readySource
	assigned interface{ Partitions() []uint32 }
}

// Partitions lists what the wrapped source is assigned.
func (source assignedReadySource) Partitions() []uint32 { return source.assigned.Partitions() }

// spawnGoverned re-scopes laser under options.Governor/options.GovernorMode
// when set, defaulting GovernorRetention when the caller left it nil, so
// every effect the spawned agent's AgentCtx runs is governed from the first
// one. Returns laser unchanged when options.Governor is nil.
func spawnGoverned(laser *Laser, options SpawnOptions) *Laser {
	if options.Governor == nil {
		return laser
	}
	retention := DefaultGovernorRetention()
	if options.GovernorRetention != nil {
		retention = *options.GovernorRetention
	}
	return laser.WithGovernorRetention(options.Governor, options.GovernorMode, retention)
}

// spawnTopic is the topic a spawned agent reads: the one it names, else its declared topic, else the session lane.
func spawnTopic(laser *Laser, identity wire.AgentID, options RuntimeOptions) string {
	if options.Topic != "" && options.Topic != wire.AgentSessions {
		return options.Topic
	}
	stream := options.Stream
	if stream == "" {
		stream = laser.defaultStream
	}
	if declared := declaredTopic(laser.layoutFor(stream), identity); declared != "" {
		return declared
	}
	return wire.AgentSessions
}

// SpawnAgent opens identity's reliable consumer on its own goroutine, joining
// a consumer group on options.Topic (named after identity unless
// options.Group says otherwise), and returns a handle to observe or stop it.
// An agent that names no topic reads the session lane, or its declared topic
// under a per-agent topic layout.
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
	if options.ConsolidateEvery != nil && *options.ConsolidateEvery <= 0 {
		return nil, &InvalidError{Reason: "consolidate_every must be greater than zero"}
	}
	laser = spawnGoverned(laser, options)
	options.Topic = spawnTopic(laser, identity, options.RuntimeOptions)
	agent := laser.Agent(identity, options.AgentOptions)
	runtimeOptions := options.RuntimeOptions
	runtimeOptions.EnforceControl = true
	runtime, err := laser.AgentRuntime(agent, runtimeOptions)
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
	// Advertised before the group join, so a presence conflict (two agents
	// sharing this connection) fails fast without joining a consumer group
	// this call is about to discard anyway.
	advertisement, err := startAdvertisement(
		ctx, laser, identity, options.Topic, options.Capabilities, options.CardRefresh)
	if err != nil {
		return nil, err
	}
	if options.Consumer.AutoCommit == nil {
		options.Consumer.ManualCommit = true
	}
	member := laser.Stream(stream).Topic(options.Topic).Group(group)
	caps := laser.Capabilities().Filters
	if laser.Connection() != nil && caps.Native && caps.Catalog && caps.GroupPolicyReads &&
		(options.Topic == wire.AgentSessions || options.Topic == wire.AgentControl) {
		_, err = member.Create().Filter(wire.AddresseeFilterName, wire.AddresseeFilter(identity)).Build(ctx)
		options.Consumer.SkipGroupCreate = true
	}
	var source *Consumer
	if err == nil {
		source, err = member.Consumer(ctx, options.Consumer)
	}
	if err != nil {
		if advertisement != nil {
			advertisement.withdraw()
		}
		return nil, err
	}
	consolidation := startConsolidation(identity, options.ConsolidateEvery, options.Consolidator)
	return startAgentHandle(identity, runtime, source, handler, advertisement, consolidation), nil
}
