package laser

import (
	"context"
	"math"
	"sync"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// HeartbeatExpiry is how long a heartbeat is kept. They only prove liveness,
// so there is nothing to learn from an old one.
const HeartbeatExpiry = time.Hour

// satelliteTopics are the agent topics beside the session lane and the
// heartbeats. The registry topic is not among them: it is created by the
// first card or registry fact, so a deployment that registers no agents never
// pays for it.
var satelliteTopics = []string{
	wire.AgentStreams, wire.AgentMemory, wire.AgentDLQ,
	wire.AgentAudit, wire.AgentJournal,
}

// TopicRetention is how long a topic keeps what it holds.
//
// The session lane is the one topic that must have a bound. It is the record
// of everything every agent did, so a lane with neither an expiry nor a size
// bound grows until the deployment runs out of disk, and the first anyone
// hears of it is a server that stops accepting writes.
type TopicRetention struct {
	// Expiry is how old a record may get, zero for the server's default.
	Expiry time.Duration
	// MaxBytes is how large the topic may get, zero for the server's default
	// and MaxUint64 for unbounded.
	MaxBytes uint64
}

// ExpireAfter keeps records for an age, under the server's own size bound.
//
// A zero age is raised to the smallest the wire can carry, because a
// retention of nothing would expire a record before anything could read it.
func ExpireAfter(age time.Duration) TopicRetention {
	if age < time.Microsecond {
		age = time.Microsecond
	}
	return TopicRetention{Expiry: age}
}

// KeepAtMost keeps records under a size bound, with no age bound.
func KeepAtMost(bytes uint64) TopicRetention {
	return TopicRetention{MaxBytes: bytes}
}

// Validate refuses a retention that bounds nothing.
func (retention TopicRetention) Validate() error {
	unbounded := retention.MaxBytes == 0 || retention.MaxBytes == math.MaxUint64
	if retention.Expiry <= 0 && unbounded {
		return &InvalidError{Reason: "the session lane's retention must set " +
			"an expiry or a size bound, or it grows until the deployment " +
			"runs out of disk"}
	}
	return nil
}

// options is the retention as the server takes it.
func (retention TopicRetention) options(partitions uint32) *iggcon.TopicOptions {
	options := &iggcon.TopicOptions{PartitionsCount: &partitions}
	if retention.Expiry > 0 {
		expiry := iggcon.Duration(retention.Expiry.Microseconds())
		options.MessageExpiry = &expiry
	}
	if retention.MaxBytes > 0 {
		bytes := retention.MaxBytes
		options.MaxTopicSize = &bytes
	}
	return options
}

// Bootstrap creates the agent topology on the default stream.
//
// The session lane takes the retention given, the heartbeats expire after an
// hour because they only prove liveness, and the satellites take the server's
// defaults. It is idempotent, and a stream that already exists is used as it
// stands, so a pre-provisioned stream needs no stream permission.
//
// The control topic is not created here. Only operators may write to it, so a
// client creating it would be claiming a privilege it does not have.
//
// Every partition costs the server open files and memory, and this creates
// seven topics, so the count is multiplied sevenfold. Keep it small on a
// small deployment.
func (laser *Laser) Bootstrap(
	ctx context.Context, partitions uint32, retention TopicRetention,
) error {
	if laser.defaultStream == "" {
		return errNoStream()
	}
	if partitions == 0 {
		return &InvalidError{Reason: "a topic needs at least one partition"}
	}
	if err := retention.Validate(); err != nil {
		return err
	}
	stream := laser.Stream(laser.defaultStream)
	if err := stream.Ensure(ctx); err != nil {
		return err
	}
	if err := laser.ensureRetained(ctx, wire.AgentSessions,
		retention.options(partitions)); err != nil {
		return err
	}
	if err := laser.ensureRetained(ctx, wire.AgentHeartbeats,
		ExpireAfter(HeartbeatExpiry).options(partitions)); err != nil {
		return err
	}
	for _, satellite := range satelliteTopics {
		if err := stream.Topic(satellite).Ensure(ctx, partitions, nil); err != nil {
			return err
		}
	}
	return nil
}

// ensureRetained creates one agent topic under an explicit retention,
// leaving one that already exists as it stands.
func (laser *Laser) ensureRetained(
	ctx context.Context, name string, options *iggcon.TopicOptions,
) error {
	client, err := laser.connected()
	if err != nil {
		return err
	}
	streamID, err := iggcon.NewIdentifier(laser.defaultStream)
	if err != nil {
		return &InvalidError{Reason: "stream name: " + err.Error()}
	}
	topicID, err := iggcon.NewIdentifier(name)
	if err != nil {
		return &InvalidError{Reason: "topic name: " + err.Error()}
	}
	switch _, err := client.GetTopic(ctx, streamID, topicID); {
	case err == nil:
		// A topic somebody already provisioned keeps the retention they chose
		// rather than having this one's imposed on it.
		return nil
	case !isMissingResource(err):
		return &TransportError{Reason: "read the topic", Cause: err}
	}
	if _, err := client.CreateTopic(ctx, streamID, name, options); err != nil {
		// A concurrent bootstrap may have created it in between, which is the
		// idempotence this call promises.
		if _, probe := client.GetTopic(ctx, streamID, topicID); probe == nil {
			return nil
		}
		return &TransportError{Reason: "create the topic", Cause: err}
	}
	return nil
}

// RefreshCapabilities probes the deployment again and returns what it serves
// now.
//
// A deployment that began serving a surface after this client connected is
// only visible by asking again, so a long-lived client that was told "no"
// once is not told it forever.
func (laser *Laser) RefreshCapabilities(ctx context.Context) Capabilities {
	client, err := laser.connected()
	if err != nil {
		return laser.capabilities
	}
	refreshed := NegotiateCapabilities(ctx, client, laser.configured())
	laser.state().mutex.Lock()
	laser.state().negotiated = &refreshed
	laser.state().mutex.Unlock()
	return refreshed
}

// configured is the capability set negotiation starts from: what the caller
// pinned, not what a previous probe concluded.
//
// Starting from the last probe would let one failed probe narrow the set
// permanently, since negotiation only ever adds.
func (laser *Laser) configured() Capabilities {
	laser.state().mutex.RLock()
	defer laser.state().mutex.RUnlock()
	if laser.state().configured != nil {
		return *laser.state().configured
	}
	return laser.capabilities
}

// Negotiated is the most recent probe's answer, or the handle's own set when
// nothing has re-probed.
func (laser *Laser) Negotiated() Capabilities {
	laser.state().mutex.RLock()
	defer laser.state().mutex.RUnlock()
	if laser.state().negotiated != nil {
		return *laser.state().negotiated
	}
	return laser.capabilities
}

// WaitUntilReady probes until the managed backends report themselves able to
// serve, or the bound passes.
//
// A deployment that announces no backends at all is refused rather than
// waited on: there is nothing there that will ever become ready, and waiting
// out the bound only delays saying so.
func (laser *Laser) WaitUntilReady(
	ctx context.Context, bound time.Duration,
) (Capabilities, error) {
	deadline := time.Now().Add(bound)
	for {
		capabilities := laser.RefreshCapabilities(ctx)
		if capabilities.Ready() {
			return capabilities, nil
		}
		if len(capabilities.EnabledBackends()) == 0 {
			return capabilities, &UnsupportedError{
				Surface: "readiness: the deployment announces no managed backend"}
		}
		if !time.Now().Before(deadline) {
			return capabilities, &StageTimeoutError{
				Stage: "the managed backends to report themselves ready"}
		}
		if err := sleepWithin(ctx, readinessPollInterval); err != nil {
			return capabilities, err
		}
	}
}

// readinessPollInterval is how often readiness is asked about again.
const readinessPollInterval = 100 * time.Millisecond

// ConnectionEvent is something that happened to this connection.
type ConnectionEvent string

const (
	// EventConnected means the transport, TLS included, is up.
	EventConnected ConnectionEvent = "connected"
	// EventDisconnected means it is not. A leader change that moves the
	// client to another node is not one of these.
	EventDisconnected ConnectionEvent = "disconnected"
	// EventSignedIn and EventSignedOut bracket an authenticated session.
	EventSignedIn  ConnectionEvent = "signed_in"
	EventSignedOut ConnectionEvent = "signed_out"
	// EventShutdown means the client was closed.
	EventShutdown ConnectionEvent = "shutdown"
)

// ConnectionEvents watches a connection get connected, disconnected, signed
// in, signed out and shut down, from the moment it is opened.
//
// It reports from now on rather than replaying what already happened: a
// caller that wants to know the current state asks the handle, and a caller
// that wants to know about changes watches them.
type ConnectionEvents struct {
	events chan ConnectionEvent
	stop   chan struct{}
	once   sync.Once
}

// ConnectionEvents opens a watcher over this connection.
//
// The Apache Iggy Go SDK publishes no diagnostic events, so nothing drives
// this on its own yet: a caller that observes the connection itself feeds it
// with Report. The shape is here so the surface a program writes against does
// not change when the transport starts publishing them.
func (laser *Laser) ConnectionEvents() *ConnectionEvents {
	return &ConnectionEvents{
		events: make(chan ConnectionEvent, connectionEventBuffer),
		stop:   make(chan struct{}),
	}
}

// connectionEventBuffer is how many events are held for a slow reader before
// the oldest is dropped. A watcher that fell behind wants the latest state,
// not a backlog of stale transitions.
const connectionEventBuffer = 16

// Events is the channel the transitions arrive on. It closes when the watcher
// is closed.
func (events *ConnectionEvents) Events() <-chan ConnectionEvent {
	return events.events
}

// Report records one transition, dropping the oldest when a reader has fallen
// behind: a watcher that cannot keep up wants the latest state rather than a
// backlog of stale ones.
func (events *ConnectionEvents) Report(event ConnectionEvent) {
	select {
	case <-events.stop:
		return
	default:
	}
	select {
	case events.events <- event:
	default:
		select {
		case <-events.events:
		default:
		}
		select {
		case events.events <- event:
		default:
		}
	}
}

// Close stops the watcher. It is safe to call more than once.
func (events *ConnectionEvents) Close() {
	events.once.Do(func() {
		close(events.stop)
		close(events.events)
	})
}
