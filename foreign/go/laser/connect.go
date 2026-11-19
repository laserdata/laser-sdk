package laser

import (
	"context"
	"errors"
	"regexp"
	"strconv"
	"sync"
	"time"

	iggy "github.com/apache/iggy/foreign/go/client"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// DefaultConnectTimeout bounds a connect that neither the caller nor
// LASER_CONNECT_TIMEOUT_MS pins. See docs/connect-timeout.md for what the
// deadline covers and what it leaves behind.
const DefaultConnectTimeout = 30 * time.Second

// LocalConnectionString reaches the Apache Iggy container the examples run
// against.
const LocalConnectionString = "iggy:iggy@127.0.0.1:8090"

// Options configure a connect. The zero value takes every default.
type Options struct {
	// Timeout bounds the whole connect: the dial, the login, and the
	// capability probe. Zero reads LASER_CONNECT_TIMEOUT_MS, then falls back to
	// DefaultConnectTimeout.
	Timeout time.Duration
	// DefaultStream pins the stream the one-word topic shortcut addresses.
	// Purely ergonomic: any other stream stays one accessor away.
	DefaultStream string
	// Capabilities is the starting point for negotiation, not a ceiling. The
	// probe adds whatever the connected deployment serves on top.
	Capabilities *Capabilities
	// ResourceNaming decides whether managed names are scoped to the default
	// stream. The zero value scopes them, as every client does by default.
	ResourceNaming ResourceNaming
	// Publish is how a publish is retried. Nil reads the LASER_PUBLISH_*
	// variables, then falls back to the shared defaults every Laser client
	// uses.
	Publish *PublishOptions
	// Environment reads the LASER_* variables, nil for the process
	// environment.
	Environment Environment
}

func (options Options) environment() Environment {
	if options.Environment != nil {
		return options.Environment
	}
	return OSEnvironment
}

var digits = regexp.MustCompile(`^\d+$`)

// timeout resolves the connect deadline, preferring the caller's value over
// the environment over the default.
func (options Options) timeout() (time.Duration, error) {
	if options.Timeout != 0 {
		return options.Timeout, nil
	}
	raw, present := options.environment()("LASER_CONNECT_TIMEOUT_MS")
	if !present || raw == "" {
		return DefaultConnectTimeout, nil
	}
	if !digits.MatchString(raw) {
		return 0, configError("LASER_CONNECT_TIMEOUT_MS must be an integer")
	}
	milliseconds, err := strconv.ParseInt(raw, 10, 32)
	if err != nil || milliseconds < 1 {
		return 0, configError(
			"LASER_CONNECT_TIMEOUT_MS must be an integer between 1 and 2147483647")
	}
	return time.Duration(milliseconds) * time.Millisecond, nil
}

// Laser is a connected client. It is safe for concurrent use.
//
// The connection and the applied frontier are the only mutable state, so they
// are the only fields the lock covers. Everything else is fixed once the
// handle exists and is read without one.
type Laser struct {
	// publish is how this handle retries a publish.
	publish PublishOptions
	// shared is held in common by every handle derived from this one, so a
	// derived handle reads through the same connection and the same applied
	// frontier rather than starting over.
	shared         *connection
	connectionInfo *ConnectionString
	capabilities   Capabilities
	defaultStream  string
	resourceNaming ResourceNaming
	// governor runs before every effect this handle has, nil when it governs
	// nothing.
	governor *governorState
}

// connection is the state every copy of one handle holds in common.
type connection struct {
	mutex  sync.RWMutex
	client iggcon.Client
	// configured is the capability set negotiation starts from, so one failed
	// probe cannot narrow a handle permanently: negotiation only ever adds.
	configured *Capabilities
	// negotiated is the most recent probe's answer, shared so a re-probe on
	// one handle is seen by every handle derived from it.
	negotiated *Capabilities
	// frontier is the highest mutation position seen per managed surface.
	// Every managed reply raises it, and a later read of that surface sends
	// it, so whichever node answers has applied at least what this client
	// already wrote or read.
	frontier wire.AppliedFrontier
	// layouts is the session layout declared per data stream, read at the
	// agent send boundary. It is shared rather than per handle because a
	// declared layout has to hold for every later send on the stream: two
	// handles that disagreed about where an agent's work goes would split
	// one agent's inbox across two topics.
	layouts map[string]SessionLayout
	// leases is where this connection's session leases are held, opened on
	// first use. One registry per connection is what keeps a process to one
	// heartbeat per stream.
	leases *LeaseRegistry
}

// unconnected stands in for the shared state of a handle that never
// connected, so a zero value is still safe to ask and to close. It holds no
// client, so the mutex it shares is never contended for one.
var unconnected connection

// state is this handle's shared connection.
func (laser *Laser) state() *connection {
	if laser.shared == nil {
		return &unconnected
	}
	return laser.shared
}

// copy derives a handle sharing this one's connection and frontier.
//
// What a derived handle may change is its own configuration, never the
// connection: two handles on one socket that disagreed about what they had
// already read would each answer from a different point in the log.
func (laser *Laser) copy() *Laser {
	derived := *laser
	return &derived
}

// declareLayout fixes where stream's agent work goes, for this connection and
// every handle derived from it.
func (laser *Laser) declareLayout(stream string, layout SessionLayout) {
	shared := laser.state()
	shared.mutex.Lock()
	defer shared.mutex.Unlock()
	if shared.layouts == nil {
		shared.layouts = map[string]SessionLayout{}
	}
	shared.layouts[stream] = layout
}

// layoutFor is the layout declared for stream, the shared one when none is.
func (laser *Laser) layoutFor(stream string) SessionLayout {
	shared := laser.state()
	shared.mutex.RLock()
	defer shared.mutex.RUnlock()
	return shared.layouts[stream]
}

// AgentDestination is the topic one agent record lands on, given the topic it
// was addressed to, its kind and its addressee.
//
// It is the empty string when the record keeps the topic it was sent on,
// which is every record under the default layout. A kind of "" is a record
// with no envelope, routed by its addressee alone.
func (laser *Laser) AgentDestination(
	topic string, kind wire.AgentKind, target *wire.AgentID,
) string {
	return destinationTopic(laser.layoutFor(laser.defaultStream), topic, kind, target)
}

// DeclaredTopic is the topic agent reads instead of agent.sessions on the
// default stream, empty when the stream declares no topic for it.
//
// A per-agent topic layout moves an agent's work off the lane, so an agent
// that kept reading agent.sessions would see its own lifecycle records and
// none of its work. This is how a consumer asks where to read.
func (laser *Laser) DeclaredTopic(agent wire.AgentID) string {
	return declaredTopic(laser.layoutFor(laser.defaultStream), agent)
}

// WithDefaultStream derives a handle whose unqualified topics resolve on
// another stream.
func (laser *Laser) WithDefaultStream(stream string) *Laser {
	derived := laser.copy()
	derived.defaultStream = stream
	return derived
}

// WithResourceNaming derives a handle that scopes managed resource names
// differently.
//
// Stream scoping is how two streams in one deployment keep separate
// namespaces, so opting out is a deployment-wide decision rather than a
// per-call one.
func (laser *Laser) WithResourceNaming(naming ResourceNaming) *Laser {
	derived := laser.copy()
	derived.resourceNaming = naming
	return derived
}

// WithCapabilities derives a handle that treats the deployment as serving
// exactly these surfaces, for a caller that knows better than the probe.
func (laser *Laser) WithCapabilities(capabilities Capabilities) *Laser {
	derived := laser.copy()
	derived.capabilities = capabilities
	return derived
}

// Connect opens a connection and negotiates what the deployment serves.
//
// The connection string takes the grammar every Laser client shares, and a
// LaserData host is encrypted automatically. See TCPSettings for the transport
// this lowers onto, and Capabilities for what the connect-time probe decides.
func Connect(ctx context.Context, connectionString string) (*Laser, error) {
	return ConnectWith(ctx, connectionString, Options{})
}

// ConnectEnv connects from the environment: LASER_CONNECTION_STRING, which is
// exactly what Connect takes, plus the optional LASER_STREAM pinning the
// default stream. The same two variables every deployment guide uses, so a
// program moves between local, staging and LaserData Cloud with no code change.
func ConnectEnv(ctx context.Context) (*Laser, error) {
	return ConnectEnvWith(ctx, Options{})
}

// ConnectEnvWith is ConnectEnv with explicit options. A DefaultStream the
// caller pins wins over LASER_STREAM.
func ConnectEnvWith(ctx context.Context, options Options) (*Laser, error) {
	connectionString, resolved, err := ConnectionFromEnv(options)
	if err != nil {
		return nil, err
	}
	return ConnectWith(ctx, connectionString, resolved)
}

// ConnectionFromEnv reads the connection string and the optional default
// stream from the environment, returning the options ConnectEnv would connect
// with. Exposed so a caller can see what the environment resolved to before
// anything is dialed.
//
// A DefaultStream the caller pinned wins over LASER_STREAM, because an explicit
// setting is never overridden by ambient configuration.
func ConnectionFromEnv(options Options) (string, Options, error) {
	environment := options.environment()
	connectionString, present := environment("LASER_CONNECTION_STRING")
	if !present || connectionString == "" {
		return "", options, configError("LASER_CONNECTION_STRING is not set")
	}
	if options.DefaultStream == "" {
		if stream, present := environment("LASER_STREAM"); present {
			options.DefaultStream = stream
		}
	}
	return connectionString, options, nil
}

// Resolve fills in what an Options left zero, reading the environment for the
// values it does not pin. Connect calls it, and a caller can call it first to
// see the deadline a connect will run under.
func (options Options) Resolve() (Options, error) {
	timeout, err := options.timeout()
	if err != nil {
		return options, err
	}
	options.Timeout = timeout
	// A misconfigured publish schedule is refused before anything is dialed,
	// because a client that connects and then fails on every send is harder
	// to diagnose than one that refuses to start.
	publish, err := options.publishOptions()
	if err != nil {
		return options, err
	}
	options.Publish = &publish
	return options, nil
}

// Local connects to the Apache Iggy container at iggy:iggy@127.0.0.1:8090.
func Local(ctx context.Context) (*Laser, error) {
	return ConnectWith(ctx, LocalConnectionString, Options{})
}

// ConnectWith is Connect with explicit options.
func ConnectWith(
	ctx context.Context, connectionString string, options Options,
) (*Laser, error) {
	options, err := options.Resolve()
	if err != nil {
		return nil, err
	}
	parsed, err := ParseConnectionStringWithEnv(connectionString, options.environment())
	if err != nil {
		return nil, err
	}
	tcpOptions, err := parsed.TCPOptions()
	if err != nil {
		return nil, err
	}
	// Every stage shares one deadline, so a server that accepts the socket and
	// then stalls cannot hold the caller past the budget it set.
	ctx, cancel := context.WithTimeout(ctx, options.Timeout)
	defer cancel()

	connected, err := iggy.NewIggyClient(iggy.WithTcp(tcpOptions...))
	if err != nil {
		return nil, &TransportError{Reason: "build the client", Cause: err}
	}
	if err := connected.Connect(ctx); err != nil {
		return nil, &TransportError{Reason: connectStage(parsed.Host), Cause: err}
	}
	// The probe is the last stage, and it is never fatal: a client that cannot
	// classify the deployment still connects, with the open surface alone.
	configured := OpenCapabilities()
	if options.Capabilities != nil {
		configured = *options.Capabilities
	}
	publish := DefaultPublishOptions()
	if options.Publish != nil {
		publish = *options.Publish
	}
	return &Laser{
		shared: &connection{
			client: connected, configured: &configured,
		},
		connectionInfo: parsed,
		capabilities:   NegotiateCapabilities(ctx, connected, configured),
		defaultStream:  options.DefaultStream,
		resourceNaming: options.ResourceNaming,
		publish:        publish,
	}, nil
}

// connectStage names what was being attempted when a connect failed. A
// LaserData deployment that never accepts usually refuses the caller's address,
// so the stage names the access rule rather than leaving a bare timeout.
func connectStage(host string) string {
	if isLaserDataHost(host) {
		return "connect: the deployment did not accept the connection, " +
			"which is usually an access rule that does not list this address"
	}
	return "connect: the deployment did not accept the connection"
}

// FromClient wraps a client the caller already connected, so an application
// that configures the Apache Iggy SDK itself still gets the Laser surface over
// it. The Rust client takes a caller-supplied client the same way.
//
// No probe runs, because the caller owns the connection and may have opened it
// against something this package never dialed. The capability set is therefore
// exactly what Options.Capabilities pins, and its Hello outcome stays
// HelloUnknown: nothing was established, as opposed to positively absent.
// Call NegotiateCapabilities explicitly to probe such a connection.
func FromClient(client iggcon.Client, options Options) *Laser {
	capabilities := OpenCapabilities()
	if options.Capabilities != nil {
		capabilities = *options.Capabilities
	}
	publish := DefaultPublishOptions()
	if options.Publish != nil {
		publish = *options.Publish
	}
	return &Laser{
		shared: &connection{
			client: client, configured: &capabilities,
		},
		capabilities:   capabilities,
		defaultStream:  options.DefaultStream,
		resourceNaming: options.ResourceNaming,
		publish:        publish,
	}
}

// Capabilities reports what the connected deployment serves, as the
// connect-time probe established it.
func (laser *Laser) Capabilities() Capabilities { return laser.capabilities }

// Connection reports the parsed connection string this client dialed with. The
// credentials it carries are the caller's own, so they are never logged here.
func (laser *Laser) Connection() *ConnectionString { return laser.connectionInfo }

// DefaultStream is the stream the one-word topic shortcut addresses, empty
// when none was pinned.
func (laser *Laser) DefaultStream() string { return laser.defaultStream }

// Client hands out the underlying Apache Iggy client, for an operation this
// package does not wrap yet. It is nil once the handle is closed.
func (laser *Laser) Client() iggcon.Client {
	laser.state().mutex.RLock()
	defer laser.state().mutex.RUnlock()
	return laser.state().client
}

// Close releases the connection. It is safe to call more than once, and safe
// to call while another goroutine is using the handle.
func (laser *Laser) Close() error {
	laser.state().mutex.Lock()
	client := laser.state().client
	laser.state().client = nil
	laser.state().mutex.Unlock()
	if client == nil {
		return nil
	}
	err := client.Close()
	if err != nil && !errors.Is(err, context.Canceled) {
		return &TransportError{Reason: "close the connection", Cause: err}
	}
	return nil
}

// publishSchedule is how this handle retries a publish.
func (laser *Laser) publishSchedule() PublishOptions {
	if laser.publish.Timeout == 0 {
		// A handle built as a zero value still publishes, under the shared
		// defaults rather than a zero timeout that would fail every attempt.
		return DefaultPublishOptions()
	}
	return laser.publish
}

// WithPublishOptions derives a handle that retries a publish on its own
// schedule.
func (laser *Laser) WithPublishOptions(options PublishOptions) (*Laser, error) {
	if err := options.Validate(); err != nil {
		return nil, err
	}
	derived := laser.copy()
	derived.publish = options
	return derived, nil
}

// reconnect reopens the connection between two publish attempts.
//
// The Apache Iggy client reconnects on its own as it dials, so this is the
// hook the recovery schedule drives rather than a second dialer: a handle
// whose connection was closed cannot be revived here, and says so.
func (laser *Laser) reconnect(ctx context.Context) error {
	client, err := laser.connected()
	if err != nil {
		return err
	}
	if reconnectable, ok := client.(interface {
		Connect(context.Context) error
	}); ok {
		if err := reconnectable.Connect(ctx); err != nil {
			return &TransportError{Reason: "reconnect", Cause: err}
		}
	}
	return nil
}
