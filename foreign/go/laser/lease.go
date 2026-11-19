package laser

import (
	"context"
	"sort"
	"sync"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// DefaultHeartbeatInterval is how often a process with session leases
// publishes its heartbeat when the caller names no interval.
const DefaultHeartbeatInterval = 10 * time.Second

// SessionLease is ownership of one session's liveness.
//
// While any lease on a session is held, the process lists that session in its
// heartbeat on agent.heartbeats, which is how a deployment tells a session
// somebody is still working on from one whose process died. Release it when
// the process stops working on the session.
//
// Go has no destructor, so a lease that is never released keeps its session
// alive for as long as the process runs. Release is the whole protocol, and
// `defer lease.Release()` is the shape that gets it right. A terminal verb on
// the session does not release leases held elsewhere in the process, because
// one part of a process ending its work says nothing about another part's.
type SessionLease struct {
	registry *LeaseRegistry
	key      leaseKey
	// released makes a second Release a no-op rather than dropping a holder
	// somebody else still has.
	released sync.Once
}

// Session is the leased session.
func (lease *SessionLease) Session() wire.ConversationID { return lease.key.session }

// Stream is the stream the leased session lives in.
func (lease *SessionLease) Stream() string { return lease.key.stream }

// Release gives up this lease. It is idempotent.
func (lease *SessionLease) Release() {
	lease.released.Do(func() { lease.registry.release(lease.key) })
}

// leaseKey is one lease identity: the stream by name and creation time, and
// the session. A recreated stream is a different lease scope, because a
// session id in the new incarnation is not the session the lease was taken
// on.
type leaseKey struct {
	stream           string
	streamGeneration uint64
	session          wire.ConversationID
}

type leaseEntry struct {
	holders  int
	agent    wire.AgentID
	interval time.Duration
}

// LeaseRegistry is the per-connection lease map the heartbeat loop reads
// every tick.
//
// One loop serves every lease on the connection: a process holding a hundred
// sessions publishes one heartbeat per stream per tick, not a hundred.
type LeaseRegistry struct {
	// process identifies this holder across restarts, so a deployment can
	// tell one process's heartbeat from another's.
	process string
	mutex   sync.Mutex
	leases  map[leaseKey]*leaseEntry
	running bool
	// beats counts published heartbeats, and loops counts the heartbeat
	// goroutines running, for a caller that wants to observe the loop
	// without reading the log. More than one loop would mean a process
	// publishing the same sessions twice a tick.
	beats uint64
	loops int
}

// NewLeaseRegistry opens a registry under a fresh process identity.
func NewLeaseRegistry() *LeaseRegistry {
	return &LeaseRegistry{
		process: wire.IDFromBytes(NewOperationID()).String(),
		leases:  map[leaseKey]*leaseEntry{},
	}
}

// Process is this holder's identity, the one its heartbeats carry.
func (registry *LeaseRegistry) Process() string { return registry.process }

// Held is how many distinct session leases are open.
func (registry *LeaseRegistry) Held() int {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	return len(registry.leases)
}

// Beats is how many heartbeats this registry has published.
func (registry *LeaseRegistry) Beats() uint64 {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	return registry.beats
}

// Loops is how many heartbeat loops are running, which is one while any
// lease is held and zero otherwise.
func (registry *LeaseRegistry) Loops() int {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	return registry.loops
}

// ShortestInterval is the interval the next tick will use: the shortest among
// the held leases, and zero when none are held.
//
// It is what the loop will wait, not what it is waiting now. A lease taken
// while the loop is mid-sleep tightens this at once and the tick that follows
// the current one, which is where a caller watching for a tighter heartbeat
// should look.
func (registry *LeaseRegistry) ShortestInterval() time.Duration {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	shortest := time.Duration(0)
	for _, entry := range registry.leases {
		if shortest == 0 || entry.interval < shortest {
			shortest = entry.interval
		}
	}
	return shortest
}

// Leases is the registry this connection's leases are held in, opened on
// first use and shared by every handle derived from this one.
//
// One registry per connection is what keeps a process to one heartbeat per
// stream: two registries on one connection would each claim the same sessions
// under a different process identity.
func (laser *Laser) Leases() *LeaseRegistry {
	shared := laser.state()
	shared.mutex.Lock()
	defer shared.mutex.Unlock()
	if shared.leases == nil {
		shared.leases = NewLeaseRegistry()
	}
	return shared.leases
}

// LeaseSession takes a lease on one session's liveness and starts the
// heartbeat loop if it is not already running.
//
// The stream's creation time is read once, here, so a lease is scoped to the
// incarnation it was taken on. A heartbeat is never published for a stream
// that has since been recreated: the sessions it named are gone, and claiming
// them alive would keep a deployment waiting on work nobody is doing.
//
// An interval of zero takes DefaultHeartbeatInterval. Several leases held at
// once beat at the shortest interval among them, because the holder that
// asked for the tightest liveness window is the one that needs it.
func (laser *Laser) LeaseSession(
	ctx context.Context, session wire.ConversationID, agent wire.AgentID,
	interval time.Duration,
) (*SessionLease, error) {
	if laser.defaultStream == "" {
		return nil, errNoStream()
	}
	if agent.IsZero() {
		return nil, &InvalidError{Reason: "a session lease needs the agent holding it"}
	}
	if interval <= 0 {
		interval = DefaultHeartbeatInterval
	}
	generation, err := laser.streamGeneration(ctx, laser.defaultStream)
	if err != nil {
		return nil, err
	}
	key := leaseKey{
		stream: laser.defaultStream, streamGeneration: generation, session: session}
	return laser.Leases().acquire(laser, key, agent, interval), nil
}

// streamGeneration reads the creation time of one stream incarnation.
func (laser *Laser) streamGeneration(ctx context.Context, stream string) (uint64, error) {
	client, err := laser.connected()
	if err != nil {
		return 0, err
	}
	streamID, err := iggcon.NewIdentifier(stream)
	if err != nil {
		return 0, &InvalidError{Reason: "stream name: " + err.Error()}
	}
	details, err := client.GetStream(ctx, streamID)
	if err != nil {
		return 0, &TransportError{Reason: "read the stream", Cause: err}
	}
	if details == nil {
		return 0, &InvalidError{Reason: "the stream " + stream + " does not exist"}
	}
	return details.CreatedAt, nil
}

// acquire takes a lease, starting the heartbeat loop when it is the first.
func (registry *LeaseRegistry) acquire(
	laser *Laser, key leaseKey, agent wire.AgentID, interval time.Duration,
) *SessionLease {
	registry.mutex.Lock()
	if registry.leases == nil {
		registry.leases = map[leaseKey]*leaseEntry{}
	}
	if entry, held := registry.leases[key]; held {
		entry.holders++
		if interval < entry.interval {
			entry.interval = interval
		}
	} else {
		registry.leases[key] = &leaseEntry{holders: 1, agent: agent, interval: interval}
	}
	start := !registry.running
	registry.running = true
	if start {
		registry.loops++
	}
	registry.mutex.Unlock()
	if start {
		go registry.heartbeatLoop(laser)
	}
	return &SessionLease{registry: registry, key: key}
}

func (registry *LeaseRegistry) release(key leaseKey) {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	// A lease is only ever built by acquire and released once, so the entry
	// is always here. The lookup is checked anyway rather than dereferenced,
	// because a missing one is a bookkeeping bug and not worth a panic.
	entry := registry.leases[key]
	if entry == nil {
		return
	}
	entry.holders--
	if entry.holders == 0 {
		delete(registry.leases, key)
	}
}

// heartbeatLoop publishes one heartbeat per stream per tick, at the shortest
// interval among the held leases.
//
// It ends when the last lease is released, so a process holding none
// publishes nothing. A failed publish is retried on the next tick and never
// fails a session: a missed heartbeat is a liveness signal the deployment did
// not get, not work that went wrong.
func (registry *LeaseRegistry) heartbeatLoop(laser *Laser) {
	defer func() {
		registry.mutex.Lock()
		registry.loops--
		registry.mutex.Unlock()
	}()
	for {
		interval, running := registry.interval()
		if !running {
			return
		}
		time.Sleep(interval)
		for _, held := range registry.snapshot() {
			// Each beat is bounded on its own, so one unreachable stream does
			// not hold up another stream's heartbeat past its interval.
			ctx, cancel := context.WithTimeout(context.Background(), interval)
			registry.beat(ctx, laser, held)
			cancel()
		}
	}
}

// interval is the shortest interval among the held leases, and reports the
// loop stopped when none are held.
func (registry *LeaseRegistry) interval() (time.Duration, bool) {
	shortest := registry.ShortestInterval()
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	if shortest == 0 {
		registry.running = false
		return 0, false
	}
	return shortest, true
}

// heldStream is one stream's share of the held leases.
type heldStream struct {
	stream     string
	generation uint64
	agent      wire.AgentID
	sessions   []wire.ConversationID
}

// snapshot groups the held sessions by stream incarnation, so one stream's
// ids never ride another stream's heartbeat.
func (registry *LeaseRegistry) snapshot() []heldStream {
	registry.mutex.Lock()
	grouped := map[leaseKey]*heldStream{}
	for key, entry := range registry.leases {
		scope := leaseKey{stream: key.stream, streamGeneration: key.streamGeneration}
		held, seen := grouped[scope]
		if !seen {
			held = &heldStream{
				stream: key.stream, generation: key.streamGeneration, agent: entry.agent}
			grouped[scope] = held
		}
		held.sessions = append(held.sessions, key.session)
	}
	registry.mutex.Unlock()

	streams := make([]heldStream, 0, len(grouped))
	for _, held := range grouped {
		sort.Slice(held.sessions, func(left, right int) bool {
			return held.sessions[left].String() < held.sessions[right].String()
		})
		streams = append(streams, *held)
	}
	// One stream's heartbeat before another's in a fixed order, so a test and
	// a reader see the same sequence every tick.
	sort.Slice(streams, func(left, right int) bool {
		if streams[left].stream != streams[right].stream {
			return streams[left].stream < streams[right].stream
		}
		return streams[left].generation < streams[right].generation
	})
	return streams
}

// beat publishes one stream's heartbeat, in chunks the contract will carry.
func (registry *LeaseRegistry) beat(ctx context.Context, laser *Laser, held heldStream) {
	// The stream is re-read every tick rather than trusted from acquire time,
	// because a stream recreated since then holds none of these sessions and
	// a heartbeat for it would claim work nobody is doing.
	// A read that failed answers zero, which is not a creation time any
	// stream has, so one comparison covers both a changed incarnation and a
	// read nobody could answer.
	generation, err := laser.streamGeneration(ctx, held.stream)
	if err != nil || generation != held.generation {
		return
	}
	scoped := laser.WithDefaultStream(held.stream)
	conversation := DeriveConversationID("heartbeat/" + registry.process)
	agdx := scoped.AgdxOn(held.stream, wire.AgentHeartbeats, held.agent, conversation)
	for start := 0; start < len(held.sessions); start += wire.MaxHeartbeatSessions {
		end := min(start+wire.MaxHeartbeatSessions, len(held.sessions))
		generation := held.generation
		body, err := wire.Marshal(wire.SessionHeartbeat{
			Process:          registry.process,
			Stream:           held.stream,
			Sessions:         held.sessions[start:end],
			StreamGeneration: &generation,
		})
		if err != nil {
			return
		}
		if _, err := agdx.Status(wire.OperationProgress).
			Body(body).ContentType(wire.ContentCBOR).Send(ctx); err != nil {
			return
		}
		registry.mutex.Lock()
		registry.beats++
		registry.mutex.Unlock()
	}
}
