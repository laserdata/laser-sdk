package laser

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"
	"sync/atomic"
	"time"

	ierror "github.com/apache/iggy/foreign/go/errors"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// DefaultCoordinationAttemptTimeout bounds one coordination attempt, above the managed backend's 10 second fold wait.
const DefaultCoordinationAttemptTimeout = 15 * time.Second

const (
	firstRepeatDelay          = 100 * time.Millisecond
	maxRepeatDelay            = 2 * time.Second
	capabilityReprobeInterval = time.Second
)

// FencedLeaseClient runs the fenced-lease contract over a ManagedKvTransport: a mutation is prepared once, then executed under a per-attempt timeout.
type FencedLeaseClient struct {
	clientID       wire.U128
	transport      ManagedKvTransport
	attemptTimeout time.Duration
	closed         atomic.Bool
	closing        sync.Mutex
}

// NewFencedLeaseClient opens a client over transport with the default attempt timeout.
func NewFencedLeaseClient(transport ManagedKvTransport) *FencedLeaseClient {
	return &FencedLeaseClient{
		clientID: NewOperationID(), transport: transport,
		attemptTimeout: DefaultCoordinationAttemptTimeout,
	}
}

// NewDedicatedFencedLeaseClient opens a client over its own connection, dialed on first use.
func NewDedicatedFencedLeaseClient(connectionString string) *FencedLeaseClient {
	return NewFencedLeaseClient(NewDedicatedKvTransport(connectionString))
}

// WithAttemptTimeout sets the per-attempt timeout before the client is shared. A zero timeout refuses every call before it connects or sends.
func (client *FencedLeaseClient) WithAttemptTimeout(timeout time.Duration) *FencedLeaseClient {
	client.attemptTimeout = timeout
	return client
}

// Close retires the transport. Later calls fail before readiness or send.
func (client *FencedLeaseClient) Close() error {
	client.closing.Lock()
	defer client.closing.Unlock()
	if client.closed.Swap(true) {
		return nil
	}
	if closer, closes := client.transport.(io.Closer); closes {
		return closer.Close()
	}
	client.transport.Reset()
	return nil
}

// PrepareAcquire validates a lease acquisition and mints its operation identity.
func (client *FencedLeaseClient) PrepareAcquire(request wire.KvLease) (*PreparedMutation, error) {
	if err := request.Validate(); err != nil {
		return nil, &InvalidError{Reason: err.Error()}
	}
	return client.prepare(wire.AgdxKvLeaseCode, request, AmbiguousMutationRecovery{
		Kind: RecoveryWaitForLeaseExpiry, TTL: microsDuration(request.LeaseTTLMicros),
	})
}

// PrepareRenew validates a lease renewal and mints its operation identity.
func (client *FencedLeaseClient) PrepareRenew(request wire.KvLeaseRenew) (*PreparedMutation, error) {
	if err := request.Validate(); err != nil {
		return nil, &InvalidError{Reason: err.Error()}
	}
	return client.prepare(wire.AgdxKvLeaseRenewCode, request,
		AmbiguousMutationRecovery{Kind: RecoveryRepeatPrepared})
}

// PrepareRelease validates a lease release and mints its operation identity.
func (client *FencedLeaseClient) PrepareRelease(request wire.KvRelease) (*PreparedMutation, error) {
	if err := request.Validate(); err != nil {
		return nil, &InvalidError{Reason: err.Error()}
	}
	return client.prepare(wire.AgdxKvReleaseCode, request,
		AmbiguousMutationRecovery{Kind: RecoveryRepeatPrepared})
}

// PrepareCasFenced validates a fenced compare-and-swap and mints its operation identity.
func (client *FencedLeaseClient) PrepareCasFenced(request wire.KvCasFenced) (*PreparedMutation, error) {
	if err := request.Validate(); err != nil {
		return nil, &InvalidError{Reason: err.Error()}
	}
	return client.prepare(wire.AgdxKvCasFencedCode, request,
		AmbiguousMutationRecovery{Kind: RecoveryReconcileTargetPrecondition})
}

// Acquire executes a prepared acquisition and returns the granted lease, valid from the first send.
func (client *FencedLeaseClient) Acquire(ctx context.Context, operation *PreparedMutation) (*Lease, error) {
	outcome, err := client.execute(ctx, operation, wire.AgdxKvLeaseCode, "acquire", client.attemptTimeout)
	if err != nil {
		return nil, err
	}
	return operation.lease(outcome, wire.KvLeased, "acquire")
}

// Renew executes a prepared renewal and returns the extended lease under the same token.
func (client *FencedLeaseClient) Renew(ctx context.Context, operation *PreparedMutation) (*Lease, error) {
	return client.renewWithin(ctx, operation, client.attemptTimeout)
}

// Release executes a prepared release and reports whether a held lease was released.
func (client *FencedLeaseClient) Release(ctx context.Context, operation *PreparedMutation) (bool, error) {
	outcome, err := client.execute(ctx, operation, wire.AgdxKvReleaseCode, "release", client.attemptTimeout)
	if err != nil {
		return false, err
	}
	if outcome.Kind != wire.KvReleased {
		return false, unexpectedCoordinationOutcome("release", outcome.Kind)
	}
	return outcome.Released, nil
}

// CasFenced executes a prepared fenced compare-and-swap and returns the entry's new version.
func (client *FencedLeaseClient) CasFenced(ctx context.Context, operation *PreparedMutation) (uint64, error) {
	outcome, err := client.execute(ctx, operation, wire.AgdxKvCasFencedCode, "cas_fenced", client.attemptTimeout)
	if err != nil {
		return 0, err
	}
	if outcome.Kind != wire.KvCommitted {
		return 0, unexpectedCoordinationOutcome("cas_fenced", outcome.Kind)
	}
	return outcome.Version, nil
}

// Get reads a key, behind request.MinPosition when it is set. A barrier the fold cannot reach is a stale KvError, never an absent value.
func (client *FencedLeaseClient) Get(ctx context.Context, request wire.KvGet) (*wire.KvEntry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := ensureAttemptTimeout(client.attemptTimeout); err != nil {
		return nil, err
	}
	if err := client.readyWithin(ctx, client.attemptTimeout); err != nil {
		return nil, err
	}
	generation := client.generation()
	frame, err := wire.Marshal(request)
	if err != nil {
		return nil, &CodecError{Reason: "encode request", Cause: err}
	}
	attempt, cancel := context.WithTimeout(ctx, client.attemptTimeout)
	reply, err := client.sendOn(attempt, generation, wire.AgdxKvGetCode, frame)
	expired := ctx.Err() == nil && errors.Is(attempt.Err(), context.DeadlineExceeded)
	cancel()
	if err != nil {
		client.retire(generation)
		// A read has no outcome to lose, so its timeout is retryable.
		if expired {
			return nil, &StageTimeoutError{Stage: "kv coordination get"}
		}
		return nil, err
	}
	outcome, err := decodeCoordinationReply(reply)
	if err != nil {
		return nil, err
	}
	if outcome.Kind != wire.KvValue {
		return nil, unexpectedCoordinationOutcome("get", outcome.Kind)
	}
	return outcome.Entry, nil
}

func (client *FencedLeaseClient) prepare(
	code uint32, request any, recovery AmbiguousMutationRecovery,
) (*PreparedMutation, error) {
	payload, err := wire.Marshal(request)
	if err != nil {
		return nil, &CodecError{Reason: "encode request", Cause: err}
	}
	operationID := NewOperationID()
	frame, err := wire.Marshal(wire.ManagedRequestEnvelope{
		V: wire.ManagedRequestVersion, OperationID: operationID, Payload: payload,
	})
	if err != nil {
		return nil, &CodecError{Reason: "encode envelope", Cause: err}
	}
	return &PreparedMutation{
		clientID: client.clientID, code: code, operationID: operationID,
		frame: frame, recovery: recovery,
	}, nil
}

func (client *FencedLeaseClient) renewWithin(
	ctx context.Context, operation *PreparedMutation, timeout time.Duration,
) (*Lease, error) {
	outcome, err := client.execute(ctx, operation, wire.AgdxKvLeaseRenewCode, "renew", timeout)
	if err != nil {
		return nil, err
	}
	return operation.lease(outcome, wire.KvRenewed, "renew")
}

// acquireResolving repeats an ambiguous acquisition under its identity while the requested TTL since the first send is open.
func (client *FencedLeaseClient) acquireResolving(ctx context.Context, operation *PreparedMutation) (*Lease, error) {
	if operation == nil || operation.recovery.Kind != RecoveryWaitForLeaseExpiry {
		return client.Acquire(ctx, operation)
	}
	window := operation.recovery.TTL
	delay := firstRepeatDelay
	for {
		lease, err := client.Acquire(ctx, operation)
		var ambiguous *AmbiguousMutationError
		switch {
		case errors.As(err, &ambiguous):
			remaining := window
			if sent, was := operation.sentAt(); was {
				remaining = window - time.Since(sent)
			}
			if remaining <= 0 || !sleepFor(ctx, min(delay, remaining)) {
				return nil, err
			}
			delay = min(delay*2, maxRepeatDelay)
		case err != nil:
			return nil, err
		case !time.Now().Before(lease.ValidUntil):
			// A grant dead on arrival is never handed out as held.
			return nil, &KvError{Failure: wire.KvError{Kind: wire.KvLeaseLost}}
		default:
			return lease, nil
		}
	}
}

// renewResolving repeats an ambiguous renewal under its identity until deadline, when the held lease stops being valid locally.
func (client *FencedLeaseClient) renewResolving(
	ctx context.Context, operation *PreparedMutation, deadline time.Time,
) (*Lease, error) {
	delay := firstRepeatDelay
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, &KvError{Failure: wire.KvError{Kind: wire.KvLeaseLost}}
		}
		lease, err := client.renewWithin(ctx, operation, min(remaining, client.attemptTimeout))
		var ambiguous *AmbiguousMutationError
		if !errors.As(err, &ambiguous) {
			return lease, err
		}
		if !sleepFor(ctx, min(delay, max(time.Until(deadline), 0))) {
			return nil, err
		}
		delay = min(delay*2, maxRepeatDelay)
	}
}

func (client *FencedLeaseClient) execute(
	ctx context.Context, operation *PreparedMutation, code uint32, what string, timeout time.Duration,
) (*wire.KvOutcome, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := ensureAttemptTimeout(timeout); err != nil {
		return nil, err
	}
	if operation == nil || operation.clientID != client.clientID {
		return nil, &InvalidError{Reason: "prepared mutation belongs to a different fenced-lease client"}
	}
	// A prepared release replayed through Acquire would coerce outcomes across operations.
	if operation.code != code {
		return nil, &InvalidError{Reason: fmt.Sprintf(
			"prepared mutation carries code %d but was executed as %s", operation.code, what)}
	}
	if err := client.readyWithin(ctx, timeout); err != nil {
		return nil, err
	}
	// A failure retires only the connection this call used, never one a later call opened.
	generation := client.generation()
	operation.markSent()
	attempt, cancel := context.WithTimeout(ctx, timeout)
	reply, err := client.sendOn(attempt, generation, operation.code, bytes.Clone(operation.frame))
	expired := ctx.Err() == nil && errors.Is(attempt.Err(), context.DeadlineExceeded)
	cancel()
	if err != nil {
		client.retire(generation)
		switch {
		case ctx.Err() != nil:
			return nil, operation.ambiguous(fmt.Sprintf(
				"%s was left in flight when the caller stopped waiting (%v)", what, ctx.Err()))
		case expired:
			return nil, operation.ambiguous(fmt.Sprintf(
				"%s attempt exceeded %s and requires operation-specific recovery", what, timeout))
		case !mayHaveWritten(err):
			return nil, err
		default:
			return nil, operation.ambiguous(fmt.Sprintf("%s transport failed mid-request: %v", what, err))
		}
	}
	// The reply arrived whole, so the connection stays in step for other requests.
	if unknown, is := unknownOutcome(reply); is {
		return nil, operation.ambiguous(fmt.Sprintf(
			"%s outcome is unknown (%s) and requires operation-specific recovery", what, unknown))
	}
	return decodeCoordinationReply(reply)
}

func (client *FencedLeaseClient) readyWithin(ctx context.Context, timeout time.Duration) error {
	if client.closed.Load() {
		return &TransportError{Reason: "the fenced-lease client is closed", Cause: ierror.ClientShutdown{}}
	}
	ready, checks := client.transport.(ReadyManagedKvTransport)
	if !checks {
		return nil
	}
	generation := client.generation()
	attempt, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	err := ready.Ready(attempt)
	if err == nil {
		return nil
	}
	// A deployment without the capability stays connected, so the next call costs no login.
	var unsupported *UnsupportedError
	if errors.As(err, &unsupported) {
		return err
	}
	client.retire(generation)
	if ctx.Err() == nil && errors.Is(attempt.Err(), context.DeadlineExceeded) {
		return &StageTimeoutError{Stage: "kv coordination connect"}
	}
	return err
}

func (client *FencedLeaseClient) generation() uint64 {
	if generational, is := client.transport.(GenerationalManagedKvTransport); is {
		return generational.Generation()
	}
	return 0
}

func (client *FencedLeaseClient) sendOn(
	ctx context.Context, generation uint64, code uint32, frame []byte,
) ([]byte, error) {
	if generational, is := client.transport.(GenerationalManagedKvTransport); is {
		return generational.SendOn(ctx, generation, code, frame)
	}
	return client.transport.Send(ctx, code, frame)
}

func (client *FencedLeaseClient) retire(generation uint64) {
	if generational, is := client.transport.(GenerationalManagedKvTransport); is {
		generational.ResetGeneration(generation)
		return
	}
	client.transport.Reset()
}

// PreparedMutation is one coordination mutation: its command code and exact framed bytes, minted once and bound to the client that prepared it.
type PreparedMutation struct {
	clientID    wire.U128
	code        uint32
	operationID wire.U128
	frame       []byte
	recovery    AmbiguousMutationRecovery
	mutex       sync.Mutex
	firstSent   time.Time
}

// OperationID identifies this mutation across attempts.
func (operation *PreparedMutation) OperationID() wire.U128 { return operation.operationID }

// AmbiguousRecovery names the action an AmbiguousMutationError from this mutation requires.
func (operation *PreparedMutation) AmbiguousRecovery() AmbiguousMutationRecovery {
	return operation.recovery
}

// markSent records the first send, which a grant or renewal can never precede.
func (operation *PreparedMutation) markSent() time.Time {
	operation.mutex.Lock()
	defer operation.mutex.Unlock()
	if operation.firstSent.IsZero() {
		operation.firstSent = time.Now()
	}
	return operation.firstSent
}

func (operation *PreparedMutation) sentAt() (time.Time, bool) {
	operation.mutex.Lock()
	defer operation.mutex.Unlock()
	return operation.firstSent, !operation.firstSent.IsZero()
}

func (operation *PreparedMutation) lease(
	outcome *wire.KvOutcome, expected wire.KvOutcomeKind, what string,
) (*Lease, error) {
	if outcome.Kind != expected || outcome.Lease == nil {
		return nil, unexpectedCoordinationOutcome(what, outcome.Kind)
	}
	granted := microsDuration(outcome.Lease.GrantedTTLMicros)
	return &Lease{
		Token: outcome.Lease.LeaseToken, GrantedTTL: granted,
		Position: outcome.Lease.Position, ValidUntil: operation.markSent().Add(granted),
	}, nil
}

func (operation *PreparedMutation) ambiguous(reason string) *AmbiguousMutationError {
	return &AmbiguousMutationError{OperationID: operation.operationID, Reason: reason}
}

// AmbiguousRecoveryKind names how a prepared mutation recovers from an unknown outcome.
type AmbiguousRecoveryKind string

const (
	// RecoveryWaitForLeaseExpiry repeats the prepared acquisition, or waits out its TTL before acquiring under a new identity.
	RecoveryWaitForLeaseExpiry AmbiguousRecoveryKind = "wait_for_lease_expiry"
	// RecoveryRepeatPrepared repeats the prepared mutation under its operation identity.
	RecoveryRepeatPrepared AmbiguousRecoveryKind = "repeat_prepared"
	// RecoveryReconcileTargetPrecondition reads the target and reconciles through the fenced precondition.
	RecoveryReconcileTargetPrecondition AmbiguousRecoveryKind = "reconcile_target_precondition"
)

// AmbiguousMutationRecovery is the recovery a prepared mutation requires after an AmbiguousMutationError.
type AmbiguousMutationRecovery struct {
	Kind AmbiguousRecoveryKind
	// TTL is the requested lease lifetime, set for RecoveryWaitForLeaseExpiry.
	TTL time.Duration
}

// ManagedKvTransport sends one framed managed command. Send returns when ctx ends, and Reset stops any request in flight before it returns.
type ManagedKvTransport interface {
	Send(ctx context.Context, code uint32, frame []byte) ([]byte, error)
	Reset()
}

// ReadyManagedKvTransport is a transport that connects and checks the fenced-lease capability before a request enters its ambiguity window.
type ReadyManagedKvTransport interface {
	Ready(ctx context.Context) error
}

// GenerationalManagedKvTransport is a transport whose connection can be replaced, so a failing call retires only the one it used.
type GenerationalManagedKvTransport interface {
	// Generation names the connection SendOn would use now.
	Generation() uint64
	// SendOn sends on the named connection and refuses before sending once it was replaced.
	SendOn(ctx context.Context, generation uint64, code uint32, frame []byte) ([]byte, error)
	// ResetGeneration resets only while the connection is still the named one.
	ResetGeneration(generation uint64)
}

// DedicatedKvTransport is a coordination transport over its own connection, dialed on first use and discarded whole on Reset.
type DedicatedKvTransport struct {
	// dial holds the connection string in its closure, so formatting the transport never prints it.
	dial func(ctx context.Context) (*Laser, error)
	// retirement serializes a dial with a retirement, so a reset waits for the connection it ends.
	retirement sync.Mutex
	mutex      sync.Mutex
	slot       *Laser
	generation uint64
	probedAt   time.Time
	closed     bool
}

// NewDedicatedKvTransport opens a transport that dials connectionString on first use.
func NewDedicatedKvTransport(connectionString string) *DedicatedKvTransport {
	return &DedicatedKvTransport{dial: func(ctx context.Context) (*Laser, error) {
		return Connect(ctx, connectionString)
	}}
}

// Ready dials the connection when none is held and checks the fenced-lease capability.
func (transport *DedicatedKvTransport) Ready(ctx context.Context) error {
	transport.retirement.Lock()
	defer transport.retirement.Unlock()
	transport.mutex.Lock()
	closed, client := transport.closed, transport.slot
	transport.mutex.Unlock()
	if closed {
		return &TransportError{Reason: "the coordination transport is closed", Cause: ierror.ClientShutdown{}}
	}
	reused := client != nil
	if !reused {
		dialed, err := transport.dial(ctx)
		if err != nil {
			return err
		}
		transport.mutex.Lock()
		transport.slot, transport.probedAt = dialed, time.Now()
		transport.mutex.Unlock()
		client = dialed
	}
	capabilities := client.Capabilities()
	if reused && !capabilities.Kv.FencedLeases && transport.reprobeDue() {
		capabilities = client.RefreshCapabilities(ctx)
	}
	if !capabilities.Kv.FencedLeases {
		return &UnsupportedError{Surface: "the fenced-lease contract"}
	}
	return nil
}

// Send sends one frame on the current connection.
func (transport *DedicatedKvTransport) Send(ctx context.Context, code uint32, frame []byte) ([]byte, error) {
	transport.mutex.Lock()
	client := transport.slot
	transport.mutex.Unlock()
	return sendCoordinationFrame(ctx, client, code, frame)
}

// Reset retires the current connection. The next Ready dials a new one.
func (transport *DedicatedKvTransport) Reset() { transport.retire(nil) }

// Generation names the connection the transport holds now.
func (transport *DedicatedKvTransport) Generation() uint64 {
	transport.mutex.Lock()
	defer transport.mutex.Unlock()
	return transport.generation
}

// SendOn sends on the named connection and refuses once it was retired.
func (transport *DedicatedKvTransport) SendOn(
	ctx context.Context, generation uint64, code uint32, frame []byte,
) ([]byte, error) {
	transport.mutex.Lock()
	current, client := transport.generation, transport.slot
	transport.mutex.Unlock()
	// The connection this call readied was retired, so nothing is sent on its replacement.
	if current != generation {
		return nil, &TransportError{
			Reason: "the coordination connection was retired before the send", Cause: ierror.NotConnected{}}
	}
	return sendCoordinationFrame(ctx, client, code, frame)
}

// ResetGeneration retires the named connection and leaves a newer one running.
func (transport *DedicatedKvTransport) ResetGeneration(generation uint64) {
	transport.retire(&generation)
}

// Close retires the connection for good. A later Ready refuses without dialing.
func (transport *DedicatedKvTransport) Close() error {
	transport.mutex.Lock()
	transport.closed = true
	transport.mutex.Unlock()
	return transport.retire(nil)
}

// reprobeDue answers from the last probe until a second has passed, so a refused call costs no round trip.
func (transport *DedicatedKvTransport) reprobeDue() bool {
	transport.mutex.Lock()
	defer transport.mutex.Unlock()
	due := transport.probedAt.IsZero() || time.Since(transport.probedAt) >= capabilityReprobeInterval
	if due {
		transport.probedAt = time.Now()
	}
	return due
}

func (transport *DedicatedKvTransport) retire(generation *uint64) error {
	transport.retirement.Lock()
	defer transport.retirement.Unlock()
	transport.mutex.Lock()
	if generation != nil && *generation != transport.generation {
		transport.mutex.Unlock()
		return nil
	}
	transport.generation++
	retired := transport.slot
	transport.slot = nil
	transport.mutex.Unlock()
	if retired == nil {
		return nil
	}
	return retired.Close()
}

// writeMarkingRequester records that an attempt wrote the frame, so a later attempt's failure cannot hide an unknown outcome.
type writeMarkingRequester struct {
	ClusterRequester
	written atomic.Bool
}

func (requester *writeMarkingRequester) SendBinaryRequest(
	ctx context.Context, code uint32, payload []byte,
) ([]byte, error) {
	reply, err := requester.ClusterRequester.SendBinaryRequest(ctx, code, payload)
	requester.mark(err)
	return reply, err
}

func (requester *writeMarkingRequester) SendBinaryRequestToReplica(
	ctx context.Context, code uint32, payload []byte, replicaID uint8,
) ([]byte, error) {
	reply, err := requester.ClusterRequester.SendBinaryRequestToReplica(ctx, code, payload, replicaID)
	requester.mark(err)
	return reply, err
}

func (requester *writeMarkingRequester) mark(err error) {
	if err == nil || driverWrote(err) {
		requester.written.Store(true)
	}
}

// sendCoordinationFrame sends one preframed command to the caller's node and opens the positioned reply.
func sendCoordinationFrame(ctx context.Context, client *Laser, code uint32, frame []byte) ([]byte, error) {
	if client == nil {
		return nil, &InvalidError{Reason: "coordination transport is not ready"}
	}
	shared := client.state()
	shared.mutex.RLock()
	connected := shared.client
	shared.mutex.RUnlock()
	if connected == nil {
		return nil, &TransportError{Reason: "the client is closed", Cause: ierror.NotConnected{}}
	}
	marking := &writeMarkingRequester{ClusterRequester: connected}
	routed, err := client.nodes().send(ctx, marking, code, frame, callerTarget(), walkingOnce())
	client.reportLink(err)
	if err != nil {
		return nil, &TransportError{Reason: "managed command", Cause: writtenOutcome(err, marking.written.Load())}
	}
	if routed.frontier != nil {
		shared.mutex.Lock()
		wire.MergeFrontier(&shared.frontier, *routed.frontier)
		shared.mutex.Unlock()
	}
	return routed.reply, nil
}

// writtenOutcome keeps the unknown outcome of a frame an earlier attempt wrote, whatever a later attempt answered.
func writtenOutcome(err error, written bool) error {
	if written && !driverWrote(err) {
		return ierror.TransientNotCommitted{}
	}
	return err
}

// mayHaveWritten reports a failed send whose frame may have reached the deployment.
func mayHaveWritten(err error) bool {
	var ambiguous *AmbiguousMutationError
	switch {
	case errors.As(err, &ambiguous), driverWrote(err):
		return true
	case driverNeverWrote(err):
		return false
	default:
		return transientTransport(err) || timedOut(err) || errors.Is(err, context.DeadlineExceeded)
	}
}

// driverWrote reports a call the driver wrote and got no reply for.
func driverWrote(err error) bool {
	return errors.Is(err, ierror.TransientNotCommitted{}) ||
		errors.Is(err, ierror.OperationUncertain{}) ||
		errors.Is(err, ierror.RequestAlreadyApplied{})
}

// driverNeverWrote reports a call the driver refused before its frame reached a connection.
func driverNeverWrote(err error) bool {
	return errors.Is(err, ierror.NotConnected{}) ||
		errors.Is(err, ierror.ClientShutdown{}) ||
		errors.Is(err, ierror.TransientNotAccepted{})
}

func decodeCoordinationReply(reply []byte) (*wire.KvOutcome, error) {
	var decoded wire.KvReply
	if err := decodeManagedReply(reply, &decoded, "decode key-value reply"); err != nil {
		return nil, err
	}
	if decoded.Err != nil {
		return nil, &KvError{Failure: *decoded.Err}
	}
	if decoded.Outcome == nil {
		return nil, &ProtocolError{Reason: "kv coordination: unknown reply variant"}
	}
	return decoded.Outcome, nil
}

func unexpectedCoordinationOutcome(what string, kind wire.KvOutcomeKind) error {
	return &ProtocolError{Reason: "kv coordination " + what + ": unexpected outcome " + kvOutcomeName(kind)}
}

func ensureAttemptTimeout(timeout time.Duration) error {
	if timeout <= 0 {
		return &InvalidError{Reason: "coordination attempt timeout must be greater than zero"}
	}
	return nil
}

func microsDuration(micros uint64) time.Duration {
	return time.Duration(min(micros, math.MaxInt64/1000)) * time.Microsecond
}
