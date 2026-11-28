package laser_test

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	ierror "github.com/apache/iggy/foreign/go/errors"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenAnUnknownOutcomeReplyWhenExecutingThenShouldKeepTheTransportAndReportAmbiguityWithTheIdentity(t *testing.T) {
	for _, code := range []wire.ResultCode{
		wire.ResultPending, wire.ResultBackendUnavailable, wire.ResultUnavailable,
	} {
		transport := answering(unknownOutcomeReply(t, code))
		client := laser.NewFencedLeaseClient(transport)
		operation := prepared(t)(client.PrepareAcquire(leaseRequest()))

		_, err := client.Acquire(context.Background(), operation)

		var ambiguous *laser.AmbiguousMutationError
		if !errors.As(err, &ambiguous) {
			t.Fatalf("code %d must be ambiguous, got %T: %v", code, err, err)
		}
		if ambiguous.OperationID != operation.OperationID() {
			t.Errorf("want the prepared identity %v, got %v", operation.OperationID(), ambiguous.OperationID)
		}
		if resets := transport.resetCount(); resets != 0 {
			t.Errorf("a whole reply leaves the connection in step, got %d resets", resets)
		}
		want := laser.AmbiguousMutationRecovery{Kind: laser.RecoveryWaitForLeaseExpiry, TTL: 30 * time.Second}
		if got := operation.AmbiguousRecovery(); got != want {
			t.Errorf("want %+v, got %+v", want, got)
		}
	}
}

func TestGivenAPreparedRenewalWhenRetriedThenShouldReuseTheExactFrameAndID(t *testing.T) {
	transport := answering(
		scripted{err: &laser.StageTimeoutError{Stage: "boom"}},
		okReply(t, renewedOutcome(7, 30*time.Second)),
	)
	client := laser.NewFencedLeaseClient(transport)
	operation := prepared(t)(client.PrepareRenew(renewRequest()))
	if got := operation.AmbiguousRecovery(); got.Kind != laser.RecoveryRepeatPrepared {
		t.Fatalf("want repeat prepared, got %+v", got)
	}

	_, first := client.Renew(context.Background(), operation)
	lease, second := client.Renew(context.Background(), operation)

	var ambiguous *laser.AmbiguousMutationError
	if !errors.As(first, &ambiguous) {
		t.Fatalf("a transport failure is ambiguous, got %T: %v", first, first)
	}
	if second != nil {
		t.Fatalf("retry: %v", second)
	}
	if lease.Token != 7 || lease.Position != leasePosition() {
		t.Errorf("want token 7 at %+v, got %+v", leasePosition(), lease)
	}
	sent := transport.frames()
	if len(sent) != 2 || sent[0].code != sent[1].code || !bytes.Equal(sent[0].frame, sent[1].frame) {
		t.Fatalf("the retry must reuse the exact frame, got %+v", sent)
	}
	var envelope wire.ManagedRequestEnvelope
	if err := wire.Unmarshal(sent[0].frame, &envelope); err != nil {
		t.Fatalf("the frame is a managed envelope: %v", err)
	}
	if envelope.OperationID != operation.OperationID() || envelope.OperationID.IsZero() {
		t.Errorf("want the prepared identity, got %v", envelope.OperationID)
	}
	var inner wire.KvLeaseRenew
	if err := wire.Unmarshal(envelope.Payload, &inner); err != nil {
		t.Fatalf("the payload is the renewal: %v", err)
	}
	if inner.HolderID != "node-1" {
		t.Errorf("want holder node-1, got %q", inner.HolderID)
	}
	if resets := transport.resetCount(); resets != 1 {
		t.Errorf("want one reset after the failure, got %d", resets)
	}
}

func TestGivenTwoPreparedMutationsWhenBuiltThenShouldMintDistinctOperationIDs(t *testing.T) {
	client := laser.NewFencedLeaseClient(answering())

	first := prepared(t)(client.PrepareAcquire(leaseRequest()))
	second := prepared(t)(client.PrepareAcquire(leaseRequest()))

	if first.OperationID() == second.OperationID() {
		t.Errorf("two preparations share the identity %v", first.OperationID())
	}
}

func TestGivenAHungTransportWhenExecutingThenShouldResetAndReportAmbiguous(t *testing.T) {
	transport := &scriptedTransport{hang: true}
	client := laser.NewFencedLeaseClient(transport).WithAttemptTimeout(100 * time.Millisecond)
	operation := prepared(t)(client.PrepareAcquire(leaseRequest()))

	_, err := client.Acquire(context.Background(), operation)

	var ambiguous *laser.AmbiguousMutationError
	if !errors.As(err, &ambiguous) {
		t.Fatalf("want an ambiguous mutation, got %T: %v", err, err)
	}
	if resets := transport.resetCount(); resets != 1 {
		t.Errorf("want one reset, got %d", resets)
	}
}

func TestGivenSlowReadinessWhenSendFitsTheBoundThenShouldNotReportAmbiguous(t *testing.T) {
	transport := &slowTransport{
		readyDelay: 125 * time.Millisecond, sendDelay: 125 * time.Millisecond,
		reply: okReply(t, leasedOutcome(9, 30*time.Second)).reply,
	}
	client := laser.NewFencedLeaseClient(transport).WithAttemptTimeout(200 * time.Millisecond)
	operation := prepared(t)(client.PrepareAcquire(leaseRequest()))

	lease, err := client.Acquire(context.Background(), operation)

	if err != nil {
		t.Fatalf("acquire after readiness: %v", err)
	}
	if lease.Token != 9 {
		t.Errorf("want token 9, got %d", lease.Token)
	}
}

func TestGivenAGrantAnsweredLateWhenAcquiredThenShouldBeValidFromTheFirstSend(t *testing.T) {
	transport := &slowTransport{
		sendDelay: 200 * time.Millisecond,
		reply:     okReply(t, leasedOutcome(9, 30*time.Second)).reply,
	}
	client := laser.NewFencedLeaseClient(transport)
	operation := prepared(t)(client.PrepareAcquire(leaseRequest()))
	before := time.Now()

	lease, err := client.Acquire(context.Background(), operation)

	after := time.Now()
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if lease.ValidUntil.Before(before.Add(lease.GrantedTTL)) {
		t.Errorf("validity %v starts before the first send %v", lease.ValidUntil, before)
	}
	if lease.ValidUntil.Add(200 * time.Millisecond).After(after.Add(lease.GrantedTTL)) {
		t.Errorf("the reply delay must be spent from the grant, valid until %v after %v", lease.ValidUntil, after)
	}
}

func TestGivenATimedOutMutationWhenResetIsSlowThenShouldWaitForRetirement(t *testing.T) {
	transport := &scriptedTransport{hang: true, resetDelay: 25 * time.Millisecond}
	client := laser.NewFencedLeaseClient(transport).WithAttemptTimeout(time.Millisecond)
	operation := prepared(t)(client.PrepareAcquire(leaseRequest()))
	started := time.Now()

	_, err := client.Acquire(context.Background(), operation)

	var ambiguous *laser.AmbiguousMutationError
	if !errors.As(err, &ambiguous) {
		t.Fatalf("want an ambiguous mutation, got %T: %v", err, err)
	}
	if transport.resetCount() != 1 || time.Since(started) < 25*time.Millisecond {
		t.Errorf("the call must return only after retirement, took %v", time.Since(started))
	}
}

func TestGivenASendWhoseConnectionWasReplacedWhenItFailsThenShouldRetireOnlyTheConnectionItUsed(t *testing.T) {
	transport := &replacedConnectionTransport{generation: 1}
	client := laser.NewFencedLeaseClient(transport)
	operation := prepared(t)(client.PrepareRenew(renewRequest()))

	_, err := client.Renew(context.Background(), operation)

	var ambiguous *laser.AmbiguousMutationError
	if !errors.As(err, &ambiguous) {
		t.Fatalf("want an ambiguous mutation, got %T: %v", err, err)
	}
	if len(transport.sentOn) != 1 || transport.sentOn[0] != 1 {
		t.Errorf("want one send on generation 1, got %v", transport.sentOn)
	}
	if len(transport.retired) != 1 || transport.retired[0] != 1 {
		t.Errorf("want generation 1 retired, got %v", transport.retired)
	}
	if transport.resets != 0 {
		t.Errorf("a plain reset would retire the replacement connection, got %d", transport.resets)
	}
}

func TestGivenAPermanentSetupFailureWhenExecutingThenShouldPreserveTheErrorAndKeepTheConnection(t *testing.T) {
	transport := &unreadyTransport{}
	client := laser.NewFencedLeaseClient(transport)

	for range 3 {
		operation := prepared(t)(client.PrepareAcquire(leaseRequest()))
		_, err := client.Acquire(context.Background(), operation)
		var unsupported *laser.UnsupportedError
		if !errors.As(err, &unsupported) {
			t.Fatalf("want an UnsupportedError, got %T: %v", err, err)
		}
	}

	if transport.resets != 0 || transport.sends != 0 {
		t.Errorf("want no reset and no send, got %d resets and %d sends", transport.resets, transport.sends)
	}
}

func TestGivenAPermanentSendFailureWhenExecutingThenShouldPreserveTheError(t *testing.T) {
	transport := answering(scripted{err: &laser.UnsupportedError{Surface: "the fenced-lease contract"}})
	client := laser.NewFencedLeaseClient(transport)
	operation := prepared(t)(client.PrepareAcquire(leaseRequest()))

	_, err := client.Acquire(context.Background(), operation)

	var unsupported *laser.UnsupportedError
	if !errors.As(err, &unsupported) {
		t.Fatalf("want an UnsupportedError, got %T: %v", err, err)
	}
	if resets := transport.resetCount(); resets != 1 {
		t.Errorf("want one reset, got %d", resets)
	}
}

func TestGivenAnAuthorizationSendFailureWhenExecutingThenShouldPreserveTheError(t *testing.T) {
	transport := answering(scripted{err: &laser.TransportError{Reason: "managed command", Cause: ierror.Unauthorized{}}})
	client := laser.NewFencedLeaseClient(transport)
	operation := prepared(t)(client.PrepareAcquire(leaseRequest()))

	_, err := client.Acquire(context.Background(), operation)

	var ambiguous *laser.AmbiguousMutationError
	if errors.As(err, &ambiguous) || !errors.Is(err, ierror.Unauthorized{}) {
		t.Fatalf("want the authorization failure unchanged, got %T: %v", err, err)
	}
	if resets := transport.resetCount(); resets != 1 {
		t.Errorf("want one reset, got %d", resets)
	}
}

func TestGivenADriverThatWroteTheFrameAndLostItsReplyWhenExecutedThenShouldReportAmbiguityWithTheIdentity(t *testing.T) {
	transport := answering(scripted{err: &laser.TransportError{Reason: "managed command", Cause: ierror.OperationUncertain{OperationID: 9}}})
	client := laser.NewFencedLeaseClient(transport)
	operation := prepared(t)(client.PrepareRelease(releaseRequest()))

	_, err := client.Release(context.Background(), operation)

	var ambiguous *laser.AmbiguousMutationError
	if !errors.As(err, &ambiguous) {
		t.Fatalf("a written frame without a reply is ambiguous, got %T: %v", err, err)
	}
	if ambiguous.OperationID != operation.OperationID() {
		t.Errorf("want the prepared identity, got %v", ambiguous.OperationID)
	}
	if resets := transport.resetCount(); resets != 1 {
		t.Errorf("want one reset, got %d", resets)
	}
}

func TestGivenADriverThatStoppedBeforeWritingTheFrameWhenExecutedThenShouldReportADefiniteFailure(t *testing.T) {
	transport := answering(scripted{err: &laser.TransportError{Reason: "managed command", Cause: ierror.NotConnected{}}})
	client := laser.NewFencedLeaseClient(transport)
	operation := prepared(t)(client.PrepareRelease(releaseRequest()))

	_, err := client.Release(context.Background(), operation)

	var ambiguous *laser.AmbiguousMutationError
	if errors.As(err, &ambiguous) || !errors.Is(err, ierror.NotConnected{}) {
		t.Fatalf("a frame never written cannot have applied, got %T: %v", err, err)
	}
	if resets := transport.resetCount(); resets != 1 {
		t.Errorf("want one reset, got %d", resets)
	}
}

func TestGivenAZeroAttemptTimeoutWhenExecutingThenShouldRejectWithoutSending(t *testing.T) {
	transport := answering()
	client := laser.NewFencedLeaseClient(transport).WithAttemptTimeout(0)
	operation := prepared(t)(client.PrepareAcquire(leaseRequest()))

	_, err := client.Acquire(context.Background(), operation)

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want an InvalidError, got %T: %v", err, err)
	}
	if sent := transport.frames(); len(sent) != 0 {
		t.Errorf("nothing may be sent, got %d frames", len(sent))
	}
}

func TestGivenAPreparedMutationFromAnotherClientWhenExecutedThenShouldReject(t *testing.T) {
	first := laser.NewFencedLeaseClient(answering())
	transport := answering()
	second := laser.NewFencedLeaseClient(transport)
	operation := prepared(t)(first.PrepareAcquire(leaseRequest()))

	_, err := second.Acquire(context.Background(), operation)

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want an InvalidError, got %T: %v", err, err)
	}
	if sent := transport.frames(); len(sent) != 0 {
		t.Errorf("nothing may be sent, got %d frames", len(sent))
	}
}

func TestGivenAMismatchedPreparedMutationWhenExecutedThenShouldReject(t *testing.T) {
	transport := answering()
	client := laser.NewFencedLeaseClient(transport)
	operation := prepared(t)(client.PrepareAcquire(leaseRequest()))

	_, err := client.Release(context.Background(), operation)

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want an InvalidError, got %T: %v", err, err)
	}
	if sent := transport.frames(); len(sent) != 0 {
		t.Errorf("nothing may be sent, got %d frames", len(sent))
	}
}

func TestGivenAnUnexpectedOutcomeWhenExecutingThenShouldFailClosed(t *testing.T) {
	transport := answering(okReply(t, leasedOutcome(7, 30*time.Second)))
	client := laser.NewFencedLeaseClient(transport)
	operation := prepared(t)(client.PrepareRelease(releaseRequest()))

	released, err := client.Release(context.Background(), operation)

	var protocol *laser.ProtocolError
	if released || !errors.As(err, &protocol) {
		t.Fatalf("a leased answer to a release is never success, got %v and %T: %v", released, err, err)
	}
}

func TestGivenARenewalWhenExecutedThenShouldReturnTheSameTokenAndTheGrantedTTL(t *testing.T) {
	transport := answering(okReply(t, renewedOutcome(7, 25*time.Second)))
	client := laser.NewFencedLeaseClient(transport)
	operation := prepared(t)(client.PrepareRenew(renewRequest()))

	lease, err := client.Renew(context.Background(), operation)

	if err != nil {
		t.Fatalf("renew: %v", err)
	}
	if lease.Token != 7 || lease.GrantedTTL != 25*time.Second {
		t.Errorf("want token 7 for 25s, got %+v", lease)
	}
}

func TestGivenAReleaseWhenExecutedThenShouldReportWhetherTheLeaseWasHeld(t *testing.T) {
	transport := answering(okReply(t, wire.KvOutcome{Kind: wire.KvReleased, Released: true}))
	client := laser.NewFencedLeaseClient(transport)
	operation := prepared(t)(client.PrepareRelease(releaseRequest()))

	released, err := client.Release(context.Background(), operation)

	if err != nil || !released {
		t.Fatalf("want a held lease released, got %v and %v", released, err)
	}
	if sent := transport.frames(); len(sent) != 1 || sent[0].code != wire.AgdxKvReleaseCode {
		t.Errorf("want one release frame, got %+v", sent)
	}
}

func TestGivenAFencedWriteWhenExecutedThenShouldReturnTheNewVersion(t *testing.T) {
	transport := answering(okReply(t, wire.KvOutcome{Kind: wire.KvCommitted, Version: 5}))
	client := laser.NewFencedLeaseClient(transport)
	operation := prepared(t)(client.PrepareCasFenced(casFencedRequest()))
	if got := operation.AmbiguousRecovery(); got.Kind != laser.RecoveryReconcileTargetPrecondition {
		t.Fatalf("want reconcile through the precondition, got %+v", got)
	}

	version, err := client.CasFenced(context.Background(), operation)

	if err != nil || version != 5 {
		t.Fatalf("want version 5, got %d and %v", version, err)
	}
}

func TestGivenALeaseLostReplyWhenExecutingThenShouldSurfaceTheTypedError(t *testing.T) {
	transport := answering(errReply(t, wire.KvError{Kind: wire.KvLeaseLost}))
	client := laser.NewFencedLeaseClient(transport)
	operation := prepared(t)(client.PrepareCasFenced(casFencedRequest()))

	_, err := client.CasFenced(context.Background(), operation)

	var refused *laser.KvError
	if !errors.As(err, &refused) || refused.Kind() != wire.KvLeaseLost {
		t.Fatalf("want lease lost, got %T: %v", err, err)
	}
	if resets := transport.resetCount(); resets != 0 {
		t.Errorf("a typed reply is not ambiguous, got %d resets", resets)
	}
}

func TestGivenAnInvalidRequestWhenPreparingThenShouldRejectBeforeAnySend(t *testing.T) {
	transport := answering()
	client := laser.NewFencedLeaseClient(transport)
	staleVersion := leaseRequest()
	staleVersion.V = wire.KvLeaseOpVersion + 1
	noHolder := leaseRequest()
	noHolder.HolderID = ""

	for _, request := range []wire.KvLease{staleVersion, noHolder} {
		var invalid *laser.InvalidError
		if _, err := client.PrepareAcquire(request); !errors.As(err, &invalid) {
			t.Errorf("want an InvalidError for %+v, got %T: %v", request, err, err)
		}
	}
	if sent := transport.frames(); len(sent) != 0 {
		t.Errorf("nothing may be sent, got %d frames", len(sent))
	}
}

func TestGivenABarrieredGetWhenAnsweredStaleThenShouldSurfaceStaleNotAbsent(t *testing.T) {
	position := leasePosition()
	transport := answering(errReply(t, wire.KvError{Kind: wire.KvStale, Required: position}))
	client := laser.NewFencedLeaseClient(transport)
	request := wire.KvGet{
		V: wire.KvOpVersion, Namespace: "connectors.state", Key: []byte("source_state/pg"),
		MinPosition: &position,
	}

	entry, err := client.Get(context.Background(), request)

	var refused *laser.KvError
	if entry != nil || !errors.As(err, &refused) || refused.Kind() != wire.KvStale {
		t.Fatalf("want stale, got %+v and %T: %v", entry, err, err)
	}
	sent := transport.frames()
	if len(sent) != 1 || sent[0].code != wire.AgdxKvGetCode {
		t.Fatalf("want one get frame, got %+v", sent)
	}
	var direct wire.KvGet
	if err := wire.Unmarshal(sent[0].frame, &direct); err != nil {
		t.Fatalf("the read rides unframed: %v", err)
	}
	if direct.MinPosition == nil || *direct.MinPosition != position {
		t.Errorf("want the barrier %+v, got %+v", position, direct.MinPosition)
	}
}

func TestGivenAStoredValueWhenReadThenShouldReturnTheEntry(t *testing.T) {
	stored := wire.KvEntry{Key: []byte("source_state/pg"), Value: []byte("cursor"), Version: 4}
	transport := answering(okReply(t, wire.KvOutcome{Kind: wire.KvValue, Entry: &stored}))
	client := laser.NewFencedLeaseClient(transport)

	entry, err := client.Get(context.Background(), wire.KvGet{
		V: wire.KvOpVersion, Namespace: "connectors.state", Key: stored.Key,
	})

	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if entry == nil || !bytes.Equal(entry.Value, stored.Value) || entry.Version != stored.Version {
		t.Errorf("want %+v, got %+v", stored, entry)
	}
}

func TestGivenAHungReadWhenTheAttemptTimesOutThenShouldReportATimeoutNotAnAmbiguity(t *testing.T) {
	transport := &scriptedTransport{hang: true}
	client := laser.NewFencedLeaseClient(transport).WithAttemptTimeout(50 * time.Millisecond)

	_, err := client.Get(context.Background(), wire.KvGet{
		V: wire.KvOpVersion, Namespace: "connectors.state", Key: []byte("k"),
	})

	var timeout *laser.StageTimeoutError
	if !errors.As(err, &timeout) {
		t.Fatalf("a read has no outcome to lose, want a timeout, got %T: %v", err, err)
	}
	if resets := transport.resetCount(); resets != 1 {
		t.Errorf("want one reset, got %d", resets)
	}
}

func TestGivenAClosedClientWhenExecutingThenShouldRefuseWithoutSending(t *testing.T) {
	transport := answering()
	client := laser.NewFencedLeaseClient(transport)
	operation := prepared(t)(client.PrepareAcquire(leaseRequest()))
	if err := client.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("a second close: %v", err)
	}

	_, err := client.Acquire(context.Background(), operation)

	if !errors.Is(err, ierror.ClientShutdown{}) {
		t.Fatalf("want a shutdown refusal, got %T: %v", err, err)
	}
	if sent, resets := transport.frames(), transport.resetCount(); len(sent) != 0 || resets != 1 {
		t.Errorf("want nothing sent and one retirement, got %d frames and %d resets", len(sent), resets)
	}
}

func TestGivenACallerThatStopsWaitingWhenTheFrameIsOutThenShouldReportAmbiguityAndRetireTheConnection(t *testing.T) {
	transport := &scriptedTransport{hang: true, entered: make(chan struct{})}
	client := laser.NewFencedLeaseClient(transport)
	operation := prepared(t)(client.PrepareRelease(releaseRequest()))
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-transport.entered
		cancel()
	}()

	_, err := client.Release(ctx, operation)

	var ambiguous *laser.AmbiguousMutationError
	if !errors.As(err, &ambiguous) || ambiguous.OperationID != operation.OperationID() {
		t.Fatalf("a frame left in flight has an unknown outcome, got %T: %v", err, err)
	}
	if resets := transport.resetCount(); resets != 1 {
		t.Errorf("want one reset, got %d", resets)
	}
}

type scripted struct {
	reply []byte
	err   error
}

type sentFrame struct {
	code  uint32
	frame []byte
}

// scriptedTransport answers each send with the next scripted reply.
type scriptedTransport struct {
	mutex      sync.Mutex
	replies    []scripted
	sent       []sentFrame
	resets     int
	hang       bool
	resetDelay time.Duration
	entered    chan struct{}
}

func answering(replies ...scripted) *scriptedTransport {
	return &scriptedTransport{replies: replies}
}

func (transport *scriptedTransport) Send(ctx context.Context, code uint32, frame []byte) ([]byte, error) {
	transport.mutex.Lock()
	transport.sent = append(transport.sent, sentFrame{code: code, frame: bytes.Clone(frame)})
	if transport.hang {
		entered := transport.entered
		transport.mutex.Unlock()
		if entered != nil {
			close(entered)
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	defer transport.mutex.Unlock()
	next := transport.replies[0]
	transport.replies = transport.replies[1:]
	return next.reply, next.err
}

func (transport *scriptedTransport) Reset() {
	if transport.resetDelay > 0 {
		<-time.After(transport.resetDelay)
	}
	transport.mutex.Lock()
	defer transport.mutex.Unlock()
	transport.resets++
}

func (transport *scriptedTransport) frames() []sentFrame {
	transport.mutex.Lock()
	defer transport.mutex.Unlock()
	return append([]sentFrame(nil), transport.sent...)
}

func (transport *scriptedTransport) resetCount() int {
	transport.mutex.Lock()
	defer transport.mutex.Unlock()
	return transport.resets
}

// slowTransport delays readiness and the reply.
type slowTransport struct {
	readyDelay time.Duration
	sendDelay  time.Duration
	reply      []byte
}

func (transport *slowTransport) Ready(ctx context.Context) error {
	return waitOut(ctx, transport.readyDelay)
}

func (transport *slowTransport) Send(ctx context.Context, _ uint32, _ []byte) ([]byte, error) {
	if err := waitOut(ctx, transport.sendDelay); err != nil {
		return nil, err
	}
	return transport.reply, nil
}

func (transport *slowTransport) Reset() {}

// unreadyTransport is a deployment without the fenced-lease contract.
type unreadyTransport struct {
	resets int
	sends  int
}

func (transport *unreadyTransport) Ready(context.Context) error {
	return &laser.UnsupportedError{Surface: "the fenced-lease contract"}
}

func (transport *unreadyTransport) Send(context.Context, uint32, []byte) ([]byte, error) {
	transport.sends++
	return nil, errors.New("a permanent setup failure must stop before send")
}

func (transport *unreadyTransport) Reset() { transport.resets++ }

// replacedConnectionTransport has its connection replaced while a send is out.
type replacedConnectionTransport struct {
	generation uint64
	sentOn     []uint64
	retired    []uint64
	resets     int
}

func (transport *replacedConnectionTransport) Send(context.Context, uint32, []byte) ([]byte, error) {
	return nil, &laser.InvalidError{Reason: "a send must name its connection"}
}

func (transport *replacedConnectionTransport) Reset() { transport.resets++ }

func (transport *replacedConnectionTransport) Generation() uint64 { return transport.generation }

func (transport *replacedConnectionTransport) SendOn(_ context.Context, generation uint64, _ uint32, _ []byte) ([]byte, error) {
	transport.sentOn = append(transport.sentOn, generation)
	transport.generation++
	return nil, &laser.TransportError{Reason: "managed command", Cause: ierror.Disconnected{}}
}

func (transport *replacedConnectionTransport) ResetGeneration(generation uint64) {
	transport.retired = append(transport.retired, generation)
}

func waitOut(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func prepared(t *testing.T) func(*laser.PreparedMutation, error) *laser.PreparedMutation {
	return func(operation *laser.PreparedMutation, err error) *laser.PreparedMutation {
		t.Helper()
		if err != nil {
			t.Fatalf("prepare: %v", err)
		}
		return operation
	}
}

func okReply(t *testing.T, outcome wire.KvOutcome) scripted {
	t.Helper()
	encoded, err := wire.Marshal(wire.OkReply(outcome))
	if err != nil {
		t.Fatalf("encode the reply: %v", err)
	}
	return scripted{reply: encoded}
}

func errReply(t *testing.T, failure wire.KvError) scripted {
	t.Helper()
	encoded, err := wire.Marshal(wire.ErrReply(failure))
	if err != nil {
		t.Fatalf("encode the reply: %v", err)
	}
	return scripted{reply: encoded}
}

func unknownOutcomeReply(t *testing.T, code wire.ResultCode) scripted {
	t.Helper()
	encoded, err := wire.Marshal(wire.NewCommandError(code, "the outcome is not known yet"))
	if err != nil {
		t.Fatalf("encode the reply: %v", err)
	}
	return scripted{reply: encoded}
}

func leasedOutcome(token uint64, ttl time.Duration) wire.KvOutcome {
	return wire.KvOutcome{Kind: wire.KvLeased, Lease: &wire.KvLeaseGrant{
		LeaseToken: token, GrantedTTLMicros: uint64(ttl.Microseconds()), Position: leasePosition(),
	}}
}

func renewedOutcome(token uint64, ttl time.Duration) wire.KvOutcome {
	return wire.KvOutcome{Kind: wire.KvRenewed, Lease: &wire.KvLeaseGrant{
		LeaseToken: token, GrantedTTLMicros: uint64(ttl.Microseconds()), Position: leasePosition(),
	}}
}

func leasePosition() wire.MutationPosition {
	return wire.MutationPosition{TopicGeneration: 1, Partition: 0, Offset: 512}
}

func leaseRequest() wire.KvLease {
	subject := uint32(42)
	return wire.KvLease{
		V: wire.KvLeaseOpVersion, Namespace: "connectors.coordination", Key: []byte("source-owner"),
		LeaseTTLMicros: 30_000_000, HolderID: "node-1", SubjectUserID: &subject,
	}
}

func renewRequest() wire.KvLeaseRenew {
	subject := uint32(42)
	return wire.KvLeaseRenew{
		V: wire.KvLeaseOpVersion, Namespace: "connectors.coordination", Key: []byte("source-owner"),
		HolderID: "node-1", SubjectUserID: &subject, LeaseToken: 7, LeaseTTLMicros: 30_000_000,
	}
}

func releaseRequest() wire.KvRelease {
	return wire.KvRelease{
		V: wire.KvLeaseOpVersion, Namespace: "connectors.coordination", Key: []byte("source-owner"),
		LeaseToken: 7, HolderID: "node-1",
	}
}

func casFencedRequest() wire.KvCasFenced {
	return wire.KvCasFenced{
		V: wire.KvLeaseOpVersion, Namespace: "connectors.state", Key: []byte("source_state/pg"),
		Value: []byte("cursor"), Expect: wire.MatchVersion(4),
		FenceNamespace: "connectors.coordination", FenceKey: []byte("source-owner"), FenceToken: 7,
	}
}
