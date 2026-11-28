package laser

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenADefaultClientWhenBuiltThenShouldAllowAttemptsBeyondTheServerFoldWait(t *testing.T) {
	client := NewFencedLeaseClient(&repeatingTransport{})

	if client.attemptTimeout != 15*time.Second {
		t.Errorf("want 15s, got %v", client.attemptTimeout)
	}
}

func TestGivenAnAmbiguousAcquisitionWhenTheRepeatIsAnsweredThenShouldReturnTheGrantUnderTheSameIdentity(t *testing.T) {
	transport := &repeatingTransport{replies: [][]byte{
		unknownCoordinationReply(t, wire.ResultPending), grantReply(t, wire.KvLeased, 4, 30*time.Second),
	}}
	client := NewFencedLeaseClient(transport)
	operation := acquisition(t, client, 30*time.Second)

	lease, err := client.acquireResolving(context.Background(), operation)

	if err != nil {
		t.Fatalf("the repeat answers the recorded grant: %v", err)
	}
	if lease.Token != 4 {
		t.Errorf("want token 4, got %d", lease.Token)
	}
	sent := transport.frames()
	if len(sent) != 2 || !bytes.Equal(sent[0], sent[1]) {
		t.Fatalf("want one repeat under the same identity, got %d frames", len(sent))
	}
	if transport.resets != 0 {
		t.Errorf("want no reset, got %d", transport.resets)
	}
}

func TestGivenAnAcquisitionThatStaysUnknownWhenItsTTLWindowPassesThenShouldReportAmbiguityWithoutAFurtherWait(t *testing.T) {
	transport := &repeatingTransport{always: unknownCoordinationReply(t, wire.ResultPending)}
	client := NewFencedLeaseClient(transport)
	operation := acquisition(t, client, time.Second)
	started := time.Now()

	_, err := client.acquireResolving(context.Background(), operation)

	var ambiguous *AmbiguousMutationError
	if !errors.As(err, &ambiguous) {
		t.Fatalf("an outcome still unknown at the window's end stays ambiguous, got %T: %v", err, err)
	}
	if elapsed := time.Since(started); elapsed < time.Second || elapsed >= 2*time.Second {
		t.Errorf("repeats fill the window and no TTL sleep follows, took %v", elapsed)
	}
	if sent := transport.frames(); len(sent) <= 2 {
		t.Errorf("want more than two attempts, got %d", len(sent))
	}
}

func TestGivenAGrantThatArrivesAfterItsValidityWhenResolvedThenShouldReportTheLeaseLost(t *testing.T) {
	transport := &repeatingTransport{
		delay: 1100 * time.Millisecond, always: grantReply(t, wire.KvLeased, 9, time.Second),
	}
	client := NewFencedLeaseClient(transport)
	operation := acquisition(t, client, time.Second)

	_, err := client.acquireResolving(context.Background(), operation)

	var refused *KvError
	if !errors.As(err, &refused) || refused.Kind() != wire.KvLeaseLost {
		t.Fatalf("a grant dead on arrival is never handed out as held, got %T: %v", err, err)
	}
}

func TestGivenAnAmbiguousRenewalWhenResolvedThenShouldRepeatTheSameIdentityUntilAnswered(t *testing.T) {
	transport := &repeatingTransport{replies: [][]byte{
		unknownCoordinationReply(t, wire.ResultUnavailable), grantReply(t, wire.KvRenewed, 7, 30*time.Second),
	}}
	client := NewFencedLeaseClient(transport)
	operation := renewal(t, client)

	lease, err := client.renewResolving(context.Background(), operation, time.Now().Add(5*time.Second))

	if err != nil {
		t.Fatalf("the repeat answers the renewal: %v", err)
	}
	if lease.Token != 7 {
		t.Errorf("want token 7, got %d", lease.Token)
	}
	if sent := transport.frames(); len(sent) != 2 || !bytes.Equal(sent[0], sent[1]) {
		t.Fatalf("want one repeat under the same identity, got %d frames", len(sent))
	}
}

func TestGivenAnAmbiguousRenewalWhenTheLeaseRunsOutThenShouldReportTheLeaseLost(t *testing.T) {
	transport := &repeatingTransport{always: unknownCoordinationReply(t, wire.ResultPending)}
	client := NewFencedLeaseClient(transport)
	operation := renewal(t, client)

	_, err := client.renewResolving(context.Background(), operation, time.Now().Add(300*time.Millisecond))

	var refused *KvError
	if !errors.As(err, &refused) || refused.Kind() != wire.KvLeaseLost {
		t.Fatalf("work stops once the held lease is no longer valid, got %T: %v", err, err)
	}
}

func TestGivenAReusedConnectionWithoutTheCapabilityWhenCheckedTwiceWithinASecondThenShouldProbeOnce(t *testing.T) {
	transport := NewDedicatedKvTransport("iggy://localhost:8090")

	if !transport.reprobeDue() {
		t.Error("a connection never probed is due")
	}
	if transport.reprobeDue() {
		t.Error("a refused call within the interval answers from the last probe")
	}
}

func TestGivenAnUnsupportedDeploymentWhenReadiedRepeatedlyThenShouldDialOnceAndKeepTheConnection(t *testing.T) {
	node := &coordinationNode{}
	dials := 0
	transport := NewDedicatedKvTransport("iggy://localhost:8090")
	transport.dial = func(context.Context) (*Laser, error) {
		dials++
		return FromClient(node, Options{}), nil
	}

	for range 3 {
		var unsupported *UnsupportedError
		if err := transport.Ready(context.Background()); !errors.As(err, &unsupported) {
			t.Fatalf("want an UnsupportedError, got %T: %v", err, err)
		}
	}

	if dials != 1 || transport.Generation() != 0 || node.closed != 0 {
		t.Errorf("want one dial and no retirement, got %d dials, generation %d, %d closes",
			dials, transport.Generation(), node.closed)
	}
}

func TestGivenAFencedDeploymentWhenReadiedAndSentThenShouldAnswerOnTheDedicatedConnection(t *testing.T) {
	node := &coordinationNode{answers: []coordinationAnswer{{reply: grantReply(t, wire.KvLeased, 3, time.Minute)}}}
	transport := NewDedicatedKvTransport("iggy://localhost:8090")
	transport.dial = func(context.Context) (*Laser, error) {
		return FromClient(node, Options{Capabilities: fencedLeaseCapabilities()}), nil
	}
	client := NewFencedLeaseClient(transport)
	operation := acquisition(t, client, time.Minute)

	lease, err := client.Acquire(context.Background(), operation)

	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if lease.Token != 3 {
		t.Errorf("want token 3, got %d", lease.Token)
	}
	if len(node.codes) != 1 || node.codes[0] != wire.AgdxKvLeaseCode {
		t.Errorf("want one lease request, got %v", node.codes)
	}
}

func TestGivenAFrameAnEarlierNodeWroteWhenALaterNodeRefusesBeforeWritingThenShouldReportAmbiguity(t *testing.T) {
	node := &coordinationNode{
		roster: []uint8{0, 1},
		answers: []coordinationAnswer{
			{err: ierror.TransientNotCommitted{}}, {err: ierror.NotConnected{}},
		},
	}
	transport := NewDedicatedKvTransport("iggy://localhost:8090")
	transport.dial = func(context.Context) (*Laser, error) {
		return FromClient(node, Options{Capabilities: fencedLeaseCapabilities()}), nil
	}
	client := NewFencedLeaseClient(transport)
	operation := acquisition(t, client, time.Minute)

	_, err := client.Acquire(context.Background(), operation)

	var ambiguous *AmbiguousMutationError
	if !errors.As(err, &ambiguous) || ambiguous.OperationID != operation.OperationID() {
		t.Fatalf("a written frame keeps its unknown outcome, got %T: %v", err, err)
	}
	if len(node.codes) != 2 || transport.Generation() != 1 {
		t.Errorf("want both nodes tried and the connection retired, got %d sends, generation %d",
			len(node.codes), transport.Generation())
	}
}

func TestGivenAFrameAnEarlierAttemptWroteWhenALaterAttemptStopsBeforeWritingThenShouldKeepTheUnknownOutcome(t *testing.T) {
	if got := writtenOutcome(ierror.ClientShutdown{}, true); !errors.Is(got, ierror.TransientNotCommitted{}) {
		t.Errorf("want an unknown outcome, got %v", got)
	}
	if got := writtenOutcome(ierror.ClientShutdown{}, false); !errors.Is(got, ierror.ClientShutdown{}) {
		t.Errorf("want the refusal unchanged, got %v", got)
	}
	uncertain := ierror.OperationUncertain{OperationID: 4}
	if got := writtenOutcome(uncertain, true); got != error(uncertain) {
		t.Errorf("want the driver's own unknown outcome, got %v", got)
	}
}

func TestGivenAClosedCoordinationClientWhenReadiedThenShouldNotReconnect(t *testing.T) {
	dials := 0
	transport := NewDedicatedKvTransport("not a connection string")
	transport.dial = func(context.Context) (*Laser, error) {
		dials++
		return nil, errors.New("a closed transport must not dial")
	}
	client := NewFencedLeaseClient(transport)
	if err := client.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("a second close: %v", err)
	}
	transport.Reset()

	err := transport.Ready(context.Background())

	if !errors.Is(err, ierror.ClientShutdown{}) || dials != 0 {
		t.Fatalf("want a shutdown refusal without a dial, got %v after %d dials", err, dials)
	}
}

func TestGivenALateFailureFromARetiredConnectionWhenResetThenShouldKeepTheNewerConnection(t *testing.T) {
	older, newer := &coordinationNode{}, &coordinationNode{}
	transport := NewDedicatedKvTransport("iggy://localhost:8090")
	transport.slot = FromClient(older, Options{})
	first := transport.Generation()

	transport.ResetGeneration(first)
	retired := transport.slot == nil && older.closed == 1
	transport.slot = FromClient(newer, Options{})
	transport.ResetGeneration(first)

	if !retired {
		t.Error("the failing call retires its own connection")
	}
	if transport.slot == nil || newer.closed != 0 {
		t.Error("a late failure from the old connection leaves the new one running")
	}
	_, err := transport.SendOn(context.Background(), first, wire.AgdxKvReleaseCode, nil)
	if !errors.Is(err, ierror.NotConnected{}) || mayHaveWritten(err) || len(newer.codes) != 0 {
		t.Errorf("a send readied on the old connection is refused before writing, got %v", err)
	}
}

// repeatingTransport answers from a script, then with its standing reply.
type repeatingTransport struct {
	mutex   sync.Mutex
	replies [][]byte
	always  []byte
	delay   time.Duration
	sent    [][]byte
	resets  int
}

func (transport *repeatingTransport) Send(ctx context.Context, _ uint32, frame []byte) ([]byte, error) {
	transport.mutex.Lock()
	transport.sent = append(transport.sent, bytes.Clone(frame))
	reply := transport.always
	if len(transport.replies) > 0 {
		reply, transport.replies = transport.replies[0], transport.replies[1:]
	}
	transport.mutex.Unlock()
	if !sleepFor(ctx, transport.delay) {
		return nil, ctx.Err()
	}
	return reply, nil
}

func (transport *repeatingTransport) Reset() {
	transport.mutex.Lock()
	defer transport.mutex.Unlock()
	transport.resets++
}

func (transport *repeatingTransport) frames() [][]byte {
	transport.mutex.Lock()
	defer transport.mutex.Unlock()
	return append([][]byte(nil), transport.sent...)
}

type coordinationAnswer struct {
	reply []byte
	err   error
}

// coordinationNode is a deployment that rejects the capability probe and answers managed requests from a script.
type coordinationNode struct {
	iggcon.Client
	roster  []uint8
	answers []coordinationAnswer
	codes   []uint32
	closed  int
}

func (node *coordinationNode) SendBinaryRequest(_ context.Context, code uint32, _ []byte) ([]byte, error) {
	return node.answer(code)
}

func (node *coordinationNode) SendBinaryRequestToReplica(
	_ context.Context, code uint32, _ []byte, _ uint8,
) ([]byte, error) {
	return node.answer(code)
}

func (node *coordinationNode) GetClusterMetadata(context.Context) (*iggcon.ClusterMetadata, error) {
	roster := node.roster
	if len(roster) == 0 {
		roster = []uint8{0}
	}
	metadata := &iggcon.ClusterMetadata{}
	for _, replica := range roster {
		metadata.Nodes = append(metadata.Nodes, iggcon.ClusterNode{ReplicaID: replica})
	}
	return metadata, nil
}

func (node *coordinationNode) Close() error {
	node.closed++
	return nil
}

func (node *coordinationNode) answer(code uint32) ([]byte, error) {
	if code == wire.AgdxHelloCode {
		return nil, ierror.InvalidCommand{}
	}
	node.codes = append(node.codes, code)
	if len(node.answers) == 0 {
		return nil, ierror.Disconnected{}
	}
	next := node.answers[0]
	node.answers = node.answers[1:]
	return next.reply, next.err
}

func fencedLeaseCapabilities() *Capabilities {
	capabilities := OpenCapabilities()
	capabilities.Managed = true
	capabilities.Hello = HelloAnswered
	capabilities.Kv = KvCaps{Available: true, CAS: true, FencedLeases: true, CASFenced: true}
	return &capabilities
}

func acquisition(t *testing.T, client *FencedLeaseClient, ttl time.Duration) *PreparedMutation {
	t.Helper()
	operation, err := client.PrepareAcquire(wire.KvLease{
		V: wire.KvLeaseOpVersion, Namespace: "connectors.coordination", Key: []byte("source-owner"),
		LeaseTTLMicros: uint64(ttl.Microseconds()), HolderID: "node-1",
	})
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	return operation
}

func renewal(t *testing.T, client *FencedLeaseClient) *PreparedMutation {
	t.Helper()
	operation, err := client.PrepareRenew(wire.KvLeaseRenew{
		V: wire.KvLeaseOpVersion, Namespace: "connectors.coordination", Key: []byte("source-owner"),
		HolderID: "node-1", LeaseToken: 7, LeaseTTLMicros: 30_000_000,
	})
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	return operation
}

func grantReply(t *testing.T, kind wire.KvOutcomeKind, token uint64, ttl time.Duration) []byte {
	t.Helper()
	encoded, err := wire.Marshal(wire.OkReply(wire.KvOutcome{Kind: kind, Lease: &wire.KvLeaseGrant{
		LeaseToken: token, GrantedTTLMicros: uint64(ttl.Microseconds()),
		Position: wire.MutationPosition{TopicGeneration: 1, Offset: 512},
	}}))
	if err != nil {
		t.Fatalf("encode the reply: %v", err)
	}
	return encoded
}

func unknownCoordinationReply(t *testing.T, code wire.ResultCode) []byte {
	t.Helper()
	return replyOf(t, code)
}
