package laser_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// nodeCall is one request the cluster saw and where it went.
type nodeCall struct {
	code uint32
	// replica is the node it was addressed to, and named reports whether it
	// named one at all: a plain band call does not.
	replica uint8
	named   bool
}

// clusterServer stands in for a cluster of named replicas.
type clusterServer struct {
	iggcon.Client
	mutex sync.Mutex
	// roster is what the metadata read answers with.
	roster []uint8
	// rosterReads counts them, so a test can tell a cached roster from one
	// read again.
	rosterReads int
	// rosterErr fails the metadata read.
	rosterErr error
	calls     []nodeCall
	// gate holds every call until a test releases it, so two requests can be
	// in flight against one node at once.
	gate chan struct{}
	// replies answers one call each, in order. A nil entry answers with an
	// empty success.
	replies []func() ([]byte, error)
}

func newCluster(roster ...uint8) *clusterServer {
	return &clusterServer{roster: roster}
}

func (server *clusterServer) Close() error { return nil }

func (server *clusterServer) GetClusterMetadata(
	_ context.Context,
) (*iggcon.ClusterMetadata, error) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	server.rosterReads++
	if server.rosterErr != nil {
		return nil, server.rosterErr
	}
	nodes := make([]iggcon.ClusterNode, 0, len(server.roster))
	for _, replica := range server.roster {
		nodes = append(nodes, iggcon.ClusterNode{ReplicaID: replica})
	}
	return &iggcon.ClusterMetadata{Nodes: nodes}, nil
}

func (server *clusterServer) SendBinaryRequest(
	_ context.Context, code uint32, _ []byte,
) ([]byte, error) {
	return server.answer(nodeCall{code: code})
}

func (server *clusterServer) SendBinaryRequestToReplica(
	_ context.Context, code uint32, _ []byte, replica uint8,
) ([]byte, error) {
	return server.answer(nodeCall{code: code, replica: replica, named: true})
}

func (server *clusterServer) answer(call nodeCall) ([]byte, error) {
	server.mutex.Lock()
	server.calls = append(server.calls, call)
	var reply func() ([]byte, error)
	if len(server.replies) > 0 {
		reply, server.replies = server.replies[0], server.replies[1:]
	}
	gate := server.gate
	server.mutex.Unlock()
	if gate != nil {
		<-gate
	}
	if reply == nil {
		return nil, nil
	}
	return reply()
}

// hold makes every call block until the returned gate is closed.
func (server *clusterServer) hold() chan struct{} {
	gate := make(chan struct{})
	server.mutex.Lock()
	defer server.mutex.Unlock()
	server.gate = gate
	return gate
}

// release lets held calls through and stops holding later ones.
func (server *clusterServer) release(gate chan struct{}) {
	server.mutex.Lock()
	server.gate = nil
	server.mutex.Unlock()
	close(gate)
}

// reorder changes what the next roster read answers with.
func (server *clusterServer) reorder(roster ...uint8) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	server.roster = roster
}

// refusalOf encodes a command error the server reasoned about.
func refusalOf(t *testing.T, code wire.ResultCode) func() ([]byte, error) {
	t.Helper()
	encoded, err := wire.Marshal(wire.NewCommandError(code, "no"))
	if err != nil {
		t.Fatalf("encode the refusal: %v", err)
	}
	return func() ([]byte, error) { return encoded, nil }
}

// routed is every call the cluster saw.
func (server *clusterServer) routed() []nodeCall {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	return append([]nodeCall(nil), server.calls...)
}

// reads is how many times the roster was read.
func (server *clusterServer) reads() int {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	return server.rosterReads
}

// answerWith queues the replies to the next calls, in order.
func (server *clusterServer) answerWith(replies ...func() ([]byte, error)) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	server.replies = replies
}

func failing(err error) func() ([]byte, error) {
	return func() ([]byte, error) { return nil, err }
}

func unavailableNode(t *testing.T) func() ([]byte, error) {
	t.Helper()
	encoded, err := wire.Marshal(wire.NewCommandError(
		wire.ResultBackendUnavailable, "the backend is out of reach"))
	if err != nil {
		t.Fatalf("encode the refusal: %v", err)
	}
	return func() ([]byte, error) { return encoded, nil }
}

func clusterHandle(server iggcon.Client) *laser.Laser {
	return laser.FromClient(server, laser.Options{
		DefaultStream: "agents",
		Capabilities:  &laser.Capabilities{Managed: true},
	})
}

// sendProbe is a managed request that names no resource, so routing is the
// only thing under test.
func sendProbe(ctx context.Context, handle *laser.Laser) ([]byte, error) {
	return handle.SendManaged(ctx, wire.AgdxKvGetCode, nil)
}

func TestGivenOneNodeWhenSendingManagedThenShouldUseThePlainBand(t *testing.T) {
	server := newCluster(0)
	handle := clusterHandle(server)

	if _, err := sendProbe(context.Background(), handle); err != nil {
		t.Fatalf("send: %v", err)
	}

	calls := server.routed()
	if len(calls) != 1 {
		t.Fatalf("want one call, got %d", len(calls))
	}
	// Naming the only replica there is adds nothing, and a deployment that
	// predates clustering would not understand it.
	if calls[0].named {
		t.Fatal("a single node must be reached over the plain band")
	}
}

func TestGivenAClusterWhenSendingManagedThenShouldKeepToOneNode(t *testing.T) {
	server := newCluster(0, 1, 2)
	handle := clusterHandle(server)

	for index := 0; index < 4; index++ {
		if _, err := sendProbe(context.Background(), handle); err != nil {
			t.Fatalf("send %d: %v", index, err)
		}
	}

	calls := server.routed()
	if len(calls) != 4 {
		t.Fatalf("want four calls, got %d", len(calls))
	}
	for _, call := range calls {
		if !call.named {
			t.Fatal("a cluster request must name its node")
		}
		if call.replica != calls[0].replica {
			t.Fatalf("the caller moved from %d to %d without a failure",
				calls[0].replica, call.replica)
		}
	}
	// The roster is read once and kept: every request re-reading it would
	// double the round trips for nothing.
	if server.reads() != 1 {
		t.Fatalf("want one roster read, got %d", server.reads())
	}
}

func TestGivenTwoClientsWhenSendingManagedThenShouldSpreadOverTheCluster(t *testing.T) {
	// The caller key is random per connection, so two clients land on the
	// same node only by chance. Over enough clients they must not all pile
	// onto one.
	roster := []uint8{0, 1, 2, 3, 4, 5, 6, 7}
	picked := map[uint8]int{}
	for index := 0; index < 64; index++ {
		server := newCluster(roster...)
		handle := clusterHandle(server)
		if _, err := sendProbe(context.Background(), handle); err != nil {
			t.Fatalf("send %d: %v", index, err)
		}
		picked[server.routed()[0].replica]++
	}
	if len(picked) < 2 {
		t.Fatalf("64 clients all picked %v, which is not spreading", picked)
	}
}

func TestGivenAFailedLinkWhenSendingManagedThenShouldMoveToTheNextNode(t *testing.T) {
	server := newCluster(0, 1, 2)
	server.answerWith(failing(ierror.Disconnected{}))
	handle := clusterHandle(server)

	if _, err := sendProbe(context.Background(), handle); err != nil {
		t.Fatalf("send: %v", err)
	}

	calls := server.routed()
	if len(calls) != 2 {
		t.Fatalf("want the failed attempt then the next node, got %d calls", len(calls))
	}
	if calls[0].replica == calls[1].replica {
		t.Fatalf("both attempts went to replica %d", calls[0].replica)
	}
	// The move sticks: the next request starts where the last one landed
	// rather than walking the failed node again.
	if _, err := sendProbe(context.Background(), handle); err != nil {
		t.Fatalf("send: %v", err)
	}
	after := server.routed()
	if after[2].replica != calls[1].replica {
		t.Fatalf("the caller moved back to %d, want %d",
			after[2].replica, calls[1].replica)
	}
}

func TestGivenEveryNodesLinkFailsWhenSendingManagedThenShouldReportIt(t *testing.T) {
	server := newCluster(0, 1, 2)
	server.answerWith(
		failing(ierror.Disconnected{}),
		failing(ierror.Disconnected{}),
		failing(ierror.Disconnected{}),
		failing(ierror.Disconnected{}),
	)
	handle := clusterHandle(server)

	_, err := sendProbe(context.Background(), handle)
	if err == nil {
		t.Fatal("a cluster that is wholly unreachable must be reported")
	}
	// Each node is tried once. Walking them again would turn an outage into
	// an endless loop.
	if calls := server.routed(); len(calls) != 3 {
		t.Fatalf("want one attempt per node, got %d", len(calls))
	}
}

func TestGivenANodeThatCannotReachItsBackendWhenSendingThenShouldMoveOn(t *testing.T) {
	server := newCluster(0, 1, 2)
	server.answerWith(unavailableNode(t))
	handle := clusterHandle(server)

	if _, err := sendProbe(context.Background(), handle); err != nil {
		t.Fatalf("send: %v", err)
	}

	// A node that answered but could not reach its own backend is a node to
	// move off, exactly like a link that dropped.
	calls := server.routed()
	if len(calls) != 2 || calls[0].replica == calls[1].replica {
		t.Fatalf("want a move to another node, got %+v", calls)
	}
}

func TestGivenEveryNodeCannotReachItsBackendWhenSendingThenShouldReturnTheRefusal(t *testing.T) {
	server := newCluster(0, 1)
	server.answerWith(unavailableNode(t), unavailableNode(t), unavailableNode(t))
	handle := clusterHandle(server)

	reply, err := sendProbe(context.Background(), handle)
	if err != nil {
		t.Fatalf("a refusal every node agrees on is an answer, not a failure: %v", err)
	}
	// The last node's refusal is handed back rather than swallowed, so the
	// caller sees what the cluster said.
	var failure wire.CommandError
	if err := wire.Unmarshal(reply, &failure); err != nil {
		t.Fatalf("decode the refusal: %v", err)
	}
	if !failure.Code.IsBackendUnavailable() {
		t.Fatalf("want the backend refusal, got %v", failure.Code)
	}
	if calls := server.routed(); len(calls) != 2 {
		t.Fatalf("want one attempt per node, got %d", len(calls))
	}
}

func TestGivenAReplicaNoLongerInTheClusterWhenSendingThenShouldReadTheRosterAgain(t *testing.T) {
	server := newCluster(0, 1)
	server.answerWith(failing(ierror.InvalidIdentifier{}))
	handle := clusterHandle(server)

	if _, err := sendProbe(context.Background(), handle); err != nil {
		t.Fatalf("send: %v", err)
	}

	// The id the client held is gone, so the roster it holds is stale. It is
	// read once more rather than on every attempt.
	if server.reads() != 2 {
		t.Fatalf("want the roster read again, got %d reads", server.reads())
	}
	// Re-read once, not once per attempt: a second stale id inside one
	// request means the roster is not the problem, so it surfaces.
	server.answerWith(
		failing(ierror.InvalidIdentifier{}), failing(ierror.InvalidIdentifier{}))
	before := server.reads()
	if _, err := sendProbe(context.Background(), handle); err == nil {
		t.Fatal("a second stale id in one request must surface")
	}
	if grew := server.reads() - before; grew != 1 {
		t.Fatalf("want one more roster read, got %d", grew)
	}
}

func TestGivenARefusalTheServerReasonedAboutWhenSendingThenShouldNotWalk(t *testing.T) {
	server := newCluster(0, 1, 2)
	server.answerWith(failing(ierror.InvalidCommand{}))
	handle := clusterHandle(server)

	if _, err := sendProbe(context.Background(), handle); err == nil {
		t.Fatal("a refused command must be reported")
	}
	// Walking the cluster for an answer every node would give is only delay.
	if calls := server.routed(); len(calls) != 1 {
		t.Fatalf("want one attempt, got %d", len(calls))
	}
}

func TestGivenAnUnreadableRosterWhenSendingThenShouldReportIt(t *testing.T) {
	server := newCluster(0, 1)
	server.rosterErr = ierror.Disconnected{}
	handle := clusterHandle(server)

	if _, err := sendProbe(context.Background(), handle); err == nil {
		t.Fatal("a roster nobody can read must be reported")
	}
	if calls := server.routed(); len(calls) != 0 {
		t.Fatalf("nothing must be sent, got %+v", calls)
	}
}

func TestGivenAnEmptyClusterWhenSendingThenShouldReportIt(t *testing.T) {
	server := newCluster()
	handle := clusterHandle(server)

	if _, err := sendProbe(context.Background(), handle); err == nil {
		t.Fatal("a cluster with no nodes must be reported")
	}
}

func TestGivenAnAuthorizationWriteWhenSentThenShouldGoToTheMetadataPrimary(t *testing.T) {
	for _, code := range []uint32{
		wire.AgdxAuthzDefineRoleCode, wire.AgdxAuthzDeleteRoleCode,
		wire.AgdxAuthzBindRolesCode,
	} {
		server := newCluster(0, 1, 2)
		handle := clusterHandle(server)
		if _, err := handle.SendManaged(context.Background(), code, nil); err != nil {
			t.Fatalf("send: %v", err)
		}
		calls := server.routed()
		if len(calls) != 1 || calls[0].named {
			t.Fatalf("a replicated write goes to the metadata primary, got %+v", calls)
		}
		// The primary is reached over the band without a roster read: it is
		// the node the connection already has.
		if server.reads() != 0 {
			t.Fatalf("want no roster read, got %d", server.reads())
		}
	}
}

func TestGivenAnExpiredDeadlineWhenWalkingThenShouldStopRatherThanSpendIt(t *testing.T) {
	server := newCluster(0, 1, 2, 3)
	server.answerWith(
		failing(ierror.Disconnected{}), failing(ierror.Disconnected{}),
		failing(ierror.Disconnected{}), failing(ierror.Disconnected{}),
	)
	handle := clusterHandle(server)
	ctx, cancel := context.WithDeadline(context.Background(), time.Now())
	defer cancel()

	if _, err := sendProbe(ctx, handle); err == nil {
		t.Fatal("an expired deadline must end the walk with the failure")
	}
	// The walk ends at the deadline so the caller's next attempt starts on
	// the next node with a fresh one, rather than spending it all here.
	if calls := server.routed(); len(calls) != 1 {
		t.Fatalf("want the walk to stop after one attempt, got %d", len(calls))
	}
}

func TestGivenAPositionedReplyWhenRoutedThenShouldRaiseTheFrontier(t *testing.T) {
	server := newCluster(0, 1)
	position := wire.MutationPosition{TopicGeneration: 7, Partition: 1, Offset: 42}
	positioned, err := wire.Marshal(wire.ManagedReplyEnvelope{
		Reply:    []byte("body"),
		Frontier: wire.AppliedFrontier{Kv: &position},
	})
	if err != nil {
		t.Fatalf("encode the envelope: %v", err)
	}
	server.answerWith(func() ([]byte, error) { return positioned, nil })
	handle := clusterHandle(server)

	reply, err := sendProbe(context.Background(), handle)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if string(reply) != "body" {
		t.Fatalf("want the opened reply, got %q", reply)
	}
	if seen := handle.SeenPositions(); seen.Kv == nil || seen.Kv.Offset != 42 {
		t.Fatalf("want the frontier raised, got %+v", seen)
	}
}

func TestGivenAClosedClientWhenSendingManagedThenShouldSaySo(t *testing.T) {
	handle := clusterHandle(newCluster(0))
	if err := handle.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	_, err := sendProbe(context.Background(), handle)
	var transport *laser.TransportError
	if !errors.As(err, &transport) {
		t.Fatalf("want a TransportError, got %T: %v", err, err)
	}
}

func TestGivenARosterReadAgainInAnotherOrderWhenSendingThenShouldKeepTheSameNode(t *testing.T) {
	server := newCluster(0, 1, 2)
	handle := clusterHandle(server)
	if _, err := sendProbe(context.Background(), handle); err != nil {
		t.Fatalf("send: %v", err)
	}
	picked := server.routed()[0].replica

	// A cluster is free to list its nodes in any order, and it does not have
	// to list them the same way twice. Sorting the roster is what keeps the
	// caller on one node across reads rather than hopping for no reason.
	server.reorder(2, 0, 1)
	server.answerWith(failing(ierror.InvalidIdentifier{}))
	if _, err := sendProbe(context.Background(), handle); err != nil {
		t.Fatalf("send: %v", err)
	}

	calls := server.routed()
	after := calls[len(calls)-1].replica
	if after != picked {
		t.Fatalf("the caller moved from %d to %d on a reordered roster", picked, after)
	}
}

func TestGivenAClusterThatListsANodeTwiceWhenWalkingThenShouldCountItOnce(t *testing.T) {
	server := newCluster(0, 0, 1)
	server.answerWith(
		failing(ierror.Disconnected{}), failing(ierror.Disconnected{}),
		failing(ierror.Disconnected{}), failing(ierror.Disconnected{}),
	)
	handle := clusterHandle(server)

	if _, err := sendProbe(context.Background(), handle); err == nil {
		t.Fatal("a cluster that is wholly unreachable must be reported")
	}
	// Two nodes, so two attempts. A roster that counted the repeat would
	// walk a third time to a node it had already given up on.
	if calls := server.routed(); len(calls) != 2 {
		t.Fatalf("want one attempt per distinct node, got %d", len(calls))
	}
}

func TestGivenARefusalThatIsNotTheBackendWhenSendingThenShouldNotWalk(t *testing.T) {
	server := newCluster(0, 1, 2)
	server.answerWith(refusalOf(t, wire.ResultNotFound))
	handle := clusterHandle(server)

	reply, err := sendProbe(context.Background(), handle)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	// Every node would say the same, so walking for it is only delay.
	if calls := server.routed(); len(calls) != 1 {
		t.Fatalf("want one attempt, got %d", len(calls))
	}
	var failure wire.CommandError
	if err := wire.Unmarshal(reply, &failure); err != nil || failure.Code != wire.ResultNotFound {
		t.Fatalf("want the refusal handed back, got %v %v", failure.Code, err)
	}
}

func TestGivenConcurrentFailuresOnOneNodeWhenWalkingThenShouldMoveOffItOnce(t *testing.T) {
	server := newCluster(0, 1, 2)
	gate := server.hold()
	server.answerWith(
		failing(ierror.Disconnected{}), failing(ierror.Disconnected{}),
	)
	handle := clusterHandle(server)

	// Both requests are held against the same node, so both report their
	// failure against it. Moving once per failure would skip a node that was
	// never tried.
	var waiting sync.WaitGroup
	for index := 0; index < 2; index++ {
		waiting.Add(1)
		go func() {
			defer waiting.Done()
			_, _ = sendProbe(context.Background(), handle)
		}()
	}
	deadline := time.Now().Add(5 * time.Second)
	for len(server.routed()) < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	server.release(gate)
	waiting.Wait()

	calls := server.routed()
	if len(calls) < 3 {
		t.Fatalf("want both requests to retry, got %d calls", len(calls))
	}
	first := calls[0].replica
	skipped := uint8((uint32(first) + 2) % 3)
	for _, call := range calls[2:] {
		if call.replica == skipped {
			t.Fatalf("the caller skipped a node it never tried: %+v", calls)
		}
	}
}
