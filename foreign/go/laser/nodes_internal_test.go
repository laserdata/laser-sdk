package laser

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// scriptedCluster answers each request from a script, so a test says what the
// roster does rather than standing a cluster up.
type scriptedCluster struct {
	answers  []func() ([]byte, error)
	attempts int
}

func (cluster *scriptedCluster) answer() ([]byte, error) {
	cluster.attempts++
	if len(cluster.answers) == 0 {
		return nil, ierror.Disconnected{}
	}
	next := cluster.answers[0]
	if len(cluster.answers) > 1 {
		cluster.answers = cluster.answers[1:]
	}
	return next()
}

func (cluster *scriptedCluster) SendBinaryRequest(
	context.Context, uint32, []byte,
) ([]byte, error) {
	return cluster.answer()
}

func (cluster *scriptedCluster) SendBinaryRequestToReplica(
	context.Context, uint32, []byte, uint8,
) ([]byte, error) {
	return cluster.answer()
}

func (cluster *scriptedCluster) GetClusterMetadata(
	context.Context,
) (*iggcon.ClusterMetadata, error) {
	return nil, ierror.Disconnected{}
}

// routerOver is a router that already read this roster, so a test drives the
// walk rather than the roster read.
func routerOver(replicas ...uint8) *nodeRouter {
	router := newNodeRouter()
	router.roster = replicas
	return router
}

func refusedBackend(t *testing.T, message string) []byte {
	t.Helper()
	encoded, err := wire.Marshal(wire.NewCommandError(wire.ResultBackendUnavailable, message))
	if err != nil {
		t.Fatalf("encode the refusal: %v", err)
	}
	return encoded
}

func answers(replies ...func() ([]byte, error)) []func() ([]byte, error) { return replies }

func replying(reply []byte) func() ([]byte, error) {
	return func() ([]byte, error) { return reply, nil }
}

func TestGivenOneNodeThatRefusesThenServesWhenReadingThenShouldReturnTheServedReply(t *testing.T) {
	served := []byte("served")
	cluster := &scriptedCluster{answers: answers(
		replying(refusedBackend(t, "replaying its logs")),
		replying(served),
	)}
	router := routerOver(0)

	walked, err := router.walkCallerUntil(context.Background(), cluster,
		wire.AgdxKvGetCode, nil, walkingUntil(time.Now().Add(5*time.Second)))
	if err != nil {
		t.Fatalf("the read is served: %v", err)
	}

	if string(walked.reply) != string(served) {
		t.Fatalf("want the served reply, got %q", walked.reply)
	}
	if cluster.attempts != 2 {
		t.Fatalf("want the refused node asked again, got %d attempts", cluster.attempts)
	}
}

func TestGivenEveryNodeRefusingPastTheDeadlineWhenReadingThenShouldReturnTheLastRefusal(t *testing.T) {
	cluster := &scriptedCluster{}
	refusals := 0
	cluster.answers = answers(func() ([]byte, error) {
		refusals++
		return refusedBackend(t, fmt.Sprintf("refusal %d", refusals)), nil
	})
	router := routerOver(0, 1)
	started := time.Now()

	walked, err := router.walkCallerUntil(context.Background(), cluster,
		wire.AgdxKvGetCode, nil, walkingUntil(started.Add(300*time.Millisecond)))
	if err != nil {
		t.Fatalf("a refusal is a reply, not a failure: %v", err)
	}

	var failure wire.CommandError
	if err := wire.Unmarshal(walked.reply, &failure); err != nil {
		t.Fatalf("decode the refusal: %v", err)
	}
	if !failure.Code.IsBackendUnavailable() {
		t.Fatalf("want the backend refusal, got %v", failure.Code)
	}
	if cluster.attempts <= 2 {
		t.Fatalf("want the roster walked again before the deadline, got %d attempts",
			cluster.attempts)
	}
	// The last refusal is what comes back, not the first one it saw.
	if want := fmt.Sprintf("refusal %d", refusals); failure.Message != want {
		t.Fatalf("want %q, got %q", want, failure.Message)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("the deadline must end the wait, took %v", elapsed)
	}
}

func TestGivenAMutationEveryNodeRefusesWhenWalkedThenShouldWalkOnce(t *testing.T) {
	cluster := &scriptedCluster{answers: answers(
		replying(refusedBackend(t, "the backend is out of reach")),
	)}
	router := routerOver(0, 1)

	if _, err := router.walkCallerUntil(context.Background(), cluster,
		wire.AgdxKvSetCode, nil, walkingOnce()); err != nil {
		t.Fatalf("a refusal every node agrees on is an answer: %v", err)
	}

	// A mutation repeats under its own operation identity, so the walk that
	// routes it runs once per node and stops.
	if cluster.attempts != 2 {
		t.Fatalf("want one attempt per node, got %d", cluster.attempts)
	}
}

func TestGivenAFilterCatalogReadRefusedAsCatalogUnavailableWhenReadingThenShouldWalkAgain(t *testing.T) {
	refusal, err := wire.Marshal(wire.FilterReply{Err: &wire.FilterError{
		Code:    wire.ResultUnavailable,
		Reason:  wire.ReasonCatalogUnavailable,
		Message: "the catalog cannot reach its backend",
	}})
	if err != nil {
		t.Fatalf("encode the catalog refusal: %v", err)
	}
	served := []byte("served")
	cluster := &scriptedCluster{answers: answers(replying(refusal), replying(served))}
	router := routerOver(0)

	walked, walkErr := router.walkCallerUntil(context.Background(), cluster,
		wire.AgdxGetFilterCode, nil, walkingUntil(time.Now().Add(5*time.Second)))
	if walkErr != nil {
		t.Fatalf("the catalog read is served: %v", walkErr)
	}

	// The streaming server gives an unreachable catalog its own refusal
	// rather than the command error, so a read that only knew the latter
	// would answer a transient outage as a final one.
	if string(walked.reply) != string(served) {
		t.Fatalf("want the served reply, got %q", walked.reply)
	}
}

func TestGivenACatalogRefusalOnAnotherSurfaceWhenWalkedThenShouldReturnItAtOnce(t *testing.T) {
	refusal, err := wire.Marshal(wire.FilterReply{Err: &wire.FilterError{
		Code:    wire.ResultUnavailable,
		Reason:  wire.ReasonCatalogUnavailable,
		Message: "the catalog cannot reach its backend",
	}})
	if err != nil {
		t.Fatalf("encode the catalog refusal: %v", err)
	}
	cluster := &scriptedCluster{answers: answers(replying(refusal))}
	router := routerOver(0)

	if _, walkErr := router.walkCallerUntil(context.Background(), cluster,
		wire.AgdxKvGetCode, nil, walkingUntil(time.Now().Add(5*time.Second))); walkErr != nil {
		t.Fatalf("the refusal is a reply: %v", walkErr)
	}

	// Only a catalog read speaks that refusal. On another surface those bytes
	// are the surface's own answer, and walking again would repeat a call the
	// server already decided.
	if cluster.attempts != 1 {
		t.Fatalf("want one attempt, got %d", cluster.attempts)
	}
}

// rosterOf is a cluster that reports these nodes, so a test drives the
// roster read rather than setting the roster behind it.
type rosterOf struct {
	scriptedCluster
	nodes []uint8
	reads int
}

func (cluster *rosterOf) GetClusterMetadata(
	context.Context,
) (*iggcon.ClusterMetadata, error) {
	cluster.reads++
	nodes := make([]iggcon.ClusterNode, 0, len(cluster.nodes))
	for _, replica := range cluster.nodes {
		nodes = append(nodes, iggcon.ClusterNode{ReplicaID: replica})
	}
	return &iggcon.ClusterMetadata{Nodes: nodes}, nil
}

func TestGivenEveryNodesLinkFailsWhenWalkedThenShouldReportIt(t *testing.T) {
	cluster := &rosterOf{nodes: []uint8{0, 1, 2}}
	cluster.answers = answers(func() ([]byte, error) {
		return nil, ierror.Disconnected{}
	})
	router := newNodeRouter()

	if _, err := router.walkCallerUntil(context.Background(), cluster,
		wire.AgdxKvGetCode, nil, walkingOnce()); err == nil {
		t.Fatal("a cluster that is wholly unreachable must be reported")
	}

	// Each node is tried once. Walking them again inside one walk would turn
	// an outage into an endless loop; repeating the whole walk is the
	// caller's decision, and a mutation or a read makes it differently.
	if cluster.attempts != 3 {
		t.Fatalf("want one attempt per node, got %d", cluster.attempts)
	}
}

func TestGivenEveryNodeCannotReachItsBackendWhenWalkedThenShouldReturnTheRefusal(t *testing.T) {
	cluster := &rosterOf{nodes: []uint8{0, 1}}
	cluster.answers = answers(replying(refusedBackend(t, "the backend is out of reach")))
	router := newNodeRouter()

	walked, err := router.walkCallerUntil(context.Background(), cluster,
		wire.AgdxKvGetCode, nil, walkingOnce())
	if err != nil {
		t.Fatalf("a refusal every node agrees on is an answer, not a failure: %v", err)
	}

	// The last node's refusal is handed back rather than swallowed, so the
	// caller sees what the cluster said.
	var failure wire.CommandError
	if err := wire.Unmarshal(walked.reply, &failure); err != nil {
		t.Fatalf("decode the refusal: %v", err)
	}
	if !failure.Code.IsBackendUnavailable() {
		t.Fatalf("want the backend refusal, got %v", failure.Code)
	}
	if cluster.attempts != 2 {
		t.Fatalf("want one attempt per node, got %d", cluster.attempts)
	}
}

func TestGivenAClusterThatListsANodeTwiceWhenWalkedThenShouldCountItOnce(t *testing.T) {
	cluster := &rosterOf{nodes: []uint8{0, 0, 1}}
	cluster.answers = answers(func() ([]byte, error) {
		return nil, ierror.Disconnected{}
	})
	router := newNodeRouter()

	if _, err := router.walkCallerUntil(context.Background(), cluster,
		wire.AgdxKvGetCode, nil, walkingOnce()); err == nil {
		t.Fatal("a cluster that is wholly unreachable must be reported")
	}

	// Two nodes, so two attempts. A roster that counted the repeat would
	// walk a third time to a node it had already given up on.
	if cluster.attempts != 2 {
		t.Fatalf("want one attempt per distinct node, got %d", cluster.attempts)
	}
}

// replyOf is one managed reply carrying code.
func replyOf(t *testing.T, code wire.ResultCode) []byte {
	t.Helper()
	encoded, err := wire.Marshal(wire.NewCommandError(code, "a refusal"))
	if err != nil {
		t.Fatalf("encode the reply: %v", err)
	}
	return encoded
}

// batchOf is a batch frame of these per-item replies.
func batchOf(t *testing.T, items ...[]byte) []byte {
	t.Helper()
	encoded, err := wire.Marshal(wire.BatchReply{Results: items})
	if err != nil {
		t.Fatalf("encode the batch reply: %v", err)
	}
	return encoded
}

func answeredWith(reply []byte) mutationAttempt {
	return mutationAttempt{kind: attemptAnswered, reply: reply}
}

func TestGivenAPendingAttemptWhenALaterOneIsForbiddenThenShouldStayAmbiguous(t *testing.T) {
	attempts := mutationAttemptsFor(wire.AgdxKvSetCode)
	if _, settled, err := attempts.settle(answeredWith(replyOf(t, wire.ResultPending))); settled ||
		err != nil {
		t.Fatalf("a pending outcome is not settled: settled=%v err=%v", settled, err)
	}

	_, _, err := attempts.settle(answeredWith(replyOf(t, wire.ResultForbidden)))

	// The pending attempt may still apply, so the refusal of this one does
	// not say the mutation did not happen.
	var ambiguous *AmbiguousMutationError
	if !errors.As(err, &ambiguous) {
		t.Fatalf("want an ambiguous outcome, got %T: %v", err, err)
	}
}

func TestGivenALostAttemptWhenALaterOneIsRefusedThenShouldStayAmbiguous(t *testing.T) {
	attempts := mutationAttemptsFor(wire.AgdxKvSetCode)
	if _, settled, err := attempts.settle(
		mutationAttempt{kind: attemptLost, reason: "connection reset"},
	); settled || err != nil {
		t.Fatalf("a lost attempt is not settled: settled=%v err=%v", settled, err)
	}

	_, _, err := attempts.settle(mutationAttempt{
		kind: attemptRefused, err: &InvalidError{Reason: "refused"}})

	var ambiguous *AmbiguousMutationError
	if !errors.As(err, &ambiguous) {
		t.Fatalf("want an ambiguous outcome, got %T: %v", err, err)
	}
}

func TestGivenAFirstAttemptThatIsRefusedThenShouldReportTheRefusalItself(t *testing.T) {
	attempts := mutationAttemptsFor(wire.AgdxKvSetCode)
	refusal := &InvalidError{Reason: "refused"}

	_, _, err := attempts.settle(mutationAttempt{kind: attemptRefused, err: refusal})

	// Nothing came before it, so the refusal is the whole answer rather than
	// an ambiguity. Reporting it as ambiguous would send callers looking for
	// an outcome that was never in doubt.
	if !errors.Is(err, error(refusal)) {
		t.Fatalf("want the refusal itself, got %T: %v", err, err)
	}
}

func TestGivenABatchItemLeftUnresolvedWhenALaterAttemptRefusesItThenShouldStayAmbiguous(t *testing.T) {
	attempts := mutationAttemptsFor(wire.AgdxBatchCode)
	if _, settled, err := attempts.settle(answeredWith(batchOf(t,
		replyOf(t, wire.ResultOk), replyOf(t, wire.ResultPending)))); settled || err != nil {
		t.Fatalf("an unresolved item leaves the frame open: settled=%v err=%v", settled, err)
	}

	_, _, err := attempts.settle(answeredWith(batchOf(t,
		replyOf(t, wire.ResultOk), replyOf(t, wire.ResultForbidden))))

	var ambiguous *AmbiguousMutationError
	if !errors.As(err, &ambiguous) {
		t.Fatalf("want an ambiguous outcome, got %T: %v", err, err)
	}
	if !strings.Contains(ambiguous.Reason, "item 1") {
		t.Errorf("want the unresolved item named, got %q", ambiguous.Reason)
	}
}

func TestGivenABatchWhoseItemsAreAllFinalWhenSettledThenShouldSettleWithIt(t *testing.T) {
	attempts := mutationAttemptsFor(wire.AgdxBatchCode)
	frame := batchOf(t, replyOf(t, wire.ResultForbidden), replyOf(t, wire.ResultOk))

	reply, settled, err := attempts.settle(answeredWith(frame))
	if err != nil || !settled {
		t.Fatalf("a definite refusal of one item is final: settled=%v err=%v", settled, err)
	}
	if string(reply) != string(frame) {
		t.Fatal("the frame is handed back as it arrived")
	}

	// Only the batch code settles item by item. Another code reads those
	// bytes as its surface's own reply.
	single := mutationAttemptsFor(wire.AgdxKvSetCode)
	pending := batchOf(t, replyOf(t, wire.ResultPending))
	if _, settled, err := single.settle(answeredWith(pending)); !settled || err != nil {
		t.Fatalf("not a batch code: settled=%v err=%v", settled, err)
	}
}
