package laser

import (
	"context"
	"fmt"
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
