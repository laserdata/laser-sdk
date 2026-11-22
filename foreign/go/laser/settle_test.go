package laser_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
)

// settleServer records the operations a caller settled and can refuse them.
type settleServer struct {
	clusterOfOne
	mutex     sync.Mutex
	resolved  []uint64
	abandoned []uint64
	refuse    error
}

func (server *settleServer) Close() error { return nil }

func (server *settleServer) ResolveOperation(_ context.Context, id uint64) error {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	if server.refuse != nil {
		return server.refuse
	}
	server.resolved = append(server.resolved, id)
	return nil
}

func (server *settleServer) AbandonOperation(_ context.Context, id uint64) error {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	server.abandoned = append(server.abandoned, id)
	return nil
}

func (server *settleServer) settled() ([]uint64, []uint64) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	return append([]uint64(nil), server.resolved...),
		append([]uint64(nil), server.abandoned...)
}

func TestGivenAnUncertainOperationWhenResolvedThenShouldAskTheDeploymentThatHoldsIt(t *testing.T) {
	server := &settleServer{}
	handle := laser.FromClient(server, laser.Options{DefaultStream: "fleet"})

	if err := handle.ResolveOperation(context.Background(), 42); err != nil {
		t.Fatalf("resolve: %v", err)
	}

	resolved, _ := server.settled()
	if len(resolved) != 1 || resolved[0] != 42 {
		t.Fatalf("want the operation replayed, got %v", resolved)
	}
}

func TestGivenAnOperationTheClientNeverProvedWhenResolvedThenShouldReportIt(t *testing.T) {
	server := &settleServer{refuse: ierror.OperationNotRetained{OperationID: 42}}
	handle := laser.FromClient(server, laser.Options{DefaultStream: "fleet"})

	err := handle.ResolveOperation(context.Background(), 42)

	// An outcome the client cannot prove is not an outcome. Reporting it as
	// settled would be the one answer a caller must never be given.
	var transport *laser.TransportError
	if !errors.As(err, &transport) {
		t.Fatalf("want a TransportError, got %T: %v", err, err)
	}
	if !errors.Is(err, ierror.OperationNotRetained{}) {
		t.Error("want the Iggy refusal kept underneath")
	}
}

func TestGivenABlockedLaneWhenAbandonedThenShouldFreeItWithoutClaimingAnOutcome(t *testing.T) {
	server := &settleServer{}
	handle := laser.FromClient(server, laser.Options{DefaultStream: "fleet"})

	if err := handle.AbandonOperation(context.Background(), 7); err != nil {
		t.Fatalf("abandon: %v", err)
	}

	_, abandoned := server.settled()
	if len(abandoned) != 1 || abandoned[0] != 7 {
		t.Fatalf("want the operation abandoned, got %v", abandoned)
	}
}

func TestGivenAFailureThatLeftARequestUncertainWhenReadThenShouldNameTheOperation(t *testing.T) {
	failure := &laser.TransportError{
		Reason: "publish", Cause: ierror.OperationUncertain{OperationID: 99}}

	operation, named := failure.UncertainOperation()

	// The id is the whole remedy: without it the lane stays blocked and
	// nothing can prove what happened.
	if !named || operation != 99 {
		t.Fatalf("want operation 99 named, got %d named=%v", operation, named)
	}
}

func TestGivenABlockedLaneFailureWhenReadThenShouldNameTheOperationBlockingIt(t *testing.T) {
	failure := &laser.TransportError{
		Reason: "publish", Cause: ierror.LaneBlocked{OperationID: 99}}

	if operation, named := failure.UncertainOperation(); !named || operation != 99 {
		t.Fatalf("want operation 99 named, got %d named=%v", operation, named)
	}
}

func TestGivenAnOrdinaryFailureWhenReadThenShouldNameNoOperation(t *testing.T) {
	failure := &laser.TransportError{Reason: "publish", Cause: ierror.Disconnected{}}

	if operation, named := failure.UncertainOperation(); named {
		t.Fatalf("want no operation named, got %d", operation)
	}
}

func TestGivenATransportFailureWhenItsCodeIsReadThenShouldGiveTheIggyOne(t *testing.T) {
	failure := &laser.TransportError{Reason: "publish", Cause: ierror.Disconnected{}}

	code, coded := failure.IggyErrorCode()

	// The code is the server's own vocabulary, which is what lets a caller
	// branch on the failure rather than on its message text.
	disconnected := ierror.Disconnected{}
	if !coded || code != disconnected.Code() {
		t.Fatalf("want the Iggy code, got %v coded=%v", code, coded)
	}
}

func TestGivenAFailureThatIsNotIggysWhenItsCodeIsReadThenShouldSaySo(t *testing.T) {
	failure := &laser.TransportError{Reason: "publish", Cause: errors.New("a torn cable")}

	if code, coded := failure.IggyErrorCode(); coded {
		t.Fatalf("want no code, got %v", code)
	}
}

var _ iggcon.Client = (*settleServer)(nil)
