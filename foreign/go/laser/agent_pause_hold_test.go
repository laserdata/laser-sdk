package laser_test

import (
	"context"
	"testing"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// A member whose assignment names no partition yet is still reading the
// session's work, so what it holds for the session runs before the new record.
func TestGivenAnAssignmentNamingNoPartitionWhenAResumedSessionsWorkArrivesThenShouldHandleTheHeldRecordFirst(t *testing.T) {
	server := pausedServer(t, controlRecord(t, wire.OperationSessionPause, 0))
	held := workRecord(t, 41, "held work")
	server.seedHistory(wire.AgentSessions, 0, 0, held.Message)
	server.aliasNumeric(3, wire.AgentSessions)

	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{EnforceControl: true})
	handler := &handling{}
	queued := sourceOf(held, workRecord(t, 42, "new work"))
	queued.beforeNext = func(index int) {
		if index != 1 {
			return
		}
		server.seedHistory(wire.AgentControl, 0, 0,
			controlRecord(t, wire.OperationSessionPause, 0),
			controlRecord(t, wire.OperationSessionResume, 1))
		waitForResume(t, runtime, conversation(1))
	}
	source := &assignedSource{queuedSource: queued}

	if err := runSource(t, runtime, source, handler, drainedOf(queued)); err != nil {
		t.Fatalf("run: %v", err)
	}
	seen := handler.seen()
	if len(seen) != 2 || seen[0] != "held work" || seen[1] != "new work" {
		t.Fatalf("want the held record handled before the new one, got %v", seen)
	}
}

// An approval that expires and is asked again must expire again, or the
// session would wait forever on a prompt nobody answers.
func TestGivenAnExpiredApprovalWhenTheHandlerAsksAgainThenShouldExpireTheNewPromptToo(t *testing.T) {
	work := workRecord(t, 41, "risky")
	server := liveServer(t, work)
	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{
		EnforceControl: true,
		Retry:          laser.RetryPolicy{MaxAttempts: 3, BaseDelay: time.Millisecond},
	})
	handler := &gating{approver: agentID(t, "boss"), timeout: 20 * time.Millisecond}

	prompted := func() bool { return len(promptsOn(t, server, wire.AgentSessions)) >= 3 }
	if err := runSource(t, runtime, sourceOf(work), handler, prompted); err != nil {
		t.Fatalf("run: %v", err)
	}
	if held := parkedOn(t, server, wire.OperationSessionParked); len(held) != 1 {
		t.Fatalf("want the record parked once across its prompts, got %d", len(held))
	}
}

// A decision on an approval whose own record can no longer be read back still
// settles the prompt, so the work held behind it is handed back.
func TestGivenAnApprovalWhoseRecordIsGoneWhenDecidedThenShouldHandBackTheWorkBehindIt(t *testing.T) {
	work := workRecord(t, 41, "risky")
	later := workRecord(t, 42, "later")
	// Only the later record is readable, the way retention leaves it.
	server := liveServer(t, later)
	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{EnforceControl: true})
	handler := &gating{
		approver: agentID(t, "boss"), timeout: time.Minute,
		gateOnly: map[string]bool{"risky": true},
	}
	if err := runUntilDrained(t, runtime, sourceOf(work, later), handler); err != nil {
		t.Fatalf("the parking run: %v", err)
	}
	if ran := handler.ran(); len(ran) != 1 {
		t.Fatalf("want only the gated record run before the decision, got %v", ran)
	}

	decision := approvalDecision(t, runtime, work, 43, wire.KindResponse, "go ahead")
	after := workRecord(t, 44, "after")
	if err := runUntilDrained(t, runtime, sourceOf(decision, after), handler); err != nil {
		t.Fatalf("the decision run: %v", err)
	}
	ran := handler.ran()
	if len(ran) != 3 || ran[1] != "later" || ran[2] != "after" {
		t.Fatalf("want the held record and then new work handled, got %v", ran)
	}
}

// A held record being replayed when the runtime is told to stop gets the
// shutdown grace, like a record read off the topic.
func TestGivenAHeldReplayInFlightWhenTheRuntimeStopsThenShouldLetItFinishWithinTheGrace(t *testing.T) {
	server := pausedServer(t,
		controlRecord(t, wire.OperationSessionPause, 0),
		controlRecord(t, wire.OperationSessionResume, 1))
	held := workRecord(t, 41, "held work")
	server.seedHistory(wire.AgentSessions, 0, 0,
		held.Message,
		parkingLaneRecord(t, wire.OperationSessionParked, parkingFor(t, "executor", 41)))
	server.aliasNumeric(3, wire.AgentSessions)
	runtime := runtimeFor(t, server, "executor", laser.RuntimeOptions{EnforceControl: true})
	handler := &lingering{
		entered: make(chan struct{}), release: make(chan struct{}), result: make(chan error, 1),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx, sourceOf(), handler) }()
	select {
	case <-handler.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the held record was never replayed")
	}
	cancel()
	close(handler.release)

	if err := <-handler.result; err != nil {
		t.Fatalf("want the replay to keep its context through the grace, got %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}
	if unparked := parkedOn(t, server, wire.OperationSessionUnparked); len(unparked) != 1 {
		t.Fatalf("want the finished replay written off the lane, got %d", len(unparked))
	}
}

// A verified agent resumes its signed parked record on a decision the approver
// signed, which is the path an unsigned decision is refused on.
func TestGivenVerifiedParkedWorkWhenASignedApprovalArrivesThenShouldResumeIt(t *testing.T) {
	plannerKey, err := laser.GenerateSigningKey()
	if err != nil {
		t.Fatal(err)
	}
	approverKey, err := laser.GenerateSigningKey()
	if err != nil {
		t.Fatal(err)
	}
	registry := laser.NewKeyRegistry()
	registry.Enroll("planner", plannerKey.VerifyingKey())
	registry.Enroll("boss", approverKey.VerifyingKey())
	work := signedRecords(t, func(agdx *laser.Agdx) *laser.AgdxSend {
		return agdx.Command(correlationID(1), []byte("risky")).
			WithTarget(agentID(t, "executor")).Signed(plannerKey)
	}, 41)[0]
	server := liveServer(t, work)
	runtime := runtimeFor(t, server, "executor",
		laser.RuntimeOptions{EnforceControl: true, Verifier: registry})
	handler := &gating{approver: agentID(t, "boss"), timeout: time.Minute}
	if err := runUntilDrained(t, runtime, sourceOf(work), handler); err != nil {
		t.Fatalf("the parking run: %v", err)
	}

	build := &streamServer{}
	approver := laser.FromClient(build, laser.Options{DefaultStream: "agents"})
	correlation := laser.ApprovalCorrelationForTest(runtime.SourcePositionForTest(work))
	if _, err := approver.Agdx(agentID(t, "boss"), conversation(1)).
		Respond(correlation, []byte("approved")).
		WithTarget(agentID(t, "executor")).
		Signed(approverKey).
		Send(context.Background()); err != nil {
		t.Fatalf("build the signed decision: %v", err)
	}
	decision := openDelivered(t, build.sent()[0], 0, 42)
	if err := runUntilDrained(t, runtime, sourceOf(decision), handler); err != nil {
		t.Fatalf("the decision run: %v", err)
	}

	if decided := handler.decided(); len(decided) != 1 || decided[0] != "approved" {
		t.Fatalf("want the signed decision to resume the record, got %v", decided)
	}
	if unparked := parkedOn(t, server, wire.OperationSessionUnparked); len(unparked) != 1 {
		t.Fatalf("want the record written off the lane, got %d", len(unparked))
	}
}

// assignedSource is a queued source that also reports a group assignment.
type assignedSource struct {
	*queuedSource
	partitions []uint32
}

func (source *assignedSource) Partitions() []uint32 { return source.partitions }

// runSource runs the runtime over source until until reports true, then stops it.
func runSource(
	t *testing.T, runtime *laser.AgentRuntime, source laser.RecordSource,
	handler laser.AgentHandler, until func() bool,
) error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx, source, handler) }()
	deadline := time.Now().Add(10 * time.Second)
	for !until() {
		select {
		case err := <-done:
			return err
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("the runtime never reached the awaited state")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("the runtime did not stop")
		return nil
	}
}

func drainedOf(source *queuedSource) func() bool {
	return func() bool {
		select {
		case <-source.drained:
			return true
		default:
			return false
		}
	}
}

// lingering is a handler that waits to be released and then reports whether
// its context was still alive.
type lingering struct {
	entered chan struct{}
	release chan struct{}
	result  chan error
}

func (handler *lingering) Handle(
	ctx context.Context, _ *laser.AgentMessage, _ *laser.AgentCtx,
) error {
	close(handler.entered)
	<-handler.release
	handler.result <- ctx.Err()
	return ctx.Err()
}
