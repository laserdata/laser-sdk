package laser_test

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	ierror "github.com/apache/iggy/foreign/go/errors"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
)

const missingTopicFix = "the topic does not exist: create the agent topics with Bootstrap, or a memory topic with MemoryTopic"

func TestGivenNoDefaultStreamWhenATopicIsUsedThenShouldNameTheCallsThatSetOne(t *testing.T) {
	handle := streamHandle(&streamServer{}, laser.Options{})

	err := handle.Topic("readings").Ensure(context.Background(), 1, nil)

	want := "no default stream set: work in a scratch stream with ScratchStream(prefix), or pick one with WithDefaultStream(name)"
	if err == nil || err.Error() != want {
		t.Fatalf("got %v, want %q", err, want)
	}
}

func TestGivenAPublishToAMissingTopicWhenItsFailureIsPrintedThenShouldNameTheCallsThatCreateTopics(t *testing.T) {
	failure := func(cause error) string {
		return (&laser.PublishFailure{
			Cause: &laser.TransportError{Reason: "send", Cause: cause}, Stream: "fleet", Topic: "readings"}).Error()
	}
	for _, cause := range []error{ierror.ErrTopicNameNotFound, ierror.ErrStreamNameNotFound} {
		if text := failure(cause); !strings.HasSuffix(text, ": "+missingTopicFix) ||
			!strings.HasPrefix(text, "publish failed to fleet/readings: ") {
			t.Errorf("got %q, want the calls that create a topic named", text)
		}
	}
	if text := failure(ierror.ErrDisconnected); strings.Contains(text, "Bootstrap") {
		t.Errorf("a lost connection must not name topic creation, got %q", text)
	}
}

func TestGivenAStreamWithoutTheSessionLaneWhenASessionWritesThenShouldNameTheCallsThatCreateIt(t *testing.T) {
	missing := laser.MissingLaneForTest("agent.sessions does not exist")

	want := "session: stale session generation: agent.sessions does not exist: " + missingTopicFix
	var stale *laser.StaleLaneError
	if !errors.As(missing, &stale) || missing.Error() != want {
		t.Fatalf("got %v, want %q", missing, want)
	}
}

func TestGivenATimedOutStepWhenItsFailureIsPrintedThenShouldNameItsTargetAndDeadline(t *testing.T) {
	timedOut := &laser.StepFailedError{Run: conversation(1), Step: "triage", Reason: laser.StepTimedOut,
		Target: "agent `worker`", Deadline: 90 * time.Second}
	unverified := &laser.StepFailedError{Run: conversation(1), Step: "triage", Reason: laser.StepUnverified,
		Target: "agent `worker`", Deadline: 90 * time.Second}

	run := conversation(1).String()
	if want := "workflow run " + run + " step `triage` failed: timed_out, target agent `worker`, deadline 90000 ms"; timedOut.Error() != want {
		t.Errorf("got %q, want %q", timedOut.Error(), want)
	}
	if want := "workflow run " + run + " step `triage` failed: unverified"; unverified.Error() != want {
		t.Errorf("got %q, want %q", unverified.Error(), want)
	}
}

func TestGivenAServerThatNeverAnswersTheSignInWhenTheBudgetEndsThenShouldNameTheLoginStage(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	_, err = laser.ConnectWith(context.Background(), "iggy:iggy@"+listener.Addr().String(),
		laser.Options{Timeout: 300 * time.Millisecond})

	var timeout *laser.StageTimeoutError
	if !errors.As(err, &timeout) || err.Error() != "timed out waiting for the Iggy login reply" {
		t.Fatalf("got %v, want the login stage named", err)
	}
}

func TestGivenAConnectStageWhenTheDeploymentIsLaserDataThenShouldNameTheAccessRule(t *testing.T) {
	plain := laser.ConnectAcceptStageForTest("fleet.example")
	managed := laser.ConnectAcceptStageForTest("acme.laserdata.cloud")

	if want := "timed out waiting for the Iggy server to accept the connection"; plain != want {
		t.Errorf("got %q, want %q", plain, want)
	}
	if want := "timed out waiting for the Iggy server to accept the connection: the deployment admits only addresses its access rules allow: add an access rule for this address"; managed != want {
		t.Errorf("got %q, want %q", managed, want)
	}
}
