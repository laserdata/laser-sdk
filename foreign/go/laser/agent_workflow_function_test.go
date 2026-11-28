package laser_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
)

func TestGivenARunWithBodyWhenRunThenShouldReturnItsValue(t *testing.T) {
	server := newWorkflowServer()
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})

	outcome, err := laser.RunWith(context.Background(),
		handle.Workflow(agentID(t, "pipeline"), "agent.sessions").InboxRoute(laser.FixedInbox("agent.sessions")),
		func(ctx context.Context, run *laser.WorkflowRun) (string, error) {
			frames, err := laser.RunStep(ctx, run, "ingest", func(context.Context) (int, error) {
				return 12, nil
			})
			if err != nil {
				return "", err
			}
			return laser.RunStep(ctx, run, "report", func(context.Context) (string, error) {
				return fmt.Sprintf("%d frames", frames), nil
			})
		})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	value, err := laser.WorkflowValue[string](outcome)
	if err != nil {
		t.Fatalf("decode value: %v", err)
	}
	if value != "12 frames" {
		t.Errorf("want `12 frames`, got %q", value)
	}
	if len(outcome.CompletedSteps) != 2 ||
		outcome.CompletedSteps[0] != "ingest" || outcome.CompletedSteps[1] != "report" {
		t.Errorf("want ingest then report, got %v", outcome.CompletedSteps)
	}
}

func TestGivenARunWithBodyThatFailsOnceWhenResumedThenShouldSkipCompletedSteps(t *testing.T) {
	server := newWorkflowServer()
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	runID := laser.NewConversationID()
	var generateRuns int32

	run := func(shouldFail bool) (*laser.WorkflowOutcome, error) {
		workflow := handle.Workflow(agentID(t, "jobs"), "agent.sessions").InboxRoute(laser.FixedInbox("agent.sessions")).RunID(runID)
		return laser.RunWith(context.Background(), workflow,
			func(ctx context.Context, run *laser.WorkflowRun) (string, error) {
				_, err := laser.RunStep(ctx, run, "generate", func(context.Context) (string, error) {
					atomic.AddInt32(&generateRuns, 1)
					return "generated", nil
				})
				if err != nil {
					return "", err
				}
				return laser.RunStep(ctx, run, "finish", func(context.Context) (string, error) {
					if shouldFail {
						return "", errors.New("finish failed")
					}
					return "finished", nil
				})
			})
	}

	if _, err := run(true); err == nil {
		t.Fatal("want the first run to fail")
	}
	if generateRuns != 1 {
		t.Fatalf("want generate to run once on the first attempt, got %d", generateRuns)
	}

	outcome, err := run(false)
	if err != nil {
		t.Fatalf("resume the workflow: %v", err)
	}
	if generateRuns != 1 {
		t.Errorf("want generate not to run again on resume, got %d runs", generateRuns)
	}
	value, err := laser.WorkflowValue[string](outcome)
	if err != nil || value != "finished" {
		t.Errorf("want `finished`, got %q (err=%v)", value, err)
	}
}

func TestGivenTwoCallsOfOneLabelWhenRunThenShouldJournalThemUnderNumberedLabels(t *testing.T) {
	server := newWorkflowServer()
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})

	outcome, err := laser.RunWith(context.Background(),
		handle.Workflow(agentID(t, "pipeline"), "agent.sessions").InboxRoute(laser.FixedInbox("agent.sessions")),
		func(ctx context.Context, run *laser.WorkflowRun) (string, error) {
			first, err := laser.RunStep(ctx, run, "branch", func(context.Context) (string, error) {
				return "a", nil
			})
			if err != nil {
				return "", err
			}
			second, err := laser.RunStep(ctx, run, "branch", func(context.Context) (string, error) {
				return "b", nil
			})
			if err != nil {
				return "", err
			}
			return first + second, nil
		})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if _, has := outcome.Outputs["branch"]; !has {
		t.Error("want the first call journaled as `branch`")
	}
	if _, has := outcome.Outputs["branch#2"]; !has {
		t.Error("want the second call journaled as `branch#2`")
	}
}

func TestGivenTwoConcurrentRunStepsWhenRunThenShouldBothCompleteSafely(t *testing.T) {
	server := newWorkflowServer()
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})

	outcome, err := laser.RunWith(context.Background(),
		handle.Workflow(agentID(t, "pipeline"), "agent.sessions").InboxRoute(laser.FixedInbox("agent.sessions")),
		func(ctx context.Context, run *laser.WorkflowRun) (string, error) {
			var wait sync.WaitGroup
			wait.Add(2)
			var leftErr, rightErr error
			go func() {
				defer wait.Done()
				_, leftErr = laser.RunStep(ctx, run, "left", func(context.Context) (string, error) {
					return "left-done", nil
				})
			}()
			go func() {
				defer wait.Done()
				_, rightErr = laser.RunStep(ctx, run, "right", func(context.Context) (string, error) {
					return "right-done", nil
				})
			}()
			wait.Wait()
			if leftErr != nil {
				return "", leftErr
			}
			if rightErr != nil {
				return "", rightErr
			}
			return "done", nil
		})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if _, has := outcome.Outputs["left"]; !has {
		t.Error("want `left` completed")
	}
	if _, has := outcome.Outputs["right"]; !has {
		t.Error("want `right` completed")
	}
}

func TestGivenANonFiniteValueWhenAStepReturnsItThenShouldRefuse(t *testing.T) {
	server := newWorkflowServer()
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})

	_, err := laser.RunWith(context.Background(),
		handle.Workflow(agentID(t, "pipeline"), "agent.sessions").InboxRoute(laser.FixedInbox("agent.sessions")),
		func(ctx context.Context, run *laser.WorkflowRun) (float64, error) {
			return laser.RunStep(ctx, run, "score", func(context.Context) (float64, error) {
				return math.NaN(), nil
			})
		})
	var codec *laser.CodecError
	if !errors.As(err, &codec) {
		t.Fatalf("want a CodecError, got %v", err)
	}
}

func TestGivenAStepLabelWithTheRepeatSeparatorWhenBegunThenShouldRefuse(t *testing.T) {
	server := newWorkflowServer()
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	run, err := handle.Workflow(agentID(t, "pipeline"), "agent.sessions").InboxRoute(laser.FixedInbox("agent.sessions")).Start(context.Background())
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	_, err = run.BeginStep("bad#label", nil)
	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want an InvalidError, got %v", err)
	}
}

func TestGivenAFreshStreamWithNoAgentTopicsWhenARunWithBodyRunsThenShouldBootstrapThemFirst(t *testing.T) {
	server := newWorkflowServer()
	server.missingUntilBootstrapped = true
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})

	outcome, err := laser.RunWith(context.Background(),
		handle.Workflow(agentID(t, "jobs"), "agent.sessions").InboxRoute(laser.FixedInbox("agent.sessions")),
		func(ctx context.Context, run *laser.WorkflowRun) (string, error) {
			return laser.RunStep(ctx, run, "generate", func(context.Context) (string, error) {
				return "generated", nil
			})
		})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if !server.bootstrapped {
		t.Error("want the missing topics to have been bootstrapped before the run wrote anything")
	}
	value, err := laser.WorkflowValue[string](outcome)
	if err != nil || value != "generated" {
		t.Errorf("want `generated`, got %q (err=%v)", value, err)
	}
}
