package bdd_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	bdd "github.com/laserdata/laser-sdk/bdd/go"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// TestGivenTheSharedWorkflowFunctionScenariosWhenRunThenShouldHold runs the
// "plain code" workflow feature: a workflow that is an ordinary function
// calling its steps, journaled per call, so a resumed run returns a recorded
// step's value instead of running it again.
func TestGivenTheSharedWorkflowFunctionScenariosWhenRunThenShouldHold(t *testing.T) {
	runFeature(t, "workflow_function.feature", time.Minute, workflowFunctionStep)
}

func workflowFunctionStep(ctx context.Context, state *world, step bdd.Step) error {
	handled, err := background(ctx, state, step)
	if handled {
		return err
	}
	flow := flowOf(state)
	if handled, err := workflowCommonThen(flow, step); handled {
		return err
	}
	text := step.Text
	arguments := argumentsOf(step)

	switch {
	case strings.Contains(text, "resumes the same run"):
		if len(arguments) != 1 {
			return fmt.Errorf("want one workflow name, got %v", arguments)
		}
		return resumedWorkflowFunction(ctx, state, flow, arguments[0])

	case strings.Contains(text, "calls the step") && strings.Contains(text, "twice and then"):
		if len(arguments) != 3 {
			return fmt.Errorf("want a name, a repeated label and a last one, got %v", arguments)
		}
		return repeatedWorkflowFunctionStep(ctx, state, flow, arguments)

	case strings.Contains(text, "calls the steps") && strings.Contains(text, "fails once"):
		if len(arguments) != 5 {
			return fmt.Errorf("want a name, three labels and a failing one, got %v", arguments)
		}
		return workflowFunctionFailingOnce(ctx, state, flow, arguments)

	case strings.Contains(text, "calls the steps"):
		if len(arguments) != 4 {
			return fmt.Errorf("want a name and three labels, got %v", arguments)
		}
		return plainWorkflowFunction(ctx, state, flow, arguments)

	case strings.HasPrefix(text, "the local step bodies ran"):
		return localStepBodiesRan(flow, arguments)

	case strings.HasPrefix(text, "the workflow fails with the local step error"):
		if len(arguments) != 1 {
			return fmt.Errorf("want one expected error, got %v", arguments)
		}
		return workflowFailsWithLocalStepError(flow, arguments[0])

	case strings.HasPrefix(text, "the workflow function returned"):
		if len(arguments) != 1 {
			return fmt.Errorf("want one expected value, got %v", arguments)
		}
		return workflowFunctionReturned(flow, arguments[0])

	default:
		return fmt.Errorf("this runner does not implement the step")
	}
}

// functionBody is one step of the workflow function: it records that it ran
// and answers "<label> done", failing the first time when it is the step the
// scenario said fails once.
//
// The body records before it decides to fail, because the scenarios assert
// on which bodies ran, and a failed attempt is a body that ran.
func functionBody(
	flow *workflowFlow, label string, failing bool,
) func(context.Context) (string, error) {
	failed := false
	return func(context.Context) (string, error) {
		flow.recordLocalRun(label)
		if failing && !failed {
			failed = true
			return "", errors.New(label + " failed")
		}
		return label + " done", nil
	}
}

// runWorkflowFunction runs a workflow whose body calls labels in order and
// returns the last step's value, which is what the feature's own wording
// describes.
func runWorkflowFunction(
	ctx context.Context, state *world, flow *workflowFlow,
	name string, labels []string, failing string, runID *wire.ConversationID,
) error {
	source, err := bridgeAgent(name)
	if err != nil {
		return err
	}
	bodies := make(map[string]func(context.Context) (string, error), len(labels))
	for _, label := range labels {
		if _, built := bodies[label]; !built {
			bodies[label] = functionBody(flow, label, label == failing)
		}
	}
	flow.functionLabels = append([]string(nil), labels...)
	workflow := state.client.Workflow(source, wire.AgentSessions)
	if runID != nil {
		workflow = workflow.RunID(*runID)
	}
	outcome, runErr := laser.RunWith(ctx, workflow,
		func(ctx context.Context, run *laser.WorkflowRun) (string, error) {
			last := ""
			for _, label := range labels {
				value, err := laser.RunStep(ctx, run, label, bodies[label])
				if err != nil {
					return "", err
				}
				last = value
			}
			return last, nil
		})
	flow.outcome, flow.err = outcome, runErr
	return nil
}

func plainWorkflowFunction(
	ctx context.Context, state *world, flow *workflowFlow, arguments []string,
) error {
	flow.runID = laser.NewConversationID()
	runID := flow.runID
	return runWorkflowFunction(
		ctx, state, flow, arguments[0], arguments[1:], "", &runID)
}

func workflowFunctionFailingOnce(
	ctx context.Context, state *world, flow *workflowFlow, arguments []string,
) error {
	flow.runID = laser.NewConversationID()
	runID := flow.runID
	labels, failing := arguments[1:4], arguments[4]
	if err := runWorkflowFunction(
		ctx, state, flow, arguments[0], labels, failing, &runID); err != nil {
		return err
	}
	if flow.err == nil {
		return fmt.Errorf("want the run to fail at step %q, it completed instead", failing)
	}
	// The scenario that resumes asserts on the failure only through the
	// bodies that ran, so the error is kept rather than reported here.
	return nil
}

// resumedWorkflowFunction runs the same workflow again under the same run
// id, which is what makes a recorded step return its value without running.
func resumedWorkflowFunction(
	ctx context.Context, state *world, flow *workflowFlow, name string,
) error {
	runID := flow.runID
	return runWorkflowFunction(
		ctx, state, flow, name, flow.functionLabels, "", &runID)
}

// repeatedWorkflowFunctionStep calls one label twice and then another, which
// journals the second call as "<label>#2".
func repeatedWorkflowFunctionStep(
	ctx context.Context, state *world, flow *workflowFlow, arguments []string,
) error {
	repeated, last := arguments[1], arguments[2]
	flow.runID = laser.NewConversationID()
	runID := flow.runID
	return runWorkflowFunction(
		ctx, state, flow, arguments[0], []string{repeated, repeated, last}, "", &runID)
}

func workflowFunctionReturned(flow *workflowFlow, expected string) error {
	if flow.err != nil {
		return fmt.Errorf("want the workflow to complete, got %v", flow.err)
	}
	value, err := laser.WorkflowValue[string](flow.outcome)
	if err != nil {
		return fmt.Errorf("read the workflow's value: %w", err)
	}
	if value != expected {
		return fmt.Errorf("want the workflow function to return %q, got %q", expected, value)
	}
	return nil
}
