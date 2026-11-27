package bdd_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	bdd "github.com/laserdata/laser-sdk/bdd/go"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// TestGivenTheSharedWorkflowScenariosWhenRunThenShouldHold runs the
// declared-steps workflow feature: dependency order, a rejecting verifier,
// saga compensation, a silent target's deadline failure, and (now that the
// client advertises agent presence, see agent_test.go) routing a step by
// capability against the registry an advertising agent publishes into.
func TestGivenTheSharedWorkflowScenariosWhenRunThenShouldHold(t *testing.T) {
	runFeature(t, "workflow.feature", time.Minute, workflowStep)
}

// TestGivenTheSharedWorkflowLocalScenariosWhenRunThenShouldHold runs the
// local-steps workflow feature: in-process steps, a resume that never
// re-runs a recorded step, and compensation of local steps.
func TestGivenTheSharedWorkflowLocalScenariosWhenRunThenShouldHold(t *testing.T) {
	runFeature(t, "workflow_local.feature", time.Minute, workflowLocalStep)
}

// workflowReceipt is one command body a spawned workflow agent saw, and which
// agent saw it.
type workflowReceipt struct {
	agent string
	body  string
}

// workflowFlow is what the workflow scenarios' steps build up: what each
// answering agent received, the last run's outcome or error, the run id a
// resume reuses, and the local step bodies and compensations that ran.
type workflowFlow struct {
	mutex         sync.Mutex
	received      []workflowReceipt
	localRuns     []string
	compensations []string

	outcome *laser.WorkflowOutcome
	err     error
	runID   wire.ConversationID
	// functionLabels are the step labels the last workflow-function run
	// called, so a resume of that run calls the same ones in the same
	// order. A resume that called different steps would be a different
	// workflow wearing the same run id.
	functionLabels []string
}

// flowOf is the scenario's one workflowFlow, created on first use.
func flowOf(state *world) *workflowFlow {
	if existing, found := state.get("workflow"); found {
		return existing.(*workflowFlow)
	}
	flow := &workflowFlow{}
	state.put("workflow", flow)
	return flow
}

func (flow *workflowFlow) record(agent, body string) {
	flow.mutex.Lock()
	flow.received = append(flow.received, workflowReceipt{agent: agent, body: body})
	flow.mutex.Unlock()
}

func (flow *workflowFlow) recordLocalRun(label string) {
	flow.mutex.Lock()
	flow.localRuns = append(flow.localRuns, label)
	flow.mutex.Unlock()
}

func (flow *workflowFlow) recordCompensation(label string) {
	flow.mutex.Lock()
	flow.compensations = append(flow.compensations, label)
	flow.mutex.Unlock()
}

func (flow *workflowFlow) receivedBy(agent string) []string {
	flow.mutex.Lock()
	defer flow.mutex.Unlock()
	var found []string
	for _, receipt := range flow.received {
		if receipt.agent == agent {
			found = append(found, receipt.body)
		}
	}
	return found
}

func (flow *workflowFlow) localRunsSnapshot() []string {
	flow.mutex.Lock()
	defer flow.mutex.Unlock()
	return append([]string(nil), flow.localRuns...)
}

func (flow *workflowFlow) compensationsSnapshot() []string {
	flow.mutex.Lock()
	defer flow.mutex.Unlock()
	return append([]string(nil), flow.compensations...)
}

// workflowStep runs one step of workflow.feature.
func workflowStep(ctx context.Context, state *world, step bdd.Step) error {
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
	case strings.Contains(text, "advertising the skill") && strings.Contains(text, "answers workflow steps"):
		if len(arguments) != 2 {
			return fmt.Errorf("want an agent and a skill, got %v", arguments)
		}
		return spawnWorkflowAgent(ctx, state, flow, arguments[0], false, arguments[1])

	case strings.Contains(text, "answers workflow steps on the session topic"):
		if len(arguments) != 1 {
			return fmt.Errorf("want one agent, got %v", arguments)
		}
		return spawnWorkflowAgent(ctx, state, flow, arguments[0], false, "")

	case strings.Contains(text, "fails every workflow step on the session topic"):
		if len(arguments) != 1 {
			return fmt.Errorf("want one agent, got %v", arguments)
		}
		return spawnWorkflowAgent(ctx, state, flow, arguments[0], true, "")

	case strings.Contains(text, "runs \"") && strings.Contains(text, "after"):
		return threeRoutedSteps(ctx, state, flow, arguments)

	case strings.Contains(text, "on an agent with the skill"):
		return skillRoutedStep(ctx, state, flow, arguments)

	case strings.Contains(text, "with a verifier that rejects every reply"):
		return rejectedVerifierStep(ctx, state, flow, arguments)

	case strings.Contains(text, "each with a compensation"):
		return compensatedSteps(ctx, state, flow, arguments)

	case strings.Contains(text, "with a deadline of"):
		return silentStep(ctx, state, flow, arguments, numbersOf(step))

	case strings.HasPrefix(text, "the workflow fails at step"):
		if len(arguments) != 2 {
			return fmt.Errorf("want a step and a reason, got %v", arguments)
		}
		return workflowFailsAtStep(flow, arguments[0], arguments[1])

	case strings.HasPrefix(text, "the failure names the target"):
		if len(arguments) != 1 {
			return fmt.Errorf("want one target, got %v", arguments)
		}
		return workflowFailureNamesTarget(flow, arguments[0], numbersOf(step))

	case strings.HasPrefix(text, "agent") && strings.Contains(text, "received the compensations"):
		if len(arguments) != 3 {
			return fmt.Errorf("want an agent and two compensations, got %v", arguments)
		}
		return workflowReceivedCompensations(flow, arguments[0], arguments[1], arguments[2])

	default:
		return fmt.Errorf("this runner does not implement the step")
	}
}

// workflowLocalStep runs one step of workflow_local.feature.
func workflowLocalStep(ctx context.Context, state *world, step bdd.Step) error {
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
	case strings.Contains(text, "resumes the same run of local steps"):
		if len(arguments) != 1 {
			return fmt.Errorf("want one workflow name, got %v", arguments)
		}
		return resumedLocalSteps(ctx, state, flow, arguments[0])

	case strings.Contains(text, "fails once"):
		return localStepsFailingOnce(ctx, state, flow, arguments)

	case strings.Contains(text, "with compensations and then"):
		return compensatedLocalSteps(ctx, state, flow, arguments)

	case strings.Contains(text, "runs the local steps"):
		return plainLocalSteps(ctx, state, flow, arguments)

	case strings.HasPrefix(text, "the local step bodies ran"):
		return localStepBodiesRan(flow, arguments)

	case strings.HasPrefix(text, "the workflow fails with the local step error"):
		if len(arguments) != 1 {
			return fmt.Errorf("want one expected error, got %v", arguments)
		}
		return workflowFailsWithLocalStepError(flow, arguments[0])

	case strings.HasPrefix(text, "the local compensations ran"):
		if len(arguments) != 2 {
			return fmt.Errorf("want two compensations, got %v", arguments)
		}
		return localCompensationsRan(flow, arguments[0], arguments[1])

	default:
		return fmt.Errorf("this runner does not implement the step")
	}
}

// workflowCommonThen runs the Then steps both features share, reporting
// whether it recognised the step.
func workflowCommonThen(flow *workflowFlow, step bdd.Step) (bool, error) {
	text := step.Text
	arguments := argumentsOf(step)
	switch {
	case strings.HasPrefix(text, "the workflow completed the steps"):
		return true, workflowCompletedInOrder(flow, arguments)

	case strings.HasPrefix(text, "the workflow output"):
		if len(arguments) != 2 {
			return true, fmt.Errorf("want a label and an expected output, got %v", arguments)
		}
		return true, workflowOutput(flow, arguments[0], arguments[1])
	}
	return false, nil
}

// spawnWorkflowAgent opens a standalone consumer over the session lane that
// answers (or, when fail, rejects) every command it sees with "<body> done by
// <name>", recording every body it was asked to run. A non-empty skill
// spawns through Laser.SpawnAgent instead of the manual
// Agent/AgentRuntime/Consumer wiring every other workflow agent here uses,
// so the agent's capability card and live presence are actually advertised:
// a skill-routed step's whole point is resolving against the registry that
// advertising populates (see skillRoutedStep).
func spawnWorkflowAgent(
	ctx context.Context, state *world, flow *workflowFlow, name string, fail bool, skill string,
) error {
	identity, err := bridgeAgent(name)
	if err != nil {
		return err
	}
	handler := laser.HandlerFunc(func(
		ctx context.Context, message *laser.AgentMessage, agent *laser.AgentCtx,
	) error {
		received := string(message.Body())
		flow.record(name, received)
		if fail {
			return &laser.InvalidError{Reason: fmt.Sprintf("%s fails every step", name)}
		}
		return agent.Respond(ctx, []byte(received+" done by "+name))
	})
	if skill != "" {
		return spawnAdvertisingWorkflowAgent(ctx, state, identity, skill, handler)
	}
	gate := state.client.Agent(identity, laser.AgentOptions{})
	runtime, err := state.client.AgentRuntime(gate, laser.RuntimeOptions{
		Topic: wire.AgentSessions, RespondOn: wire.AgentSessions,
	})
	if err != nil {
		return err
	}
	partitions, err := allPartitions(ctx, state, wire.AgentSessions)
	if err != nil {
		return err
	}
	source, err := state.topic(wire.AgentSessions).Consumer(
		ctx, fmt.Sprintf("bdd-workflow-%s-%d", name, time.Now().UnixNano()),
		laser.ConsumerOptions{Partitions: partitions},
	)
	if err != nil {
		return err
	}
	runAgentRuntime(state, runtime, source, handler, name)
	return nil
}

// spawnAdvertisingWorkflowAgent spawns identity through Laser.SpawnAgent with
// skill as its one advertised capability, waits for it to join and start
// reading, and registers its shutdown as test cleanup -- the AgentHandle
// lifecycle in place of runAgentRuntime's manual cancel-and-wait, since
// SpawnAgent owns both the consumer and the card/presence advertise this
// scenario needs.
func spawnAdvertisingWorkflowAgent(
	ctx context.Context, state *world, identity wire.AgentID, skill string, handler laser.AgentHandler,
) error {
	handle, err := state.client.SpawnAgent(ctx, identity, laser.SpawnOptions{
		RuntimeOptions: laser.RuntimeOptions{Topic: wire.AgentSessions, RespondOn: wire.AgentSessions},
		Capabilities:   []wire.CapabilityDescriptor{{SkillID: skill}},
	}, handler)
	if err != nil {
		return err
	}
	readyCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := handle.Ready(readyCtx); err != nil {
		return err
	}
	state.t.Cleanup(func() { _ = handle.Shutdown(context.Background()) })
	return nil
}

// skillRoutedStep runs a one-step workflow whose step is routed by skill
// rather than to a named agent, resolving against a fresh read of the
// registry the skill's advertising agent (spawnAdvertisingWorkflowAgent)
// published its card into.
func skillRoutedStep(ctx context.Context, state *world, flow *workflowFlow, arguments []string) error {
	if len(arguments) != 3 {
		return fmt.Errorf("want a workflow name, a label and a skill, got %v", arguments)
	}
	source, err := bridgeAgent(arguments[0])
	if err != nil {
		return err
	}
	label, skill := arguments[1], arguments[2]
	registry, err := refreshedRegistry(ctx, state)
	if err != nil {
		return err
	}
	outcome, runErr := state.client.Workflow(source, wire.AgentSessions).
		Registry(registry).
		Step(label, laser.ToCapable(skill, laser.RouteAny), routedBody(label)).
		Run(ctx)
	flow.outcome, flow.err = outcome, runErr
	return nil
}

// refreshedRegistry reads the registry topic once, which a step's
// capability route resolves against. The agent(s) advertising into it must
// already have published their card by the time this runs.
func refreshedRegistry(ctx context.Context, state *world) (*laser.AgentRegistry, error) {
	cursor, err := state.topic(wire.AgentRegistry).Replay()
	if err != nil {
		return nil, err
	}
	registry := laser.NewAgentRegistry()
	if err := registry.Refresh(ctx, cursor); err != nil {
		return nil, err
	}
	return registry, nil
}

// routedBody is a directed step's payload: the step's own label, which a
// spawned workflow agent echoes back as "<label> done by <agent>".
func routedBody(label string) laser.StepFunc {
	return func(context.Context, map[string][]byte) ([]byte, error) {
		return []byte(label), nil
	}
}

// undoRoutedBody is a directed step's compensation payload.
func undoRoutedBody(label string) laser.StepFunc {
	return func(context.Context, map[string][]byte) ([]byte, error) {
		return []byte("undo " + label), nil
	}
}

func threeRoutedSteps(ctx context.Context, state *world, flow *workflowFlow, arguments []string) error {
	if len(arguments) != 7 {
		return fmt.Errorf("want name, three labels, two dependencies and an agent, got %v", arguments)
	}
	source, err := bridgeAgent(arguments[0])
	if err != nil {
		return err
	}
	target, err := bridgeAgent(arguments[6])
	if err != nil {
		return err
	}
	first, second, secondAfter, third, thirdAfter := arguments[1], arguments[2], arguments[3], arguments[4], arguments[5]
	outcome, runErr := state.client.Workflow(source, wire.AgentSessions).
		Step(first, laser.To(target), routedBody(first)).
		Step(second, laser.To(target), routedBody(second)).After(secondAfter).
		Step(third, laser.To(target), routedBody(third)).After(thirdAfter).
		Run(ctx)
	flow.outcome, flow.err = outcome, runErr
	return nil
}

func rejectedVerifierStep(ctx context.Context, state *world, flow *workflowFlow, arguments []string) error {
	if len(arguments) != 3 {
		return fmt.Errorf("want name, label and agent, got %v", arguments)
	}
	source, err := bridgeAgent(arguments[0])
	if err != nil {
		return err
	}
	target, err := bridgeAgent(arguments[2])
	if err != nil {
		return err
	}
	label := arguments[1]
	outcome, runErr := state.client.Workflow(source, wire.AgentSessions).
		Step(label, laser.To(target), routedBody(label)).
		VerifyWith(func(context.Context, []byte) (bool, error) { return false, nil }).
		Run(ctx)
	flow.outcome, flow.err = outcome, runErr
	return nil
}

func compensatedSteps(ctx context.Context, state *world, flow *workflowFlow, arguments []string) error {
	if len(arguments) != 6 {
		return fmt.Errorf("want name, two labels, an agent, a third label and a breaker, got %v", arguments)
	}
	source, err := bridgeAgent(arguments[0])
	if err != nil {
		return err
	}
	agent, err := bridgeAgent(arguments[3])
	if err != nil {
		return err
	}
	breaker, err := bridgeAgent(arguments[5])
	if err != nil {
		return err
	}
	first, second, third := arguments[1], arguments[2], arguments[4]
	outcome, runErr := state.client.Workflow(source, wire.AgentSessions).
		Step(first, laser.To(agent), routedBody(first)).CompensateWith(undoRoutedBody(first)).
		Step(second, laser.To(agent), routedBody(second)).CompensateWith(undoRoutedBody(second)).
		Step(third, laser.To(breaker), routedBody(third)).CompensateWith(undoRoutedBody(third)).
		Run(ctx)
	flow.outcome, flow.err = outcome, runErr
	return nil
}

func silentStep(
	ctx context.Context, state *world, flow *workflowFlow, arguments []string, deadlines []uint64,
) error {
	if len(arguments) != 3 || len(deadlines) != 1 {
		return fmt.Errorf("want name, label, agent and one deadline, got %v / %v", arguments, deadlines)
	}
	source, err := bridgeAgent(arguments[0])
	if err != nil {
		return err
	}
	target, err := bridgeAgent(arguments[2])
	if err != nil {
		return err
	}
	label := arguments[1]
	outcome, runErr := state.client.Workflow(source, wire.AgentSessions).
		Step(label, laser.To(target), routedBody(label)).
		Deadline(time.Duration(deadlines[0]) * time.Millisecond).
		Run(ctx)
	flow.outcome, flow.err = outcome, runErr
	return nil
}

// localBody is a local step's body: it records that it ran before deciding
// whether to fail, matching every call site (including a retried one) rather
// than only a successful one.
func localBody(flow *workflowFlow, label string, fail bool) laser.StepFunc {
	return func(context.Context, map[string][]byte) ([]byte, error) {
		flow.recordLocalRun(label)
		if fail {
			return nil, fmt.Errorf("%s failed", label)
		}
		return []byte(label + " done in process"), nil
	}
}

// localUndo is a local step's compensation: the call itself is the undo,
// nothing is dispatched anywhere.
func localUndo(flow *workflowFlow, label string) laser.StepFunc {
	return func(context.Context, map[string][]byte) ([]byte, error) {
		flow.recordCompensation("undo " + label)
		return nil, nil
	}
}

func plainLocalSteps(ctx context.Context, state *world, flow *workflowFlow, arguments []string) error {
	if len(arguments) != 4 {
		return fmt.Errorf("want name and three labels, got %v", arguments)
	}
	source, err := bridgeAgent(arguments[0])
	if err != nil {
		return err
	}
	first, second, third := arguments[1], arguments[2], arguments[3]
	outcome, runErr := state.client.Workflow(source, wire.AgentSessions).
		StepLocal(first, localBody(flow, first, false)).
		StepLocal(second, localBody(flow, second, false)).
		StepLocal(third, localBody(flow, third, false)).
		Run(ctx)
	flow.outcome, flow.err = outcome, runErr
	return nil
}

func localStepsFailingOnce(ctx context.Context, state *world, flow *workflowFlow, arguments []string) error {
	if len(arguments) != 5 {
		return fmt.Errorf("want name, three labels and a failing one, got %v", arguments)
	}
	source, err := bridgeAgent(arguments[0])
	if err != nil {
		return err
	}
	first, second, third, failing := arguments[1], arguments[2], arguments[3], arguments[4]
	flow.runID = laser.NewConversationID()
	outcome, runErr := state.client.Workflow(source, wire.AgentSessions).
		RunID(flow.runID).
		StepLocal(first, localBody(flow, first, first == failing)).
		StepLocal(second, localBody(flow, second, second == failing)).
		StepLocal(third, localBody(flow, third, third == failing)).
		Run(ctx)
	flow.outcome, flow.err = outcome, runErr
	if runErr == nil {
		return fmt.Errorf("want the run to fail at step %q, it completed instead", failing)
	}
	return nil
}

func resumedLocalSteps(ctx context.Context, state *world, flow *workflowFlow, name string) error {
	source, err := bridgeAgent(name)
	if err != nil {
		return err
	}
	outcome, runErr := state.client.Workflow(source, wire.AgentSessions).
		RunID(flow.runID).
		StepLocal("generate", localBody(flow, "generate", false)).
		StepLocal("process", localBody(flow, "process", false)).
		StepLocal("finish", localBody(flow, "finish", false)).
		Run(ctx)
	flow.outcome, flow.err = outcome, runErr
	return nil
}

func compensatedLocalSteps(ctx context.Context, state *world, flow *workflowFlow, arguments []string) error {
	if len(arguments) != 4 {
		return fmt.Errorf("want name, two labels and a failing one, got %v", arguments)
	}
	source, err := bridgeAgent(arguments[0])
	if err != nil {
		return err
	}
	first, second, failing := arguments[1], arguments[2], arguments[3]
	outcome, runErr := state.client.Workflow(source, wire.AgentSessions).
		StepLocal(first, localBody(flow, first, false)).CompensateWith(localUndo(flow, first)).
		StepLocal(second, localBody(flow, second, false)).CompensateWith(localUndo(flow, second)).
		StepLocal(failing, localBody(flow, failing, true)).
		Run(ctx)
	flow.outcome, flow.err = outcome, runErr
	return nil
}

func workflowCompletedInOrder(flow *workflowFlow, labels []string) error {
	if flow.err != nil {
		return fmt.Errorf("want the workflow to complete, got %v", flow.err)
	}
	if !equalStrings(flow.outcome.CompletedSteps, labels) {
		return fmt.Errorf("want completed steps %v, got %v", labels, flow.outcome.CompletedSteps)
	}
	if flow.outcome.Status != wire.SessionCompleted {
		return fmt.Errorf("want status completed, got %v", flow.outcome.Status)
	}
	return nil
}

func workflowOutput(flow *workflowFlow, label, expected string) error {
	if flow.err != nil {
		return fmt.Errorf("want the workflow to complete, got %v", flow.err)
	}
	text, ok := flow.outcome.Text(label)
	if !ok || text != expected {
		return fmt.Errorf("want step %q output %q, got %q (present=%v)", label, expected, text, ok)
	}
	return nil
}

// stepFailureReason maps the shared scenarios' snake_case reason text to the
// StepFailure it names.
func stepFailureReason(text string) (laser.StepFailure, error) {
	switch text {
	case "unverified":
		return laser.StepUnverified, nil
	case "failed":
		return laser.StepFailed, nil
	case "timed_out":
		return laser.StepTimedOut, nil
	case "not_consumed":
		return laser.StepNotConsumed, nil
	case "no_completion":
		return laser.StepNoCompletion, nil
	default:
		return 0, fmt.Errorf("unknown step failure reason %q", text)
	}
}

func workflowFailsAtStep(flow *workflowFlow, label, reasonText string) error {
	var failed *laser.StepFailedError
	if !errors.As(flow.err, &failed) {
		return fmt.Errorf("want a StepFailedError, got %v", flow.err)
	}
	if failed.Step != label {
		return fmt.Errorf("want the failure at step %q, got %q", label, failed.Step)
	}
	reason, err := stepFailureReason(reasonText)
	if err != nil {
		return err
	}
	if failed.Reason != reason {
		return fmt.Errorf("want reason %v, got %v", reason, failed.Reason)
	}
	return nil
}

func workflowFailureNamesTarget(flow *workflowFlow, target string, deadlines []uint64) error {
	var failed *laser.StepFailedError
	if !errors.As(flow.err, &failed) {
		return fmt.Errorf("want a StepFailedError, got %v", flow.err)
	}
	if !strings.Contains(failed.Target, target) {
		return fmt.Errorf("want the failure to name target %q, got %q", target, failed.Target)
	}
	if len(deadlines) != 1 {
		return fmt.Errorf("want one deadline, got %v", deadlines)
	}
	want := time.Duration(deadlines[0]) * time.Millisecond
	if failed.Deadline != want {
		return fmt.Errorf("want deadline %s, got %s", want, failed.Deadline)
	}
	return nil
}

func workflowReceivedCompensations(flow *workflowFlow, agent, first, second string) error {
	var undos []string
	for _, body := range flow.receivedBy(agent) {
		if strings.HasPrefix(body, "undo ") {
			undos = append(undos, body)
		}
	}
	want := []string{first, second}
	if !equalStrings(undos, want) {
		return fmt.Errorf("want compensations %v, got %v", want, undos)
	}
	return nil
}

func localStepBodiesRan(flow *workflowFlow, labels []string) error {
	got := flow.localRunsSnapshot()
	if !equalStrings(got, labels) {
		return fmt.Errorf("want local step bodies %v, got %v", labels, got)
	}
	return nil
}

func workflowFailsWithLocalStepError(flow *workflowFlow, expected string) error {
	if flow.err == nil {
		return fmt.Errorf("want the workflow to fail")
	}
	if !strings.Contains(flow.err.Error(), expected) {
		return fmt.Errorf("want the error to mention %q, got %v", expected, flow.err)
	}
	return nil
}

func localCompensationsRan(flow *workflowFlow, first, second string) error {
	got := flow.compensationsSnapshot()
	want := []string{first, second}
	if !equalStrings(got, want) {
		return fmt.Errorf("want local compensations %v, got %v", want, got)
	}
	return nil
}
