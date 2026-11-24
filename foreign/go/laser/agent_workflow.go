package laser

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// defaultStepDeadline bounds a step whose caller names none.
const defaultStepDeadline = 30 * time.Second

// StepFunc builds a step's task payload, or a local step's output, from the
// outputs of the steps before it. Bounded by the step's own deadline: a
// build that runs past it fails the step StepTimedOut.
type StepFunc func(ctx context.Context, outputs map[string][]byte) ([]byte, error)

// StepVerifier checks a step's reply before it counts as done. A false
// verdict fails the step StepUnverified. Bounded by the step's own deadline.
type StepVerifier func(ctx context.Context, output []byte) (bool, error)

// StepFailure is why a workflow step did not complete, carried by
// StepFailedError.
type StepFailure int

const (
	// StepTimedOut is a step that did not finish within its deadline: no
	// terminal reply, or a build or verification that ran past it.
	// Retryable.
	StepTimedOut StepFailure = iota
	// StepFailed is a target replying with a terminal error.
	StepFailed
	// StepNotConsumed is the command not consumed within its consumption
	// expiry.
	StepNotConsumed
	// StepUnverified is the step's verifier rejecting the reply.
	StepUnverified
	// StepNoCompletion is no capable agent completing an all-capable step.
	StepNoCompletion
)

// String names the failure the way an error message wants it.
func (reason StepFailure) String() string {
	switch reason {
	case StepTimedOut:
		return "timed out"
	case StepFailed:
		return "failed"
	case StepNotConsumed:
		return "not consumed"
	case StepUnverified:
		return "unverified"
	case StepNoCompletion:
		return "no completion"
	default:
		return "unknown"
	}
}

// Retryable reports whether a later attempt of the same step can succeed.
func (reason StepFailure) Retryable() bool { return reason == StepTimedOut }

// StepFailedError reports a workflow step that did not complete.
type StepFailedError struct {
	Run      wire.ConversationID
	Step     string
	Reason   StepFailure
	Target   string
	Deadline time.Duration
}

func (e *StepFailedError) Error() string {
	return fmt.Sprintf("workflow run %s: step `%s` on %s %s (deadline %s)",
		e.Run, e.Step, e.Target, e.Reason, e.Deadline)
}

// Retryable reports whether a later attempt of the same step can succeed.
func (e *StepFailedError) Retryable() bool { return e.Reason.Retryable() }

// The workflow-specific budget dimensions a BudgetExceededError names,
// alongside BudgetTokens (shared with the session-level budget).
const (
	BudgetWallClock   = "wall_clock"
	BudgetInvocations = "invocations"
)

// JournalTruncatedError reports a resumed run whose journal retention
// trimmed past its start, so which steps completed can no longer be told.
type JournalTruncatedError struct{ Run wire.ConversationID }

func (e *JournalTruncatedError) Error() string {
	return "the workflow journal for run " + e.Run.String() + " was truncated by retention"
}

// WorkflowBudget is a spend ceiling for a workflow, summed from the
// advisory usage on replies and the engine's own counters. Any unset
// dimension is unbounded (the zero value). A breach is a
// BudgetExceededError, read from the engine's running totals.
type WorkflowBudget struct {
	tokens      *uint64
	wallClock   *time.Duration
	invocations *uint32
}

// WithTokens caps the summed input-plus-output tokens across all step
// replies. Only counts the usage a step's reply carries; a plain reply with
// no usage, and an all-capable step's scatter, count zero, so this ceiling
// is advisory and bites only where handlers attach usage.
func (budget WorkflowBudget) WithTokens(tokens uint64) WorkflowBudget {
	budget.tokens = &tokens
	return budget
}

// WithWallClock caps the wall-clock time the whole workflow may run.
func (budget WorkflowBudget) WithWallClock(wallClock time.Duration) WorkflowBudget {
	budget.wallClock = &wallClock
	return budget
}

// WithInvocations caps how many steps the workflow may dispatch.
func (budget WorkflowBudget) WithInvocations(invocations uint32) WorkflowBudget {
	budget.invocations = &invocations
	return budget
}

// workflowStep is one declared step.
type workflowStep struct {
	label      string
	router     *Router // nil runs the step body in this process.
	run        StepFunc
	after      []string
	verifier   StepVerifier
	deadline   time.Duration
	compensate StepFunc // nil: this step is not rolled back on a later failure.
}

// Workflow is a journalled directed-acyclic workflow over Contract and
// Scatter. Each step is a directed request (Contract) to its router, or an
// all-capable one (Scatter), ordered by its declared dependencies, with an
// optional verifier and deadline. Built by Laser.Workflow, described with
// fluent Step/StepLocal calls, and driven by Run.
//
// Unlike ContractBuilder and the other fluent builders in this package,
// Workflow and StepHandle mutate in place rather than returning a derived
// copy per call: Rust's own builder here consumes and returns `self` to
// grow one Vec<Step>, which Go has no move semantics to mirror cheaply:
// copying the whole step slice (or sharing its backing array unsafely)
// on every chained call would be worse than the shared, explicit mutation
// used here.
//
// Not yet ported: exclusive (fenced, at-most-once) steps and
// OnTimeout(Reassign), and the "plain code" run_with/WorkflowRun form where
// the caller's own async body is the workflow. Both are self-contained
// future slices; see foreign/go/IMPLEMENTATION_STATUS.md.
type Workflow struct {
	laser    *Laser
	name     string
	source   wire.AgentID
	topic    string
	registry *AgentRegistry
	budget   WorkflowBudget
	runID    *wire.ConversationID
	steps    []workflowStep
}

// Workflow opens a workflow dispatching as source, publishing and awaiting
// every step's reply on topic (Go's single-lane model: see Contract and
// Scatter, which Workflow dispatches through).
func (laser *Laser) Workflow(source wire.AgentID, topic string) *Workflow {
	return &Workflow{laser: laser, name: source.String(), source: source, topic: topic}
}

// Budget sets the workflow's spend ceiling.
func (workflow *Workflow) Budget(budget WorkflowBudget) *Workflow {
	workflow.budget = budget
	return workflow
}

// Registry is where an all-capable step's agents are resolved from.
// Unneeded for a workflow with only directed/local steps.
func (workflow *Workflow) Registry(registry *AgentRegistry) *Workflow {
	workflow.registry = registry
	return workflow
}

// RunID runs under an earlier run's id (from WorkflowOutcome.RunID, or one
// the caller derives for a task): Run replays that run's whole journal,
// skips the steps recorded complete, and dispatches only the unfinished
// ones. Omit this to start a fresh run with a minted id.
func (workflow *Workflow) RunID(runID wire.ConversationID) *Workflow {
	workflow.runID = &runID
	return workflow
}

// Step adds a step named label that dispatches to router, building its
// payload from the prior outputs with run. Returns a StepHandle so the step
// can be refined (After, VerifyWith, Deadline) before the next Step.
func (workflow *Workflow) Step(label string, router Router, run StepFunc) *StepHandle {
	return workflow.addStep(label, &router, run)
}

// StepLocal adds a step named label whose body run runs in this process: it
// gets the prior outputs and its result is the step output, so the step
// needs no agent. Its completion is journaled like a routed step.
func (workflow *Workflow) StepLocal(label string, run StepFunc) *StepHandle {
	return workflow.addStep(label, nil, run)
}

func (workflow *Workflow) addStep(label string, router *Router, run StepFunc) *StepHandle {
	workflow.steps = append(workflow.steps, workflowStep{
		label: label, router: router, run: run, deadline: defaultStepDeadline,
	})
	return &StepHandle{workflow: workflow, index: len(workflow.steps) - 1}
}

// StepHandle is a step under construction, returned by Workflow.Step and
// Workflow.StepLocal. Refines the step it was returned for, then chains the
// next Step/StepLocal or Run.
type StepHandle struct {
	workflow *Workflow
	index    int
}

// After declares that this step runs only once label has completed.
func (handle *StepHandle) After(label string) *StepHandle {
	step := &handle.workflow.steps[handle.index]
	step.after = append(step.after, label)
	return handle
}

// VerifyWith checks this step's reply with verifier before it counts as
// done.
func (handle *StepHandle) VerifyWith(verifier StepVerifier) *StepHandle {
	handle.workflow.steps[handle.index].verifier = verifier
	return handle
}

// Deadline bounds the step's build, dispatch, verification, and completion
// record (default 30s), never past the workflow's own wall-clock budget.
// The command carries it, so a target that picks the task up later drops
// it.
func (handle *StepHandle) Deadline(deadline time.Duration) *StepHandle {
	handle.workflow.steps[handle.index].deadline = deadline
	return handle
}

// CompensateWith provides the compensation to run if a later step fails
// (the saga rollback): a directed or all-capable step's compensation is
// sent as a fresh Contract to the agent(s) that ran the original step; a
// local step's compensation runs in process when it builds (the build
// itself is the undo). A completed compensation is journaled, so a resume
// runs the undone step again.
func (handle *StepHandle) CompensateWith(run StepFunc) *StepHandle {
	handle.workflow.steps[handle.index].compensate = run
	return handle
}

// Step adds the next step (forwards to Workflow.Step).
func (handle *StepHandle) Step(label string, router Router, run StepFunc) *StepHandle {
	return handle.workflow.Step(label, router, run)
}

// StepLocal adds the next step running in this process (forwards to
// Workflow.StepLocal).
func (handle *StepHandle) StepLocal(label string, run StepFunc) *StepHandle {
	return handle.workflow.StepLocal(label, run)
}

// Run runs the workflow (forwards to Workflow.Run).
func (handle *StepHandle) Run(ctx context.Context) (*WorkflowOutcome, error) {
	return handle.workflow.Run(ctx)
}

// WorkflowOutcome is the result of a completed Workflow.Run: each step's
// output by label, the run id, the labels of the completed steps in
// completion order, and the run's end status (Completed for every returned
// outcome, since a failed run is an error). Pass RunID to Workflow.RunID to
// resume the same run.
type WorkflowOutcome struct {
	Outputs        map[string][]byte
	RunID          wire.ConversationID
	CompletedSteps []string
	Status         wire.SessionStatus
}

// Text is the output of step label as text (invalid UTF-8 replaced), or
// false when no completed step has that label.
func (outcome *WorkflowOutcome) Text(label string) (string, bool) {
	output, has := outcome.Outputs[label]
	if !has {
		return "", false
	}
	return string(output), true
}

// Run runs the workflow's steps in dependency order to completion, or
// resumes one under Workflow.RunID. A step that does not complete fails the
// run with a StepFailedError. A misconfigured workflow (a duplicate label,
// a broadcast step, a dependency cycle) is refused before anything is
// written.
//
// The run is a session on agent.sessions whose id is the run id, and every
// directed or all-capable step is a child session of it (Contract.Parent /
// Scatter dispatch through Contract, so the ancestry comes free). The run
// session starts before the first step and ends by the outcome.
func (workflow *Workflow) Run(ctx context.Context) (*WorkflowOutcome, error) {
	order, err := workflow.validate()
	if err != nil {
		return nil, err
	}
	resumed := workflow.runID != nil
	runID := NewConversationID()
	if workflow.runID != nil {
		runID = *workflow.runID
	}

	journal, err := workflow.replay(ctx, runID, resumed)
	if err != nil {
		return nil, err
	}
	session := workflow.laser.Sessions().Open(runID, "").As(workflow.source)
	if resumed {
		session = session.Resume()
	}
	if _, err := session.Begin(ctx); err != nil {
		return nil, err
	}

	outcome, runErr := workflow.execute(ctx, runID, order, journal)

	var endErr error
	switch typed := runErr.(type) {
	case nil:
		_, endErr = session.Complete(ctx, "")
	case *BudgetExceededError:
		_, endErr = session.FailOverBudget(ctx, typed.Breach)
	default:
		message := runErr.Error()
		retryable := false
		if failed, ok := runErr.(*StepFailedError); ok {
			retryable = failed.Retryable()
		}
		_, endErr = session.Fail(ctx, wire.AgentErrorBody{
			Code: wire.ErrorInternal, Message: &message, Retryable: retryable,
		})
	}
	if runErr != nil {
		return nil, runErr
	}
	if endErr != nil {
		return nil, endErr
	}
	return outcome, nil
}

// validate checks every configuration error before the run writes anything,
// and orders the steps by their declared dependencies.
func (workflow *Workflow) validate() ([]int, error) {
	labels := make(map[string]bool, len(workflow.steps))
	for _, step := range workflow.steps {
		if labels[step.label] {
			return nil, &InvalidError{
				Reason: "workflow step label `" + step.label + "` is declared twice"}
		}
		labels[step.label] = true
		if step.router != nil && step.router.Kind == RouteBroadcast {
			return nil, &InvalidError{
				Reason: "a broadcast workflow step has no gather target"}
		}
	}
	return topologicalWorkflowOrder(workflow.steps)
}

// topologicalWorkflowOrder orders the steps so every step follows its after
// dependencies (Kahn's algorithm). Errors on an unknown dependency or a
// cycle.
func topologicalWorkflowOrder(steps []workflowStep) ([]int, error) {
	indexOf := make(map[string]int, len(steps))
	for index, step := range steps {
		indexOf[step.label] = index
	}
	inDegree := make([]int, len(steps))
	dependents := make([][]int, len(steps))
	for index, step := range steps {
		for _, dependency := range step.after {
			dep, known := indexOf[dependency]
			if !known {
				return nil, &InvalidError{Reason: "workflow step `" + step.label +
					"` depends on unknown step `" + dependency + "`"}
			}
			inDegree[index]++
			dependents[dep] = append(dependents[dep], index)
		}
	}
	// Seed with the dependency-free steps in declared order, so an
	// independent chain keeps its authored sequence.
	ready := make([]int, 0, len(steps))
	for index := range steps {
		if inDegree[index] == 0 {
			ready = append(ready, index)
		}
	}
	order := make([]int, 0, len(steps))
	for cursor := 0; cursor < len(ready); cursor++ {
		index := ready[cursor]
		order = append(order, index)
		for _, dependent := range dependents[index] {
			inDegree[dependent]--
			if inDegree[dependent] == 0 {
				ready = append(ready, dependent)
			}
		}
	}
	if len(order) != len(steps) {
		return nil, &InvalidError{Reason: "workflow steps form a dependency cycle"}
	}
	return order, nil
}

// routeText is the step's route in words, for a failure message.
func routeText(router Router) string {
	switch router.Kind {
	case RouteTo:
		return "agent `" + router.Agent.String() + "`"
	case RouteToCapable:
		return "an agent with capability `" + router.Skill + "`"
	case RouteAllCapable:
		return "every agent with capability `" + router.Skill + "`"
	default:
		return "every agent"
	}
}

// workflowAttempt is one dispatch of a step: its number, and the journaled
// reply positions when it resumes a dispatch sent before the run stopped.
type workflowAttempt struct {
	number    uint32
	replyFrom []uint64
}

// workflowDispatch is the ids one attempt keeps across every send: its
// child conversation, its correlation, and the idempotency key the target
// deduplicates by. Derived from the run, the step, and the attempt number,
// so a resumed attempt rebuilds the exact same ids without the engine
// having journaled them.
type workflowDispatch struct {
	conversation wire.ConversationID
	correlation  wire.CorrelationID
	key          wire.IdempotencyKey
}

func newWorkflowDispatch(runID wire.ConversationID, label string, attempt uint32) (workflowDispatch, error) {
	conversation := DeriveConversationID(fmt.Sprintf("%s/%s/%d", runID, label, attempt))
	correlationSeed := DeriveConversationID(fmt.Sprintf("%s/%s/%d/correlation", runID, label, attempt))
	key, err := wire.NewIdempotencyKey(fmt.Sprintf("workflow:%s", conversation))
	if err != nil {
		return workflowDispatch{}, err
	}
	return workflowDispatch{
		conversation: conversation,
		correlation:  wire.CorrelationID{ID: correlationSeed.ID},
		key:          key,
	}, nil
}

// workflowEffect is a step whose effect a rollback must undo, and the
// agents that ran it (empty, not nil, for a local step: the build itself
// is the effect).
type workflowEffect struct {
	index  int
	agents []wire.AgentID
}

// execute is the engine body: steps in dependency order, compensating the
// effects recorded so far (the saga rollback) before returning any failure.
func (workflow *Workflow) execute(
	ctx context.Context, runID wire.ConversationID, order []int, journal workflowJournal,
) (*WorkflowOutcome, error) {
	started := time.Now()
	if !journal.started {
		if err := workflow.record(ctx, runID, workflowJournalEntry{
			Kind: journalRunStarted, Workflow: workflow.name,
		}); err != nil {
			return nil, err
		}
	}
	outputs := journal.outputs
	completed := append([]string(nil), journal.completed...)
	var tokensSpent uint64
	var invocations uint32
	var effects []workflowEffect
	for _, index := range order {
		if _, done := outputs[workflow.steps[index].label]; done {
			effects = append(effects, workflowEffect{
				index: index, agents: journal.agents[workflow.steps[index].label],
			})
		}
	}

	for _, index := range order {
		step := workflow.steps[index]
		if _, done := outputs[step.label]; done {
			continue
		}
		invocations++
		if err := workflow.checkBudget(invocations, tokensSpent, started); err != nil {
			workflow.compensate(ctx, runID, effects, outputs)
			return nil, err
		}
		attempt := workflowAttempt{number: 1}
		if dispatched, inFlight := journal.inFlight[step.label]; inFlight {
			attempt = workflowAttempt{number: dispatched.attempt, replyFrom: dispatched.replyFrom}
		} else if last, attempted := journal.attempts[step.label]; attempted {
			attempt.number = last + 1
		}
		output, tokens, agents, err := workflow.runStep(ctx, runID, step, outputs, attempt, started)
		if err != nil {
			if agents != nil {
				effects = append(effects, workflowEffect{index: index, agents: agents})
			}
			workflow.compensate(ctx, runID, effects, outputs)
			return nil, err
		}
		tokensSpent += tokens
		outputs[step.label] = output
		completed = append(completed, step.label)
		effects = append(effects, workflowEffect{index: index, agents: agents})
		if err := workflow.checkBudget(invocations, tokensSpent, started); err != nil {
			workflow.compensate(ctx, runID, effects, outputs)
			return nil, err
		}
	}

	return &WorkflowOutcome{
		Outputs: outputs, RunID: runID, CompletedSteps: completed,
		Status: wire.SessionCompleted,
	}, nil
}

// compensate runs the compensations of the steps with an effect in reverse
// order (the saga rollback), each to the agents that ran its step. A
// compensation that completes is journaled, so a resume runs the undone
// step again. One that does not is left unjournaled and the rollback goes
// on; it never replaces the original error, which the caller already has.
func (workflow *Workflow) compensate(
	ctx context.Context, runID wire.ConversationID, effects []workflowEffect, outputs map[string][]byte,
) {
	for cursor := len(effects) - 1; cursor >= 0; cursor-- {
		effect := effects[cursor]
		step := workflow.steps[effect.index]
		if step.compensate == nil {
			continue
		}
		deadlineAt := time.Now().Add(step.deadline)
		buildCtx, cancel := context.WithDeadline(ctx, deadlineAt)
		payload, err := step.compensate(buildCtx, outputs)
		cancel()
		if err != nil {
			continue
		}
		undone := workflow.runCompensation(ctx, runID, step, effect.agents, payload, deadlineAt)
		if undone {
			_ = workflow.record(ctx, runID, workflowJournalEntry{
				Kind: journalStepCompensated, Label: step.label,
			})
		}
	}
}

// runCompensation dispatches one step's built compensation payload to the
// agent(s) that ran it (none, for a local step, whose build was already
// the undo), reporting whether every dispatch completed.
func (workflow *Workflow) runCompensation(
	ctx context.Context, runID wire.ConversationID, step workflowStep,
	agents []wire.AgentID, payload []byte, deadlineAt time.Time,
) bool {
	if step.router == nil {
		return true
	}
	targets := []Router{*step.router}
	if len(agents) > 0 {
		targets = make([]Router, len(agents))
		for index, agent := range agents {
			targets[index] = compensationRoute(*step.router, agent)
		}
	}
	undone := true
	for index, target := range targets {
		conversation := DeriveConversationID(
			fmt.Sprintf("%s/%s/compensate/%d", runID, step.label, index))
		_, contract, err := workflow.laser.Contract(target, workflow.topic).
			From(workflow.source).
			Payload(payload).
			Registry(workflow.registry).
			Conversation(conversation).
			Parent(runID, runID).
			Deadline(time.Until(deadlineAt)).
			SendResolved(ctx)
		if err != nil || contract.State != ContractCompleted {
			undone = false
		}
	}
	return undone
}

// compensationRoute is a directed route to one specific agent, the same
// router a step's own targeting would have resolved, for the rollback to
// address the agent that actually ran the original step rather than
// whichever one a fresh capability resolution might now pick.
func compensationRoute(router Router, agent wire.AgentID) Router {
	return To(agent)
}

// stepDeadline bounds a step's own deadline by the workflow's remaining
// wall-clock budget, when one is set.
func (workflow *Workflow) stepDeadline(step workflowStep, started time.Time) time.Duration {
	deadline := step.deadline
	if workflow.budget.wallClock != nil {
		if remaining := *workflow.budget.wallClock - time.Since(started); remaining < deadline {
			deadline = remaining
		}
	}
	return deadline
}

// checkBudget checks the running totals against the budget, erroring on the
// first breach.
func (workflow *Workflow) checkBudget(
	invocations uint32, tokensSpent uint64, started time.Time,
) error {
	if ceiling := workflow.budget.invocations; ceiling != nil && invocations > *ceiling {
		return &BudgetExceededError{Breach: BudgetBreach{
			Dimension: BudgetInvocations, Ceiling: uint64(*ceiling), Spent: uint64(invocations),
		}}
	}
	if ceiling := workflow.budget.tokens; ceiling != nil && tokensSpent > *ceiling {
		return &BudgetExceededError{Breach: BudgetBreach{
			Dimension: BudgetTokens, Ceiling: *ceiling, Spent: tokensSpent,
		}}
	}
	if ceiling := workflow.budget.wallClock; ceiling != nil && time.Since(started) > *ceiling {
		return &BudgetExceededError{Breach: BudgetBreach{
			Dimension: BudgetWallClock,
			Ceiling:   uint64(ceiling.Microseconds()), Spent: uint64(time.Since(started).Microseconds()),
		}}
	}
	return nil
}

func (workflow *Workflow) stepFailedError(
	runID wire.ConversationID, step workflowStep, reason StepFailure,
) error {
	target := "local step"
	if step.router != nil {
		target = routeText(*step.router)
	}
	return &StepFailedError{
		Run: runID, Step: step.label, Reason: reason, Target: target, Deadline: step.deadline,
	}
}

// runStep builds the step's payload, then dispatches it: in process for a
// local step, as a Scatter for an all-capable one, else as a Contract.
//
// The returned agents is nil when the step produced no effect at all worth
// compensating (the build failed, or a directed/all-capable dispatch never
// reached a completing reply), and non-nil — empty for a local step, whose
// build is its own effect — once it did, whether or not a later verifier
// then rejected it: the handler already ran.
func (workflow *Workflow) runStep(
	ctx context.Context, runID wire.ConversationID, step workflowStep,
	outputs map[string][]byte, attempt workflowAttempt, started time.Time,
) ([]byte, uint64, []wire.AgentID, error) {
	deadline := workflow.stepDeadline(step, started)
	buildCtx, cancel := context.WithTimeout(ctx, deadline)
	payload, err := step.run(buildCtx, outputs)
	cancel()
	if err != nil {
		if buildCtx.Err() == context.DeadlineExceeded {
			return nil, 0, nil, workflow.stepFailedError(runID, step, StepTimedOut)
		}
		return nil, 0, nil, err
	}
	deadlineAt := time.Now().Add(deadline)
	switch {
	case step.router == nil:
		output, tokens, err := workflow.finishStep(ctx, runID, step, payload, 0, nil, deadlineAt)
		return output, tokens, []wire.AgentID{}, err
	case step.router.Kind == RouteAllCapable:
		return workflow.runAllCapable(ctx, runID, step, payload, attempt, deadlineAt)
	default:
		return workflow.runDirected(ctx, runID, step, payload, attempt, deadlineAt)
	}
}

// runDirected is a directed step: one Contract to the routed target.
func (workflow *Workflow) runDirected(
	ctx context.Context, runID wire.ConversationID, step workflowStep,
	payload []byte, attempt workflowAttempt, deadlineAt time.Time,
) ([]byte, uint64, []wire.AgentID, error) {
	dispatch, err := newWorkflowDispatch(runID, step.label, attempt.number)
	if err != nil {
		return nil, 0, nil, err
	}
	replyFrom, err := workflow.dispatchPoint(ctx, runID, step, attempt)
	if err != nil {
		return nil, 0, nil, err
	}
	target, contract, err := workflow.laser.Contract(*step.router, workflow.topic).
		From(workflow.source).
		Payload(payload).
		Registry(workflow.registry).
		Conversation(dispatch.conversation).
		Parent(runID, runID).
		Correlation(dispatch.correlation).
		IdempotencyKey(dispatch.key).
		ResumeFrom(replyFrom).
		Deadline(time.Until(deadlineAt)).
		SendResolved(ctx)
	if err != nil {
		return nil, 0, nil, err
	}
	output, tokens, failErr := workflow.completion(runID, step, contract)
	if failErr != nil {
		return nil, 0, nil, failErr
	}
	agents := []wire.AgentID{target}
	finished, tokens, err := workflow.finishStep(ctx, runID, step, output, tokens, agents, deadlineAt)
	return finished, tokens, agents, err
}

// dispatchPoint is where the reply to this attempt lands at or after. A
// fresh attempt journals the reply topic's tail before it sends; a resumed
// one hands the journaled positions back for Contract.ResumeFrom.
func (workflow *Workflow) dispatchPoint(
	ctx context.Context, runID wire.ConversationID, step workflowStep, attempt workflowAttempt,
) ([]uint64, error) {
	if attempt.replyFrom != nil {
		return attempt.replyFrom, nil
	}
	replyFrom, err := workflow.laser.ReplyPositions(ctx, workflow.topic)
	if err != nil {
		return nil, err
	}
	if err := workflow.record(ctx, runID, workflowJournalEntry{
		Kind: journalStepDispatched, Label: step.label, Attempt: attempt.number, ReplyFrom: replyFrom,
	}); err != nil {
		return nil, err
	}
	return nil, nil
}

// completion is the completing reply's body and tokens, or the step
// failure the other terminals mean.
func (workflow *Workflow) completion(
	runID wire.ConversationID, step workflowStep, contract *Contract,
) ([]byte, uint64, error) {
	switch contract.State {
	case ContractCompleted:
		var tokens uint64
		if usage := contract.Reply.Envelope.Usage; usage != nil {
			tokens = usage.InputTokens + usage.OutputTokens
		}
		return contract.Reply.Envelope.Body, tokens, nil
	case ContractFailed:
		return nil, 0, workflow.stepFailedError(runID, step, StepFailed)
	case ContractNotConsumed:
		return nil, 0, workflow.stepFailedError(runID, step, StepNotConsumed)
	default:
		return nil, 0, workflow.stepFailedError(runID, step, StepTimedOut)
	}
}

// runAllCapable dispatches a Scatter to every agent advertising the step's
// skill and joins the completed replies.
func (workflow *Workflow) runAllCapable(
	ctx context.Context, runID wire.ConversationID, step workflowStep,
	payload []byte, attempt workflowAttempt, deadlineAt time.Time,
) ([]byte, uint64, []wire.AgentID, error) {
	if attempt.replyFrom == nil {
		if err := workflow.record(ctx, runID, workflowJournalEntry{
			Kind: journalStepDispatched, Label: step.label, Attempt: attempt.number,
		}); err != nil {
			return nil, 0, nil, err
		}
	}
	report, err := workflow.laser.ScatterReport(ctx, ScatterRequest{
		Skill: step.router.Skill, Routing: step.router.Policy, Registry: workflow.registry,
		From: workflow.source, Topic: workflow.topic, Payload: payload,
		Policy: GatherAll(), Deadline: time.Until(deadlineAt),
	})
	if err != nil {
		return nil, 0, nil, err
	}
	completed := report.Completed()
	if len(completed) == 0 {
		return nil, 0, nil, workflow.stepFailedError(runID, step, StepNoCompletion)
	}
	agents := make([]wire.AgentID, 0, len(completed))
	bodies := make([]string, 0, len(completed))
	for _, outcome := range completed {
		agents = append(agents, outcome.Agent)
		bodies = append(bodies, string(outcome.Contract.Reply.Envelope.Body))
	}
	output, tokens, err := workflow.finishStep(
		ctx, runID, step, []byte(strings.Join(bodies, "\n")), 0, agents, deadlineAt)
	return output, tokens, agents, err
}

// finishStep verifies the reply and journals the completion inside the
// step deadline.
func (workflow *Workflow) finishStep(
	ctx context.Context, runID wire.ConversationID, step workflowStep,
	output []byte, tokens uint64, agents []wire.AgentID, deadlineAt time.Time,
) ([]byte, uint64, error) {
	if step.verifier != nil {
		verifyCtx, cancel := context.WithDeadline(ctx, deadlineAt)
		ok, err := step.verifier(verifyCtx, output)
		cancel()
		switch {
		case err != nil:
			return nil, 0, err
		case !ok:
			return nil, 0, workflow.stepFailedError(runID, step, StepUnverified)
		}
	}
	agentNames := make([]string, len(agents))
	for index, agent := range agents {
		agentNames[index] = agent.String()
	}
	if err := workflow.record(ctx, runID, workflowJournalEntry{
		Kind: journalStepCompleted, Label: step.label, Output: output, Agents: agentNames,
	}); err != nil {
		return nil, 0, err
	}
	return output, tokens, nil
}
