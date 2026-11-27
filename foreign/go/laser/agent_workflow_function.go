package laser

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// stepCallSeparator separates a step's name from the number of a repeated
// call of it in its journal label.
const stepCallSeparator = '#'

// stepSettleGrace is how long a run's end waits for its own due calls to
// finish before the run gives up on them and leaves them for a resume.
const stepSettleGrace = 5 * time.Second

// StepCallKind is what WorkflowRun.BeginStep learned about a step call.
type StepCallKind int

const (
	// StepCallRecorded is a step that completed earlier in this run, with
	// its recorded output.
	StepCallRecorded StepCallKind = iota
	// StepCallDue is a step that has to run. Pass its journal label to
	// WorkflowRun.CompleteStep, or WorkflowRun.AbandonStep when its body
	// fails.
	StepCallDue
)

// StepCallResult is WorkflowRun.BeginStep's answer.
type StepCallResult struct {
	Kind StepCallKind
	// Output is set when Kind is StepCallRecorded.
	Output []byte
	// Label is the journal label to complete or abandon, set when Kind is
	// StepCallDue.
	Label string
}

// stepCallKey orders repeated calls of one step name numerically: within one
// name, sorting by (length, text) puts "name", "name#2", ..., "name#9",
// "name#10" in call order, which plain lexicographic text order would not
// (it would put "name#10" before "name#2").
type stepCallKey struct {
	length int
	label  string
}

func (key stepCallKey) less(other stepCallKey) bool {
	if key.length != other.length {
		return key.length < other.length
	}
	return key.label < other.label
}

func (key stepCallKey) lessOrEqual(other stepCallKey) bool {
	return key == other || key.less(other)
}

// runState is a WorkflowRun's own mutable state: what each step call has
// settled on, and the bookkeeping the run's end waits on before it is safe
// to end the session.
//
// Rust's equivalent (sdk/src/agent/workflow.rs's RunState) signals its own
// "unsettled" count change with a tokio::sync::watch channel; Go has no
// stdlib watch channel, so this uses the "close and replace a channel"
// idiom instead (signal below): every waiter reading <-signal wakes the
// instant publishLocked runs, with no goroutine left blocked afterward, and
// no busy-poll needed.
type runState struct {
	mutex sync.Mutex

	outputs   map[string][]byte
	completed []string
	// unclaimed is a resumed run's own recorded calls not yet claimed by a
	// call this process has made, by step name then position key.
	unclaimed map[string]map[stepCallKey]*string
	// inFlight is a due call's own input digest, by journal label.
	inFlight map[string]*string
	// nextCall is the lowest call number of each step name that may still
	// be free.
	nextCall    map[string]int
	invocations uint32
	closing     bool
	finished    bool
	recording   int
	signal      chan struct{}
}

func newRunState() *runState {
	return &runState{
		outputs:   map[string][]byte{},
		unclaimed: map[string]map[stepCallKey]*string{},
		inFlight:  map[string]*string{},
		nextCall:  map[string]int{},
		signal:    make(chan struct{}),
	}
}

// resumedRunState seeds a run's state from its replayed journal. A fresh
// run's journal is empty, so this also serves as the fresh-run constructor.
func resumedRunState(journal workflowJournal) *runState {
	state := newRunState()
	state.outputs = journal.outputs
	state.completed = append([]string(nil), journal.completed...)
	for label := range journal.outputs {
		name, _ := labelCall(label)
		key := stepCallKey{length: len(label), label: label}
		var digest *string
		if input, has := journal.inputs[label]; has {
			value := input
			digest = &value
		}
		if state.unclaimed[name] == nil {
			state.unclaimed[name] = map[stepCallKey]*string{}
		}
		state.unclaimed[name][key] = digest
	}
	return state
}

// publishLocked wakes every waiter blocked on the current signal. Call with
// mutex held.
func (state *runState) publishLocked() {
	close(state.signal)
	state.signal = make(chan struct{})
}

// unclaimedCallLocked is the lowest-keyed unclaimed call of name whose digest
// matches. Call with mutex held.
func (state *runState) unclaimedCallLocked(
	name string, matches func(digest *string) bool,
) (stepCallKey, bool) {
	var best stepCallKey
	found := false
	for key, digest := range state.unclaimed[name] {
		if !matches(digest) {
			continue
		}
		if !found || key.less(best) {
			best, found = key, true
		}
	}
	return best, found
}

// beginCallLocked answers a call of step name from its record, or names the
// journal label the call is due under. Call with mutex held.
//
// A record made with equal arguments answers first. Failing that (or for a
// call with no input at all, which always falls through here), the call
// walks name, name#2, name#3, ... and stops at the first record where
// either side has no digest, or at a free label.
func (state *runState) beginCallLocked(name string, digest *string) StepCallResult {
	var recorded stepCallKey
	claimed := false
	if digest != nil {
		if key, ok := state.unclaimedCallLocked(name, func(candidate *string) bool {
			return candidate != nil && *candidate == *digest
		}); ok {
			recorded, claimed = key, true
		}
	}
	if !claimed {
		undigested, hasUndigested := state.unclaimedCallLocked(name, func(candidate *string) bool {
			return candidate == nil || digest == nil
		})
		call := state.nextCall[name]
		if call == 0 {
			call = 1
		}
		for {
			label := journalLabel(name, call)
			candidate := stepCallKey{length: len(label), label: label}
			if hasUndigested && undigested.lessOrEqual(candidate) {
				state.nextCall[name] = call
				recorded, claimed = undigested, true
				break
			}
			_, hasOutput := state.outputs[label]
			_, dueAlready := state.inFlight[label]
			if !hasOutput && !dueAlready {
				state.nextCall[name] = call + 1
				state.inFlight[label] = digest
				state.publishLocked()
				return StepCallResult{Kind: StepCallDue, Label: label}
			}
			call++
		}
	}
	if records := state.unclaimed[name]; records != nil {
		delete(records, recorded)
	}
	return StepCallResult{Kind: StepCallRecorded, Output: state.outputs[recorded.label]}
}

// abandon gives up the due call with journalLabel because its body failed,
// so a retry takes the label again. A no-op for a call already recorded
// (including one this same call just completed: completing a call removes
// it from inFlight first, so abandoning it afterward is always safe).
func (state *runState) abandon(journalLabel string) {
	state.mutex.Lock()
	defer state.mutex.Unlock()
	if _, due := state.inFlight[journalLabel]; !due {
		return
	}
	delete(state.inFlight, journalLabel)
	name, numberText := labelCall(journalLabel)
	// Tolerates a number too large for an int, the same way Rust's own
	// abandon() tolerates one too large for a usize: next_call bookkeeping is
	// an optimization (where to start the free-label search), not a
	// correctness requirement, so skipping the update here is safe.
	if number, err := strconv.Atoi(numberText); err == nil {
		if existing, has := state.nextCall[name]; !has || number < existing {
			state.nextCall[name] = number
		}
	}
	state.publishLocked()
}

// startRecordLocked reports whether a step record may start; the run's end
// waits for one that did. Call with mutex held.
func (state *runState) startRecordLocked() bool {
	if state.finished {
		return false
	}
	state.recording++
	state.publishLocked()
	return true
}

func (state *runState) endRecord() {
	state.mutex.Lock()
	defer state.mutex.Unlock()
	if state.recording > 0 {
		state.recording--
	}
	state.publishLocked()
}

// recorded is the due call journalLabel reaching the journal with output.
func (state *runState) recorded(journalLabel string, output []byte) {
	state.mutex.Lock()
	defer state.mutex.Unlock()
	state.outputs[journalLabel] = output
	state.completed = append(state.completed, journalLabel)
	delete(state.inFlight, journalLabel)
	state.publishLocked()
}

// waitForNoInFlight waits for every due call to settle, until deadline.
func (state *runState) waitForNoInFlight(ctx context.Context, deadline time.Time) {
	for {
		state.mutex.Lock()
		empty := len(state.inFlight) == 0
		signal := state.signal
		state.mutex.Unlock()
		if empty {
			return
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return
		}
		select {
		case <-signal:
		case <-time.After(remaining):
			return
		case <-ctx.Done():
			return
		}
	}
}

// waitForNoRecording waits for every step record already being written to
// finish. Unbounded, like Rust's own equivalent wait: an append has the
// client's own timeout, so this needs no bound of its own; ctx.Done() is a
// deliberate Go addition beyond Rust's literal behavior, so a cancelled
// caller is never left blocked here forever.
func (state *runState) waitForNoRecording(ctx context.Context) {
	for {
		state.mutex.Lock()
		empty := state.recording == 0
		signal := state.signal
		state.mutex.Unlock()
		if empty {
			return
		}
		select {
		case <-signal:
		case <-ctx.Done():
			return
		}
	}
}

// journalLabel is the journal label of the numberth call of step label.
func journalLabel(label string, number int) string {
	if number <= 1 {
		return label
	}
	return fmt.Sprintf("%s%c%d", label, stepCallSeparator, number)
}

// labelCall is the step name and the call number journalLabel holds, the
// number as canonical decimal text rather than parsed: matching (via
// stepCallKey's length-then-text order) never depends on a call number
// fitting in a machine word, only abandon()'s next_call bookkeeping does,
// and it parses lazily, tolerating a number too large to fit.
//
// Only the canonical decimal text of a whole number above 1 after the last
// separator numbers a call; any other label names a step of its own, call 1.
func labelCall(journalLabel string) (string, string) {
	index := strings.LastIndexByte(journalLabel, stepCallSeparator)
	if index < 0 {
		return journalLabel, "1"
	}
	name, digits := journalLabel[:index], journalLabel[index+1:]
	if digits == "" || digits == "1" || digits[0] == '0' || !allDigits(digits) {
		return journalLabel, "1"
	}
	return name, digits
}

func allDigits(text string) bool {
	for _, r := range text {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// inputDigest is the digest two clients agree on for the same canonical
// JSON, so a run resumes across them.
func inputDigest(input []byte) string {
	sum := sha256.Sum256(input)
	return hex.EncodeToString(sum[:16])
}

// stepOutput is the JSON a step's or a RunWith body's value is stored as.
// encoding/json already refuses a non-finite float (NaN/Inf), which is the
// one thing Rust's own step_value.rs needs a custom float-walking Serializer
// for; Go's standard encoder does that refusal natively.
func stepOutput(label string, value any) ([]byte, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, &CodecError{Reason: fmt.Sprintf(
			"workflow step `%s` returned a value JSON cannot hold", label), Cause: err}
	}
	return encoded, nil
}

func workflowFunctionValue(name string, value any) ([]byte, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, &CodecError{Reason: fmt.Sprintf(
			"workflow `%s` returned a value JSON cannot hold", name), Cause: err}
	}
	return encoded, nil
}

func decodeStepOutput(label string, output []byte, target any) error {
	if err := json.Unmarshal(output, target); err != nil {
		return &CodecError{Reason: fmt.Sprintf(
			"the output of workflow step `%s` does not decode as the JSON asked for", label), Cause: err}
	}
	return nil
}

// WorkflowRun is a workflow run whose steps the caller's own code calls,
// from Workflow.Start or RunWith. Steps may run concurrently.
type WorkflowRun struct {
	workflow *Workflow
	runID    wire.ConversationID
	session  *Session
	started  time.Time

	state *runState

	finishMutex sync.Mutex
	finished    bool
	// lease keeps the run's root session heartbeating while it works. It is
	// taken, once, by whichever of Finish or Release ends the run, so the
	// lease is also what marks the run as ended.
	lease *SessionLease
}

// Start opens a run the caller drives step by step, for a step body that is
// not a single Go func. RunWith is this with the steps in a closure. End it
// with Finish.
func (workflow *Workflow) Start(ctx context.Context) (*WorkflowRun, error) {
	resumed := workflow.runID != nil
	runID := NewConversationID()
	if workflow.runID != nil {
		runID = *workflow.runID
	}
	journal, session, lease, err := workflow.open(ctx, runID, resumed)
	if err != nil {
		return nil, err
	}
	if !journal.started {
		if err := workflow.record(ctx, runID, workflowJournalEntry{
			Kind: journalRunStarted, Workflow: workflow.name,
		}); err != nil {
			_ = endWorkflowSession(ctx, session, err)
			// The run never opened, so nothing will ever call Finish or
			// Release to give the lease up.
			if lease != nil {
				lease.Release()
			}
			return nil, err
		}
	}
	return &WorkflowRun{
		workflow: workflow, runID: runID, session: session, started: time.Now(),
		state: resumedRunState(journal), lease: lease,
	}, nil
}

// RunID is the run id, which resumes this run when passed to Workflow.RunID.
func (run *WorkflowRun) RunID() wire.ConversationID { return run.runID }

// BeginStep counts one call of step label and says whether it is recorded.
// Runs before anything awaits, so call order fixes the journal labels. input
// holds the canonical JSON of the call's arguments when it has any; label
// may not contain '#', the journal's own repeat-call separator. A call
// after RefuseNewSteps, or once the run has ended, is refused.
func (run *WorkflowRun) BeginStep(label string, input []byte) (StepCallResult, error) {
	if strings.ContainsRune(label, stepCallSeparator) {
		return StepCallResult{}, &InvalidError{Reason: fmt.Sprintf(
			"workflow step name `%s` cannot contain `%c`, the journal numbers repeated calls with it",
			label, stepCallSeparator)}
	}
	var digest *string
	if input != nil {
		value := inputDigest(input)
		digest = &value
	}
	run.state.mutex.Lock()
	defer run.state.mutex.Unlock()
	if run.state.closing {
		return StepCallResult{}, &InvalidError{Reason: fmt.Sprintf(
			"workflow run %s has ended, so step `%s` cannot start", run.runID, label)}
	}
	return run.state.beginCallLocked(label, digest), nil
}

// Checkpoint is the check between steps: a breached workflow budget fails
// with a BudgetExceededError. Unlike the declared-steps engine's own
// per-step boundary check, this (like Rust's) tracks no token spend of its
// own; a session cancel/over-budget check is not ported here either, the
// same documented gap the declared-steps engine's own boundary check
// carries (see Workflow.execute).
func (run *WorkflowRun) Checkpoint(ctx context.Context) error {
	run.state.mutex.Lock()
	run.state.invocations++
	invocations := run.state.invocations
	run.state.mutex.Unlock()
	return run.workflow.checkBudget(invocations, 0, run.started)
}

// CompleteStep journals output as the result of the step call BeginStep
// answered with journalLabel. Refused once the run has ended, so a resume
// runs that step again.
func (run *WorkflowRun) CompleteStep(ctx context.Context, journalLabel string, output []byte) error {
	run.state.mutex.Lock()
	if !run.state.startRecordLocked() {
		run.state.mutex.Unlock()
		return &InvalidError{Reason: fmt.Sprintf(
			"workflow run %s has ended, so step `%s` is not recorded", run.runID, journalLabel)}
	}
	var input string
	if digest := run.state.inFlight[journalLabel]; digest != nil {
		input = *digest
	}
	run.state.mutex.Unlock()
	defer run.state.endRecord()

	if err := run.workflow.record(ctx, run.runID, workflowJournalEntry{
		Kind: journalStepCompleted, Label: journalLabel, Output: output, Input: input,
	}); err != nil {
		return err
	}
	run.state.recorded(journalLabel, output)
	return nil
}

// AbandonStep gives up the due call with journalLabel because its body
// failed, so a retry takes the label again. A no-op for a recorded call.
func (run *WorkflowRun) AbandonStep(journalLabel string) {
	run.state.abandon(journalLabel)
}

// RefuseNewSteps refuses every step call from now on, for a caller whose
// body ended before Finish runs.
func (run *WorkflowRun) RefuseNewSteps() {
	run.state.mutex.Lock()
	run.state.closing = true
	run.state.mutex.Unlock()
}

// Finish ends the run from value (the JSON of its own value, nil when it has
// none) and resultErr (the error that stopped it, nil on success), and
// returns the outcome or that error. Steps still running get a few seconds
// to finish first, and a step record already being written is waited for,
// so the run's end is its last record.
func (run *WorkflowRun) Finish(
	ctx context.Context, value []byte, resultErr error,
) (*WorkflowOutcome, error) {
	lease, taken := run.end()
	if !taken {
		return nil, &InvalidError{Reason: fmt.Sprintf(
			"workflow run %s has already finished", run.runID)}
	}

	run.settle(ctx, stepSettleGrace)
	endErr := endWorkflowSession(ctx, run.session, resultErr)
	// After the session's ending record, so the run holds its lease for
	// exactly as long as it is working.
	if lease != nil {
		lease.Release()
	}
	if resultErr != nil {
		return nil, resultErr
	}
	if endErr != nil {
		return nil, endErr
	}

	run.state.mutex.Lock()
	outcome := &WorkflowOutcome{
		Outputs:        copyWorkflowOutputs(run.state.outputs),
		RunID:          run.runID,
		CompletedSteps: append([]string(nil), run.state.completed...),
		Status:         wire.SessionCompleted,
		Value:          value,
	}
	run.state.mutex.Unlock()
	return outcome, nil
}

// Release stops the run at once and leaves it open, for a caller that was
// cancelled or interrupted. The run stays resumable by its id.
func (run *WorkflowRun) Release() {
	run.state.mutex.Lock()
	run.state.closing = true
	run.state.finished = true
	run.state.mutex.Unlock()
	// Releasing ends the run as far as this process is concerned, so a
	// later Finish reports it already finished rather than writing an
	// ending record for a run nobody is driving. The session stays open and
	// resumable by its id; only the lease goes.
	if lease, taken := run.end(); taken && lease != nil {
		lease.Release()
	}
}

// end marks the run ended and hands back its lease, once. The second caller
// gets (nil, false), so Finish and Release cannot both end one run.
func (run *WorkflowRun) end() (*SessionLease, bool) {
	run.finishMutex.Lock()
	defer run.finishMutex.Unlock()
	if run.finished {
		return nil, false
	}
	run.finished = true
	lease := run.lease
	run.lease = nil
	return lease, true
}

// settle ends a run's steps: no new step, at most grace for the due calls,
// no new record, then every record already being written.
func (run *WorkflowRun) settle(ctx context.Context, grace time.Duration) {
	run.state.mutex.Lock()
	run.state.closing = true
	run.state.mutex.Unlock()

	run.state.waitForNoInFlight(ctx, time.Now().Add(grace))

	run.state.mutex.Lock()
	run.state.finished = true
	run.state.mutex.Unlock()

	run.state.waitForNoRecording(ctx)
}

func copyWorkflowOutputs(outputs map[string][]byte) map[string][]byte {
	copied := make(map[string][]byte, len(outputs))
	for label, output := range outputs {
		copied[label] = output
	}
	return copied
}

// RunStep runs body as the step label on run and returns its value. A step
// run already recorded returns the recorded value and body does not run.
// The value is stored as JSON and read back the same way, so a fresh run and
// a resumed one see one value. The second call of a label in one run is
// journaled as "label#2", the third as "label#3", and a call whose body
// failed gives its label back to a retry: a call takes the first of those
// labels no earlier call of this run took, so concurrent branches whose
// order can change between runs should use a label of their own.
//
// A free function, not a WorkflowRun method, because Go has no generic
// methods: a type parameter can only be declared on the type itself, and
// WorkflowRun is not generic (one run serves steps of many different T).
func RunStep[T any](
	ctx context.Context, run *WorkflowRun, label string, body func(context.Context) (T, error),
) (T, error) {
	var zero T
	result, err := run.BeginStep(label, nil)
	if err != nil {
		return zero, err
	}
	if result.Kind == StepCallRecorded {
		var value T
		if err := decodeStepOutput(label, result.Output, &value); err != nil {
			return zero, err
		}
		return value, nil
	}
	journalLabel := result.Label
	defer run.AbandonStep(journalLabel)

	if err := run.Checkpoint(ctx); err != nil {
		return zero, err
	}
	value, err := body(ctx)
	if err != nil {
		return zero, err
	}
	output, err := stepOutput(label, value)
	if err != nil {
		return zero, err
	}
	if err := run.CompleteStep(ctx, journalLabel, output); err != nil {
		return zero, err
	}
	return value, nil
}

// RunWith runs body as the workflow: each RunStep call it makes is
// journaled, so a run resumed with Workflow.RunID returns a recorded step's
// value without running it again. The body's value, as JSON, is
// WorkflowOutcome.Value, read back with WorkflowValue.
//
// A free function for the same reason RunStep is: Go has no generic methods.
func RunWith[T any](
	ctx context.Context, workflow *Workflow, body func(context.Context, *WorkflowRun) (T, error),
) (*WorkflowOutcome, error) {
	run, err := workflow.Start(ctx)
	if err != nil {
		return nil, err
	}
	value, bodyErr := body(ctx, run)
	var encoded []byte
	if bodyErr == nil {
		encoded, bodyErr = workflowFunctionValue(workflow.name, value)
	}
	return run.Finish(ctx, encoded, bodyErr)
}
