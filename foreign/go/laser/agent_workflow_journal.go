package laser

import (
	"context"
	"math"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// workflowJournalKind discriminates a workflowJournalEntry.
//
// The struct is this client's own shape, a flat record with a discriminator,
// which is the natural Go form. What goes on the log is not: see
// agent_workflow_journal_codec.go, which writes the externally tagged shape
// every client reads, pinned against the Rust client's own bytes. A run
// started on any client resumes on any other.
type workflowJournalKind uint8

const (
	journalRunStarted workflowJournalKind = iota
	journalStepDispatched
	journalStepCompleted
	journalStepCompensated
)

// workflowJournalEntry is one workflow-journal record, keyed on the log by
// the run id so one journal topic carries every run. A resume folds the
// run's records: a step recorded complete keeps its recorded output and is
// never dispatched again, and a step dispatched without an outcome is
// looked for on the reply topic before it is sent again.
type workflowJournalEntry struct {
	Kind      workflowJournalKind `cbor:"kind"`
	Workflow  string              `cbor:"workflow,omitempty"`
	Label     string              `cbor:"label,omitempty"`
	Attempt   uint32              `cbor:"attempt,omitempty"`
	ReplyFrom []uint64            `cbor:"reply_from,omitempty"`
	Output    []byte              `cbor:"output,omitempty"`
	Agents    []string            `cbor:"agents,omitempty"`
	// Input is the digest of a WorkflowRun step call's arguments, set only by
	// the "plain code" form (RunStep/WorkflowRun.CompleteStep); a declared
	// step (StepHandle) never sets it. Empty means no digest, which is safe
	// because a real digest (inputDigest) is never the empty string.
	Input string `cbor:"input,omitempty"`
}

// workflowInFlight is a dispatch the journal holds without an outcome.
type workflowInFlight struct {
	attempt   uint32
	replyFrom []uint64
}

// workflowJournal is what a run's journal folds to.
type workflowJournal struct {
	started   bool
	records   int
	outputs   map[string][]byte
	completed []string
	agents    map[string][]wire.AgentID
	inFlight  map[string]workflowInFlight
	attempts  map[string]uint32
	// inputs is a completed step's own call-argument digest, by journal
	// label, for the "plain code" form's own resume matching
	// (runState.resumed). A declared step never has an entry here.
	inputs map[string]string
}

func newWorkflowJournal() workflowJournal {
	return workflowJournal{
		outputs:  map[string][]byte{},
		agents:   map[string][]wire.AgentID{},
		inFlight: map[string]workflowInFlight{},
		attempts: map[string]uint32{},
		inputs:   map[string]string{},
	}
}

// foldWorkflowJournal folds a run's journal entries, in log order, into
// what the run has done.
func foldWorkflowJournal(entries []workflowJournalEntry) workflowJournal {
	journal := newWorkflowJournal()
	for _, entry := range entries {
		journal.records++
		switch entry.Kind {
		case journalRunStarted:
			journal.started = true
		case journalStepDispatched:
			if entry.Attempt > journal.attempts[entry.Label] {
				journal.attempts[entry.Label] = entry.Attempt
			}
			journal.inFlight[entry.Label] = workflowInFlight{
				attempt: entry.Attempt, replyFrom: entry.ReplyFrom,
			}
		case journalStepCompleted:
			delete(journal.inFlight, entry.Label)
			already := false
			for _, label := range journal.completed {
				if label == entry.Label {
					already = true
					break
				}
			}
			if !already {
				journal.completed = append(journal.completed, entry.Label)
			}
			agents := make([]wire.AgentID, 0, len(entry.Agents))
			for _, name := range entry.Agents {
				if agent, err := wire.NewAgentID(name); err == nil {
					agents = append(agents, agent)
				}
			}
			journal.agents[entry.Label] = agents
			journal.outputs[entry.Label] = entry.Output
			if entry.Input != "" {
				journal.inputs[entry.Label] = entry.Input
			} else {
				delete(journal.inputs, entry.Label)
			}
		case journalStepCompensated:
			for index, label := range journal.completed {
				if label == entry.Label {
					journal.completed = append(journal.completed[:index], journal.completed[index+1:]...)
					break
				}
			}
			delete(journal.inFlight, entry.Label)
			delete(journal.agents, entry.Label)
			delete(journal.outputs, entry.Label)
			delete(journal.inputs, entry.Label)
		}
	}
	return journal
}

// replay replays this run's whole journal, every retained record of the
// run's partition, never a window. A freshly minted run id has no records.
// A non-empty journal whose retained records do not begin with the run's
// start cannot say which steps completed, so it is refused rather than
// read as a run with nothing done.
func (workflow *Workflow) replay(
	ctx context.Context, runID wire.ConversationID, resumed bool,
) (workflowJournal, error) {
	if !resumed {
		return newWorkflowJournal(), nil
	}
	messages, err := workflow.laser.Context(runID).
		FetchWith(ctx, []string{wire.AgentJournal}, LastN(math.MaxInt32))
	if err != nil {
		return workflowJournal{}, err
	}
	entries := make([]workflowJournalEntry, 0, len(messages))
	for _, message := range messages {
		var entry workflowJournalEntry
		if err := wire.Unmarshal(message.Body(), &entry); err != nil {
			continue
		}
		entries = append(entries, entry)
	}
	journal := foldWorkflowJournal(entries)
	if !journal.started && journal.records > 0 {
		return workflowJournal{}, &JournalTruncatedError{Run: runID}
	}
	return journal, nil
}

// record appends one record of this run to the journal topic, keyed by the
// run.
func (workflow *Workflow) record(
	ctx context.Context, runID wire.ConversationID, entry workflowJournalEntry,
) error {
	body, err := wire.Marshal(entry)
	if err != nil {
		return &CodecError{Reason: "encode the workflow journal entry", Cause: err}
	}
	return workflow.laser.Context(runID).Append(ctx, wire.AgentJournal, body)
}
