package laser

import (
	"context"
	"testing"
	"time"
)

// stepCallJournalEntry is a StepCompleted entry, the shape workflow_test.go's
// own resumedRunState tests feed through foldWorkflowJournal.
func stepCallJournalEntry(label string, output []byte, input string) workflowJournalEntry {
	return workflowJournalEntry{Kind: journalStepCompleted, Label: label, Output: output, Input: input}
}

func resumedFrom(entries ...workflowJournalEntry) *runState {
	return resumedRunState(foldWorkflowJournal(entries))
}

func TestGivenJournalLabelsWhenParsedThenShouldNumberOnlyAWholeNumberAboveOne(t *testing.T) {
	cases := []struct {
		label  string
		name   string
		number string
	}{
		{"scan", "scan", "1"},
		{"scan#2", "scan", "2"},
		{"scan#x", "scan#x", "1"},
		{"scan#1", "scan#1", "1"},
		{"a#b#3", "a#b", "3"},
		{"scan#02", "scan#02", "1"},
		{"scan#+2", "scan#+2", "1"},
		{"#2", "", "2"},
	}
	for _, testCase := range cases {
		name, number := labelCall(testCase.label)
		if name != testCase.name || number != testCase.number {
			t.Errorf("labelCall(%q) = (%q, %q), want (%q, %q)",
				testCase.label, name, number, testCase.name, testCase.number)
		}
	}
}

func TestGivenLargeRecordedCallNumbersWhenArgumentsMatchThenShouldClaimInNumericOrder(t *testing.T) {
	state := resumedFrom(
		stepCallJournalEntry("scan#100000000000000000000", []byte("3"), "same"),
		stepCallJournalEntry("scan#99999999999999999999", []byte("2"), "same"),
		stepCallJournalEntry("scan#4294967296", []byte("1"), "same"),
	)
	same := "same"
	for _, expected := range []string{"1", "2", "3"} {
		result := state.beginCallLocked("scan", &same)
		if result.Kind != StepCallRecorded || string(result.Output) != expected {
			t.Fatalf("want Recorded(%q), got %+v", expected, result)
		}
	}
	result := state.beginCallLocked("scan", nil)
	if result.Kind != StepCallDue || result.Label != "scan" {
		t.Fatalf("want Due(\"scan\"), got %+v", result)
	}
}

func TestGivenAFailedFirstCallBesideARecordedSecondWhenResumedThenShouldRunTheFirstAndAnswerTheSecond(t *testing.T) {
	bandA, bandB := "band-1", "band-2"
	cases := [][2]*string{{nil, nil}, {&bandA, &bandB}}
	for _, digests := range cases {
		state := resumedFrom(stepCallJournalEntry("downlink#2", []byte("2"), ""))
		first := state.beginCallLocked("downlink", digests[0])
		if first.Kind != StepCallDue || first.Label != "downlink" {
			t.Fatalf("want Due(\"downlink\"), got %+v", first)
		}
		second := state.beginCallLocked("downlink", digests[1])
		if second.Kind != StepCallRecorded || string(second.Output) != "2" {
			t.Fatalf("want Recorded(\"2\"), got %+v", second)
		}
	}
}

func TestGivenRecordsWithAndWithoutDigestsWhenACallFindsNoEqualDigestThenShouldClaimOnlyOneWithout(t *testing.T) {
	state := resumedFrom(
		stepCallJournalEntry("scan", []byte("1"), "band-1"),
		stepCallJournalEntry("scan#2", []byte("2"), ""),
		stepCallJournalEntry("scan#3", []byte("3"), "band-3"),
	)
	band9 := "band-9"
	first := state.beginCallLocked("scan", &band9)
	if first.Kind != StepCallRecorded || string(first.Output) != "2" {
		t.Fatalf("want Recorded(\"2\"), got %+v", first)
	}
	second := state.beginCallLocked("scan", &band9)
	if second.Kind != StepCallDue || second.Label != "scan#4" {
		t.Fatalf("want Due(\"scan#4\"), got %+v", second)
	}
	third := state.beginCallLocked("scan", nil)
	if third.Kind != StepCallRecorded || string(third.Output) != "1" {
		t.Fatalf("want Recorded(\"1\"), got %+v", third)
	}
}

func TestGivenTheSecondOfThreeCallsFailedWhenRetriedThenShouldTakeItsLabelAgain(t *testing.T) {
	state := newRunState()
	for _, label := range []string{"scan", "scan#2", "scan#3"} {
		result := state.beginCallLocked("scan", nil)
		if result.Kind != StepCallDue || result.Label != label {
			t.Fatalf("want Due(%q), got %+v", label, result)
		}
	}
	state.abandon("scan#2")
	again := state.beginCallLocked("scan", nil)
	if again.Kind != StepCallDue || again.Label != "scan#2" {
		t.Fatalf("want the abandoned label reused, got %+v", again)
	}
	next := state.beginCallLocked("scan", nil)
	if next.Kind != StepCallDue || next.Label != "scan#4" {
		t.Fatalf("want Due(\"scan#4\"), got %+v", next)
	}
}

func TestGivenRecordsWithEqualArgumentsWhenCalledThenShouldAnswerTheLowestCallFirst(t *testing.T) {
	state := resumedFrom(
		stepCallJournalEntry("scan", []byte("1"), "band-1"),
		stepCallJournalEntry("scan#2", []byte("2"), "band-2"),
		stepCallJournalEntry("scan#3", []byte("3"), "band-2"),
	)
	band1, band2 := "band-1", "band-2"
	cases := []struct {
		digest *string
		output string
	}{
		{&band2, "2"}, {&band2, "3"}, {&band1, "1"},
	}
	for _, testCase := range cases {
		result := state.beginCallLocked("scan", testCase.digest)
		if result.Kind != StepCallRecorded || string(result.Output) != testCase.output {
			t.Fatalf("want Recorded(%q), got %+v", testCase.output, result)
		}
	}
}

func TestGivenThreeRecordedCallsWhenResumedInOrderThenShouldAnswerEachAndRunTheFourth(t *testing.T) {
	state := resumedFrom(
		stepCallJournalEntry("scan", []byte("1"), ""),
		stepCallJournalEntry("scan#2", []byte("2"), ""),
		stepCallJournalEntry("scan#3", []byte("3"), ""),
	)
	for _, expected := range []string{"1", "2", "3"} {
		result := state.beginCallLocked("scan", nil)
		if result.Kind != StepCallRecorded || string(result.Output) != expected {
			t.Fatalf("want Recorded(%q), got %+v", expected, result)
		}
	}
	result := state.beginCallLocked("scan", nil)
	if result.Kind != StepCallDue || result.Label != "scan#4" {
		t.Fatalf("want Due(\"scan#4\"), got %+v", result)
	}
}

func TestGivenAClaimedRecordWhenItsStepIsCalledAgainThenShouldNotAnswerTwice(t *testing.T) {
	state := resumedFrom(stepCallJournalEntry("scan", []byte("1"), "band-1"))
	band1 := "band-1"
	first := state.beginCallLocked("scan", &band1)
	if first.Kind != StepCallRecorded || string(first.Output) != "1" {
		t.Fatalf("want Recorded(\"1\"), got %+v", first)
	}
	second := state.beginCallLocked("scan", &band1)
	if second.Kind != StepCallDue || second.Label != "scan#2" {
		t.Fatalf("want Due(\"scan#2\") (the record is already claimed), got %+v", second)
	}
	third := state.beginCallLocked("scan", nil)
	if third.Kind != StepCallDue || third.Label != "scan#3" {
		t.Fatalf("want Due(\"scan#3\"), got %+v", third)
	}
}

func TestGivenACallRecordedInThisRunWhenItsStepIsCalledAgainThenShouldRunUnderTheNextLabel(t *testing.T) {
	state := newRunState()
	result := state.beginCallLocked("scan", nil)
	if result.Kind != StepCallDue || result.Label != "scan" {
		t.Fatalf("want Due(\"scan\"), got %+v", result)
	}
	state.recorded("scan", []byte("1"))
	again := state.beginCallLocked("scan", nil)
	if again.Kind != StepCallDue || again.Label != "scan#2" {
		t.Fatalf("want Due(\"scan#2\"), got %+v", again)
	}
	if len(state.completed) != 1 || state.completed[0] != "scan" {
		t.Errorf("want completed [\"scan\"], got %v", state.completed)
	}
}

func TestGivenAStepRecordBeingWrittenWhenTheGraceEndsThenShouldEndTheRunAfterIt(t *testing.T) {
	state := newRunState()
	call := state.beginCallLocked("downlink", nil)
	if call.Kind != StepCallDue || call.Label != "downlink" {
		t.Fatalf("want Due(\"downlink\"), got %+v", call)
	}
	state.mutex.Lock()
	if !state.startRecordLocked() {
		t.Fatal("want the record to be allowed to start")
	}
	state.mutex.Unlock()

	settled := make(chan struct{})
	go func() {
		waitForNoInFlightState(state, time.Now().Add(0))
		state.mutex.Lock()
		state.finished = true
		state.mutex.Unlock()
		waitForNoRecordingState(state)
		close(settled)
	}()

	for {
		state.mutex.Lock()
		finished := state.finished
		state.mutex.Unlock()
		if finished {
			break
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case <-settled:
		t.Fatal("want settle still waiting on the in-flight record")
	default:
	}
	state.mutex.Lock()
	if state.startRecordLocked() {
		t.Error("want a finished run to refuse a new record")
	}
	state.mutex.Unlock()
	state.recorded("downlink", []byte("1"))
	state.endRecord()

	select {
	case <-settled:
	case <-time.After(time.Second):
		t.Fatal("want settle to finish once the record ends")
	}
	if len(state.completed) != 1 || state.completed[0] != "downlink" {
		t.Errorf("want completed [\"downlink\"], got %v", state.completed)
	}
}

func TestGivenAStepBodyThatOutlastsTheGraceWhenTheRunEndsThenShouldRefuseItsRecord(t *testing.T) {
	state := newRunState()
	call := state.beginCallLocked("downlink", nil)
	if call.Kind != StepCallDue || call.Label != "downlink" {
		t.Fatalf("want Due(\"downlink\"), got %+v", call)
	}

	waitForNoInFlightState(state, time.Now())
	state.mutex.Lock()
	state.closing = true
	state.finished = true
	state.mutex.Unlock()

	state.mutex.Lock()
	defer state.mutex.Unlock()
	if !state.closing {
		t.Error("want closing after settle")
	}
	if state.startRecordLocked() {
		t.Error("want a finished run to refuse a new record")
	}
	if len(state.completed) != 0 {
		t.Errorf("want nothing completed, got %v", state.completed)
	}
}

// waitForNoInFlightState/waitForNoRecordingState call runState's own wait
// helpers directly (no WorkflowRun needed), for the two settle-shaped tests
// above that exercise the grace/finished/recording interplay without a full
// network-backed WorkflowRun.
func waitForNoInFlightState(state *runState, deadline time.Time) {
	state.waitForNoInFlight(context.Background(), deadline)
}

func waitForNoRecordingState(state *runState) {
	state.waitForNoRecording(context.Background())
}
