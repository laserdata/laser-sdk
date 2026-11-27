package laser_test

import (
	"strconv"
	"strings"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// evidenceFor is one decision an agent triggered.
func evidenceFor(
	source, decision string, atMicros uint64, id string,
) laser.PolicyEvidence {
	record := laser.PolicyEvidence{
		DecisionID: id, Decision: decision, Mode: "enforce",
		Kind: "publish", Stream: "agents", Topic: "events",
		ChainID: "c1", Outcome: laser.OutcomeEffected, AtMicros: atMicros,
	}
	if source != "" {
		record.Source = &source
	}
	return record
}

// A coordinator asks what an agent has been doing and gets it from evidence
// already read, rather than re-deriving the same counts by hand.
func TestGivenEvidenceWhenFoldedThenShouldCountItByVerdict(t *testing.T) {
	swarm := laser.NewSwarmActivity()

	swarm.Observe(evidenceFor("planner", "allow", 1, "d1"))
	swarm.Observe(evidenceFor("planner", "block", 2, "d2"))
	swarm.Observe(evidenceFor("planner", "block", 3, "d3"))

	activity := swarm.Agent("planner")
	if activity == nil {
		t.Fatal("want the agent's activity")
	}
	if activity.Decisions != 3 {
		t.Errorf("want three decisions, got %d", activity.Decisions)
	}
	if activity.Count("block") != 2 || activity.Count("allow") != 1 {
		t.Errorf("want them counted by verdict, got %v", activity.Verdicts())
	}
	// A verdict this view never folded is zero rather than missing.
	if activity.Count("defer") != 0 {
		t.Errorf("want zero for an unseen verdict, got %d", activity.Count("defer"))
	}
	// An agent nobody has evidence for is absent rather than empty.
	if swarm.Agent("nobody") != nil {
		t.Error("want no activity for an agent nothing was folded for")
	}
}

// Evidence can be read back out of order, so the latest decision is the one
// that happened last rather than the one that arrived last.
func TestGivenOutOfOrderEvidenceWhenFoldedThenShouldKeepTheLatestByTime(t *testing.T) {
	swarm := laser.NewSwarmActivity()

	swarm.Observe(evidenceFor("planner", "allow", 100, "d-late"))
	swarm.Observe(evidenceFor("planner", "block", 50, "d-early"))

	last := swarm.Agent("planner").LastDecision
	if last == nil || last.DecisionID != "d-late" {
		t.Fatalf("want the latest by time, got %+v", last)
	}

	// Two at one instant are ordered by their identity, so the answer is the
	// same however they arrived.
	tied := laser.NewSwarmActivity()
	tied.Observe(evidenceFor("planner", "allow", 7, "b"))
	tied.Observe(evidenceFor("planner", "block", 7, "a"))
	forward := laser.NewSwarmActivity()
	forward.Observe(evidenceFor("planner", "block", 7, "a"))
	forward.Observe(evidenceFor("planner", "allow", 7, "b"))
	if tied.Agent("planner").LastDecision.DecisionID !=
		forward.Agent("planner").LastDecision.DecisionID {
		t.Error("want one answer whichever order they arrived in")
	}
}

// Evidence is replayable, so a replay must not double what it reports.
func TestGivenReplayedEvidenceWhenFoldedThenShouldCountItOnce(t *testing.T) {
	swarm := laser.NewSwarmActivity()
	record := evidenceFor("planner", "allow", 1, "d1")

	swarm.Observe(record)
	swarm.Observe(record)
	swarm.Observe(record)

	if swarm.Agent("planner").Decisions != 1 {
		t.Errorf("want it counted once, got %d",
			swarm.Agent("planner").Decisions)
	}
}

// A decision naming no acting agent is not attributable to one, so it is
// dropped rather than counted against a name nobody chose.
func TestGivenEvidenceWithNoAgentWhenFoldedThenShouldDropIt(t *testing.T) {
	swarm := laser.NewSwarmActivity()

	swarm.Observe(evidenceFor("", "block", 1, "d1"))
	empty := evidenceFor("planner", "block", 2, "d2")
	blank := ""
	empty.Source = &blank
	swarm.Observe(empty)

	if len(swarm.Agents()) != 0 {
		t.Errorf("want nothing attributed, got %+v", swarm.Agents())
	}
}

// A coordinator's summary reads in the order that matters most, and reads the
// same way twice.
func TestGivenSeveralAgentsWhenListedThenShouldOrderThemBusiestFirst(t *testing.T) {
	swarm := laser.NewSwarmActivity()
	for index := range 5 {
		swarm.Observe(evidenceFor("busy", "allow", uint64(index),
			"busy-"+strconv.Itoa(index)))
	}
	for index := range 2 {
		swarm.Observe(evidenceFor("quiet", "allow", uint64(index),
			"quiet-"+strconv.Itoa(index)))
	}
	// Two with the same count, so the tie-break shows.
	swarm.Observe(evidenceFor("alpha", "allow", 1, "a1"))
	swarm.Observe(evidenceFor("alpha", "allow", 2, "a2"))

	agents := swarm.Agents()

	if len(agents) != 3 {
		t.Fatalf("want three agents, got %d", len(agents))
	}
	if agents[0].Agent != "busy" {
		t.Errorf("want the busiest first, got %q", agents[0].Agent)
	}
	// A tie reads by name, so the order is the same every time.
	if agents[1].Agent != "alpha" || agents[2].Agent != "quiet" {
		t.Errorf("want a tie broken by name, got %q then %q",
			agents[1].Agent, agents[2].Agent)
	}
	// An empty view lists nothing rather than failing.
	if len(laser.NewSwarmActivity().Agents()) != 0 {
		t.Error("want an empty view to list nothing")
	}
}

// Verdicts hands back a copy, so a caller counting them cannot change what
// the view holds.
func TestGivenVerdictCountsWhenMutatedThenShouldNotChangeTheView(t *testing.T) {
	swarm := laser.NewSwarmActivity()
	swarm.Observe(evidenceFor("planner", "allow", 1, "d1"))

	counts := swarm.Agent("planner").Verdicts()
	counts["allow"] = 99

	if swarm.Agent("planner").Count("allow") != 1 {
		t.Error("want the view unchanged")
	}
}

// A recovery tool answers what was happening right before a crash from one
// call, rather than stitching three reads together by hand.
func TestGivenAFullBundleWhenSummarizedThenShouldRenderEveryPiece(t *testing.T) {
	source, _ := wire.NewAgentID("planner")
	record := wire.RecordID{ID: wire.IDFromBytes([16]byte{15: 1})}
	detail := "the handler panicked"
	reason := "over budget"
	bundle := laser.AssembleCrashContext(
		[]laser.ContextMessage{{
			Envelope: &wire.AgentEnvelope{
				Kind: wire.KindEvent, Record: &record, Source: source,
				Body: []byte("what it was doing"),
			},
		}},
		&wire.AgentDeadLetter{
			Attempts: 3, Reason: wire.DeadLetterRetryExhausted, Detail: &detail,
		},
		&laser.PolicyEvidence{
			Decision: "block", Outcome: laser.OutcomeBlocked, Reason: &reason,
		},
	)

	summary := bundle.Summarize()

	// One fixed order every time, so two identical crashes read identically.
	journal := strings.Index(summary, "journal (1 message(s))")
	letter := strings.Index(summary, "dead letter:")
	decision := strings.Index(summary, "last decision:")
	if journal != 0 || letter < journal || decision < letter {
		t.Fatalf("want the fixed order, got:\n%s", summary)
	}
	for _, want := range []string{
		"[planner]", "what it was doing", "3 attempt(s)",
		"the handler panicked", "block", "over budget",
	} {
		if !strings.Contains(summary, want) {
			t.Errorf("want %q in the summary, got:\n%s", want, summary)
		}
	}
	// It renders the same way twice.
	if bundle.Summarize() != summary {
		t.Error("want a deterministic summary")
	}
}

// A bundle missing a piece says so rather than leaving a reader guessing
// whether it was absent or forgotten.
func TestGivenAPartialBundleWhenSummarizedThenShouldSayWhatIsAbsent(t *testing.T) {
	bundle := laser.AssembleCrashContext(nil, nil, nil)

	summary := bundle.Summarize()

	for _, want := range []string{
		"journal (0 message(s))", "dead letter: none", "last decision: none",
	} {
		if !strings.Contains(summary, want) {
			t.Errorf("want %q, got:\n%s", want, summary)
		}
	}
}

// A payload is arbitrary bytes, and cutting one at a byte offset can split a
// character in half, so the preview counts characters.
func TestGivenALongPayloadWhenSummarizedThenShouldTruncateAtACharacter(t *testing.T) {
	source, _ := wire.NewAgentID("planner")
	record := wire.RecordID{ID: wire.IDFromBytes([16]byte{15: 1})}
	// Multi-byte characters, so a byte-offset cut would split one.
	long := strings.Repeat("é", 500)
	bundle := laser.AssembleCrashContext([]laser.ContextMessage{{
		Envelope: &wire.AgentEnvelope{
			Kind: wire.KindEvent, Record: &record, Source: source,
			Body: []byte(long),
		},
	}}, nil, nil)

	summary := bundle.Summarize()

	if !strings.Contains(summary, "...") {
		t.Error("want the preview truncated")
	}
	if strings.Contains(summary, "\uFFFD") {
		t.Error("want no character split in half")
	}
	// Bounded: the preview plus its ellipsis, not the whole payload.
	if strings.Count(summary, "é") > laser.MaxPreviewRunes {
		t.Errorf("want at most %d characters, got %d",
			laser.MaxPreviewRunes, strings.Count(summary, "é"))
	}
}

// One line per field is what makes the summary readable, so a payload
// carrying a newline cannot forge a field of its own.
func TestGivenControlCharactersWhenSummarizedThenShouldKeepOneLinePerField(t *testing.T) {
	source, _ := wire.NewAgentID("planner")
	record := wire.RecordID{ID: wire.IDFromBytes([16]byte{15: 1})}
	forged := "ordinary\ndead letter: 99 attempt(s), reason forged\ttab\x00null"
	bundle := laser.AssembleCrashContext([]laser.ContextMessage{{
		Envelope: &wire.AgentEnvelope{
			Kind: wire.KindEvent, Record: &record, Source: source,
			Body: []byte(forged),
		},
	}}, nil, nil)

	summary := bundle.Summarize()

	// The journal is one line, the dead letter is the real one.
	lines := strings.Split(strings.TrimRight(summary, "\n"), "\n")
	if len(lines) != 4 {
		t.Fatalf("want four lines, got %d:\n%s", len(lines), summary)
	}
	if !strings.Contains(summary, "dead letter: none") {
		t.Error("want the real dead-letter line rather than the forged one")
	}
	if strings.Contains(summary, "99 attempt(s)") &&
		strings.HasPrefix(lines[2], "dead letter: 99") {
		t.Error("a payload must not forge a field")
	}
}

// A journal entry that is not an agent record still names who wrote it, from
// the record's own provenance.
func TestGivenAPlainRecordWhenSummarizedThenShouldNameItsAuthor(t *testing.T) {
	source, _ := wire.NewAgentID("planner")
	bundle := laser.AssembleCrashContext([]laser.ContextMessage{
		{Record: &laser.Record{Provenance: &laser.Provenance{Agent: &source}}},
		{},
	}, nil, nil)

	summary := bundle.Summarize()

	if !strings.Contains(summary, "[planner]") {
		t.Errorf("want the author from the provenance, got:\n%s", summary)
	}
	// An entry naming nobody says so rather than leaving a blank.
	if !strings.Contains(summary, "[unknown]") {
		t.Errorf("want an unnamed author reported, got:\n%s", summary)
	}
}
