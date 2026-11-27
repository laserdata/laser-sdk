package laser_test

import (
	"context"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"strings"
	"sync"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// budgetServer is a session lane plus a session index a test can steer.
type budgetServer struct {
	stateServer
	indexMutex sync.Mutex
	// info is what the index answers with, nil to refuse the read.
	info *wire.SessionInfo
	// failure is how the index refuses when info is nil.
	failure wire.SessionError
	// reads counts the index reads, so a test can tell a cached verdict
	// from one read again.
	indexReads int
}

func (server *budgetServer) SendBinaryRequest(
	_ context.Context, code uint32, _ []byte,
) ([]byte, error) {
	// A session write checks the registered lane against the source it is
	// writing to, so this has to agree with laneStream.
	if code == wire.AgdxSessionSourcesCode {
		return wire.Marshal(wire.SessionReply{Outcome: &wire.SessionOutcome{
			Kind: wire.SessionSourcesOutcome,
			Sources: &wire.SessionSources{Lane: &wire.LaneIdentity{
				TopicID: 1, Generation: 1, Partitions: 1}},
		}})
	}
	if code != wire.AgdxSessionGetCode {
		return nil, nil
	}
	server.indexMutex.Lock()
	server.indexReads++
	info, failure := server.info, server.failure
	server.indexMutex.Unlock()
	reply := wire.SessionReply{Outcome: &wire.SessionOutcome{
		Kind: wire.SessionInfoOutcome, Info: info}}
	if info == nil {
		reply = wire.SessionReply{Err: &failure}
	}
	encoded, err := wire.Marshal(reply)
	if err != nil {
		return nil, err
	}
	return encoded, nil
}

func (server *budgetServer) indexed() int {
	server.indexMutex.Lock()
	defer server.indexMutex.Unlock()
	return server.indexReads
}

func (server *budgetServer) answerIndex(info *wire.SessionInfo) {
	server.indexMutex.Lock()
	defer server.indexMutex.Unlock()
	server.info = info
}

func (server *budgetServer) refuseIndex(kind wire.SessionErrorKind) {
	server.indexMutex.Lock()
	defer server.indexMutex.Unlock()
	server.info = nil
	server.failure = wire.SessionError{Kind: kind, Message: "no"}
}

// budgetSession opens a session over a lane, with the index on or off.
func budgetSession(
	t *testing.T, server *budgetServer, indexed bool,
) *laser.Session {
	t.Helper()
	server.session = conversation(1)
	capabilities := laser.Capabilities{Managed: indexed, Sessions: indexed}
	handle := laser.FromClient(server, laser.Options{
		DefaultStream: "agents", Capabilities: &capabilities,
	})
	return handle.Sessions().Open(server.session, "").As(agentID(t, "worker"))
}

// usageRecord puts one record carrying usage on the lane.
func usageRecord(
	t *testing.T, server *budgetServer,
	session wire.ConversationID, usage wire.TokenUsage,
) {
	t.Helper()
	operation := wire.OperationChat
	message := laneRecord(t, session, wire.AgentEnvelope{
		Kind: wire.KindResponse, Source: agentID(t, "worker"),
		Operation: &operation, Usage: &usage,
	})
	server.mutex.Lock()
	defer server.mutex.Unlock()
	message.Header.Offset = uint64(len(server.lane))
	server.lane = append(server.lane, message)
}

// startWithBudget puts a start record carrying a budget on the lane.
func startWithBudget(
	t *testing.T, server *budgetServer,
	session wire.ConversationID, tokens, cost *uint64,
) {
	t.Helper()
	start := wire.SessionStart{Agent: agentID(t, "worker")}
	if tokens != nil || cost != nil {
		start.Budget = &wire.Budget{Tokens: tokens, CostMicros: cost}
	}
	body, err := wire.Marshal(start)
	if err != nil {
		t.Fatalf("encode the start: %v", err)
	}
	operation := wire.OperationSession
	state := wire.TaskSubmitted
	message := laneRecord(t, session, wire.AgentEnvelope{
		Kind: wire.KindStatus, Source: agentID(t, "worker"),
		Operation: &operation, TaskState: &state, Body: body,
	})
	server.mutex.Lock()
	defer server.mutex.Unlock()
	message.Header.Offset = uint64(len(server.lane))
	server.lane = append(server.lane, message)
}

func uintOf(value uint64) *uint64 { return &value }

func TestGivenNoBudgetWhenUsageGrowsThenShouldNeverBreach(t *testing.T) {
	if _, over := laser.BudgetOf(nil, ^uint64(0), ^uint64(0)); over {
		t.Fatal("a session without a budget is never over it")
	}
	if _, over := laser.BudgetOf(&wire.Budget{}, 10, 10); over {
		t.Fatal("a budget with no ceilings sets none")
	}
}

func TestGivenACeilingWhenUsageReachesItThenShouldBreachOnlyPastIt(t *testing.T) {
	budget := &wire.Budget{Tokens: uintOf(100)}
	if _, over := laser.BudgetOf(budget, 100, 0); over {
		t.Fatal("reaching the token ceiling is not passing it")
	}
	cost := &wire.Budget{CostMicros: uintOf(100)}
	if _, over := laser.BudgetOf(cost, 0, 100); over {
		t.Fatal("reaching the cost ceiling is not passing it")
	}
	if _, over := laser.BudgetOf(cost, 0, 101); !over {
		t.Fatal("passing the cost ceiling is passing it")
	}
	breach, over := laser.BudgetOf(budget, 101, 0)
	if !over || breach.Dimension != laser.BudgetTokens ||
		breach.Ceiling != 100 || breach.Spent != 101 {
		t.Fatalf("got %+v %v", breach, over)
	}
}

func TestGivenBothCeilingsPassedWhenCheckedThenShouldNameTheTokensFirst(t *testing.T) {
	// The session index checks tokens before cost, so a client folding the
	// lane must report the same dimension as one reading the index.
	budget := &wire.Budget{Tokens: uintOf(10), CostMicros: uintOf(5)}
	breach, over := laser.BudgetOf(budget, 11, 6)
	if !over || breach.Dimension != laser.BudgetTokens {
		t.Fatalf("got %+v %v", breach, over)
	}
	breach, over = laser.BudgetOf(budget, 10, 6)
	if !over || breach.Dimension != laser.BudgetCostMicros ||
		breach.Ceiling != 5 || breach.Spent != 6 {
		t.Fatalf("got %+v %v", breach, over)
	}
}

func TestGivenABreachWhenDescribedThenShouldNameWhatWasPassed(t *testing.T) {
	for _, probe := range []struct {
		breach  laser.BudgetBreach
		says    string
		carries bool
	}{
		{laser.BudgetBreach{Dimension: laser.BudgetTokens, Ceiling: 5, Spent: 9},
			"token budget", true},
		{laser.BudgetBreach{Dimension: laser.BudgetCostMicros, Ceiling: 5, Spent: 9},
			"cost budget", true},
		{laser.BudgetBreach{Ceiling: 0, Spent: 9}, "over its budget", false},
	} {
		body := probe.breach.ErrorBody()
		if body.Message == nil || !strings.Contains(*body.Message, probe.says) {
			t.Fatalf("got %v, want %q", body.Message, probe.says)
		}
		// Not retryable: the same work against the same ceiling spends the
		// same again, so a retry is only a second way to fail.
		if body.Retryable {
			t.Fatal("a budget breach is not retryable")
		}
		if body.Detail["ceiling"].Uint != probe.breach.Ceiling ||
			body.Detail["spent"].Uint != probe.breach.Spent {
			t.Fatalf("got %+v", body.Detail)
		}
		named, carried := body.Detail["budget"]
		if carried != probe.carries {
			t.Fatalf("got %v %v", named, carried)
		}
		if carried && named.Str != probe.breach.Dimension {
			t.Fatalf("got %q", named.Str)
		}
	}
}

func TestGivenNoIndexWhenCheckedThenShouldFoldTheLane(t *testing.T) {
	server := &budgetServer{}
	session := budgetSession(t, server, false)
	startWithBudget(t, server, server.session, uintOf(100), nil)
	usageRecord(t, server, server.session,
		wire.TokenUsage{InputTokens: 60, OutputTokens: 50})

	breach, over, err := session.BudgetBreach(context.Background())
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	// Input and output are summed: a budget on one of them alone would be a
	// ceiling on half the spend.
	if !over || breach.Spent != 110 || breach.Ceiling != 100 {
		t.Fatalf("got %+v %v", breach, over)
	}
	if server.indexed() != 0 {
		t.Fatal("a deployment without the index must not be asked")
	}
	if got, err := session.OverBudget(context.Background()); err != nil || !got {
		t.Fatalf("got %v %v", got, err)
	}
}

func TestGivenNoStartRecordWhenCheckedThenShouldReadAsWithinBudget(t *testing.T) {
	// Retention removed the start, so the ceiling it was given is no longer
	// on the log. Inventing one would end sessions nobody limited.
	server := &budgetServer{}
	session := budgetSession(t, server, false)
	usageRecord(t, server, server.session,
		wire.TokenUsage{InputTokens: 1_000_000})

	_, over, err := session.BudgetBreach(context.Background())
	if err != nil || over {
		t.Fatalf("got %v %v", over, err)
	}
}

func TestGivenTheIndexWhenItAnswersThenShouldTakeItsVerdict(t *testing.T) {
	server := &budgetServer{}
	session := budgetSession(t, server, true)
	// The lane says nothing at all. The index's flag is what decides.
	server.answerIndex(&wire.SessionInfo{
		OverBudget: true, TokensIn: 60, TokensOut: 50,
		Budget: &wire.Budget{Tokens: uintOf(100)},
	})

	breach, over, err := session.BudgetBreach(context.Background())
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if !over || breach.Dimension != laser.BudgetTokens || breach.Spent != 110 {
		t.Fatalf("got %+v %v", breach, over)
	}
	if server.indexed() == 0 {
		t.Fatal("a deployment with the index must be asked")
	}
}

func TestGivenTheIndexSaysOverButItsCountersDoNotThenShouldStillReportIt(t *testing.T) {
	// The index said the session is over. Denying it because its counters
	// do not line up would let a session the deployment stopped keep going.
	server := &budgetServer{}
	session := budgetSession(t, server, true)
	server.answerIndex(&wire.SessionInfo{
		OverBudget: true, TokensIn: 1, TokensOut: 1,
		Budget: &wire.Budget{Tokens: uintOf(1_000)},
	})

	breach, over, err := session.BudgetBreach(context.Background())
	if err != nil || !over {
		t.Fatalf("got %v %v", over, err)
	}
	if breach.Dimension != "" || breach.Spent != 2 {
		t.Fatalf("got %+v", breach)
	}
}

func TestGivenTheIndexCannotAnswerYetWhenCheckedThenShouldFoldTheLane(t *testing.T) {
	for _, kind := range []wire.SessionErrorKind{
		wire.SessionNotFound, wire.SessionNotRegistered, wire.SessionStale,
		wire.SessionUnavailable,
	} {
		server := &budgetServer{}
		session := budgetSession(t, server, true)
		server.refuseIndex(kind)
		startWithBudget(t, server, server.session, uintOf(10), nil)
		usageRecord(t, server, server.session, wire.TokenUsage{InputTokens: 11})

		// A session the index has not caught up to is not a session with no
		// budget, so the lane answers instead.
		breach, over, err := session.BudgetBreach(context.Background())
		if err != nil || !over || breach.Spent != 11 {
			t.Fatalf("%v: got %+v %v %v", kind, breach, over, err)
		}
	}
}

func TestGivenTheIndexRefusesForAnotherReasonWhenCheckedThenShouldReportIt(t *testing.T) {
	// A read refused for want of a grant is not a reason to fold the lane
	// and act as though the deployment agreed.
	server := &budgetServer{}
	session := budgetSession(t, server, true)
	server.refuseIndex(wire.SessionUnauthorized)
	startWithBudget(t, server, server.session, uintOf(10), nil)

	if _, _, err := session.BudgetBreach(context.Background()); err == nil {
		t.Fatal("a refused read must surface")
	}
}

func TestGivenACostCeilingWhenFoldedThenShouldSumTheRecordedCost(t *testing.T) {
	server := &budgetServer{}
	session := budgetSession(t, server, false)
	startWithBudget(t, server, server.session, nil, uintOf(50))
	usageRecord(t, server, server.session,
		wire.TokenUsage{InputTokens: 1, CostMicros: uintOf(30)})
	usageRecord(t, server, server.session,
		wire.TokenUsage{InputTokens: 1, CostMicros: uintOf(21)})

	breach, over, err := session.BudgetBreach(context.Background())
	if err != nil || !over {
		t.Fatalf("got %v %v", over, err)
	}
	if breach.Dimension != laser.BudgetCostMicros || breach.Spent != 51 {
		t.Fatalf("got %+v", breach)
	}
}

func TestGivenUsageThatWouldWrapWhenFoldedThenShouldStayAtTheCeiling(t *testing.T) {
	// A total that wrapped would read as under budget, which is the one
	// answer a runaway session must not get.
	lifecycle, tokens, _ := laser.FoldLaneUsage([]wire.AgentEnvelope{
		{Usage: &wire.TokenUsage{InputTokens: ^uint64(0), OutputTokens: 5}},
	})
	if tokens != ^uint64(0) {
		t.Fatalf("got %d", tokens)
	}
	if lifecycle.Started {
		t.Fatal("a usage record is not a start")
	}
}

func TestGivenASessionOverItsBudgetWhenFailedThenShouldSayWhy(t *testing.T) {
	server := &budgetServer{}
	session := budgetSession(t, server, false)
	breach := laser.BudgetBreach{
		Dimension: laser.BudgetTokens, Ceiling: 100, Spent: 110}

	if _, err := session.FailOverBudget(context.Background(), breach); err != nil {
		t.Fatalf("fail: %v", err)
	}

	written := recorded(t, &server.stateServer)
	if len(written) != 1 {
		t.Fatalf("got %d records", len(written))
	}
	if written[0].TaskState == nil || *written[0].TaskState != wire.TaskFailed {
		t.Fatalf("got %v", written[0].TaskState)
	}
	var end wire.SessionEnd
	if err := wire.Unmarshal(written[0].Body, &end); err != nil {
		t.Fatalf("decode the end: %v", err)
	}
	// The reason is a pinned word, so a reader can tell a budget stop from
	// any other failure without parsing the message.
	if end.Reason == nil || *end.Reason != laser.BudgetEndReason {
		t.Fatalf("got %v", end.Reason)
	}
	if end.Error == nil || end.Error.Message == nil ||
		!strings.Contains(*end.Error.Message, "token budget") {
		t.Fatalf("got %+v", end.Error)
	}
}

func TestGivenNoIndexWhenAdmittingThenShouldLetTheRecordThrough(t *testing.T) {
	server := &budgetServer{}
	session := budgetSession(t, server, false)
	gate := laser.NewBudgetGate()

	// The lane would say this session is over its budget. Without the index
	// there is nothing to enforce against, so the gate never asks and never
	// stands in the way.
	startWithBudget(t, server, server.session, uintOf(1), nil)
	usageRecord(t, server, server.session, wire.TokenUsage{InputTokens: 100})
	written := server.written()

	if !gate.Admit(context.Background(), session, 0, 1) {
		t.Fatal("a deployment without the index enforces nothing here")
	}
	if server.indexed() != 0 {
		t.Fatal("the index must not be asked")
	}
	if server.written() != written {
		t.Fatal("nothing must be written")
	}
}

func TestGivenASessionWithinBudgetWhenAdmittedThenShouldReadTheIndexOnce(t *testing.T) {
	server := &budgetServer{}
	session := budgetSession(t, server, true)
	server.answerIndex(&wire.SessionInfo{Status: wire.SessionActive})
	gate := laser.NewBudgetGate()

	for index := 0; index < 3; index++ {
		if !gate.Admit(context.Background(), session, 0, 7) {
			t.Fatalf("record %d must be admitted", index)
		}
	}
	// Records of one poll share their partition's head, so one read serves
	// them all rather than one read per record.
	if server.indexed() != 1 {
		t.Fatalf("got %d index reads", server.indexed())
	}

	// A later poll moved the head, so the verdict is read again.
	if !gate.Admit(context.Background(), session, 0, 8) {
		t.Fatal("must be admitted")
	}
	if server.indexed() != 2 {
		t.Fatalf("got %d index reads", server.indexed())
	}
}

func TestGivenASessionOverBudgetWhenAdmittedThenShouldEndItOnce(t *testing.T) {
	server := &budgetServer{}
	session := budgetSession(t, server, true)
	server.answerIndex(&wire.SessionInfo{
		Status: wire.SessionActive, OverBudget: true,
		TokensIn: 110, Budget: &wire.Budget{Tokens: uintOf(100)},
	})
	gate := laser.NewBudgetGate()

	if gate.Admit(context.Background(), session, 0, 1) {
		t.Fatal("a session over its budget must not have its work handled")
	}
	if server.written() != 1 {
		t.Fatalf("got %d records", server.written())
	}
	// A second record of the same session must not write a second terminal
	// record: the session already ended.
	if gate.Admit(context.Background(), session, 0, 2) {
		t.Fatal("must not be admitted")
	}
	if server.written() != 1 {
		t.Fatalf("the session was ended twice, got %d records", server.written())
	}
}

func TestGivenASessionTheIndexShowsEndedWhenAdmittedThenShouldWriteNothing(t *testing.T) {
	server := &budgetServer{}
	session := budgetSession(t, server, true)
	server.answerIndex(&wire.SessionInfo{
		Status: wire.SessionFailed, OverBudget: true,
		TokensIn: 110, Budget: &wire.Budget{Tokens: uintOf(100)},
	})
	gate := laser.NewBudgetGate()

	// Already ended, so its work is skipped and nothing is written: ending
	// it again would append a terminal record to a finished session.
	if gate.Admit(context.Background(), session, 0, 1) {
		t.Fatal("must not be admitted")
	}
	if server.written() != 0 {
		t.Fatalf("got %d records", server.written())
	}
}

func TestGivenAnIndexReadThatFailsWhenAdmittingThenShouldHandleTheRecord(t *testing.T) {
	// A budget nobody could check is not a reason to drop work.
	server := &budgetServer{}
	session := budgetSession(t, server, true)
	server.refuseIndex(wire.SessionUnavailable)
	gate := laser.NewBudgetGate()

	if !gate.Admit(context.Background(), session, 0, 1) {
		t.Fatal("an unreadable budget must not stop the work")
	}
	// Nothing was cached, so the next poll asks again rather than carrying
	// a verdict it never got.
	if !gate.Admit(context.Background(), session, 0, 1) {
		t.Fatal("must be admitted")
	}
	if server.indexed() != 2 {
		t.Fatalf("got %d index reads", server.indexed())
	}
}

func TestGivenASessionTheIndexDoesNotKnowWhenAdmittingThenShouldHandleIt(t *testing.T) {
	server := &budgetServer{}
	session := budgetSession(t, server, true)
	server.refuseIndex(wire.SessionNotFound)
	gate := laser.NewBudgetGate()

	// A session the index has not seen is not a session over its budget,
	// and that verdict is worth caching.
	if !gate.Admit(context.Background(), session, 0, 1) {
		t.Fatal("must be admitted")
	}
	if !gate.Admit(context.Background(), session, 0, 1) {
		t.Fatal("must be admitted")
	}
	if server.indexed() != 1 {
		t.Fatalf("got %d index reads", server.indexed())
	}
}

func TestGivenNoSessionWhenAdmittingThenShouldLetItThrough(t *testing.T) {
	if !laser.NewBudgetGate().Admit(context.Background(), nil, 0, 1) {
		t.Fatal("a record with no session is not one to hold")
	}
}

// declaredRecord puts one record on a per-agent topic a layout declared.
func declaredRecord(
	t *testing.T, server *budgetServer, topic string,
	session wire.ConversationID, envelope wire.AgentEnvelope,
) {
	t.Helper()
	message := laneRecord(t, session, envelope)
	server.mutex.Lock()
	defer server.mutex.Unlock()
	if server.declared == nil {
		server.declared = map[string][]iggcon.IggyMessage{}
	}
	message.Header.Offset = uint64(len(server.declared[topic]))
	server.declared[topic] = append(server.declared[topic], message)
}

func TestGivenUsageOnADeclaredTopicWhenFoldedThenShouldStillCountIt(t *testing.T) {
	// A per-agent topic layout moves a model result off the shared lane. The
	// usage it spent moved with it, and a fold that read the lane alone
	// would report a session over its budget as comfortably within it.
	server := &budgetServer{}
	server.session = conversation(1)
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	worker := agentID(t, "worker")
	handle.SessionsWith(laser.SessionConfig{
		Layout: laser.PerAgentTopicLayout(
			map[wire.AgentID]string{worker: "worker.inbox"}),
	})
	session := handle.Sessions().Open(server.session, "").As(worker)

	startWithBudget(t, server, server.session, uintOf(100), nil)
	operation := wire.OperationChat
	declaredRecord(t, server, "worker.inbox", server.session, wire.AgentEnvelope{
		Kind: wire.KindResponse, Source: worker, Target: &worker,
		Operation: &operation,
		Usage:     &wire.TokenUsage{InputTokens: 60, OutputTokens: 50},
	})

	breach, over, err := session.BudgetBreach(context.Background())
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if !over || breach.Spent != 110 {
		t.Fatalf("the usage off the lane must still count, got %+v %v", breach, over)
	}
}
