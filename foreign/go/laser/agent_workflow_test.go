package laser_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// workflowServer is a topic-aware log: every topic a workflow run touches
// (agent.sessions for the run's own session and every step's contract,
// agent.workflow_journal for the journal) keeps its own append-only list,
// so a journal replay never sees session or contract traffic, and vice
// versa, the way contractServer's single flat list would.
type workflowServer struct {
	clusterOfOne
	mutex   sync.Mutex
	records map[string][]iggcon.IggyMessage

	// replyKind, when set, auto-answers every published command inline, the
	// way a target agent would. Empty sends no reply.
	replyKind   wire.AgentKind
	replyBody   []byte
	replySource wire.AgentID
	silent      map[string]bool
}

func newWorkflowServer() *workflowServer {
	return &workflowServer{records: map[string][]iggcon.IggyMessage{}}
}

func (server *workflowServer) Close() error                  { return nil }
func (server *workflowServer) Connect(context.Context) error { return nil }

func (server *workflowServer) GetTopic(
	_ context.Context, _, _ iggcon.Identifier,
) (*iggcon.TopicDetails, error) {
	details := &iggcon.TopicDetails{}
	details.PartitionsCount = 1
	return details, nil
}

func (server *workflowServer) push(topic string, message iggcon.IggyMessage) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	message.Header.Offset = uint64(len(server.records[topic]))
	server.records[topic] = append(server.records[topic], message)
}

func (server *workflowServer) SendMessages(
	_ context.Context, _, topicID iggcon.Identifier,
	_ iggcon.Partitioning, messages []iggcon.IggyMessage,
) (*iggcon.SendMessagesResponse, error) {
	name, _ := topicID.String()
	for index := range messages {
		server.push(name, messages[index])
	}
	if server.replyKind == "" {
		return &iggcon.SendMessagesResponse{}, nil
	}
	for _, message := range messages {
		var command wire.AgentEnvelope
		if err := wire.Unmarshal(message.Payload, &command); err != nil ||
			command.Kind != wire.KindCommand || command.Correlation == nil {
			continue
		}
		target := ""
		if command.Target != nil {
			target = command.Target.String()
		}
		if server.silent[target] {
			continue
		}
		reply := wire.AgentEnvelope{
			Kind:         server.replyKind,
			Record:       &wire.RecordID{ID: wire.IDFromBytes(laser.NewOperationID())},
			Conversation: command.Conversation,
			Source:       server.replySource,
			Target:       &command.Source,
			Correlation:  command.Correlation,
			Body:         server.replyBody,
		}
		payload, err := wire.Marshal(&reply)
		if err != nil {
			continue
		}
		headers, err := laser.EncodeHeaderBlock(
			wire.HeadersForEnvelope(&reply, wire.ContentCBOR, false))
		if err != nil {
			continue
		}
		replyMessage, err := iggcon.NewIggyMessage(payload, iggcon.WithUserHeaders(headers))
		if err != nil {
			continue
		}
		server.push(name, replyMessage)
	}
	return &iggcon.SendMessagesResponse{}, nil
}

func (server *workflowServer) PollMessages(
	_ context.Context, _, topicID iggcon.Identifier, _ uint32,
	_ iggcon.Consumer, strategy iggcon.PollingStrategy, _ iggcon.PollOptions,
) (*iggcon.PolledMessage, error) {
	name, _ := topicID.String()
	server.mutex.Lock()
	defer server.mutex.Unlock()
	records := server.records[name]
	from := uint64(0)
	if strategy.Kind == iggcon.POLLING_OFFSET {
		from = strategy.Value
	}
	if from >= uint64(len(records)) {
		return &iggcon.PolledMessage{}, nil
	}
	return &iggcon.PolledMessage{
		Messages: append([]iggcon.IggyMessage(nil), records[from:]...),
	}, nil
}

// on is every record published to one topic.
func (server *workflowServer) on(topic string) []iggcon.IggyMessage {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	return append([]iggcon.IggyMessage(nil), server.records[topic]...)
}

func noopStep(context.Context, map[string][]byte) ([]byte, error) { return nil, nil }

func TestGivenLocalStepsWhenRunThenShouldPassOutputsInDependencyOrder(t *testing.T) {
	server := newWorkflowServer()
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})

	outcome, err := handle.Workflow(agentID(t, "pipeline"), "agent.sessions").
		StepLocal("ingest", func(context.Context, map[string][]byte) ([]byte, error) {
			return []byte("12 frames"), nil
		}).
		StepLocal("report", func(_ context.Context, outputs map[string][]byte) ([]byte, error) {
			return append([]byte("report: "), outputs["ingest"]...), nil
		}).After("ingest").
		Run(context.Background())
	if err != nil {
		t.Fatalf("run the workflow: %v", err)
	}
	if len(outcome.CompletedSteps) != 2 ||
		outcome.CompletedSteps[0] != "ingest" || outcome.CompletedSteps[1] != "report" {
		t.Fatalf("want ingest then report, got %v", outcome.CompletedSteps)
	}
	if text, ok := outcome.Text("report"); !ok || text != "report: 12 frames" {
		t.Errorf("want the report step's output to build on ingest's, got %q (ok=%v)", text, ok)
	}
	if len(server.on("agent.workflow_journal")) == 0 {
		t.Error("want journal records written")
	}
	if len(server.on("agent.sessions")) == 0 {
		t.Error("want the run's own session lifecycle records on agent.sessions")
	}
}

func TestGivenADirectedStepWhenRunThenShouldDispatchViaContract(t *testing.T) {
	server := newWorkflowServer()
	server.replyKind = wire.KindResponse
	server.replyBody = []byte("done")
	server.replySource = agentID(t, "worker")
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})

	outcome, err := handle.Workflow(agentID(t, "pipeline"), "agent.sessions").
		Step("ingest", laser.To(agentID(t, "worker")),
			func(context.Context, map[string][]byte) ([]byte, error) {
				return []byte("task"), nil
			}).
		Run(context.Background())
	if err != nil {
		t.Fatalf("run the workflow: %v", err)
	}
	if text, ok := outcome.Text("ingest"); !ok || text != "done" {
		t.Errorf("want the dispatched reply as the step output, got %q (ok=%v)", text, ok)
	}
}

func TestGivenAnAllCapableStepWhenRunThenShouldJoinCompletedReplies(t *testing.T) {
	server := newWorkflowServer()
	server.replyKind = wire.KindResponse
	server.replyBody = []byte("ok")
	server.replySource = agentID(t, "worker-a")
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	registry := registryWith(t, time.Now(), map[string]wire.AgentCard{
		"worker-a": cardFor("review", nil, nil, nil, nil),
		"worker-b": cardFor("review", nil, nil, nil, nil),
	})

	outcome, err := handle.Workflow(agentID(t, "pipeline"), "agent.sessions").
		Registry(registry).
		Step("review", laser.AllCapable("review", laser.RouteAny),
			func(context.Context, map[string][]byte) ([]byte, error) {
				return []byte("please review"), nil
			}).
		Run(context.Background())
	if err != nil {
		t.Fatalf("run the workflow: %v", err)
	}
	text, ok := outcome.Text("review")
	if !ok {
		t.Fatal("want the review step's output present")
	}
	if text != "ok\nok" {
		t.Errorf("want both agents' replies joined, got %q", text)
	}
}

func TestGivenADuplicateLabelWhenRunThenShouldRefuseBeforeWriting(t *testing.T) {
	server := newWorkflowServer()
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})

	_, err := handle.Workflow(agentID(t, "pipeline"), "agent.sessions").
		StepLocal("ingest", noopStep).
		StepLocal("ingest", noopStep).
		Run(context.Background())

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want an invalid-argument refusal, got %T: %v", err, err)
	}
	if len(server.on("agent.workflow_journal")) != 0 || len(server.on("agent.sessions")) != 0 {
		t.Error("want nothing written before validation refused the run")
	}
}

func TestGivenADependencyCycleWhenRunThenShouldRefuse(t *testing.T) {
	server := newWorkflowServer()
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})

	_, err := handle.Workflow(agentID(t, "pipeline"), "agent.sessions").
		StepLocal("a", noopStep).After("b").
		StepLocal("b", noopStep).After("a").
		Run(context.Background())

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want a cycle refusal, got %T: %v", err, err)
	}
}

func TestGivenAVerifierThatRejectsWhenRunThenShouldFailTheStep(t *testing.T) {
	server := newWorkflowServer()
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})

	_, err := handle.Workflow(agentID(t, "pipeline"), "agent.sessions").
		StepLocal("ingest", func(context.Context, map[string][]byte) ([]byte, error) {
			return []byte("bad"), nil
		}).VerifyWith(func(context.Context, []byte) (bool, error) { return false, nil }).
		Run(context.Background())

	var failed *laser.StepFailedError
	if !errors.As(err, &failed) {
		t.Fatalf("want a step failure, got %T: %v", err, err)
	}
	if failed.Reason != laser.StepUnverified {
		t.Errorf("want unverified, got %v", failed.Reason)
	}
}

func TestGivenAnInvocationsBudgetWhenRunThenShouldFailOnceExceeded(t *testing.T) {
	server := newWorkflowServer()
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})

	_, err := handle.Workflow(agentID(t, "pipeline"), "agent.sessions").
		Budget(laser.WorkflowBudget{}.WithInvocations(1)).
		StepLocal("a", noopStep).
		StepLocal("b", noopStep).After("a").
		Run(context.Background())

	var budget *laser.BudgetExceededError
	if !errors.As(err, &budget) {
		t.Fatalf("want a budget refusal, got %T: %v", err, err)
	}
}

func TestGivenAResumedRunWhenRunAgainThenShouldSkipCompletedSteps(t *testing.T) {
	server := newWorkflowServer()
	server.replyKind = wire.KindResponse
	server.replyBody = []byte("done")
	server.replySource = agentID(t, "worker")
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	runID := laser.NewConversationID()
	source := agentID(t, "pipeline")
	worker := agentID(t, "worker")
	task := func(context.Context, map[string][]byte) ([]byte, error) { return []byte("task"), nil }
	build := func() *laser.Workflow { return handle.Workflow(source, "agent.sessions").RunID(runID) }

	// First run: a budget of one invocation lets step "a" complete and
	// stops before "b" is ever dispatched.
	_, err := build().
		Budget(laser.WorkflowBudget{}.WithInvocations(1)).
		Step("a", laser.To(worker), task).
		Step("b", laser.To(worker), task).After("a").
		Run(context.Background())
	var budget *laser.BudgetExceededError
	if !errors.As(err, &budget) {
		t.Fatalf("want the first run to stop on its budget, got %T: %v", err, err)
	}

	// Resume under the same run id, no budget this time: "b" now
	// dispatches and completes. "a" must not be dispatched a second time.
	outcome, err := build().
		Step("a", laser.To(worker), task).
		Step("b", laser.To(worker), task).After("a").
		Run(context.Background())
	if err != nil {
		t.Fatalf("resume the workflow: %v", err)
	}
	if len(outcome.CompletedSteps) != 2 {
		t.Fatalf("want both steps completed on resume, got %v", outcome.CompletedSteps)
	}

	commands := 0
	for _, message := range server.on("agent.sessions") {
		var envelope wire.AgentEnvelope
		if err := wire.Unmarshal(message.Payload, &envelope); err == nil &&
			envelope.Kind == wire.KindCommand {
			commands++
		}
	}
	if commands != 2 {
		t.Errorf("want exactly one command per step across both runs (a not re-dispatched), got %d", commands)
	}
}

func TestGivenAFailedLocalStepWhenRunThenShouldCompensateCompletedStepsInReverseOrder(t *testing.T) {
	server := newWorkflowServer()
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	var mutex sync.Mutex
	var compensations []string
	undo := func(label string) laser.StepFunc {
		return func(context.Context, map[string][]byte) ([]byte, error) {
			mutex.Lock()
			compensations = append(compensations, "undo "+label)
			mutex.Unlock()
			return []byte("undo " + label), nil
		}
	}

	_, err := handle.Workflow(agentID(t, "billing"), "agent.sessions").
		StepLocal("reserve", func(context.Context, map[string][]byte) ([]byte, error) {
			return []byte("reserve done in process"), nil
		}).CompensateWith(undo("reserve")).
		StepLocal("charge", func(context.Context, map[string][]byte) ([]byte, error) {
			return []byte("charge done in process"), nil
		}).After("reserve").CompensateWith(undo("charge")).
		StepLocal("ship", func(context.Context, map[string][]byte) ([]byte, error) {
			return nil, errors.New("ship failed")
		}).After("charge").
		Run(context.Background())

	if err == nil || !strings.Contains(err.Error(), "ship failed") {
		t.Fatalf("want the ship step's own error, got %v", err)
	}
	want := []string{"undo charge", "undo reserve"}
	if !reflect.DeepEqual(compensations, want) {
		t.Fatalf("want compensations in reverse order %v, got %v", want, compensations)
	}
}

func TestGivenADirectedStepCompensationWhenALaterStepFailsThenShouldDispatchItToTheOriginalAgent(t *testing.T) {
	server := newWorkflowServer()
	server.replyKind = wire.KindResponse
	server.replyBody = []byte("reserved")
	server.replySource = agentID(t, "worker")
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	worker := agentID(t, "worker")

	_, err := handle.Workflow(agentID(t, "billing"), "agent.sessions").
		Step("reserve", laser.To(worker), func(context.Context, map[string][]byte) ([]byte, error) {
			return []byte("reserve"), nil
		}).CompensateWith(func(context.Context, map[string][]byte) ([]byte, error) {
		return []byte("undo reserve"), nil
	}).
		StepLocal("ship", func(context.Context, map[string][]byte) ([]byte, error) {
			return nil, errors.New("ship failed")
		}).After("reserve").
		Run(context.Background())
	if err == nil {
		t.Fatal("want the run to fail")
	}

	commandsToWorker := 0
	for _, message := range server.on("agent.sessions") {
		var envelope wire.AgentEnvelope
		if err := wire.Unmarshal(message.Payload, &envelope); err == nil &&
			envelope.Kind == wire.KindCommand &&
			envelope.Target != nil && *envelope.Target == worker {
			commandsToWorker++
		}
	}
	// The original "reserve" dispatch, plus its compensation, both addressed
	// to the same agent that ran the original step.
	if commandsToWorker != 2 {
		t.Errorf("want the original dispatch and its compensation both addressed to worker, got %d", commandsToWorker)
	}
}

func TestGivenAResumedRunAfterCompensationWhenRunAgainThenShouldRerunTheCompensatedStep(t *testing.T) {
	server := newWorkflowServer()
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	runID := laser.NewConversationID()
	source := agentID(t, "billing")
	var reserveRuns int32
	var shipShouldFail atomic.Bool
	shipShouldFail.Store(true)

	runOnce := func() (*laser.WorkflowOutcome, error) {
		return handle.Workflow(source, "agent.sessions").RunID(runID).
			StepLocal("reserve", func(context.Context, map[string][]byte) ([]byte, error) {
				atomic.AddInt32(&reserveRuns, 1)
				return []byte("reserved"), nil
			}).CompensateWith(func(context.Context, map[string][]byte) ([]byte, error) {
			return []byte("undo reserve"), nil
		}).
			StepLocal("ship", func(context.Context, map[string][]byte) ([]byte, error) {
				if shipShouldFail.Load() {
					return nil, errors.New("ship failed")
				}
				return []byte("shipped"), nil
			}).After("reserve").
			Run(context.Background())
	}

	if _, err := runOnce(); err == nil {
		t.Fatal("want the first run to fail")
	}
	if reserveRuns != 1 {
		t.Fatalf("want reserve to run once on the first attempt, got %d", reserveRuns)
	}

	shipShouldFail.Store(false)
	outcome, err := runOnce()
	if err != nil {
		t.Fatalf("resume the workflow: %v", err)
	}
	if reserveRuns != 2 {
		t.Errorf("want reserve to run again after its compensation, got %d runs", reserveRuns)
	}
	if _, ok := outcome.Text("reserve"); !ok {
		t.Error("want reserve present in the resumed outcome")
	}
}
