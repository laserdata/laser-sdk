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
	ierror "github.com/apache/iggy/foreign/go/errors"
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
	// answerOn, when set, pushes every auto-reply to that topic instead of
	// the one the command arrived on, which is what a step addressed to an
	// agent's own inbox but answering on the workflow's topic looks like.
	answerOn string

	// missingUntilBootstrapped, when set, makes GetTopic report every agent
	// topic absent until CreateTopic is called, proving Workflow.open's own
	// bootstrap-on-missing-topics fallback rather than a GetTopic that is
	// always found.
	missingUntilBootstrapped bool
	bootstrapped             bool
}

func newWorkflowServer() *workflowServer {
	return &workflowServer{records: map[string][]iggcon.IggyMessage{}}
}

func (server *workflowServer) Close() error                  { return nil }
func (server *workflowServer) Connect(context.Context) error { return nil }

func (server *workflowServer) GetTopic(
	_ context.Context, _, _ iggcon.Identifier,
) (*iggcon.TopicDetails, error) {
	server.mutex.Lock()
	missing := server.missingUntilBootstrapped && !server.bootstrapped
	server.mutex.Unlock()
	if missing {
		return nil, ierror.TopicNameNotFound{}
	}
	details := &iggcon.TopicDetails{}
	details.PartitionsCount = 1
	return details, nil
}

// GetStream reports the stream as already there, so Bootstrap's own
// Stream.Ensure never needs CreateStream.
func (server *workflowServer) GetStream(
	_ context.Context, _ iggcon.Identifier,
) (*iggcon.StreamDetails, error) {
	return laneStream(), nil
}

// CreateTopic answers Bootstrap's own topic creation, flipping GetTopic from
// missing to found from here on.
func (server *workflowServer) CreateTopic(
	_ context.Context, _ iggcon.Identifier, _ string, _ *iggcon.TopicOptions,
) (*iggcon.TopicDetails, error) {
	server.mutex.Lock()
	server.bootstrapped = true
	server.mutex.Unlock()
	return &iggcon.TopicDetails{}, nil
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
	server.mutex.Lock()
	missing := server.missingUntilBootstrapped && !server.bootstrapped
	server.mutex.Unlock()
	if missing {
		return nil, ierror.TopicNameNotFound{}
	}
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
		answerOn := name
		if server.answerOn != "" {
			answerOn = server.answerOn
		}
		server.push(answerOn, replyMessage)
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

// TestGivenAFreshStreamWithNoAgentTopicsWhenALocalWorkflowRunsThenShouldBootstrapThemFirst
// proves Workflow.open's own fallback: a local-steps-only run needs no
// caller-made Bootstrap call, matching workflow_local.feature's own premise
// ("The first run on a stream without agent topics creates them").
func TestGivenAFreshStreamWithNoAgentTopicsWhenALocalWorkflowRunsThenShouldBootstrapThemFirst(t *testing.T) {
	server := newWorkflowServer()
	server.missingUntilBootstrapped = true
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	source := agentID(t, "jobs")

	outcome, err := handle.Workflow(source, "agent.sessions").
		StepLocal("generate", func(context.Context, map[string][]byte) ([]byte, error) {
			return []byte("generate done in process"), nil
		}).
		Run(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if !server.bootstrapped {
		t.Error("want the missing topics to have been bootstrapped before the run wrote anything")
	}
	if text, ok := outcome.Text("generate"); !ok || text != "generate done in process" {
		t.Fatalf("want step output `generate done in process`, got %q (ok=%v)", text, ok)
	}
}

// A rollback is at least as sensitive as the work it undoes, so a step that
// would only run as an authenticated principal must not be undone by
// whoever currently holds the agent's name.
func TestGivenAPrincipalBoundStepWhenCompensatedThenShouldKeepThePrincipal(t *testing.T) {
	worker := agentID(t, "worker")
	route := laser.ToPrincipal(agentID(t, "planner"), laser.PrincipalID(7))

	compensation := laser.CompensationRouteForTest(route, worker)
	if compensation.Kind != laser.RouteToPrincipal {
		t.Fatalf("want a principal-bound compensation, got kind %v", compensation.Kind)
	}
	if compensation.Agent != worker {
		t.Errorf("want the agent that ran the step, got %q", compensation.Agent)
	}
	if compensation.Principal == nil || *compensation.Principal != laser.PrincipalID(7) {
		t.Errorf("want the step's own principal kept, got %v", compensation.Principal)
	}

	// A capability route's principal is carried the same way, since the
	// requirement came from the caller, not from how the agent was named.
	capable := laser.ToCapable("settle", laser.RouteAny).WithPrincipal(laser.PrincipalID(9))
	if kept := laser.CompensationRouteForTest(capable, worker).Principal; kept == nil ||
		*kept != laser.PrincipalID(9) {
		t.Errorf("want a capability route's principal kept, got %v", kept)
	}

	// An unbound step compensates as a plain directed route, with no
	// requirement invented that the original never had.
	plain := laser.CompensationRouteForTest(laser.To(agentID(t, "planner")), worker)
	if plain.Kind != laser.RouteTo || plain.Principal != nil {
		t.Errorf("want a plain directed compensation, got kind %v principal %v",
			plain.Kind, plain.Principal)
	}
}

// A workflow can address each step to the agent's own inbox while its
// answers still come back to the topic the workflow owns.
func TestGivenAnAdvertisedInboxWhenAStepRunsThenShouldAddressTheAgentAndAnswerOnTheWorkflowTopic(t *testing.T) {
	registry, _ := seedPresence(t, []wire.ClientMetadata{
		advertised(t, "worker", "worker-inbox", principalOf(1)),
	})
	server := newWorkflowServer()
	server.replyKind = wire.KindResponse
	server.replyBody = []byte("done")
	server.replySource = agentID(t, "worker")
	// The step's target answers on the workflow's own topic, which is where
	// the contract now awaits it.
	server.answerOn = "agent.sessions"
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})

	outcome, err := handle.Workflow(agentID(t, "pipeline"), "agent.sessions").
		Registry(registry).
		InboxRoute(laser.AdvertisedInbox()).
		Step("settle", laser.To(agentID(t, "worker")),
			func(context.Context, map[string][]byte) ([]byte, error) {
				return []byte("go"), nil
			}).
		Run(context.Background())
	if err != nil {
		t.Fatalf("run the workflow: %v", err)
	}
	if len(outcome.CompletedSteps) != 1 || outcome.CompletedSteps[0] != "settle" {
		t.Fatalf("want the step completed, got %v", outcome.CompletedSteps)
	}

	// The command went to the agent's own inbox, not the workflow topic.
	commands := 0
	for _, message := range server.on("worker-inbox") {
		var envelope wire.AgentEnvelope
		if err := wire.Unmarshal(message.Payload, &envelope); err != nil {
			continue
		}
		if envelope.Kind == wire.KindCommand {
			commands++
		}
	}
	if commands != 1 {
		t.Errorf("want the step's command on the advertised inbox, got %d", commands)
	}
	// The journal records the workflow topic's tail as the dispatch point,
	// so a resumed run scans where the answers actually land. If the reply
	// topic followed the command's inbox, those offsets would name another
	// topic entirely.
	if len(server.on("agent.workflow_journal")) == 0 {
		t.Error("want journal records written")
	}
}

// A run's root session must heartbeat while the run is working, or a
// deployment reads a live run as an abandoned one once its lease expires.
func TestGivenADeclaredStepsRunWhenRunningThenShouldHoldItsLeaseUntilItEnds(t *testing.T) {
	server := newWorkflowServer()
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})

	heldDuringRun := -1
	outcome, err := handle.Workflow(agentID(t, "pipeline"), "agent.sessions").
		StepLocal("work", func(context.Context, map[string][]byte) ([]byte, error) {
			heldDuringRun = handle.Leases().Held()
			return []byte("done"), nil
		}).
		Run(context.Background())
	if err != nil {
		t.Fatalf("run the workflow: %v", err)
	}
	if len(outcome.CompletedSteps) != 1 {
		t.Fatalf("want the step completed, got %v", outcome.CompletedSteps)
	}
	if heldDuringRun != 1 {
		t.Errorf("want the run's lease held while a step runs, got %d", heldDuringRun)
	}
	if held := handle.Leases().Held(); held != 0 {
		t.Errorf("want the lease released once the run ended, got %d", held)
	}
}

// The plain-code form holds its lease across every step the caller drives,
// and gives it up when the run is finished.
func TestGivenAPlainCodeRunWhenFinishedThenShouldHoldItsLeaseUntilThen(t *testing.T) {
	server := newWorkflowServer()
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})

	run, err := handle.Workflow(agentID(t, "pipeline"), "agent.sessions").
		Start(context.Background())
	if err != nil {
		t.Fatalf("start the run: %v", err)
	}
	if held := handle.Leases().Held(); held != 1 {
		t.Fatalf("want the lease held while the run is open, got %d", held)
	}

	if _, err := run.Finish(context.Background(), nil, nil); err != nil {
		t.Fatalf("finish the run: %v", err)
	}
	if held := handle.Leases().Held(); held != 0 {
		t.Errorf("want the lease released once finished, got %d", held)
	}

	// Finishing twice is still refused, and the second attempt has no lease
	// left to drop.
	if _, err := run.Finish(context.Background(), nil, nil); err == nil {
		t.Error("want a second Finish refused")
	}
	if held := handle.Leases().Held(); held != 0 {
		t.Errorf("want no lease after a refused second Finish, got %d", held)
	}
}

// Releasing leaves the run open and resumable but gives the lease up, so an
// interrupted caller stops claiming a session it is no longer driving.
func TestGivenAReleasedRunWhenFinishedLaterThenShouldRefuseAndHoldNoLease(t *testing.T) {
	server := newWorkflowServer()
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})

	run, err := handle.Workflow(agentID(t, "pipeline"), "agent.sessions").
		Start(context.Background())
	if err != nil {
		t.Fatalf("start the run: %v", err)
	}
	run.Release()
	if held := handle.Leases().Held(); held != 0 {
		t.Errorf("want the lease released, got %d", held)
	}
	// Release ends the run for this process, so an ending record must not
	// be written afterwards for a run nobody is driving.
	if _, err := run.Finish(context.Background(), nil, nil); err == nil {
		t.Error("want Finish after Release refused")
	}
	// Releasing twice is a no-op rather than dropping somebody else's lease.
	run.Release()
	if held := handle.Leases().Held(); held != 0 {
		t.Errorf("want no lease after a second Release, got %d", held)
	}
}
