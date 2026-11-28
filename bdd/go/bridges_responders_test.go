package bdd_test

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/bridges"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// responderStarted is how long a spawned responder's consumer is given to
// begin reading before a step publishes the work it answers.
//
// The Go client has no ready signal to await, unlike the Rust and TypeScript
// runtimes' AgentHandle.ready(): a standalone consumer with no stored offset
// starts from the first record either way, so the wait only covers the time
// between opening the reader and its first poll actually reaching the server.
const responderStarted = 300 * time.Millisecond

// bddResponder answers every command addressed to it with its own name,
// recording what each command was: a bridged A2A task, a bridged MCP tool
// call, or a direct input request.
type bddResponder struct {
	name string

	mutex    sync.Mutex
	answered []string
}

func (responder *bddResponder) record(label string) {
	responder.mutex.Lock()
	responder.answered = append(responder.answered, label)
	responder.mutex.Unlock()
}

func (responder *bddResponder) snapshot() []string {
	responder.mutex.Lock()
	defer responder.mutex.Unlock()
	return append([]string(nil), responder.answered...)
}

// handle answers every AGDX command with the responder's own name, labelling
// it by what it was: a tool carries a bridged MCP call, the chat operation a
// bridged A2A task, and anything else a direct input request.
func (responder *bddResponder) handle(
	ctx context.Context, message *laser.AgentMessage, agent *laser.AgentCtx,
) error {
	envelope := message.Envelope
	if envelope == nil || envelope.Kind != wire.KindCommand {
		return nil
	}
	label := "input"
	switch {
	case envelope.Tool != nil:
		label = "tool"
	case envelope.Operation != nil && *envelope.Operation == wire.OperationChat:
		label = "task"
	}
	responder.record(label)
	return agent.RespondInput(ctx, wire.AgentSessions, []byte(responder.name))
}

// spawnResponderPair spawns both names of "responders X and Y answer every
// command with their own name", each serving every operation.
func spawnResponderPair(ctx context.Context, state *world, arguments []string) error {
	if len(arguments) < 2 {
		return fmt.Errorf("want two responders, got %v", arguments)
	}
	for _, name := range arguments {
		if err := spawnResponder(ctx, state, name, nil); err != nil {
			return err
		}
	}
	return nil
}

// spawnResponderStep spawns the one name a singular "responder" step names,
// under operations, nil for every operation.
func spawnResponderStep(
	ctx context.Context, state *world, arguments []string, operations []string,
) error {
	if len(arguments) < 1 {
		return fmt.Errorf("want a responder name, got %v", arguments)
	}
	return spawnResponder(ctx, state, arguments[0], operations)
}

// spawnResponder opens a standalone consumer over the session lane, runs it
// under the reliable AgentRuntime behind name's gates, and registers the pair
// to stop with the scenario.
func spawnResponder(
	ctx context.Context, state *world, name string, operations []string,
) error {
	identity, err := bridgeAgent(name)
	if err != nil {
		return err
	}
	gate := state.client.Agent(identity, laser.AgentOptions{})
	runtime, err := state.client.AgentRuntime(gate, laser.RuntimeOptions{
		Topic:      wire.AgentSessions,
		RespondOn:  wire.AgentSessions,
		Operations: operations,
	})
	if err != nil {
		return err
	}
	source, err := state.topic(wire.AgentSessions).Consumer(
		ctx, fmt.Sprintf("bdd-responder-%s-%d", name, time.Now().UnixNano()),
		laser.ConsumerOptions{Partitions: []uint32{0}},
	)
	if err != nil {
		return err
	}

	responder := &bddResponder{name: name}
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(runCtx, source, laser.HandlerFunc(responder.handle)) }()
	state.t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			state.t.Logf("responder %s did not stop", name)
		}
	})

	select {
	case <-time.After(responderStarted):
	case <-ctx.Done():
		return ctx.Err()
	}
	state.put("responder:"+name, responder)
	return nil
}

func checkResponderAnswered(state *world, arguments []string) error {
	if len(arguments) < 2 {
		return fmt.Errorf("want a responder and its answers, got %v", arguments)
	}
	responderAny, found := state.get("responder:" + arguments[0])
	responder, ok := responderAny.(*bddResponder)
	if !found || !ok {
		return fmt.Errorf("no responder %q was spawned", arguments[0])
	}
	got := responder.snapshot()
	want := arguments[1:]
	if len(got) != len(want) {
		return fmt.Errorf("want %v, got %v", want, got)
	}
	for index := range want {
		if got[index] != want[index] {
			return fmt.Errorf("want %v, got %v", want, got)
		}
	}
	return nil
}

// submitA2ATaskTo submits an A2A task addressed to target, so only that agent
// is asked to handle it on the shared session topic.
func submitA2ATaskTo(ctx context.Context, state *world, arguments []string) error {
	if len(arguments) < 1 {
		return fmt.Errorf("want a target, got %v", arguments)
	}
	source, err := bridgeAgent("a2a-gateway")
	if err != nil {
		return err
	}
	target, err := bridgeAgent(arguments[0])
	if err != nil {
		return err
	}
	bridge := bridges.NewA2ABridge(state.client, source, wire.AgentSessions, wire.AgentSessions)
	task, err := bridge.SubmitTo(ctx, target, []byte(`{"message":{"role":"user","text":"addressed"}}`))
	if err != nil {
		return err
	}
	state.put("addressedA2ABridge", bridge)
	state.put("addressedA2ATaskID", task.ID)
	return nil
}

func checkA2ATaskCompletesWith(ctx context.Context, state *world, arguments []string) error {
	if len(arguments) < 1 {
		return fmt.Errorf("want the expected text, got %v", arguments)
	}
	bridge, id, err := addressedA2A(state)
	if err != nil {
		return err
	}
	deadline := time.Now().Add(20 * time.Second)
	var last *bridges.A2ATask
	for {
		last, err = bridge.Task(ctx, id)
		if err == nil && last.Status.State == "TASK_STATE_COMPLETED" {
			if len(last.Artifacts) == 0 || len(last.Artifacts[0].Parts) == 0 ||
				last.Artifacts[0].Parts[0].Text != arguments[0] {
				return fmt.Errorf("want artifact text %q, got %+v", arguments[0], last.Artifacts)
			}
			return nil
		}
		if time.Now().After(deadline) {
			if err != nil {
				return err
			}
			return fmt.Errorf("the addressed A2A task did not complete: %+v", last)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func checkA2ACancelRefused(ctx context.Context, state *world) error {
	bridge, id, err := addressedA2A(state)
	if err != nil {
		return err
	}
	if _, err := bridge.Cancel(ctx, id); err == nil {
		return fmt.Errorf("want the cancel of a finished task refused")
	}
	answer := bridge.HandleRPC(ctx,
		[]byte(fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"CancelTask","params":{"id":%q}}`, id)))
	if answer.Error == nil || answer.Error.Code != bridges.CodeTaskNotCancelable {
		return fmt.Errorf("want task not cancelable over JSON-RPC too, got %+v", answer.Error)
	}
	return nil
}

func checkUnknownA2ATaskNotFound(ctx context.Context, state *world) error {
	bridge, _, err := addressedA2A(state)
	if err != nil {
		return err
	}
	unknown := laser.NewConversationID().String()
	if _, err := bridge.Task(ctx, unknown); err == nil {
		return fmt.Errorf("want an id this bridge published nothing for refused")
	}
	answer := bridge.HandleRPC(ctx,
		[]byte(fmt.Sprintf(`{"jsonrpc":"2.0","id":2,"method":"GetTask","params":{"id":%q}}`, unknown)))
	if answer.Error == nil || answer.Error.Code != bridges.CodeTaskNotFound {
		return fmt.Errorf("want task not found over JSON-RPC too, got %+v", answer.Error)
	}
	return nil
}

func addressedA2A(state *world) (*bridges.A2ABridge, string, error) {
	bridgeAny, found := state.get("addressedA2ABridge")
	bridge, ok := bridgeAny.(*bridges.A2ABridge)
	if !found || !ok {
		return nil, "", fmt.Errorf("no addressed task: the submit step never ran")
	}
	idAny, _ := state.get("addressedA2ATaskID")
	id, _ := idAny.(string)
	return bridge, id, nil
}

// callMCPToolOn calls an MCP tool addressed to target, awaiting its reply the
// same way a real tools/call does: a bounded live wait on the reply topic.
func callMCPToolOn(ctx context.Context, state *world, arguments []string) error {
	if len(arguments) < 2 {
		return fmt.Errorf("want a tool and a target, got %v", arguments)
	}
	source, err := bridgeAgent("mcp-gateway")
	if err != nil {
		return err
	}
	target, err := bridgeAgent(arguments[1])
	if err != nil {
		return err
	}
	bridge := bridges.NewMCPBridge(
		state.client, source, wire.AgentSessions, wire.AgentSessions, "tools").
		WithTimeout(15 * time.Second)
	result, err := bridge.CallToolTo(ctx, target, arguments[0], []byte(`{"q":"addressed"}`))
	if err != nil {
		return err
	}
	if len(result.Content) == 0 {
		return fmt.Errorf("want tool content, got none")
	}
	state.put("mcpToolResult", result.Content[0].Text)
	return nil
}

// requestInputFrom asks target for a decision and waits for its answer.
func requestInputFrom(ctx context.Context, state *world, arguments []string) error {
	if len(arguments) < 1 {
		return fmt.Errorf("want a target, got %v", arguments)
	}
	orchestrator, err := bridgeAgent("orchestrator")
	if err != nil {
		return err
	}
	target, err := bridgeAgent(arguments[0])
	if err != nil {
		return err
	}
	decision, err := state.client.Agdx(orchestrator, laser.NewConversationID()).
		RequestInputFrom(ctx, target, wire.AgentSessions, []byte("approve?"), 15*time.Second)
	if err != nil {
		return err
	}
	state.put("inputDecision", string(decision))
	return nil
}

func checkMCPToolResult(state *world, arguments []string) error {
	if len(arguments) < 1 {
		return fmt.Errorf("want the expected result, got %v", arguments)
	}
	return checkStoredState(state, "mcpToolResult", arguments)
}

func checkInputDecision(state *world, arguments []string) error {
	if len(arguments) < 1 {
		return fmt.Errorf("want the expected decision, got %v", arguments)
	}
	return checkStoredState(state, "inputDecision", arguments)
}
