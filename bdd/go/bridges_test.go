package bdd_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	bdd "github.com/laserdata/laser-sdk/bdd/go"
	"github.com/laserdata/laser-sdk/foreign/go/bridges"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// TestGivenTheSharedBridgeScenariosWhenRunThenShouldHold runs the external
// protocol feature: the hop guard, the A2A task lifecycle, the AG-UI state
// documents, and addressed dispatch between bridges and responding agents.
func TestGivenTheSharedBridgeScenariosWhenRunThenShouldHold(t *testing.T) {
	runFeaturePlanned(t, "bridges.feature", 2*time.Minute, bridgesStep, bridgesPlan)
}

// bridgesPlan names the scenarios this runner does not cover yet, and why.
var bridgesPlan = map[string]string{
	// Both need an agent actually consuming the lane: the Go client has the
	// AGDX gates (sdk/src/agent.rs's Agent) but no handler-dispatch runtime
	// yet to run a responder under, the gap session.feature's own plan names.
	"Addressed bridge calls and input requests reach only their addressee":  "needs an agent consuming the lane",
	"A finished A2A task refuses a cancel and an unknown task is not found": "needs an agent consuming the lane",

	// Both need a reader that frames a whole conversation's chunk stream into
	// AG-UI runs. bridges.EventsFor and AgUiStream render one envelope at a
	// time and do not synthesize the RUN_STARTED/RUN_FINISHED around a chunk
	// stream the way Rust and TypeScript's agui_events does, so there is
	// nothing yet to call for these two.
	"A chunked chat renders a complete AG-UI stream":     "needs an agui_events reader over the conversation",
	"A long chunked chat renders from its opening chunk": "needs an agui_events reader over the conversation",
}

func bridgesStep(ctx context.Context, state *world, step bdd.Step) error {
	handled, err := background(ctx, state, step)
	if handled {
		return err
	}
	text := step.Text
	arguments := argumentsOf(step)

	switch {
	case strings.HasPrefix(text, "bridge \"") && strings.Contains(text, "enters after hops "):
		return bridgeEntersAfterHops(state, arguments)

	case strings.HasPrefix(text, "bridge \"") && strings.Contains(text, "enters the same route"):
		return bridgeEntersSameRoute(state, arguments)

	case strings.HasPrefix(text, "the bridge hops are"):
		return checkBridgeHops(state, arguments)

	case text == "the bridge route is rejected as a loop":
		return checkBridgeLoopRejected(state)

	case text == "I submit and cancel an A2A task":
		return submitAndCancelA2A(ctx, state)

	case strings.HasPrefix(text, "the replayed A2A task state is"):
		return checkStoredState(state, "replayedA2AState", arguments)

	case strings.HasPrefix(text, "caller \"") && strings.Contains(text, "submits a bound A2A task"):
		return submitBoundA2A(ctx, state, arguments)

	case strings.HasPrefix(text, "caller \"") && strings.Contains(text, "tries to cancel the bound A2A task"):
		return recordBoundCancel(ctx, state, arguments[0])

	case text == "an anonymous caller tries to cancel the bound A2A task":
		return recordBoundCancel(ctx, state, "")

	case strings.HasPrefix(text, "the bound A2A cancel outcomes are"):
		return checkBoundOutcomes(state, arguments)

	case text == "I publish an AG-UI count snapshot of 1 and replace it with 2":
		return publishAGUIState(ctx, state)

	case strings.HasPrefix(text, "the reconstructed AG-UI count is"):
		return checkReconstructedCount(state, step)

	default:
		return fmt.Errorf("this runner does not implement the step")
	}
}

// bridgeAgent parses an agent id, wrapping the parse failure with the step's
// own value so a bad fixture names itself.
func bridgeAgent(name string) (wire.AgentID, error) {
	agent, err := wire.NewAgentID(name)
	if err != nil {
		return wire.AgentID{}, fmt.Errorf("agent %q: %w", name, err)
	}
	return agent, nil
}

func bridgeEntersAfterHops(state *world, arguments []string) error {
	if len(arguments) < 2 {
		return fmt.Errorf("want a bridge and hops, got %v", arguments)
	}
	hops, err := bridges.EnterBridge(arguments[0], list(arguments[1]))
	if err != nil {
		return err
	}
	state.put("bridgeHops", hops)
	return nil
}

func checkBridgeHops(state *world, arguments []string) error {
	if len(arguments) < 1 {
		return fmt.Errorf("want the expected hops, got %v", arguments)
	}
	got, _ := state.get("bridgeHops")
	hops, _ := got.([]string)
	if want := list(arguments[0]); !slices.Equal(hops, want) {
		return fmt.Errorf("want hops %v, got %v", want, hops)
	}
	return nil
}

func bridgeEntersSameRoute(state *world, arguments []string) error {
	if len(arguments) < 1 {
		return fmt.Errorf("want a bridge, got %v", arguments)
	}
	previousAny, _ := state.get("bridgeHops")
	previous, _ := previousAny.([]string)
	_, err := bridges.EnterBridge(arguments[0], previous)
	state.put("bridgeLoopRejected", err != nil)
	return nil
}

func checkBridgeLoopRejected(state *world) error {
	rejectedAny, found := state.get("bridgeLoopRejected")
	rejected, _ := rejectedAny.(bool)
	if !found || !rejected {
		return fmt.Errorf("want the route rejected as a loop")
	}
	return nil
}

// submitAndCancelA2A submits a task, cancels it, and polls until the
// cancellation itself is what a fresh read of the task replays: an append is
// acknowledged before every replica serves it, so one read racing the write
// is expected rather than a failure.
func submitAndCancelA2A(ctx context.Context, state *world) error {
	source, err := bridgeAgent("a2a-gateway")
	if err != nil {
		return err
	}
	bridge := bridges.NewA2ABridge(state.client, source, wire.AgentSessions, wire.AgentSessions)
	task, err := bridge.Submit(ctx, []byte(`{"message":{"role":"user","text":"cancel me"}}`))
	if err != nil {
		return err
	}
	if _, err := bridge.Cancel(ctx, task.ID); err != nil {
		return err
	}
	return eventuallyA2A(ctx, func() (*bridges.A2ATask, error) {
		return bridge.Task(ctx, task.ID)
	}, "TASK_STATE_CANCELED", func() {
		state.put("replayedA2AState", "Canceled")
	})
}

// eventuallyA2A polls read until its task settles into wantState, or fails
// once the bound runs out.
func eventuallyA2A(
	ctx context.Context, read func() (*bridges.A2ATask, error),
	wantState string, onSettled func(),
) error {
	deadline := time.Now().Add(2 * time.Second)
	var last *bridges.A2ATask
	var lastErr error
	for {
		last, lastErr = read()
		if lastErr == nil && last.Status.State == wantState {
			onSettled()
			return nil
		}
		if time.Now().After(deadline) {
			if lastErr != nil {
				return lastErr
			}
			return fmt.Errorf("the task did not settle into %s: %+v", wantState, last)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func checkStoredState(state *world, key string, arguments []string) error {
	if len(arguments) < 1 {
		return fmt.Errorf("want the expected state, got %v", arguments)
	}
	got, _ := state.get(key)
	if got != arguments[0] {
		return fmt.Errorf("want %q, got %v", arguments[0], got)
	}
	return nil
}

// boundTaskBindingKey is this scenario's own task-binding secret. It never
// leaves the process, so any fixed value serves: what matters is that every
// step of the scenario shares the one bridge built from it.
var boundTaskBindingKey = [32]byte{3: 1, 7: 1, 11: 1, 19: 1}

func boundA2ABridge(state *world) (*bridges.A2ABridge, error) {
	if bridgeAny, found := state.get("boundBridge"); found {
		return bridgeAny.(*bridges.A2ABridge), nil
	}
	source, err := bridgeAgent("a2a-gateway")
	if err != nil {
		return nil, err
	}
	bridge := bridges.NewA2ABridge(state.client, source, wire.AgentSessions, wire.AgentSessions).
		WithTaskBinding(boundTaskBindingKey, time.Minute)
	state.put("boundBridge", bridge)
	return bridge, nil
}

func submitBoundA2A(ctx context.Context, state *world, arguments []string) error {
	if len(arguments) < 1 {
		return fmt.Errorf("want a caller, got %v", arguments)
	}
	bridge, err := boundA2ABridge(state)
	if err != nil {
		return err
	}
	task, err := bridge.Caller(arguments[0]).Submit(ctx, []byte(`{"message":{"role":"user","text":"bound"}}`))
	if err != nil {
		return err
	}
	state.put("boundTaskID", task.ID)
	state.put("boundOutcomes", []string{})
	return nil
}

// recordBoundCancel tries one caller's cancel and appends what happened: a
// refusal from a caller the id was not bound to reads the same as any other
// rejection a scenario might assert on, "<caller>:refused".
func recordBoundCancel(ctx context.Context, state *world, caller string) error {
	bridge, err := boundA2ABridge(state)
	if err != nil {
		return err
	}
	idAny, found := state.get("boundTaskID")
	id, _ := idAny.(string)
	if !found {
		return fmt.Errorf("no bound task: the submit step never ran")
	}
	var task *bridges.A2ATask
	if caller == "" {
		task, err = bridge.Cancel(ctx, id)
	} else {
		task, err = bridge.Caller(caller).Cancel(ctx, id)
	}
	label := caller
	if label == "" {
		label = "anonymous"
	}
	outcome := "refused"
	if err == nil {
		outcome = task.Status.State
		if outcome == "TASK_STATE_CANCELED" {
			outcome = "Canceled"
		}
	}
	outcomesAny, _ := state.get("boundOutcomes")
	outcomes, _ := outcomesAny.([]string)
	state.put("boundOutcomes", append(outcomes, label+":"+outcome))
	return nil
}

func checkBoundOutcomes(state *world, arguments []string) error {
	if len(arguments) < 1 {
		return fmt.Errorf("want the expected outcomes, got %v", arguments)
	}
	outcomesAny, _ := state.get("boundOutcomes")
	outcomes, _ := outcomesAny.([]string)
	if got := strings.Join(outcomes, ","); got != arguments[0] {
		return fmt.Errorf("want outcomes %q, got %q", arguments[0], got)
	}
	return nil
}

// publishAGUIState writes the session state document a front end mirrors: a
// whole-document replace (the count starting at 1) followed by a
// revision-guarded patch (the count moving to 2), then polls the folded lane
// until the patch is visible there too.
func publishAGUIState(ctx context.Context, state *world) error {
	agent, err := bridgeAgent("agui-gateway")
	if err != nil {
		return err
	}
	session := state.client.Sessions().Open(state.conversation, "").As(agent)

	var one wire.DynValue
	if err := wire.UnmarshalJSON([]byte(`{"count":1}`), &one); err != nil {
		return err
	}
	if _, err := session.State().Replace(ctx, one); err != nil {
		return err
	}
	var two wire.DynValue
	if err := wire.UnmarshalJSON([]byte(`2`), &two); err != nil {
		return err
	}
	if _, err := session.State().Patch(ctx, []wire.PatchOperation{
		wire.ReplaceOperation("/count", two),
	}); err != nil {
		return err
	}

	want := `{"count":2}`
	deadline := time.Now().Add(2 * time.Second)
	for {
		view, err := session.State().Get(ctx)
		if err == nil {
			if encoded, encodeErr := wire.MarshalJSON(view.Document); encodeErr == nil &&
				string(encoded) == want {
				state.put("reconstructedState", string(encoded))
				return nil
			}
		}
		if time.Now().After(deadline) {
			if err != nil {
				return err
			}
			return fmt.Errorf("the state delta did not become visible")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func checkReconstructedCount(state *world, step bdd.Step) error {
	counts := numbersOf(step)
	if len(counts) != 1 {
		return fmt.Errorf("want one count, got %v", counts)
	}
	got, _ := state.get("reconstructedState")
	if want := fmt.Sprintf(`{"count":%d}`, counts[0]); got != want {
		return fmt.Errorf("want %s, got %v", want, got)
	}
	return nil
}
