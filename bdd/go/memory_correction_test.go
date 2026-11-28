package bdd_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	bdd "github.com/laserdata/laser-sdk/bdd/go"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// TestGivenTheSharedMemoryCorrectionScenariosWhenRunThenShouldHold runs the
// log-memory feature: the first write creates the topic, a correction
// supersedes the old fact for the user in every conversation, and semantic
// and keyword recall each rank the folded facts their own way.
func TestGivenTheSharedMemoryCorrectionScenariosWhenRunThenShouldHold(t *testing.T) {
	runFeature(t, "memory_correction.feature", 2*time.Minute, memoryCorrectionStep)
}

const (
	// memoryNamespace and memoryUser are what the feature says every fact
	// here is remembered under.
	memoryNamespace = "prefs"
	memoryUser      = "ana"
)

// correctionFlow is what the scenarios build up: the last remembered id, so
// a correction can supersede it, and a conversation per label the feature
// names.
type correctionFlow struct {
	lastID        laser.MemoryID
	conversations map[string]wire.ConversationID
}

func correctionFlowOf(state *world) *correctionFlow {
	if existing, found := state.get("memory-correction"); found {
		return existing.(*correctionFlow)
	}
	flow := &correctionFlow{conversations: map[string]wire.ConversationID{}}
	state.put("memory-correction", flow)
	return flow
}

// conversationFor is the conversation a label names, minted on first use so
// each label is its own conversation across the scenario.
func (flow *correctionFlow) conversationFor(label string) wire.ConversationID {
	if label == "" {
		return wire.ConversationID{}
	}
	if known, found := flow.conversations[label]; found {
		return known
	}
	minted := laser.NewConversationID()
	flow.conversations[label] = minted
	return minted
}

func memoryCorrectionStep(ctx context.Context, state *world, step bdd.Step) error {
	handled, err := background(ctx, state, step)
	if handled {
		return err
	}
	flow := correctionFlowOf(state)
	text := step.Text
	arguments := argumentsOf(step)

	switch {
	case strings.HasPrefix(text, "the user states "):
		if len(arguments) < 1 {
			return fmt.Errorf("want a fact, got %v", arguments)
		}
		return userStates(ctx, state, flow, arguments)

	case strings.HasPrefix(text, "the user corrects it to "):
		if len(arguments) < 1 {
			return fmt.Errorf("want a corrected fact, got %v", arguments)
		}
		return userCorrects(ctx, state, flow, arguments)

	case text == "the memory topic exists on the stream":
		return memoryTopicExists(ctx, state)

	case strings.HasPrefix(text, "a folded recall for the user returns"):
		return checkMemoryRecall(ctx, state, flow, laser.MemoryQuery{}, arguments, true)

	case strings.HasPrefix(text, "a folded semantic recall for"):
		if len(arguments) < 2 {
			return fmt.Errorf("want a query and the expected facts, got %v", arguments)
		}
		return checkMemoryRecall(ctx, state, flow, laser.MemoryQuery{
			Query: arguments[0], Strategy: laser.RecallAuto,
		}, arguments[1:], true)

	case strings.HasPrefix(text, "a folded keyword recall for"):
		if len(arguments) < 2 {
			return fmt.Errorf("want a query and the expected facts, got %v", arguments)
		}
		return checkMemoryRecall(ctx, state, flow, laser.MemoryQuery{
			Query: arguments[0], Strategy: laser.RecallKeyword,
		}, arguments[1:], true)

	case strings.HasPrefix(text, "a recall for the user returns"):
		return checkMemoryRecall(ctx, state, flow, laser.MemoryQuery{}, arguments, false)

	default:
		return fmt.Errorf("this runner does not implement the step")
	}
}

// memoryFor opens the feature's memory, scoped to the user and, when the
// step named one, a conversation.
func memoryFor(state *world, flow *correctionFlow, label string) (*laser.Memory, error) {
	user, err := wire.NewAgentID(memoryUser)
	if err != nil {
		return nil, err
	}
	scope := laser.MemoryScope{Agent: &user}
	if label != "" {
		conversation := flow.conversationFor(label)
		scope.Conversation = &conversation
	}
	return state.client.Memory(memoryNamespace).For(scope), nil
}

// labelOf is the conversation label a step named, or empty.
func labelOf(arguments []string) string {
	if len(arguments) > 1 {
		return arguments[1]
	}
	return ""
}

func userStates(
	ctx context.Context, state *world, flow *correctionFlow, arguments []string,
) error {
	memory, err := memoryFor(state, flow, labelOf(arguments))
	if err != nil {
		return err
	}
	id, err := memory.Remember(ctx, laser.MemoryFact, []byte(arguments[0]))
	if err != nil {
		return err
	}
	flow.lastID = id
	return nil
}

// userCorrects supersedes the last fact, which is one call that remembers
// the new one and forgets the old for the user in every conversation.
func userCorrects(
	ctx context.Context, state *world, flow *correctionFlow, arguments []string,
) error {
	memory, err := memoryFor(state, flow, labelOf(arguments))
	if err != nil {
		return err
	}
	id, err := memory.Supersede(ctx, flow.lastID, laser.MemoryFact, []byte(arguments[0]))
	if err != nil {
		return err
	}
	flow.lastID = id
	return nil
}

func memoryTopicExists(ctx context.Context, state *world) error {
	exists, err := state.client.Stream(state.stream).Topic(wire.AgentMemory).Exists(ctx)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("want the memory topic created by the first write")
	}
	return nil
}

// foldedRecall recalls for the user across every conversation and checks the
// facts and their order.
//
// Scoped to the user alone, not to a conversation: the feature's point is
// that a correction holds for the user everywhere.
func checkMemoryRecall(
	ctx context.Context, state *world, flow *correctionFlow,
	query laser.MemoryQuery, expected []string, folded bool,
) error {
	memory, err := memoryFor(state, flow, "")
	if err != nil {
		return err
	}
	recall := memory.Recall
	if folded {
		recall = memory.RecallFolded
	}
	items, err := recall(ctx, query)
	if err != nil {
		return err
	}
	got := make([]string, 0, len(items))
	for _, item := range items {
		got = append(got, item.Text())
	}
	if !equalStrings(got, expected) {
		return fmt.Errorf("want the recall %v, got %v", expected, got)
	}
	return nil
}
