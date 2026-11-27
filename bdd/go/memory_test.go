package bdd_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	bdd "github.com/laserdata/laser-sdk/bdd/go"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
)

// TestGivenTheSharedMemoryScenariosWhenRunThenShouldHold runs the agentic
// memory feature: content-addressed dedup, recency order, feedback
// re-ranking and forget, against this module's reference engine, plus the
// keyword scenario against the client's own in-process semantic memory.
//
// A local feature: these pin the recall contract rather than a deployment.
func TestGivenTheSharedMemoryScenariosWhenRunThenShouldHold(t *testing.T) {
	runLocalFeature(t, "memory.feature", time.Minute, memoryStep)
}

// memoryBench is the scenario's reference store, or the semantic one for the
// scenarios that name it, and the last recall.
type memoryBench struct {
	engine   *bdd.MemoryEngine
	semantic *laser.VectorMemory
	recalled []string
}

func memoryBenchOf(state *world) *memoryBench {
	if existing, found := state.get("memory"); found {
		return existing.(*memoryBench)
	}
	bench := &memoryBench{}
	state.put("memory", bench)
	return bench
}

func memoryStep(ctx context.Context, state *world, step bdd.Step) error {
	bench := memoryBenchOf(state)
	text := step.Text
	arguments := argumentsOf(step)

	switch {
	case text == "an empty memory store":
		agent, err := bridgeAgent("agent")
		if err != nil {
			return err
		}
		bench.engine = bdd.NewMemoryEngine(agent)
		return nil

	case text == "an empty semantic memory":
		bench.semantic = laser.NewVectorMemory(tokenEmbedder{})
		return nil

	case strings.HasPrefix(text, "I remember the fact "):
		if len(arguments) != 1 {
			return fmt.Errorf("want one fact, got %v", arguments)
		}
		return rememberSemantic(ctx, bench, arguments[0])

	case strings.HasPrefix(text, "I remember ") && strings.Contains(text, " in conversation "):
		if len(arguments) != 2 {
			return fmt.Errorf("want a fact and a conversation, got %v", arguments)
		}
		bench.engine.Remember([]byte(arguments[0]), true, arguments[1])
		return nil

	case strings.HasPrefix(text, "I remember "):
		if len(arguments) != 1 {
			return fmt.Errorf("want one fact, got %v", arguments)
		}
		bench.engine.Remember(
			[]byte(arguments[0]), strings.Contains(text, "with dedup"), "")
		return nil

	case strings.HasPrefix(text, "I forget "):
		if len(arguments) != 1 {
			return fmt.Errorf("want one fact, got %v", arguments)
		}
		return forgetFact(bench, arguments[0])

	case strings.HasPrefix(text, "I give ") && strings.Contains(text, "feedback weight"):
		weight, named := after(text, "feedback weight of ")
		if len(arguments) != 1 || !named {
			return fmt.Errorf("want a fact and a weight in %q", text)
		}
		return improveFact(bench, arguments[0], float64(weight))

	case strings.HasPrefix(text, "the memory holds"):
		held, named := after(text, "the memory holds ")
		if !named {
			return fmt.Errorf("want an item count in %q", text)
		}
		if got := bench.engine.Len(); got != int(held) {
			return fmt.Errorf("want %d items held, got %d", held, got)
		}
		return nil

	case strings.HasPrefix(text, "recalling "):
		limit, named := after(text, "recalling ")
		if !named {
			return fmt.Errorf("want a recall limit in %q", text)
		}
		bench.recalled = bench.engine.Recall(int(limit))
		return recalledIs(bench, arguments)

	case strings.HasPrefix(text, "keyword recall for "):
		if len(arguments) != 2 {
			return fmt.Errorf("want a query and an expected first item, got %v", arguments)
		}
		return semanticRecallFirst(ctx, bench, laser.RecallKeyword, arguments[0], arguments[1])

	case strings.HasPrefix(text, "hybrid recall for "):
		if len(arguments) != 2 {
			return fmt.Errorf("want a query and an expected first item, got %v", arguments)
		}
		return semanticRecallFirst(ctx, bench, laser.RecallHybrid, arguments[0], arguments[1])

	default:
		return fmt.Errorf("this runner does not implement the step")
	}
}

// forgetFact drops the item a body was stored under.
func forgetFact(bench *memoryBench, body string) error {
	id, held := bench.engine.IDOf(body)
	if !held {
		return fmt.Errorf("the store holds no item %q to forget", body)
	}
	bench.engine.Forget(id)
	return nil
}

// improveFact reweights the item a body was stored under.
func improveFact(bench *memoryBench, body string, weight float64) error {
	id, held := bench.engine.IDOf(body)
	if !held {
		return fmt.Errorf("the store holds no item %q to reweight", body)
	}
	bench.engine.Improve(id, weight)
	return nil
}

// recalledIs asserts the recall answered the expected bodies in order, when
// the step named any.
func recalledIs(bench *memoryBench, expected []string) error {
	if len(expected) == 0 {
		return nil
	}
	if !equalStrings(bench.recalled, expected) {
		return fmt.Errorf("want the recall %v, got %v", expected, bench.recalled)
	}
	return nil
}

// rememberSemantic stores a fact in the in-process semantic memory.
func rememberSemantic(ctx context.Context, bench *memoryBench, body string) error {
	if bench.semantic == nil {
		return fmt.Errorf("the scenario opened no semantic memory")
	}
	_, err := bench.semantic.Remember(
		ctx, laser.MemoryScope{}, laser.MemoryFact, []byte(body))
	return err
}

// semanticRecallFirst asserts which item a strategy puts first.
func semanticRecallFirst(
	ctx context.Context, bench *memoryBench,
	strategy laser.RecallStrategy, query, expected string,
) error {
	if bench.semantic == nil {
		return fmt.Errorf("the scenario opened no semantic memory")
	}
	items, err := bench.semantic.Recall(ctx, laser.MemoryScope{}, laser.MemoryQuery{
		Query: query, Strategy: strategy,
	})
	if err != nil {
		return err
	}
	if len(items) == 0 {
		return fmt.Errorf("want %q recalled first, got nothing", expected)
	}
	if items[0].Text() != expected {
		return fmt.Errorf("want %q recalled first, got %q", expected, items[0].Text())
	}
	return nil
}

// tokenEmbedder embeds a text as the tokens it holds, over a fixed set of
// buckets.
//
// A deterministic stand-in for a model, as every client's runner uses: the
// contract under test is the recall ordering, not an embedding's quality,
// and the SDK ships no model.
type tokenEmbedder struct{}

func (tokenEmbedder) Embed(_ context.Context, text string) ([]float32, error) {
	const dimensions = 64
	vector := make([]float32, dimensions)
	for _, token := range strings.Fields(strings.ToLower(text)) {
		bucket := 0
		for _, letter := range []byte(token) {
			bucket = (bucket*31 + int(letter)) % dimensions
		}
		vector[bucket] += 1
	}
	return vector, nil
}
