package bdd_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	bdd "github.com/laserdata/laser-sdk/bdd/go"
)

// TestGivenTheSharedGraphScenariosWhenRunThenShouldHold runs the knowledge
// graph feature against this module's reference engine: convergence on one
// node per entity, typed and directed traversal, valid-time windows, and
// the provenance an element records.
func TestGivenTheSharedGraphScenariosWhenRunThenShouldHold(t *testing.T) {
	runLocalFeature(t, "graph.feature", time.Minute, graphStep)
}

func graphEngineOf(state *world) *bdd.GraphEngine {
	if existing, found := state.get("graph"); found {
		return existing.(*bdd.GraphEngine)
	}
	engine := bdd.NewGraphEngine()
	state.put("graph", engine)
	return engine
}

func graphStep(_ context.Context, state *world, step bdd.Step) error {
	engine := graphEngineOf(state)
	text := step.Text
	arguments := argumentsOf(step)

	switch {
	case text == "an empty graph":
		state.put("graph", bdd.NewGraphEngine())
		return nil

	case strings.HasPrefix(text, "I observe "):
		return observeEdge(state, text, arguments)

	case strings.HasPrefix(text, "the graph holds"):
		wanted, named := after(text, "the graph holds ")
		if !named {
			return fmt.Errorf("want a node count in %q", text)
		}
		if got := engine.NodeCount(); got != int(wanted) {
			return fmt.Errorf("want %d nodes, got %d", wanted, got)
		}
		return nil

	case strings.HasPrefix(text, "traversing from "):
		return assertTraversal(engine, text, arguments)

	case strings.HasPrefix(text, "the source of node "):
		if len(arguments) != 2 {
			return fmt.Errorf("want a node and its source, got %v", arguments)
		}
		if got := engine.NodeSource(arguments[0]); got != arguments[1] {
			return fmt.Errorf("want node %q from %q, got %q",
				arguments[0], arguments[1], got)
		}
		return nil

	case strings.HasPrefix(text, "the source of edge "):
		// The edge type is unquoted here too, so the quoted values are the
		// two endpoints and the expected source.
		if len(arguments) != 3 {
			return fmt.Errorf("want two endpoints and a source, got %v", arguments)
		}
		edgeType := between(text, 2)
		got := engine.EdgeSource(arguments[0], edgeType, arguments[1])
		if got != arguments[2] {
			return fmt.Errorf("want edge %q-%s->%q from %q, got %q",
				arguments[0], edgeType, arguments[1], arguments[2], got)
		}
		return nil

	default:
		return fmt.Errorf("this runner does not implement the step")
	}
}

// observeEdge records one observation: both endpoints as nodes, and the
// typed edge between them.
//
// The edge type rides unquoted in the step text, so it is read from the
// words between the two quoted endpoints rather than from the arguments.
func observeEdge(state *world, text string, arguments []string) error {
	engine := graphEngineOf(state)
	if len(arguments) < 2 {
		return fmt.Errorf("want a subject and an object, got %v", arguments)
	}
	subject, object := arguments[0], arguments[1]
	// The edge type is unquoted in the step, so it is the words between the
	// two quoted endpoints rather than one of the arguments.
	edgeType := between(text, 2)
	if edgeType == "" {
		return fmt.Errorf("want an edge type between the endpoints in %q", text)
	}
	source := ""
	if len(arguments) > 2 {
		source = arguments[2]
	}
	window := bdd.GraphEdgeWindow{}
	if from, named := after(text, "valid from "); named {
		window.From, window.HasFrom = from, true
	}
	if to, named := after(text, "valid to "); named {
		window.To, window.HasTo = to, true
	}
	from := engine.UpsertNode(subject, source)
	to := engine.UpsertNode(object, source)
	engine.AddEdge(from, edgeType, to, window, source)
	return nil
}

// between is the unquoted text at one position of a step, counting the
// segments a quote splits it into.
//
// Steps name some values in quotes and some in prose; this reads the prose
// ones without each caller re-deriving where they sit.
func between(text string, index int) string {
	parts := strings.Split(text, `"`)
	if index >= len(parts) {
		return ""
	}
	return strings.TrimSpace(parts[index])
}

// assertTraversal runs the traversal a step describes and checks whether it
// reached what the step said.
func assertTraversal(engine *bdd.GraphEngine, text string, arguments []string) error {
	if len(arguments) < 2 {
		return fmt.Errorf("want a start, hops and a target, got %v", arguments)
	}
	start := arguments[0]
	target := arguments[len(arguments)-1]
	direction := bdd.GraphOut
	if strings.Contains(text, " incoming ") {
		direction = bdd.GraphIn
	}
	hops := make([]bdd.GraphHop, 0, len(arguments)-2)
	for _, edgeType := range arguments[1 : len(arguments)-1] {
		hops = append(hops, bdd.GraphHop{Type: edgeType, Direction: direction})
	}
	at, asOf := after(text, " as of ")
	reached := engine.Traverse(start, hops, at, asOf)

	found := false
	for _, value := range reached {
		if value == target {
			found = true
		}
	}
	if strings.Contains(text, "does not reach") {
		if found {
			return fmt.Errorf("want %q not reached, got %v", target, reached)
		}
		return nil
	}
	if !found {
		return fmt.Errorf("want %q reached, got %v", target, reached)
	}
	return nil
}
