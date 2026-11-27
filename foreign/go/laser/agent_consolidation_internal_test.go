package laser

import (
	"context"
	"testing"
	"time"
)

// recordingConsolidator records every scope it is asked to consolidate, the
// Go form of Rust's own RecordingConsolidator test fake (sdk/src/agent/builder.rs).
type recordingConsolidator struct {
	passes chan MemoryScope
}

func (consolidator recordingConsolidator) Consolidate(
	_ context.Context, scope *MemoryScope,
) (ConsolidationReport, error) {
	consolidator.passes <- *scope
	return ConsolidationReport{}, nil
}

func TestGivenAConsolidationTickWhenItFiresThenShouldScopeThePassToTheAgent(t *testing.T) {
	agent := presenceAgentID(t, "planner")
	every := 5 * time.Millisecond
	consolidator := recordingConsolidator{passes: make(chan MemoryScope, 4)}
	consolidation := startConsolidation(agent, &every, consolidator)
	defer consolidation.abort()

	select {
	case scope := <-consolidator.passes:
		if scope.Agent == nil || *scope.Agent != agent {
			t.Fatalf("want the pass scoped to %q, got %+v", agent, scope.Agent)
		}
		if scope.Conversation != nil || scope.User != "" || scope.App != "" || scope.Stream != "" {
			t.Fatalf("want a pass scoped to the agent alone, got %+v", scope)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a pass never ran")
	}
}

func TestGivenNoConsolidatorWhenStartingConsolidationThenShouldConsolidateNothing(t *testing.T) {
	every := time.Second
	if consolidation := startConsolidation(presenceAgentID(t, "planner"), &every, nil); consolidation != nil {
		t.Fatalf("expected no consolidation with no consolidator, got %+v", consolidation)
	}
}

func TestGivenNoIntervalWhenStartingConsolidationThenShouldConsolidateNothing(t *testing.T) {
	consolidator := recordingConsolidator{passes: make(chan MemoryScope, 1)}
	if consolidation := startConsolidation(presenceAgentID(t, "planner"), nil, consolidator); consolidation != nil {
		t.Fatalf("expected no consolidation with no interval, got %+v", consolidation)
	}
}

func TestGivenAnAbortedConsolidationWhenCalledThenShouldStopFurtherPasses(t *testing.T) {
	agent := presenceAgentID(t, "planner")
	every := 5 * time.Millisecond
	consolidator := recordingConsolidator{passes: make(chan MemoryScope, 16)}
	consolidation := startConsolidation(agent, &every, consolidator)

	select {
	case <-consolidator.passes:
	case <-time.After(2 * time.Second):
		t.Fatal("the first pass never ran")
	}
	consolidation.abort()
	// Drain whatever was already in flight the instant abort fired.
drain:
	for {
		select {
		case <-consolidator.passes:
		default:
			break drain
		}
	}
	select {
	case <-consolidator.passes:
		t.Fatal("a pass ran after abort")
	case <-time.After(50 * time.Millisecond):
	}
}
