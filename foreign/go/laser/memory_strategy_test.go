package laser_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
)

func TestGivenAnUnknownStrategyWordWhenRecalledThenShouldRefuseItNamingTheStrategies(t *testing.T) {
	memory := recallMemory(&memoryServer{}, laser.MemoryScope{})
	want := "unknown recall strategy 'nearest': use one of auto, recent, semantic, keyword, graph, temporal, hybrid"
	for name, recall := range map[string]func(context.Context, laser.MemoryQuery) ([]laser.MemoryItem, error){
		"view": memory.Recall, "fold": memory.RecallFolded,
	} {
		_, err := recall(context.Background(), laser.MemoryQuery{Query: "disk", Strategy: "nearest"})

		var invalid *laser.InvalidError
		if !errors.As(err, &invalid) || invalid.Reason != want {
			t.Errorf("%s: want %q, got %v", name, want, err)
		}
	}
}

func TestGivenEveryStrategyWordWhenRecalledThenShouldAcceptIt(t *testing.T) {
	memory := recallMemory(&memoryServer{}, laser.MemoryScope{})
	for _, strategy := range []laser.RecallStrategy{
		"", laser.RecallAuto, laser.RecallRecent, laser.RecallSemantic, laser.RecallKeyword,
		laser.RecallGraph, laser.RecallTemporal, laser.RecallHybrid,
	} {
		if _, err := memory.Recall(context.Background(), laser.MemoryQuery{Query: "disk", Strategy: strategy}); err != nil {
			t.Errorf("strategy %q: %v", strategy, err)
		}
	}
}

func TestGivenAGraphOrTemporalStrategyWhenRecalledFromTheLogThenShouldKeepOnlyItemsSharingAQueryWord(t *testing.T) {
	memory := recallMemory(&memoryServer{}, laser.MemoryScope{})
	mustRemember(t, memory, "the disk on node-7 is full")
	mustRemember(t, memory, "rotate keys monthly")
	for _, strategy := range []laser.RecallStrategy{laser.RecallKeyword, laser.RecallGraph, laser.RecallTemporal} {
		items := mustRecall(t, memory, laser.MemoryQuery{Query: "disk", Strategy: strategy})

		var bodies []string
		for _, item := range items {
			bodies = append(bodies, item.Text())
		}
		if want := []string{"the disk on node-7 is full"}; !reflect.DeepEqual(bodies, want) {
			t.Errorf("strategy %q: want %v, got %v", strategy, want, bodies)
		}
	}
}
