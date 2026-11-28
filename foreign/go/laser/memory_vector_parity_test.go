package laser_test

import (
	"context"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
)

func TestGivenAKeywordStrategyWhenRecalledThenShouldScoreByWordsWithoutEmbedding(t *testing.T) {
	memory, embedder := vectorMemoryWith(t, "alpha beta", "gamma delta")
	embedded := embedder.calls

	items, err := memory.Recall(context.Background(), laser.MemoryScope{}, laser.MemoryQuery{
		Query: "gamma", Strategy: laser.RecallKeyword,
	})

	if err != nil {
		t.Fatalf("recall: %v", err)
	}
	if got := itemTexts(items); len(got) != 2 || got[0] != "gamma delta" || got[1] != "alpha beta" {
		t.Fatalf("want the matching item first and the other kept behind it, got %v", got)
	}
	if items[0].Score != 1 || items[1].Score != 0 {
		t.Errorf("want the overlap as the score, got %v and %v", items[0].Score, items[1].Score)
	}
	if len(items[0].Signals) != 1 || items[0].Signals[0].Strategy != laser.RecallKeyword {
		t.Errorf("want only the keyword signal, got %+v", items[0].Signals)
	}
	if embedder.calls != embedded {
		t.Errorf("want no embedding for a keyword recall, got %d calls", embedder.calls-embedded)
	}
}

func TestGivenPositiveFeedbackWhenRecalledWithoutAQueryThenShouldRankTheTargetFirst(t *testing.T) {
	memory, _ := vectorMemoryWith(t, "the cat sat", "the dog ran")
	before, err := memory.Recall(context.Background(), laser.MemoryScope{}, laser.MemoryQuery{})
	if err != nil {
		t.Fatalf("recall: %v", err)
	}
	if before[0].Text() != "the dog ran" || len(before[0].Signals) != 0 {
		t.Fatalf("want the newest item first and unranked, got %q with %+v", before[0].Text(), before[0].Signals)
	}
	older := before[1].ID

	if err := memory.Improve(context.Background(), laser.Feedback{Target: older, Weight: 5}); err != nil {
		t.Fatalf("improve: %v", err)
	}
	after, err := memory.Recall(context.Background(), laser.MemoryScope{}, laser.MemoryQuery{})

	if err != nil {
		t.Fatalf("recall: %v", err)
	}
	if after[0].ID != older || after[0].Score != 5 {
		t.Fatalf("want the promoted item first with its weight as the score, got %q at %v", after[0].Text(), after[0].Score)
	}
	want := []laser.RecallSignal{{Strategy: laser.RecallAuto, Rank: 0, Score: 5}}
	if len(after[0].Signals) != 1 || after[0].Signals[0] != want[0] {
		t.Errorf("want the feedback signal labelled auto, got %+v", after[0].Signals)
	}
	if after[1].Score != 0 || len(after[1].Signals) != 0 {
		t.Errorf("want the item without feedback unscored, got %v with %+v", after[1].Score, after[1].Signals)
	}
}

func TestGivenFeedbackWhenVectorRecallIsRecentThenShouldKeepRecencyOrder(t *testing.T) {
	memory, _ := vectorMemoryWith(t, "cat", "dog")
	all, err := memory.Recall(context.Background(), laser.MemoryScope{}, laser.MemoryQuery{})
	if err != nil {
		t.Fatalf("recall: %v", err)
	}
	if err := memory.Improve(context.Background(), laser.Feedback{Target: all[1].ID, Weight: 5}); err != nil {
		t.Fatalf("improve: %v", err)
	}

	items, err := memory.Recall(context.Background(), laser.MemoryScope{}, laser.MemoryQuery{
		Strategy: laser.RecallRecent,
	})

	if err != nil {
		t.Fatalf("recall: %v", err)
	}
	if got := itemTexts(items); len(got) != 2 || got[0] != "dog" || got[1] != "cat" {
		t.Fatalf("want write recency, newest first, got %v", got)
	}
	for _, item := range items {
		if len(item.Signals) != 0 || item.Score != 0 {
			t.Errorf("want a recent recall unranked, got %v with %+v", item.Score, item.Signals)
		}
	}
}

func TestGivenARecentStrategyWithQueryTextWhenRecalledThenShouldReturnWriteRecencyWithoutEmbedding(t *testing.T) {
	embedder := &wordEmbedder{vocabulary: []string{"old", "new"}}
	memory := laser.NewVectorMemory(embedder)
	one, two := laser.MemoryScope{Stream: "one"}, laser.MemoryScope{Stream: "two"}
	for _, kept := range []struct {
		scope laser.MemoryScope
		body  string
	}{{one, "old"}, {one, "new"}, {two, "another stream"}} {
		if _, err := memory.Remember(context.Background(), kept.scope, laser.MemoryFact, []byte(kept.body)); err != nil {
			t.Fatalf("remember %q: %v", kept.body, err)
		}
	}

	items, err := memory.Recall(context.Background(), one, laser.MemoryQuery{
		Limit: 1, Query: "ignored", Strategy: laser.RecallRecent,
	})

	if err != nil {
		t.Fatalf("recall: %v", err)
	}
	if got := itemTexts(items); len(got) != 1 || got[0] != "new" {
		t.Fatalf("want the newest item of the stream, got %v", got)
	}
	if embedder.calls != 3 {
		t.Errorf("want only the three remembers embedded, got %d calls", embedder.calls)
	}
}

func TestGivenHybridSignalsWhenRankedThenShouldReportAdditiveScoresAndEachSignalRank(t *testing.T) {
	memory := laser.NewVectorMemory(&wordEmbedder{vocabulary: []string{"cat", "dog", "fish"}})
	ids := make([]laser.MemoryID, 0, 2)
	for _, body := range []string{"auth", "auth latency"} {
		id, err := memory.Remember(context.Background(), laser.MemoryScope{}, laser.MemoryFact, []byte(body))
		if err != nil {
			t.Fatalf("remember %q: %v", body, err)
		}
		ids = append(ids, id)
	}
	if err := memory.Improve(context.Background(), laser.Feedback{Target: ids[1], Weight: 1.5}); err != nil {
		t.Fatalf("improve: %v", err)
	}

	items, err := memory.Recall(context.Background(), laser.MemoryScope{}, laser.MemoryQuery{
		Query: "auth", Strategy: laser.RecallHybrid,
	})

	if err != nil {
		t.Fatalf("recall: %v", err)
	}
	if len(items) != 2 || items[0].ID != ids[1] || items[1].ID != ids[0] {
		t.Fatalf("want the item with feedback first, got %v", itemTexts(items))
	}
	for _, item := range items {
		sum := 0.0
		held := map[laser.RecallStrategy]laser.RecallSignal{}
		for _, signal := range item.Signals {
			sum += signal.Score
			held[signal.Strategy] = signal
		}
		if item.Score != sum {
			t.Errorf("want the score to be the sum of its signals, got %v for %v", item.Score, sum)
		}
		if _, semantic := held[laser.RecallSemantic]; !semantic {
			t.Errorf("want a semantic signal on %q, got %+v", item.Text(), item.Signals)
		}
		if _, keyword := held[laser.RecallKeyword]; !keyword {
			t.Errorf("want a keyword signal on %q, got %+v", item.Text(), item.Signals)
		}
	}
	if rank := signalOf(t, items[1], laser.RecallSemantic).Rank; rank != 0 {
		t.Errorf("want the first written item ranked 0 among equal similarities, got %d", rank)
	}
	if feedback := signalOf(t, items[0], laser.RecallAuto); feedback.Rank != 0 || feedback.Score != 1.5 {
		t.Errorf("want the feedback signal ranked 0 with its weight, got %+v", feedback)
	}
}

func TestGivenAGraphOrTemporalStrategyWhenRecalledFromTheVectorIndexThenShouldRankByTheEmbeddingOnly(t *testing.T) {
	for _, strategy := range []laser.RecallStrategy{laser.RecallGraph, laser.RecallTemporal, laser.RecallAuto, laser.RecallSemantic} {
		memory, _ := vectorMemoryWith(t, "invoice overdue", "cats sleep")

		items, err := memory.Recall(context.Background(), laser.MemoryScope{}, laser.MemoryQuery{
			Query: "cats", Strategy: strategy,
		})

		if err != nil {
			t.Fatalf("%s: recall: %v", strategy, err)
		}
		if got := itemTexts(items); len(got) != 2 || got[0] != "cats sleep" {
			t.Fatalf("%s: want the nearest item first and the other kept, got %v", strategy, got)
		}
		for _, item := range items {
			if len(item.Signals) != 1 || item.Signals[0].Strategy != laser.RecallSemantic {
				t.Errorf("%s: want only the semantic signal on %q, got %+v", strategy, item.Text(), item.Signals)
			}
		}
	}
}

func TestGivenAUserAppOrStreamScopeWhenRecalledThenShouldNarrowToIt(t *testing.T) {
	memory := laser.NewVectorMemory(&wordEmbedder{vocabulary: []string{"note"}})
	for _, scope := range []laser.MemoryScope{
		{User: "ada", App: "desk", Stream: "one"},
		{User: "bob", App: "desk", Stream: "one"},
		{User: "ada", App: "ops", Stream: "two"},
	} {
		body := "note " + scope.User + " " + scope.App + " " + scope.Stream
		if _, err := memory.Remember(context.Background(), scope, laser.MemoryFact, []byte(body)); err != nil {
			t.Fatalf("remember %q: %v", body, err)
		}
	}
	for name, narrowed := range map[string]struct {
		scope laser.MemoryScope
		want  int
	}{
		"user":   {laser.MemoryScope{User: "ada"}, 2},
		"app":    {laser.MemoryScope{App: "desk"}, 2},
		"stream": {laser.MemoryScope{Stream: "two"}, 1},
		"all":    {laser.MemoryScope{User: "ada", App: "desk", Stream: "one"}, 1},
		"none":   {laser.MemoryScope{}, 3},
	} {
		items, err := memory.Recall(context.Background(), narrowed.scope, laser.MemoryQuery{})

		if err != nil {
			t.Fatalf("%s: recall: %v", name, err)
		}
		if len(items) != narrowed.want {
			t.Errorf("%s: want %d items, got %v", name, narrowed.want, itemTexts(items))
		}
	}
}

func signalOf(t *testing.T, item laser.MemoryItem, strategy laser.RecallStrategy) laser.RecallSignal {
	t.Helper()
	for _, signal := range item.Signals {
		if signal.Strategy == strategy {
			return signal
		}
	}
	t.Fatalf("want a %s signal on %q, got %+v", strategy, item.Text(), item.Signals)
	return laser.RecallSignal{}
}
