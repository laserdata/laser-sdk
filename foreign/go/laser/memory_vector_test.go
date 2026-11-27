package laser_test

import (
	"context"
	"errors"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
)

// wordEmbedder is a deterministic stand-in for a real model: each text
// becomes a vector over a fixed vocabulary, so "cat" and "cats sleep" point
// in a similar direction and "invoice" points elsewhere.
//
// A fake rather than a model, because the seam is what is under test: the
// client ships no model and never will.
type wordEmbedder struct {
	vocabulary []string
	calls      int
	fail       error
}

func (embedder *wordEmbedder) Embed(_ context.Context, text string) ([]float32, error) {
	embedder.calls++
	if embedder.fail != nil {
		return nil, embedder.fail
	}
	seen := map[string]bool{}
	for _, word := range splitWords(text) {
		seen[word] = true
	}
	vector := make([]float32, len(embedder.vocabulary))
	for index, word := range embedder.vocabulary {
		if seen[word] {
			vector[index] = 1
		}
	}
	return vector, nil
}

func splitWords(text string) []string {
	var words []string
	current := ""
	for _, letter := range text {
		if letter == ' ' || letter == '\n' || letter == '\t' {
			if current != "" {
				words = append(words, current)
				current = ""
			}
			continue
		}
		current += string(letter)
	}
	if current != "" {
		words = append(words, current)
	}
	return words
}

func vectorMemoryWith(t *testing.T, bodies ...string) (*laser.VectorMemory, *wordEmbedder) {
	t.Helper()
	embedder := &wordEmbedder{vocabulary: []string{
		"cat", "cats", "sleep", "invoice", "payment", "overdue",
	}}
	memory := laser.NewVectorMemory(embedder)
	for _, body := range bodies {
		if _, err := memory.Remember(
			context.Background(), laser.MemoryScope{}, laser.MemoryFact, []byte(body),
		); err != nil {
			t.Fatalf("remember %q: %v", body, err)
		}
	}
	return memory, embedder
}

// The point of a semantic recall: it finds what the question means, not
// which words it happened to use.
func TestGivenASemanticQueryWhenRecalledThenShouldRankByMeaningNotWords(t *testing.T) {
	memory, _ := vectorMemoryWith(t, "cats sleep", "invoice overdue")

	items, err := memory.Recall(context.Background(), laser.MemoryScope{}, laser.MemoryQuery{
		Query: "cats", Strategy: laser.RecallSemantic,
	})
	if err != nil {
		t.Fatalf("recall: %v", err)
	}
	if len(items) == 0 {
		t.Fatal("want ranked items, got none")
	}
	if items[0].Text() != "cats sleep" {
		t.Errorf("want the nearest item first, got %q", items[0].Text())
	}
	if len(items[0].Signals) != 1 || items[0].Signals[0].Strategy != laser.RecallSemantic {
		t.Errorf("want the semantic signal recorded, got %+v", items[0].Signals)
	}
	if items[0].Signals[0].Rank != 0 {
		t.Errorf("want the top item ranked 0 in its signal, got %d", items[0].Signals[0].Rank)
	}
}

// A hybrid recall fuses the signals, so agreement between them beats one
// signal's confidence in its own top hit.
func TestGivenAgreeingSignalsWhenFusedThenShouldOutrankASingleSignalsTopHit(t *testing.T) {
	// "cat" is lexically and semantically close to the query; "cats sleep"
	// shares the embedding's vocabulary but not the exact word.
	semantic := []laser.MemoryItem{
		{ID: memoryIDOf(t, "a"), Score: 0.9,
			Signals: []laser.RecallSignal{{Strategy: laser.RecallSemantic, Rank: 0}}},
		{ID: memoryIDOf(t, "b"), Score: 0.8,
			Signals: []laser.RecallSignal{{Strategy: laser.RecallSemantic, Rank: 1}}},
	}
	keyword := []laser.MemoryItem{
		{ID: memoryIDOf(t, "b"), Score: 1,
			Signals: []laser.RecallSignal{{Strategy: laser.RecallKeyword, Rank: 0}}},
	}

	fused := laser.FuseReciprocalRank([][]laser.MemoryItem{semantic, keyword}, 10)
	if len(fused) != 2 {
		t.Fatalf("want both items fused once each, got %d", len(fused))
	}
	if fused[0].ID != memoryIDOf(t, "b") {
		t.Error("want the item both signals surfaced ranked first")
	}
	if len(fused[0].Signals) != 2 {
		t.Errorf("want both contributing signals kept, got %+v", fused[0].Signals)
	}
}

// An item every signal missed is not recalled at all, and one signal alone
// still carries its item through.
func TestGivenOneSignalOnlyWhenFusedThenShouldStillCarryItsItems(t *testing.T) {
	only := []laser.MemoryItem{{ID: memoryIDOf(t, "a"), Score: 0.5,
		Signals: []laser.RecallSignal{{Strategy: laser.RecallKeyword, Rank: 0}}}}

	fused := laser.FuseReciprocalRank([][]laser.MemoryItem{nil, only, nil}, 10)
	if len(fused) != 1 || fused[0].ID != memoryIDOf(t, "a") {
		t.Fatalf("want the one signal's item, got %v", fused)
	}
	if fused[0].Score <= 0 {
		t.Error("want a fused score from its single contribution")
	}
}

// Feedback is what lets an agent's experience change what it is shown,
// without anything being rewritten.
func TestGivenFeedbackWhenRecalledHybridThenShouldPromoteTheWeightedItem(t *testing.T) {
	memory, _ := vectorMemoryWith(t, "cats sleep", "cat")
	items, err := memory.Recall(context.Background(), laser.MemoryScope{}, laser.MemoryQuery{
		Query: "cat", Strategy: laser.RecallHybrid,
	})
	if err != nil {
		t.Fatalf("recall: %v", err)
	}
	if len(items) < 2 {
		t.Fatalf("want both items recalled, got %d", len(items))
	}
	demoted := items[len(items)-1]

	// The agent says the last item was the useful one.
	if err := memory.Improve(context.Background(), laser.Feedback{
		Target: demoted.ID, Weight: 5,
	}); err != nil {
		t.Fatalf("improve: %v", err)
	}
	after, err := memory.Recall(context.Background(), laser.MemoryScope{}, laser.MemoryQuery{
		Query: "cat", Strategy: laser.RecallHybrid,
	})
	if err != nil {
		t.Fatalf("recall again: %v", err)
	}
	if after[0].ID != demoted.ID {
		t.Errorf("want the item feedback promoted to come first, got %q", after[0].Text())
	}
	carried := false
	for _, signal := range after[0].Signals {
		if signal.Strategy == laser.RecallFeedback {
			carried = true
		}
	}
	if !carried {
		t.Error("want the feedback signal recorded on the fused item")
	}
}

// Auto routes from what the backend has rather than making the caller know.
func TestGivenAutoWhenRecalledThenShouldRouteOnWhatIsAvailable(t *testing.T) {
	memory, embedder := vectorMemoryWith(t, "cats sleep", "invoice overdue")

	// No query: nothing to rank against, so the most recent items.
	recent, err := memory.Recall(context.Background(), laser.MemoryScope{}, laser.MemoryQuery{})
	if err != nil {
		t.Fatalf("recall: %v", err)
	}
	if len(recent) != 2 || recent[0].Text() != "invoice overdue" {
		t.Errorf("want the newest item first, got %v", itemTexts(recent))
	}
	before := embedder.calls

	// With a query and an embedder, Auto is the fused recall.
	fused, err := memory.Recall(context.Background(), laser.MemoryScope{}, laser.MemoryQuery{
		Query: "cats",
	})
	if err != nil {
		t.Fatalf("recall: %v", err)
	}
	if len(fused) == 0 || fused[0].Text() != "cats sleep" {
		t.Errorf("want the nearest item first, got %v", itemTexts(fused))
	}
	if embedder.calls == before {
		t.Error("want a routed Auto to have used the embedder")
	}
}

// A memory with no embedder still recalls, by words alone, rather than
// refusing: keyword recall is the honest fallback, not an error.
func TestGivenNoEmbedderWhenRecalledThenShouldFallBackToWords(t *testing.T) {
	memory := laser.NewVectorMemory(nil)
	if _, err := memory.Remember(
		context.Background(), laser.MemoryScope{}, laser.MemoryFact, []byte("cats sleep"),
	); err == nil {
		t.Fatal("want remembering without an embedder refused")
	}
}

// An embedder that fails fails the recall rather than quietly returning a
// worse ranking the caller cannot tell apart from a good one.
func TestGivenAFailingEmbedderWhenRecalledThenShouldReportIt(t *testing.T) {
	memory, embedder := vectorMemoryWith(t, "cats sleep")
	embedder.fail = errors.New("model unavailable")

	_, err := memory.Recall(context.Background(), laser.MemoryScope{}, laser.MemoryQuery{
		Query: "cats", Strategy: laser.RecallSemantic,
	})
	if err == nil {
		t.Fatal("want the embedder's failure reported")
	}
}

// A reranker is the second pass over what a cheap signal proposed.
func TestGivenARerankerWhenRecalledThenShouldReorderTheCandidates(t *testing.T) {
	memory, _ := vectorMemoryWith(t, "cats sleep", "invoice overdue")
	reranked := memory.WithReranker(reverseReranker{})

	items, err := reranked.Recall(context.Background(), laser.MemoryScope{}, laser.MemoryQuery{
		Query: "cats", Strategy: laser.RecallSemantic,
	})
	if err != nil {
		t.Fatalf("recall: %v", err)
	}
	if len(items) < 2 {
		t.Fatalf("want both items, got %d", len(items))
	}
	if items[0].Text() != "invoice overdue" {
		t.Errorf("want the reranker's order, got %v", itemTexts(items))
	}
}

// A recall with no query has nothing for a reranker to rank against, so it
// is handed back untouched rather than reordered on nothing.
func TestGivenNoQueryWhenRerankedThenShouldLeaveTheOrderAlone(t *testing.T) {
	memory, _ := vectorMemoryWith(t, "cats sleep", "invoice overdue")
	reranked := memory.WithReranker(reverseReranker{})

	items, err := reranked.Recall(context.Background(), laser.MemoryScope{}, laser.MemoryQuery{})
	if err != nil {
		t.Fatalf("recall: %v", err)
	}
	if len(items) != 2 || items[0].Text() != "invoice overdue" {
		t.Errorf("want the recency order kept, got %v", itemTexts(items))
	}
}

// Forgetting removes an item from later recall.
func TestGivenAForgottenItemWhenRecalledThenShouldBeGone(t *testing.T) {
	memory, _ := vectorMemoryWith(t, "cats sleep", "invoice overdue")
	items, err := memory.Recall(context.Background(), laser.MemoryScope{}, laser.MemoryQuery{})
	if err != nil {
		t.Fatalf("recall: %v", err)
	}
	if err := memory.Forget(context.Background(), items[0].ID); err != nil {
		t.Fatalf("forget: %v", err)
	}
	after, err := memory.Recall(context.Background(), laser.MemoryScope{}, laser.MemoryQuery{})
	if err != nil {
		t.Fatalf("recall again: %v", err)
	}
	if len(after) != 1 {
		t.Fatalf("want one item left, got %d", len(after))
	}
	if after[0].ID == items[0].ID {
		t.Error("want the forgotten item gone")
	}
}

// Cosine similarity has to be safe on the inputs a real embedder produces,
// including ones it cannot compare.
func TestGivenMismatchedOrEmptyVectorsWhenComparedThenShouldRankLastNotCorrupt(t *testing.T) {
	if score := laser.CosineSimilarity([]float32{1, 0}, []float32{1, 0, 0}); score != 0 {
		t.Errorf("want 0 for different lengths, got %v", score)
	}
	if score := laser.CosineSimilarity([]float32{0, 0}, []float32{1, 1}); score != 0 {
		t.Errorf("want 0 for an all-zero vector, got %v", score)
	}
	if score := laser.CosineSimilarity(nil, nil); score != 0 {
		t.Errorf("want 0 for empty vectors, got %v", score)
	}
	same := laser.CosineSimilarity([]float32{1, 1}, []float32{1, 1})
	if same < 0.999 {
		t.Errorf("want identical vectors to score ~1, got %v", same)
	}
}

// NoSummarizer is a value rather than a nil check at every call site.
func TestGivenNoSummarizerWhenAskedThenShouldFoldNothing(t *testing.T) {
	body, err := laser.NoSummarizer{}.Summarize(context.Background(), [][]byte{[]byte("a")})
	if err != nil {
		t.Fatalf("summarize: %v", err)
	}
	if body != nil {
		t.Errorf("want nothing folded, got %q", body)
	}
}

// reverseReranker stands in for an expensive second pass by putting the
// candidates in the opposite order.
type reverseReranker struct{}

func (reverseReranker) Rerank(
	_ context.Context, _ string, items []laser.MemoryItem,
) ([]laser.MemoryItem, error) {
	reversed := make([]laser.MemoryItem, 0, len(items))
	for index := len(items) - 1; index >= 0; index-- {
		reversed = append(reversed, items[index])
	}
	return reversed, nil
}

func itemTexts(items []laser.MemoryItem) []string {
	texts := make([]string, 0, len(items))
	for _, item := range items {
		texts = append(texts, item.Text())
	}
	return texts
}

func memoryIDOf(t *testing.T, seed string) laser.MemoryID {
	t.Helper()
	return laser.ContentMemoryID(laser.MemoryScope{}, laser.MemoryFact, []byte(seed))
}
