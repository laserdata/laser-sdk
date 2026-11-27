package laser

import (
	"context"
	"math"
	"sort"
	"sync"
	"time"
)

// Embedder turns text into a vector, the seam a semantic memory ranks on.
//
// The client ships no model, ever: an embedding API, a local model, or a
// hosted service lives in application code. This is the boundary, and it is
// the same one Reranker and Summarizer sit on.
type Embedder interface {
	Embed(ctx context.Context, text string) ([]float32, error)
}

// Reranker reorders recalled items against the query that found them.
//
// The second pass a quality recall wants: a cheap signal proposes
// candidates, and a more expensive model puts them in order. Returning the
// items unchanged is a valid implementation.
type Reranker interface {
	Rerank(ctx context.Context, query string, items []MemoryItem) ([]MemoryItem, error)
}

// Summarizer folds a batch of bodies into one, the seam a consolidation
// summarize pass calls.
type Summarizer interface {
	Summarize(ctx context.Context, bodies [][]byte) ([]byte, error)
}

// NoSummarizer summarizes nothing, for a consolidator that prunes and
// reweights but does not fold.
//
// It exists so "no summarizer" is a value rather than a nil check at every
// call site.
type NoSummarizer struct{}

// Summarize reports that nothing was folded.
func (NoSummarizer) Summarize(context.Context, [][]byte) ([]byte, error) {
	return nil, nil
}

// vectorEntry is one remembered item with what ranking needs.
type vectorEntry struct {
	item      MemoryItem
	embedding []float32
	// feedback accumulates Improve's weights, read by recall. It rides with
	// its entry under the one lock rather than in a map of its own, since
	// recall already walks every entry.
	feedback float32
}

// VectorMemory keeps facts in this process and ranks recall by how close
// their embeddings are to the query's.
//
// It loses everything when the program exits. The durable memory is
// Laser.Memory(namespace), which writes to the log; this is the semantic
// backend for a process that wants similarity ranking without one.
type VectorMemory struct {
	embedder Embedder
	mutex    sync.Mutex
	entries  []*vectorEntry
}

// NewVectorMemory opens an in-process semantic memory over embedder.
func NewVectorMemory(embedder Embedder) *VectorMemory {
	return &VectorMemory{embedder: embedder}
}

// Remember embeds body and keeps it under scope.
func (memory *VectorMemory) Remember(
	ctx context.Context, scope MemoryScope, kind MemoryKind, body []byte,
) (MemoryID, error) {
	if memory.embedder == nil {
		return MemoryID{}, &InvalidError{Reason: "a vector memory needs an embedder"}
	}
	embedding, err := memory.embedder.Embed(ctx, string(body))
	if err != nil {
		return MemoryID{}, err
	}
	id := ContentMemoryID(scope, kind, body)
	item := MemoryItem{
		ID: id, Body: body, Kind: kind, Scope: scope, At: time.Now(),
	}
	memory.mutex.Lock()
	defer memory.mutex.Unlock()
	memory.entries = append(memory.entries,
		&vectorEntry{item: item, embedding: embedding})
	return id, nil
}

// Forget drops an item.
func (memory *VectorMemory) Forget(_ context.Context, target MemoryID) error {
	memory.mutex.Lock()
	defer memory.mutex.Unlock()
	kept := memory.entries[:0]
	for _, entry := range memory.entries {
		if entry.item.ID != target {
			kept = append(kept, entry)
		}
	}
	memory.entries = kept
	return nil
}

// Improve reweights an item, so what an agent found useful is ranked higher
// next time without anything being rewritten.
func (memory *VectorMemory) Improve(_ context.Context, feedback Feedback) error {
	memory.mutex.Lock()
	defer memory.mutex.Unlock()
	for _, entry := range memory.entries {
		if entry.item.ID == feedback.Target {
			entry.feedback += feedback.Weight
			return nil
		}
	}
	return nil
}

// Recall ranks what it holds against the query.
//
// RecallSemantic ranks by cosine similarity alone, RecallKeyword by lexical
// overlap alone, and RecallHybrid fuses both with the feedback weight.
// RecallAuto routes: a query with an embedder behind it is hybrid, a query
// without one is keyword, and no query at all is the most recent items.
func (memory *VectorMemory) Recall(
	ctx context.Context, scope MemoryScope, query MemoryQuery,
) ([]MemoryItem, error) {
	limit := query.Limit
	if limit <= 0 {
		limit = DefaultRecallLimit
	}
	memory.mutex.Lock()
	candidates := make([]*vectorEntry, 0, len(memory.entries))
	for _, entry := range memory.entries {
		if scopeHolds(scope, entry.item.Scope) && matchesKind(entry.item, query.Kinds) {
			candidates = append(candidates, entry)
		}
	}
	memory.mutex.Unlock()

	strategy := memory.route(query)
	if strategy == RecallRecent {
		return mostRecent(candidates, limit), nil
	}

	var semantic []MemoryItem
	if strategy == RecallSemantic || strategy == RecallHybrid {
		ranked, err := memory.semanticSignal(ctx, query.Query, candidates)
		if err != nil {
			return nil, err
		}
		semantic = ranked
	}
	if strategy == RecallSemantic {
		return truncateItems(semantic, limit), nil
	}

	keyword := keywordSignal(query.Query, candidates)
	if strategy == RecallKeyword {
		return truncateItems(keyword, limit), nil
	}
	fused := FuseReciprocalRank([][]MemoryItem{semantic, keyword}, 0)
	return truncateItems(memory.reweight(fused, candidates), limit), nil
}

// reweight applies the feedback an agent gave to a fused ranking.
//
// Added to the score rather than fused as a third signal, which is both what
// Rust specifies ("every strategy except recent ranks by the folded feedback
// weight first") and what this client's own durable memory already does. As
// one more reciprocal-rank vote it could never outweigh two signals
// agreeing, so an agent saying "this one was useful" would change nothing --
// which is the opposite of the point.
func (memory *VectorMemory) reweight(
	fused []MemoryItem, candidates []*vectorEntry,
) []MemoryItem {
	weights := make(map[MemoryID]float32, len(candidates))
	memory.mutex.Lock()
	for _, entry := range candidates {
		if entry.feedback != 0 {
			weights[entry.item.ID] = entry.feedback
		}
	}
	memory.mutex.Unlock()
	if len(weights) == 0 {
		return fused
	}
	for index := range fused {
		weight, judged := weights[fused[index].ID]
		if !judged {
			continue
		}
		fused[index].Score += float64(weight)
		fused[index].Signals = append(fused[index].Signals, RecallSignal{
			Strategy: RecallFeedback,
			Rank:     len(fused[index].Signals),
			Score:    float64(weight),
		})
	}
	sort.SliceStable(fused, func(left, right int) bool {
		return fused[left].Score > fused[right].Score
	})
	return fused
}

// route picks the strategy an Auto recall resolves to.
func (memory *VectorMemory) route(query MemoryQuery) RecallStrategy {
	if query.Strategy != RecallAuto && query.Strategy != "" {
		return query.Strategy
	}
	if query.Query == "" {
		return RecallRecent
	}
	if memory.embedder == nil {
		return RecallKeyword
	}
	return RecallHybrid
}

// semanticSignal ranks the candidates by cosine similarity to the query.
func (memory *VectorMemory) semanticSignal(
	ctx context.Context, query string, candidates []*vectorEntry,
) ([]MemoryItem, error) {
	if memory.embedder == nil || query == "" {
		return nil, nil
	}
	wanted, err := memory.embedder.Embed(ctx, query)
	if err != nil {
		return nil, err
	}
	scored := make([]MemoryItem, 0, len(candidates))
	for _, entry := range candidates {
		item := entry.item
		score := CosineSimilarity(wanted, entry.embedding)
		item.Score = float64(score)
		scored = append(scored, item)
	}
	return rankedBy(scored, RecallSemantic), nil
}

// keywordSignal ranks the candidates by how much of the query their bodies
// contain, on the same 0..1 scale as cosine similarity so the two compose.
func keywordSignal(query string, candidates []*vectorEntry) []MemoryItem {
	wanted := words(query)
	if len(wanted) == 0 {
		return nil
	}
	scored := make([]MemoryItem, 0, len(candidates))
	for _, entry := range candidates {
		item := entry.item
		item.Score = overlap(words(item.Text()), wanted)
		if item.Score <= 0 {
			continue
		}
		scored = append(scored, item)
	}
	return rankedBy(scored, RecallKeyword)
}

// rankedBy sorts by score, best first, and stamps each item's standing in
// this signal so a fused recall stays inspectable.
func rankedBy(items []MemoryItem, strategy RecallStrategy) []MemoryItem {
	sort.SliceStable(items, func(left, right int) bool {
		return items[left].Score > items[right].Score
	})
	for index := range items {
		score := items[index].Score
		items[index].Signals = []RecallSignal{{
			Strategy: strategy, Rank: index, Score: score,
		}}
	}
	return items
}

// mostRecent is the newest items, for a recall with nothing to rank on.
func mostRecent(candidates []*vectorEntry, limit int) []MemoryItem {
	items := make([]MemoryItem, 0, len(candidates))
	for _, entry := range candidates {
		items = append(items, entry.item)
	}
	sort.SliceStable(items, func(left, right int) bool {
		return items[left].At.After(items[right].At)
	})
	return truncateItems(items, limit)
}

// truncateItems keeps at most limit items.
func truncateItems(items []MemoryItem, limit int) []MemoryItem {
	if limit > 0 && len(items) > limit {
		return items[:limit]
	}
	return items
}

// rrfDamping is the standard reciprocal-rank-fusion constant. It flattens
// the difference between the top ranks, so agreement between signals counts
// for more than one signal's confidence in its own first place.
const rrfDamping = 60.0

// FuseReciprocalRank fuses several signals' ranked lists into one.
//
// Each item scores the sum of 1/(60 + rank) over the signals that surfaced
// it, so an item two signals agree on outranks one signal's top hit. Pure,
// and every contributing signal is kept on the fused item, so a caller can
// see why something was recalled rather than only that it was.
func FuseReciprocalRank(signals [][]MemoryItem, limit int) []MemoryItem {
	fused := make([]MemoryItem, 0)
	at := map[MemoryID]int{}
	for _, ranked := range signals {
		for _, item := range ranked {
			contribution := 0.0
			if len(item.Signals) > 0 {
				contribution = 1.0 / (rrfDamping + float64(item.Signals[0].Rank))
			}
			if index, held := at[item.ID]; held {
				fused[index].Score += contribution
				fused[index].Signals = append(fused[index].Signals, item.Signals...)
				continue
			}
			item.Score = contribution
			at[item.ID] = len(fused)
			fused = append(fused, item)
		}
	}
	sort.SliceStable(fused, func(left, right int) bool {
		return fused[left].Score > fused[right].Score
	})
	return truncateItems(fused, limit)
}

// CosineSimilarity is how close two embeddings point, in -1..1.
//
// Zero for vectors of different lengths or either one all zeros, rather than
// a NaN that would sort unpredictably: an embedding nothing can be compared
// against should rank last, not corrupt the ordering.
func CosineSimilarity(left, right []float32) float64 {
	if len(left) != len(right) || len(left) == 0 {
		return 0
	}
	var dot, leftNorm, rightNorm float64
	for index := range left {
		first, second := float64(left[index]), float64(right[index])
		dot += first * second
		leftNorm += first * first
		rightNorm += second * second
	}
	if leftNorm == 0 || rightNorm == 0 {
		return 0
	}
	return dot / (math.Sqrt(leftNorm) * math.Sqrt(rightNorm))
}

// scopeHolds reports whether an item stored under held answers a recall
// scoped to wanted.
//
// An unset field in the recall scope matches anything, so a recall scoped to
// an agent sees that agent's items across conversations.
func scopeHolds(wanted, held MemoryScope) bool {
	if wanted.Agent != nil && (held.Agent == nil || *held.Agent != *wanted.Agent) {
		return false
	}
	if wanted.Conversation != nil &&
		(held.Conversation == nil || *held.Conversation != *wanted.Conversation) {
		return false
	}
	return true
}

// RerankedMemory wraps a memory so recall's candidates go through a second,
// more expensive pass before a caller sees them.
type RerankedMemory struct {
	inner    *VectorMemory
	reranker Reranker
}

// WithReranker puts reranker over this memory's recall.
func (memory *VectorMemory) WithReranker(reranker Reranker) *RerankedMemory {
	return &RerankedMemory{inner: memory, reranker: reranker}
}

// Recall ranks with the inner memory, then reranks against the query.
//
// A recall with no query is handed back untouched: there is nothing for a
// reranker to rank against, and asking it anyway would invent an ordering.
func (memory *RerankedMemory) Recall(
	ctx context.Context, scope MemoryScope, query MemoryQuery,
) ([]MemoryItem, error) {
	items, err := memory.inner.Recall(ctx, scope, query)
	if err != nil || query.Query == "" || memory.reranker == nil {
		return items, err
	}
	return memory.reranker.Rerank(ctx, query.Query, items)
}

// Remember keeps a fact, through the memory underneath.
func (memory *RerankedMemory) Remember(
	ctx context.Context, scope MemoryScope, kind MemoryKind, body []byte,
) (MemoryID, error) {
	return memory.inner.Remember(ctx, scope, kind, body)
}

// Forget drops an item, through the memory underneath.
func (memory *RerankedMemory) Forget(ctx context.Context, target MemoryID) error {
	return memory.inner.Forget(ctx, target)
}

// Improve reweights an item, through the memory underneath.
func (memory *RerankedMemory) Improve(ctx context.Context, feedback Feedback) error {
	return memory.inner.Improve(ctx, feedback)
}
