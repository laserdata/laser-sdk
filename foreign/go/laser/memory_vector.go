package laser

import (
	"bytes"
	"context"
	"math"
	"sort"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
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
// Laser.Memory(namespace), which writes to the log. This is the semantic
// backend for a process that wants similarity ranking without one.
type VectorMemory struct {
	embedder Embedder
	// governed is the handle whose governor sees every write, nil for a standalone memory.
	governed *Laser
	mutex    sync.Mutex
	entries  []*vectorEntry
}

// NewVectorMemory opens an in-process semantic memory over embedder.
func NewVectorMemory(embedder Embedder) *VectorMemory {
	return &VectorMemory{embedder: embedder}
}

// NewGovernedVectorMemory opens an in-process semantic memory whose writes run through the governor enrolled on laser.
func NewGovernedVectorMemory(laser *Laser, embedder Embedder) *VectorMemory {
	return &VectorMemory{embedder: embedder, governed: laser}
}

// Remember embeds body and keeps it under scope.
func (memory *VectorMemory) Remember(
	ctx context.Context, scope MemoryScope, kind MemoryKind, body []byte,
) (MemoryID, error) {
	id := ContentMemoryID(scope, kind, body)
	if err := memory.append(ctx, scope, id, kind, body); err != nil {
		return MemoryID{}, err
	}
	return id, nil
}

// Forget drops an item.
func (memory *VectorMemory) Forget(ctx context.Context, target MemoryID) error {
	return memory.forget(ctx, MemoryScope{}, target)
}

// Improve reweights an item, so what an agent found useful is ranked higher
// next time without anything being rewritten.
func (memory *VectorMemory) Improve(ctx context.Context, feedback Feedback) error {
	return memory.improve(ctx, MemoryScope{}, feedback)
}

// append keeps one item under an explicit id. An id already held is one item.
func (memory *VectorMemory) append(
	ctx context.Context, scope MemoryScope, id MemoryID, kind MemoryKind, body []byte,
) error {
	if memory.embedder == nil {
		return &InvalidError{Reason: "a vector memory needs an embedder"}
	}
	body, err := memory.govern(ctx, scope, body)
	if err != nil {
		return err
	}
	embedding, err := memory.embedder.Embed(ctx, string(body))
	if err != nil {
		return err
	}
	item := MemoryItem{ID: id, Body: body, Kind: kind, Scope: scope, At: time.Now()}
	memory.mutex.Lock()
	defer memory.mutex.Unlock()
	for _, entry := range memory.entries {
		if entry.item.ID == id {
			return nil
		}
	}
	memory.entries = append(memory.entries, &vectorEntry{item: item, embedding: embedding})
	return nil
}

// forget drops an item, in the scope's conversation when it names one.
func (memory *VectorMemory) forget(ctx context.Context, scope MemoryScope, target MemoryID) error {
	record := wire.ForgetItem(target.String())
	if scope.Conversation != nil {
		conversation := scope.Conversation.String()
		record.Conversation = &conversation
	}
	record, err := memory.governRecord(ctx, scope, record)
	if err != nil {
		return err
	}
	governed, known := memoryTargetOf(record.Target, record.Conversation)
	if record.Kind != wire.MemoryForget || !known {
		return &InvalidError{Reason: "a memory-forget governor modification must remain a tombstone"}
	}
	var conversation *wire.ConversationID
	if governed.scoped {
		conversation = &governed.conversation
	}
	memory.mutex.Lock()
	defer memory.mutex.Unlock()
	kept := memory.entries[:0]
	for _, entry := range memory.entries {
		if entry.item.ID != governed.id || !vectorInConversation(entry, conversation) {
			kept = append(kept, entry)
		}
	}
	clear(memory.entries[len(kept):])
	memory.entries = kept
	return nil
}

// improve adds a weight to an item, in the scope's conversation when it names one.
func (memory *VectorMemory) improve(ctx context.Context, scope MemoryScope, feedback Feedback) error {
	record := wire.FeedbackForItem(feedback.Target.String(), feedback.Weight)
	if scope.Conversation != nil {
		conversation := scope.Conversation.String()
		record.Conversation = &conversation
	}
	record, err := memory.governRecord(ctx, scope, record)
	if err != nil {
		return err
	}
	governed, known := memoryTargetOf(record.Target, record.Conversation)
	if record.Kind != wire.MemoryFeedback || !known {
		return &InvalidError{Reason: "a memory-feedback governor modification must remain feedback"}
	}
	var conversation *wire.ConversationID
	if governed.scoped {
		conversation = &governed.conversation
	}
	memory.mutex.Lock()
	defer memory.mutex.Unlock()
	for _, entry := range memory.entries {
		if entry.item.ID == governed.id && vectorInConversation(entry, conversation) {
			entry.feedback += record.Weight
			return nil
		}
	}
	return nil
}

// govern offers an item body to the governor and returns the body to keep.
func (memory *VectorMemory) govern(ctx context.Context, scope MemoryScope, payload []byte) ([]byte, error) {
	if memory.governed == nil {
		return payload, nil
	}
	if memory.governed.defaultStream == "" {
		return nil, errNoStream()
	}
	conversation := memoryConversationOf(scope)
	action := &GovernedAction{
		Kind: ActionMemoryWrite, Stream: memory.governed.defaultStream, Topic: wire.AgentMemory,
		Conversation: &conversation, Payload: payload,
	}
	if scope.Agent != nil {
		action.Source = scope.Agent.String()
	}
	modified, err := memory.governed.govern(ctx, action)
	if err != nil {
		return nil, err
	}
	if modified != nil {
		return modified, nil
	}
	return payload, nil
}

// governRecord offers a tombstone or a feedback record to the governor and returns the record to apply.
func (memory *VectorMemory) governRecord(
	ctx context.Context, scope MemoryScope, record wire.MemoryRecord,
) (wire.MemoryRecord, error) {
	if memory.governed == nil {
		return record, nil
	}
	payload, err := wire.Marshal(record)
	if err != nil {
		return record, &CodecError{Reason: "encode memory mutation", Cause: err}
	}
	governed, err := memory.govern(ctx, scope, payload)
	if err != nil || bytes.Equal(governed, payload) {
		return record, err
	}
	var modified wire.MemoryRecord
	if err := wire.Unmarshal(governed, &modified); err != nil {
		return record, &CodecError{Reason: "decode the governed memory mutation", Cause: err}
	}
	return modified, nil
}

func vectorInConversation(entry *vectorEntry, conversation *wire.ConversationID) bool {
	return conversation == nil ||
		entry.item.Scope.Conversation != nil && *entry.item.Scope.Conversation == *conversation
}

// Recall ranks what it holds against the query.
//
// Every strategy but keyword and recent scores an item by the cosine
// similarity of its embedding to the query's. Keyword and hybrid add the share
// of the query's words the item holds. Feedback adds its weight under every
// strategy but recent. An item's score is the sum of its signals, and with
// nothing to rank on the newest items come first.
func (memory *VectorMemory) Recall(
	ctx context.Context, scope MemoryScope, query MemoryQuery,
) ([]MemoryItem, error) {
	if err := query.Strategy.validate(); err != nil {
		return nil, err
	}
	limit := query.Limit
	if limit <= 0 {
		limit = DefaultRecallLimit
	}
	strategy := query.Strategy
	asked := query.Query != ""
	wantsSemantic := asked && strategy != RecallKeyword && strategy != RecallRecent && memory.embedder != nil
	wantsKeyword := asked && (strategy == RecallKeyword || strategy == RecallHybrid)
	// The query is embedded before the lock, so a model call never holds it.
	var wanted []float32
	if wantsSemantic {
		embedded, err := memory.embedder.Embed(ctx, query.Query)
		if err != nil {
			return nil, err
		}
		wanted = embedded
	}
	var tokens map[string]struct{}
	if wantsKeyword {
		tokens = words(query.Query)
	}

	feedbackRanks := strategy != RecallRecent
	anyFeedback := false
	memory.mutex.Lock()
	matched := make([]vectorCandidate, 0, len(memory.entries))
	for _, entry := range memory.entries {
		if !scopeHolds(scope, entry.item.Scope) || !matchesKind(entry.item, query.Kinds) {
			continue
		}
		candidate := vectorCandidate{entry: entry}
		if feedbackRanks {
			candidate.feedback = entry.feedback
			anyFeedback = anyFeedback || entry.feedback != 0
		}
		matched = append(matched, candidate)
	}
	memory.mutex.Unlock()

	if !wantsSemantic && !wantsKeyword && !anyFeedback {
		return newestFirst(matched, limit), nil
	}
	for index := range matched {
		candidate := &matched[index]
		if wantsSemantic {
			candidate.add(RecallSemantic, CosineSimilarity(wanted, candidate.entry.embedding))
		}
		if wantsKeyword {
			candidate.add(RecallKeyword, keywordScore(tokens, candidate.entry.item.Body))
		}
		if candidate.feedback != 0 {
			candidate.add(RecallAuto, float64(candidate.feedback))
		}
	}
	for _, signal := range []RecallStrategy{RecallSemantic, RecallKeyword, RecallAuto} {
		rankSignal(matched, signal)
	}
	sort.SliceStable(matched, func(left, right int) bool {
		return matched[left].score > matched[right].score
	})
	if len(matched) > limit {
		matched = matched[:limit]
	}
	items := make([]MemoryItem, 0, len(matched))
	for _, candidate := range matched {
		item := candidate.entry.item
		item.Score = candidate.score
		item.Signals = candidate.signals
		items = append(items, item)
	}
	return items, nil
}

// vectorCandidate is one matched entry with the signals a recall scored it on.
type vectorCandidate struct {
	entry    *vectorEntry
	feedback float32
	score    float64
	signals  []RecallSignal
}

// add records one signal and counts it into the candidate's score.
func (candidate *vectorCandidate) add(strategy RecallStrategy, score float64) {
	candidate.signals = append(candidate.signals, RecallSignal{Strategy: strategy, Score: score})
	candidate.score += score
}

// rankSignal stamps each candidate's standing within one signal, best first.
func rankSignal(candidates []vectorCandidate, strategy RecallStrategy) {
	type standing struct {
		signal *RecallSignal
		score  float64
	}
	var held []standing
	for index := range candidates {
		for at := range candidates[index].signals {
			if signal := &candidates[index].signals[at]; signal.Strategy == strategy {
				held = append(held, standing{signal: signal, score: signal.Score})
				break
			}
		}
	}
	sort.SliceStable(held, func(left, right int) bool { return held[left].score > held[right].score })
	for rank, standing := range held {
		standing.signal.Rank = rank
	}
}

// keywordScore is the share of the query's words a body holds, from 0 to 1,
// and 0 for a body that is not text.
func keywordScore(query map[string]struct{}, body []byte) float64 {
	if len(query) == 0 || !utf8.Valid(body) {
		return 0
	}
	return overlap(words(string(body)), query)
}

// newestFirst is the most recently written items, unranked.
func newestFirst(candidates []vectorCandidate, limit int) []MemoryItem {
	items := make([]MemoryItem, 0, min(len(candidates), limit))
	for index := len(candidates) - 1; index >= 0 && len(items) < limit; index-- {
		items = append(items, candidates[index].entry.item)
	}
	return items
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
	return (wanted.User == "" || held.User == wanted.User) &&
		(wanted.App == "" || held.App == wanted.App) &&
		(wanted.Stream == "" || held.Stream == wanted.Stream)
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
