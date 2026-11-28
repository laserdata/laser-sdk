package laser

import (
	"sort"
	"time"
	"unicode/utf8"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// MemoryFold is the read view recall is served from: every item folded off the
// memory topic in arrival order, the tombstoned ids, and the feedback weight
// each item has accumulated.
//
// It is pure and owns no reader, so the same fold serves a recall off a live
// topic and a replay of records a caller already holds, and so the ordering
// rules can be tested without a server. The log is the source of truth: a fold
// is only what a reader has seen of it so far.
type MemoryFold struct {
	stream    string
	namespace string
	// arrival numbers items as they are folded, which is the order recall
	// reverses to get the most recent first. It is the fold's own counter
	// rather than a timestamp, because two records can share an append time
	// and a recall that reorders between runs is not reproducible.
	arrival   uint64
	items     map[MemoryID]*foldedMemory
	forgotten map[memoryTarget]uint64
	feedback  map[memoryTarget]float32
	// named is the latest body per named key, folded from the same topic.
	named map[string][]byte
}

// foldedMemory is one remembered item with what the fold orders it by.
type foldedMemory struct {
	item MemoryItem
	// writtenMicros is the server's append time, which decides a tombstone
	// against a write folded from another partition.
	writtenMicros uint64
	arrival       uint64
}

// memoryTarget keys a tombstone or a feedback signal: the item it is about and
// the conversation it is limited to.
//
// An unscoped signal is kept apart from one scoped to the zero conversation,
// because collapsing them would let a weight written both ways count twice.
type memoryTarget struct {
	id           MemoryID
	conversation wire.ConversationID
	scoped       bool
}

// NewMemoryFold opens an empty fold over one namespace on one stream.
//
// The namespace is what a record must be stamped with to be folded, so two
// namespaces sharing a memory topic never read each other's items.
func NewMemoryFold(stream, namespace string) *MemoryFold {
	return &MemoryFold{
		stream:    stream,
		namespace: namespace,
		items:     map[MemoryID]*foldedMemory{},
		forgotten: map[memoryTarget]uint64{},
		feedback:  map[memoryTarget]float32{},
		named:     map[string][]byte{},
	}
}

// Observe folds one record off the memory topic in, reporting whether it was a
// memory record of this fold's namespace.
//
// A record that is not one is skipped rather than reported as an error,
// because a memory topic is a topic like any other and a reader must survive
// whatever else was written to it.
func (fold *MemoryFold) Observe(record *Record) bool {
	if record == nil {
		return false
	}
	entries, err := iggcon.DeserializeHeaders(record.Message.UserHeaders)
	if err != nil {
		return false
	}
	headers := map[string]string{}
	for _, entry := range entries {
		if entry.Value.Kind == iggcon.String {
			headers[string(entry.Key.Value)] = string(entry.Value.Value)
		}
	}
	if headers[wire.HeaderMemoryNamespace] != fold.namespace {
		return false
	}
	var decoded wire.MemoryRecord
	if err := wire.Unmarshal(record.Payload(), &decoded); err != nil {
		return false
	}
	conversation := wire.ConversationID{}
	var agent *wire.AgentID
	if record.Provenance != nil {
		conversation = record.Provenance.Conversation
		agent = record.Provenance.Agent
	}
	fold.absorb(decoded, memoryOrigin{
		scope: MemoryScope{
			Stream:       fold.stream,
			User:         headers[wire.HeaderMemoryUser],
			App:          headers[wire.HeaderMemoryApp],
			Agent:        agent,
			Conversation: &conversation,
		},
		writtenMicros: record.Message.Header.Timestamp,
	})
	return true
}

// memoryOrigin is what the log carried alongside one memory record: the scope
// its headers named, and the server's append time.
type memoryOrigin struct {
	scope         MemoryScope
	writtenMicros uint64
}

// absorb applies one decoded record to the view.
//
// A tombstone removes the item and is remembered, a feedback signal
// accumulates on its target, and an item is kept unless it was already
// forgotten or already held.
func (fold *MemoryFold) absorb(record wire.MemoryRecord, origin memoryOrigin) {
	switch record.Kind {
	case wire.MemoryForget:
		fold.forget(record, origin.writtenMicros)
	case wire.MemoryFeedback:
		target, known := memoryTargetOf(record.Target, record.Conversation)
		if !known {
			return
		}
		fold.feedback[target] += record.Weight
	case wire.MemoryItem:
		fold.remember(record, origin)
	}
}

func (fold *MemoryFold) forget(record wire.MemoryRecord, at uint64) {
	target, known := memoryTargetOf(record.Target, record.Conversation)
	if !known {
		if _, err := wire.ParseID(record.Target); err != nil {
			// A target that is not a memory id is a named key.
			delete(fold.named, record.Target)
		}
		return
	}
	// The item goes rather than being flagged, so a tombstoned body is not
	// pinned for the life of the fold. A write that landed after the forget
	// stays, which is what lets a fact be remembered again after a correction.
	if held, found := fold.items[target.id]; found &&
		held.writtenMicros <= at &&
		(!target.scoped || memoryConversationOf(held.item.Scope) == target.conversation) {
		delete(fold.items, target.id)
	}
	// Kept whether or not an item was removed, so a tombstone read before the
	// write it cancels still suppresses it when that write arrives from
	// another partition.
	if existing, found := fold.forgotten[target]; !found || existing < at {
		fold.forgotten[target] = at
	}
}

func (fold *MemoryFold) remember(record wire.MemoryRecord, origin memoryOrigin) {
	id, err := wire.ParseID(record.ID)
	if err != nil {
		// An id that is not a memory id is a named key: point state, never a recall item.
		fold.named[record.ID] = append([]byte(nil), record.Body...)
		return
	}
	target := MemoryID{ID: id}
	if fold.isForgotten(target, memoryConversationOf(origin.scope), origin.writtenMicros) {
		return
	}
	if _, held := fold.items[target]; held {
		// A content-addressed id appended twice is one item, so remembering
		// the same body again converges rather than growing the view.
		return
	}
	scope := origin.scope
	scope.Origin = record.Origin
	scope.Producer = record.Producer
	fold.arrival++
	fold.items[target] = &foldedMemory{
		item: MemoryItem{
			ID:    target,
			Body:  append([]byte(nil), record.Body...),
			Kind:  MemoryKindFromWord(record.ItemKind),
			Scope: scope,
			At:    time.UnixMicro(int64(origin.writtenMicros)).UTC(),
		},
		writtenMicros: origin.writtenMicros,
		arrival:       fold.arrival,
	}
}

// isForgotten reports a write at or before a tombstone's time, which stays
// forgotten. A later write is not: the tombstone cancelled what came before it.
func (fold *MemoryFold) isForgotten(
	id MemoryID, conversation wire.ConversationID, at uint64,
) bool {
	for _, target := range [...]memoryTarget{
		{id: id},
		{id: id, conversation: conversation, scoped: true},
	} {
		if forgotten, found := fold.forgotten[target]; found && forgotten >= at {
			return true
		}
	}
	return false
}

// feedbackFor is the weight folded onto one item, zero when none was.
//
// A signal left unscoped and one scoped to the item's conversation both count,
// so an agent correcting a fact everywhere and one correcting it here add up
// rather than one replacing the other.
func (fold *MemoryFold) feedbackFor(item MemoryItem) float32 {
	return fold.feedback[memoryTarget{id: item.ID}] +
		fold.feedback[memoryTarget{
			id:           item.ID,
			conversation: memoryConversationOf(item.Scope),
			scoped:       true,
		}]
}

// Recall folds the view down to what one query asks for, most relevant first.
//
// It is the same pipeline every client runs, so an agent moved between clients
// recalls the same items in the same order.
func (fold *MemoryFold) Recall(scope MemoryScope, query MemoryQuery) []MemoryItem {
	words := newQueryWords(query)
	kept := make([]*foldedMemory, 0, len(fold.items))
	for _, held := range fold.items {
		if !memoryScopeMatches(held.item.Scope, scope) {
			continue
		}
		if !matchesKind(held.item, query.Kinds) {
			continue
		}
		if !words.keeps(held.item.Body) {
			continue
		}
		kept = append(kept, held)
	}
	// Newest first before anything is scored, so a stable sort by score leaves
	// the most recent ahead among items that rank the same.
	sort.Slice(kept, func(first, second int) bool {
		return kept[first].arrival > kept[second].arrival
	})

	// Recent recall is recency alone, so feedback never reorders it.
	weighted := query.Strategy != RecallRecent && len(fold.feedback) > 0
	ranked := make([]MemoryItem, 0, len(kept))
	for _, held := range kept {
		item := held.item
		item.Body = append([]byte(nil), item.Body...)
		item.Score = words.overlap(item.Body)
		if weighted {
			item.Score += float64(fold.feedbackFor(item))
		}
		ranked = append(ranked, item)
	}
	sort.SliceStable(ranked, func(first, second int) bool {
		return ranked[first].Score > ranked[second].Score
	})
	if limit := recallLimit(query); len(ranked) > limit {
		ranked = ranked[:limit]
	}
	return ranked
}

// Len is how many items the fold currently holds, before any query narrows
// them.
func (fold *MemoryFold) Len() int { return len(fold.items) }

// memoryScopeMatches reports whether a stored scope is inside the one a recall
// asked for. An unset field widens rather than narrows, which is what lets one
// recall span every conversation a user has had.
func memoryScopeMatches(stored, requested MemoryScope) bool {
	if requested.Stream != "" && stored.Stream != requested.Stream {
		return false
	}
	if requested.User != "" && stored.User != requested.User {
		return false
	}
	if requested.App != "" && stored.App != requested.App {
		return false
	}
	if requested.Agent != nil &&
		(stored.Agent == nil || *stored.Agent != *requested.Agent) {
		return false
	}
	if requested.Conversation != nil &&
		memoryConversationOf(stored) != *requested.Conversation {
		return false
	}
	return true
}

// memoryConversationOf is the conversation a scope names, zero when it names
// none. The contract reads an all-zero id as absent, so the two agree.
func memoryConversationOf(scope MemoryScope) wire.ConversationID {
	if scope.Conversation == nil {
		return wire.ConversationID{}
	}
	return *scope.Conversation
}

// memoryTargetOf reads the item and conversation a tombstone or a feedback
// record is about. A target that is not a memory id addresses no item this
// client can recall, so it is reported as unknown rather than stored.
func memoryTargetOf(text string, conversation *string) (memoryTarget, bool) {
	id, err := wire.ParseID(text)
	if err != nil {
		return memoryTarget{}, false
	}
	target := memoryTarget{id: MemoryID{ID: id}}
	if conversation == nil {
		return target, true
	}
	scoped, err := wire.ParseID(*conversation)
	if err != nil {
		// A conversation that does not parse is not the same as none: taking
		// it as none would widen a correction meant for one session into every
		// session, so the record is left out.
		return memoryTarget{}, false
	}
	target.conversation = wire.ConversationID{ID: scoped}
	target.scoped = true
	return target, true
}

// queryWords is how a recall uses the words of its query: a ranking strategy
// scores every item by overlap and drops none, a keyword strategy keeps only
// the items sharing a word and contributes no score, and recent ignores them.
type queryWords struct {
	words  map[string]struct{}
	ranks  bool
	filter bool
}

func newQueryWords(query MemoryQuery) queryWords {
	if query.Strategy == RecallRecent || query.Query == "" {
		return queryWords{}
	}
	found := words(query.Query)
	if len(found) == 0 {
		// Text holding no word ranks nothing, so the recall stays recency
		// alone rather than scoring every item zero.
		return queryWords{}
	}
	return queryWords{
		words:  found,
		ranks:  query.Strategy != RecallKeyword,
		filter: query.Strategy == RecallKeyword,
	}
}

// keeps reports whether an item survives the query at all.
func (query queryWords) keeps(body []byte) bool {
	return !query.filter || bodyOverlap(query.words, body) > 0
}

// overlap is the item's score against the query, zero for a strategy that
// filters by the words rather than ranking on them.
func (query queryWords) overlap(body []byte) float64 {
	if !query.ranks {
		return 0
	}
	return bodyOverlap(query.words, body)
}

// bodyOverlap scores one body against a query's words.
//
// A body that is not text shares no word, because the words a query is made of
// are text and scoring them against arbitrary bytes would rank noise.
func bodyOverlap(query map[string]struct{}, body []byte) float64 {
	if !utf8.Valid(body) {
		return 0
	}
	return overlap(words(string(body)), query)
}

// recallLimit is how many items a query takes, which is the default when it
// names none.
func recallLimit(query MemoryQuery) int {
	if query.Limit <= 0 {
		return DefaultRecallLimit
	}
	return query.Limit
}
