package laser_test

import (
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// memoryWrite is one record as it reached the memory topic, with everything
// the fold reads off it.
type memoryWrite struct {
	record       wire.MemoryRecord
	namespace    string
	user         string
	app          string
	agent        *wire.AgentID
	conversation *wire.ConversationID
	// at is the server's append time in microseconds, which is what decides a
	// tombstone against a write from another partition.
	at     uint64
	offset uint64
	// payload overrides the encoded record, for a record the fold must survive
	// rather than decode.
	payload []byte
}

func foldRecord(t *testing.T, write memoryWrite) *laser.Record {
	t.Helper()
	payload := write.payload
	if payload == nil {
		encoded, err := wire.Marshal(write.record)
		if err != nil {
			t.Fatalf("encode the memory record: %v", err)
		}
		payload = encoded
	}
	provenance := &laser.Provenance{Agent: write.agent}
	if write.conversation != nil {
		provenance.Conversation = *write.conversation
	}
	entries, err := provenance.Headers()
	if err != nil {
		t.Fatalf("encode the provenance: %v", err)
	}
	for key, value := range map[string]string{
		wire.HeaderMemoryNamespace: write.namespace,
		wire.HeaderMemoryUser:      write.user,
		wire.HeaderMemoryApp:       write.app,
	} {
		if value == "" {
			continue
		}
		name, err := iggcon.NewHeaderKeyString(key)
		if err != nil {
			t.Fatalf("header name %s: %v", key, err)
		}
		entries = append(entries, iggcon.HeaderEntry{
			Key:   name,
			Value: iggcon.HeaderValue{Kind: iggcon.String, Value: []byte(value)},
		})
	}
	message, err := iggcon.NewIggyMessage(payload, iggcon.WithUserHeaders(entries))
	if err != nil {
		t.Fatalf("build the record: %v", err)
	}
	message.Header.Timestamp = write.at
	message.Header.Offset = write.offset
	record, err := laser.OpenRecord(message)
	if err != nil {
		t.Fatalf("open the record: %v", err)
	}
	return record
}

// rememberedIn builds a fold holding the given bodies, each remembered one
// microsecond after the last under one scope.
func rememberedIn(
	t *testing.T, stream, namespace, user string,
	conversation *wire.ConversationID, bodies ...string,
) (*laser.MemoryFold, []laser.MemoryID) {
	t.Helper()
	fold := laser.NewMemoryFold(stream, namespace)
	scope := laser.MemoryScope{Stream: stream, User: user, Conversation: conversation}
	ids := make([]laser.MemoryID, 0, len(bodies))
	for index, body := range bodies {
		id := laser.ContentMemoryID(scope, laser.MemoryFact, []byte(body))
		ids = append(ids, id)
		if !fold.Observe(foldRecord(t, memoryWrite{
			record:       wire.RememberItem(id.String(), string(laser.MemoryFact), []byte(body)),
			namespace:    namespace,
			user:         user,
			conversation: conversation,
			at:           uint64(index + 1),
			offset:       uint64(index),
		})) {
			t.Fatalf("the fold refused %q", body)
		}
	}
	return fold, ids
}

func foldTexts(items []laser.MemoryItem) []string {
	out := make([]string, 0, len(items))
	for _, item := range items {
		out = append(out, item.Text())
	}
	return out
}

func wantTexts(t *testing.T, got []laser.MemoryItem, want ...string) {
	t.Helper()
	texts := foldTexts(got)
	if len(texts) != len(want) {
		t.Fatalf("want %v, got %v", want, texts)
	}
	for index, text := range want {
		if texts[index] != text {
			t.Fatalf("want %v, got %v", want, texts)
		}
	}
}

// Two namespaces share one memory topic, so a record the other wrote must not
// reach this fold's recall.
func TestGivenAForeignNamespaceWhenObservedThenShouldNotFoldIt(t *testing.T) {
	fold := laser.NewMemoryFold("agents", "support")
	id := laser.ContentMemoryID(laser.MemoryScope{Stream: "agents"}, laser.MemoryFact, []byte("x"))

	folded := fold.Observe(foldRecord(t, memoryWrite{
		record:    wire.RememberItem(id.String(), string(laser.MemoryFact), []byte("x")),
		namespace: "billing",
		at:        1,
	}))

	if folded {
		t.Error("a record of another namespace must not fold")
	}
	if fold.Len() != 0 {
		t.Errorf("want an empty fold, got %d items", fold.Len())
	}
}

// A memory topic is a topic like any other, so a reader must survive whatever
// else was written to it rather than failing the whole recall.
func TestGivenANonMemoryPayloadWhenObservedThenShouldSkipIt(t *testing.T) {
	fold := laser.NewMemoryFold("agents", "support")

	folded := fold.Observe(foldRecord(t, memoryWrite{
		namespace: "support", payload: []byte("not a memory record"), at: 1,
	}))

	if folded {
		t.Error("a payload that is not a memory record must not fold")
	}
	if fold.Len() != 0 {
		t.Errorf("want an empty fold, got %d items", fold.Len())
	}
}

func TestGivenARememberedItemWhenRecalledThenShouldCarryWhatItWasStoredWith(t *testing.T) {
	session := conversation(7)
	fold, ids := rememberedIn(t, "agents", "support", "ana", &session,
		"the user prefers green tea")

	items := fold.Recall(laser.MemoryScope{Stream: "agents", User: "ana"}, laser.MemoryQuery{})

	if len(items) != 1 {
		t.Fatalf("want the remembered item, got %v", foldTexts(items))
	}
	item := items[0]
	if item.ID != ids[0] {
		t.Errorf("want the stored id, got %s", item.ID)
	}
	if item.Kind != laser.MemoryFact {
		t.Errorf("want a fact, got %s", item.Kind)
	}
	if item.Scope.User != "ana" || item.Scope.Stream != "agents" {
		t.Errorf("want the stored scope, got %+v", item.Scope)
	}
	if item.Scope.Conversation == nil || *item.Scope.Conversation != session {
		t.Errorf("want the conversation kept, got %v", item.Scope.Conversation)
	}
	// The append time is the one clock every partition shares, so it is what a
	// reader sorts and ages items by.
	if !item.At.Equal(time.UnixMicro(1).UTC()) {
		t.Errorf("want the server append time, got %s", item.At)
	}
}

func TestGivenATombstoneWhenFoldedThenShouldRemoveTheItemFromRecall(t *testing.T) {
	fold, ids := rememberedIn(t, "agents", "support", "ana", nil, "allergic to peanuts")

	fold.Observe(foldRecord(t, memoryWrite{
		record: wire.ForgetItem(ids[0].String()), namespace: "support", at: 9,
	}))

	if items := fold.Recall(laser.MemoryScope{}, laser.MemoryQuery{}); len(items) != 0 {
		t.Errorf("a forgotten item must not be recalled, got %v", foldTexts(items))
	}
	if fold.Len() != 0 {
		t.Error("a tombstoned body must not stay pinned in the fold")
	}
}

// A correction meant for one session must not erase the fact everywhere, so a
// scoped tombstone matches only the conversation it names.
func TestGivenATombstoneForAnotherConversationWhenFoldedThenShouldKeepTheItem(t *testing.T) {
	session := conversation(1)
	other := conversation(2)
	fold, ids := rememberedIn(t, "agents", "support", "ana", &session, "allergic to peanuts")
	scoped := other.String()

	fold.Observe(foldRecord(t, memoryWrite{
		record: wire.MemoryRecord{
			Kind: wire.MemoryForget, Target: ids[0].String(), Conversation: &scoped,
		},
		namespace: "support", conversation: &other, at: 9,
	}))

	items := fold.Recall(laser.MemoryScope{}, laser.MemoryQuery{})
	wantTexts(t, items, "allergic to peanuts")
}

// Partitions are ordered among themselves and not against each other, so a
// tombstone can be read before the write it cancels and must still suppress it.
func TestGivenATombstoneBeforeAnOlderWriteWhenFoldedThenShouldSuppressIt(t *testing.T) {
	fold := laser.NewMemoryFold("agents", "support")
	id := laser.ContentMemoryID(laser.MemoryScope{Stream: "agents"}, laser.MemoryFact, []byte("old"))

	fold.Observe(foldRecord(t, memoryWrite{
		record: wire.ForgetItem(id.String()), namespace: "support", at: 50,
	}))
	fold.Observe(foldRecord(t, memoryWrite{
		record:    wire.RememberItem(id.String(), string(laser.MemoryFact), []byte("old")),
		namespace: "support", at: 40,
	}))

	if items := fold.Recall(laser.MemoryScope{}, laser.MemoryQuery{}); len(items) != 0 {
		t.Errorf("a write before the tombstone stays forgotten, got %v", foldTexts(items))
	}
}

// A tombstone cancels what came before it and nothing after, which is what
// lets a fact be remembered again once it has been corrected.
func TestGivenAWriteAfterATombstoneWhenFoldedThenShouldRecallItAgain(t *testing.T) {
	fold := laser.NewMemoryFold("agents", "support")
	id := laser.ContentMemoryID(laser.MemoryScope{Stream: "agents"}, laser.MemoryFact, []byte("fresh"))

	fold.Observe(foldRecord(t, memoryWrite{
		record: wire.ForgetItem(id.String()), namespace: "support", at: 40,
	}))
	fold.Observe(foldRecord(t, memoryWrite{
		record:    wire.RememberItem(id.String(), string(laser.MemoryFact), []byte("fresh")),
		namespace: "support", at: 50,
	}))

	wantTexts(t, fold.Recall(laser.MemoryScope{}, laser.MemoryQuery{}), "fresh")
}

// A content-addressed id appended twice is one item, so remembering the same
// body again converges rather than growing the view.
func TestGivenOneIdAppendedTwiceWhenFoldedThenShouldHoldOneItem(t *testing.T) {
	fold := laser.NewMemoryFold("agents", "support")
	id := laser.ContentMemoryID(laser.MemoryScope{Stream: "agents"}, laser.MemoryFact, []byte("once"))
	record := memoryWrite{
		record:    wire.RememberItem(id.String(), string(laser.MemoryFact), []byte("once")),
		namespace: "support", at: 1,
	}

	fold.Observe(foldRecord(t, record))
	record.at = 2
	fold.Observe(foldRecord(t, record))

	if fold.Len() != 1 {
		t.Errorf("want one item, got %d", fold.Len())
	}
}

// A repeat of an id already held is dropped rather than replacing it, so
// remembering a fact again does not pass it off as freshly learnt.
func TestGivenOneIdAppendedTwiceWhenRecalledThenShouldKeepItsFirstArrival(t *testing.T) {
	fold := laser.NewMemoryFold("agents", "support")
	for index, body := range []string{"first", "second"} {
		id := laser.ContentMemoryID(
			laser.MemoryScope{Stream: "agents"}, laser.MemoryFact, []byte(body))
		fold.Observe(foldRecord(t, memoryWrite{
			record:    wire.RememberItem(id.String(), string(laser.MemoryFact), []byte(body)),
			namespace: "support", at: uint64(index + 1),
		}))
	}
	repeat := laser.ContentMemoryID(
		laser.MemoryScope{Stream: "agents"}, laser.MemoryFact, []byte("first"))

	fold.Observe(foldRecord(t, memoryWrite{
		record:    wire.RememberItem(repeat.String(), string(laser.MemoryFact), []byte("first")),
		namespace: "support", at: 9,
	}))

	wantTexts(t, fold.Recall(laser.MemoryScope{}, laser.MemoryQuery{}), "second", "first")
}

// The lineage a scope was remembered with is what lets a reader trace a memory
// back to what produced it, so the fold must hand it back.
func TestGivenARememberedLineageWhenRecalledThenShouldHandItBack(t *testing.T) {
	fold := laser.NewMemoryFold("agents", "support")
	id := laser.ContentMemoryID(
		laser.MemoryScope{Stream: "agents"}, laser.MemoryFact, []byte("traced"))
	record := wire.RememberItem(id.String(), string(laser.MemoryFact), []byte("traced"))
	record.Producer = &wire.ProducerInfo{Name: "extractor", Version: "1.0.0"}

	fold.Observe(foldRecord(t, memoryWrite{record: record, namespace: "support", at: 1}))

	items := fold.Recall(laser.MemoryScope{}, laser.MemoryQuery{})
	if len(items) != 1 {
		t.Fatalf("want the item, got %v", foldTexts(items))
	}
	if items[0].Scope.Producer == nil || items[0].Scope.Producer.Name != "extractor" {
		t.Errorf("want the producer kept, got %+v", items[0].Scope.Producer)
	}
}

// A conversation that does not parse is not the same as none: reading it as
// none would widen a correction meant for one session into every session.
func TestGivenATombstoneNamingAnUnreadableConversationWhenFoldedThenShouldLeaveTheItem(t *testing.T) {
	session := conversation(5)
	fold, ids := rememberedIn(t, "agents", "support", "ana", &session, "kept")
	unreadable := "not-a-conversation"

	fold.Observe(foldRecord(t, memoryWrite{
		record: wire.MemoryRecord{
			Kind: wire.MemoryForget, Target: ids[0].String(), Conversation: &unreadable,
		},
		namespace: "support", conversation: &session, at: 9,
	}))

	wantTexts(t, fold.Recall(laser.MemoryScope{}, laser.MemoryQuery{}), "kept")
}

// Two users must never see each other's memories, and a recall naming no lens
// must widen rather than return nothing.
func TestGivenAScopedRecallWhenFoldedThenShouldKeepOnlyWhatIsInside(t *testing.T) {
	session := conversation(1)
	other := conversation(2)
	planner := agentID(t, "planner")
	executor := agentID(t, "executor")
	fold := laser.NewMemoryFold("agents", "support")
	writes := []memoryWrite{
		{user: "ana", app: "support", agent: &planner, conversation: &session},
		{user: "bo", app: "support", agent: &planner, conversation: &session},
		{user: "ana", app: "billing", agent: &planner, conversation: &session},
		{user: "ana", app: "support", agent: &executor, conversation: &session},
		{user: "ana", app: "support", agent: &planner, conversation: &other},
	}
	for index, write := range writes {
		body := []byte{byte('a' + index)}
		id := laser.ContentMemoryID(laser.MemoryScope{Stream: "agents"}, laser.MemoryFact, body)
		write.record = wire.RememberItem(id.String(), string(laser.MemoryFact), body)
		write.namespace = "support"
		write.at = uint64(index + 1)
		fold.Observe(foldRecord(t, write))
	}

	for name, lens := range map[string]laser.MemoryScope{
		"user":         {User: "ana"},
		"app":          {App: "support"},
		"agent":        {Agent: &planner},
		"conversation": {Conversation: &session},
	} {
		t.Run(name, func(t *testing.T) {
			items := fold.Recall(lens, laser.MemoryQuery{})
			if len(items) != 4 {
				t.Errorf("the %s lens must narrow to four, got %v", name, foldTexts(items))
			}
		})
	}
	if got := len(fold.Recall(laser.MemoryScope{}, laser.MemoryQuery{})); got != 5 {
		t.Errorf("a recall naming no lens widens, got %d", got)
	}
	if got := len(fold.Recall(laser.MemoryScope{Stream: "agents"}, laser.MemoryQuery{})); got != 5 {
		t.Errorf("the fold's own stream keeps everything, got %d", got)
	}
	// The stream is the isolation boundary rather than a filter among others,
	// so a recall naming another one reaches nothing at all.
	if got := len(fold.Recall(laser.MemoryScope{Stream: "other"}, laser.MemoryQuery{})); got != 0 {
		t.Errorf("want nothing from another stream, got %d", got)
	}
}

func TestGivenAKindFilterWhenRecalledThenShouldKeepOnlyThoseKinds(t *testing.T) {
	fold := laser.NewMemoryFold("agents", "support")
	for index, kind := range []laser.MemoryKind{
		laser.MemoryFact, laser.MemoryProcedure, laser.MemoryMessage,
	} {
		body := []byte(string(kind))
		id := laser.ContentMemoryID(laser.MemoryScope{Stream: "agents"}, kind, body)
		fold.Observe(foldRecord(t, memoryWrite{
			record:    wire.RememberItem(id.String(), string(kind), body),
			namespace: "support", at: uint64(index + 1),
		}))
	}

	items := fold.Recall(laser.MemoryScope{}, laser.MemoryQuery{
		Kinds: []laser.MemoryKind{laser.MemoryProcedure},
	})

	wantTexts(t, items, string(laser.MemoryProcedure))
}

// Keyword recall keeps only what shares a word: an item matching nothing is
// not a weak answer but no answer.
func TestGivenAKeywordRecallWhenFoldedThenShouldDropWhatSharesNothing(t *testing.T) {
	fold, _ := rememberedIn(t, "agents", "support", "ana", nil,
		"the user is allergic to peanuts", "the user lives in Lisbon")

	items := fold.Recall(laser.MemoryScope{}, laser.MemoryQuery{
		Query: "Lisbon", Strategy: laser.RecallKeyword,
	})

	wantTexts(t, items, "the user lives in Lisbon")
}

// A ranking recall orders by overlap and drops none, because an item sharing
// no word is still worth showing when the reader asked to rank rather than
// filter.
func TestGivenARankingRecallWhenFoldedThenShouldOrderByOverlapAndDropNone(t *testing.T) {
	fold, _ := rememberedIn(t, "agents", "support", "ana", nil,
		"the user is allergic to peanuts",
		"green tea every morning",
		"the user lives in Lisbon")

	items := fold.Recall(laser.MemoryScope{}, laser.MemoryQuery{
		Query: "which city does the user live in",
	})

	wantTexts(t, items,
		"the user lives in Lisbon",
		"the user is allergic to peanuts",
		"green tea every morning")
	if !(items[0].Score > items[1].Score && items[1].Score > items[2].Score) {
		t.Errorf("want the scores ordered, got %v %v %v",
			items[0].Score, items[1].Score, items[2].Score)
	}
}

// The words a query is made of are text, so scoring them against bytes that
// are not would rank noise.
func TestGivenABodyThatIsNotTextWhenRecalledThenShouldShareNoWord(t *testing.T) {
	fold := laser.NewMemoryFold("agents", "support")
	body := []byte("deploy \xff friday")
	id := laser.ContentMemoryID(laser.MemoryScope{Stream: "agents"}, laser.MemoryFact, body)
	fold.Observe(foldRecord(t, memoryWrite{
		record:    wire.RememberItem(id.String(), string(laser.MemoryFact), body),
		namespace: "support", at: 1,
	}))

	keyword := fold.Recall(laser.MemoryScope{}, laser.MemoryQuery{
		Query: "deploy friday", Strategy: laser.RecallKeyword,
	})
	ranked := fold.Recall(laser.MemoryScope{}, laser.MemoryQuery{Query: "deploy friday"})

	if len(keyword) != 0 {
		t.Errorf("want nothing kept by a keyword recall, got %v", foldTexts(keyword))
	}
	if len(ranked) != 1 || ranked[0].Score != 0 {
		t.Errorf("want the item kept but unscored, got %v", ranked)
	}
}

// Query text holding no word is no query at all: `QueryWords::of` in
// sdk/src/memory.rs returns `Ignore` for empty tokens, whatever the strategy,
// so a keyword recall keeps every item rather than filtering them all out and
// a ranking recall stays recency alone.
func TestGivenAQueryOfPunctuationWhenFoldedThenShouldIgnoreIt(t *testing.T) {
	fold, _ := rememberedIn(t, "agents", "support", "ana", nil, "older", "newer")

	for name, strategy := range map[string]laser.RecallStrategy{
		"ranking": laser.RecallAuto,
		"keyword": laser.RecallKeyword,
	} {
		t.Run(name, func(t *testing.T) {
			items := fold.Recall(laser.MemoryScope{}, laser.MemoryQuery{
				Query: "!!! ...", Strategy: strategy,
			})

			wantTexts(t, items, "newer", "older")
			if items[0].Score != 0 {
				t.Errorf("nothing ranked it, so the score stays zero, got %v", items[0].Score)
			}
		})
	}
}

// Recent recall ignores the query rather than filtering or ranking by it:
// `QueryWords::of` returns `Ignore` for `RecallStrategy::Recent` before it
// ever looks at the text.
func TestGivenRecentRecallWithQueryTextWhenFoldedThenShouldIgnoreTheQuery(t *testing.T) {
	fold, _ := rememberedIn(t, "agents", "support", "ana", nil,
		"deploy friday", "shares nothing with the query")

	items := fold.Recall(laser.MemoryScope{}, laser.MemoryQuery{
		Query: "deploy friday", Strategy: laser.RecallRecent,
	})

	wantTexts(t, items, "shares nothing with the query", "deploy friday")
	for _, item := range items {
		if item.Score != 0 {
			t.Errorf("recent recall ranks nothing, got %v for %q", item.Score, item.Text())
		}
	}
}

func TestGivenNoQueryWhenFoldedThenShouldReturnTheMostRecentFirst(t *testing.T) {
	fold, _ := rememberedIn(t, "agents", "support", "ana", nil, "oldest", "middle", "newest")

	items := fold.Recall(laser.MemoryScope{}, laser.MemoryQuery{})

	wantTexts(t, items, "newest", "middle", "oldest")
}

// Feedback changes what an agent is shown later without anything being
// rewritten, so a promoted item outranks a more recent one.
func TestGivenPromotingFeedbackWhenFoldedThenShouldOutrankTheMoreRecent(t *testing.T) {
	fold, ids := rememberedIn(t, "agents", "support", "ana", nil, "promoted", "newer")

	fold.Observe(foldRecord(t, memoryWrite{
		record: wire.FeedbackForItem(ids[0].String(), 2), namespace: "support", at: 9,
	}))

	wantTexts(t, fold.Recall(laser.MemoryScope{}, laser.MemoryQuery{}), "promoted", "newer")
}

func TestGivenDemotingFeedbackWhenFoldedThenShouldSinkTheItem(t *testing.T) {
	fold, ids := rememberedIn(t, "agents", "support", "ana", nil, "older", "demoted")

	fold.Observe(foldRecord(t, memoryWrite{
		record: wire.FeedbackForItem(ids[1].String(), -2), namespace: "support", at: 9,
	}))

	wantTexts(t, fold.Recall(laser.MemoryScope{}, laser.MemoryQuery{}), "older", "demoted")
}

// Recent recall is recency alone, so feedback must never reorder it.
func TestGivenRecentRecallWhenFeedbackWasFoldedThenShouldIgnoreTheWeight(t *testing.T) {
	fold, ids := rememberedIn(t, "agents", "support", "ana", nil, "promoted", "newer")

	fold.Observe(foldRecord(t, memoryWrite{
		record: wire.FeedbackForItem(ids[0].String(), 2), namespace: "support", at: 9,
	}))

	items := fold.Recall(laser.MemoryScope{}, laser.MemoryQuery{Strategy: laser.RecallRecent})

	wantTexts(t, items, "newer", "promoted")
	if items[0].Score != 0 || items[1].Score != 0 {
		t.Error("recent recall ranks nothing, so no item carries a score")
	}
}

// An agent correcting a fact everywhere and one correcting it in this session
// both count, rather than one replacing the other.
func TestGivenScopedAndUnscopedFeedbackWhenFoldedThenShouldAddUp(t *testing.T) {
	session := conversation(4)
	fold, ids := rememberedIn(t, "agents", "support", "ana", &session, "target", "rival")
	scoped := session.String()

	fold.Observe(foldRecord(t, memoryWrite{
		record:    wire.FeedbackForItem(ids[0].String(), 0.25),
		namespace: "support", conversation: &session, at: 8,
	}))
	fold.Observe(foldRecord(t, memoryWrite{
		record: wire.MemoryRecord{
			Kind: wire.MemoryFeedback, Target: ids[0].String(),
			Weight: 0.25, Conversation: &scoped,
		},
		namespace: "support", conversation: &session, at: 9,
	}))

	items := fold.Recall(laser.MemoryScope{}, laser.MemoryQuery{})

	wantTexts(t, items, "target", "rival")
	if items[0].Score != 0.5 {
		t.Errorf("want both weights counted, got %v", items[0].Score)
	}
}

// Partitions are ordered among themselves only, so a weight folded before the
// item it is about must still apply once that item arrives.
func TestGivenFeedbackBeforeItsItemWhenFoldedThenShouldStillApply(t *testing.T) {
	fold := laser.NewMemoryFold("agents", "support")
	id := laser.ContentMemoryID(laser.MemoryScope{Stream: "agents"}, laser.MemoryFact, []byte("late"))

	fold.Observe(foldRecord(t, memoryWrite{
		record: wire.FeedbackForItem(id.String(), 3), namespace: "support", at: 1,
	}))
	fold.Observe(foldRecord(t, memoryWrite{
		record:    wire.RememberItem(id.String(), string(laser.MemoryFact), []byte("late")),
		namespace: "support", at: 2,
	}))

	items := fold.Recall(laser.MemoryScope{}, laser.MemoryQuery{})

	if len(items) != 1 || items[0].Score != 3 {
		t.Errorf("want the weight applied to the late item, got %v", items)
	}
}

// Ties must break the same way on every client, or a fold silently changes
// what an agent recalls.
func TestGivenEquallyRankedItemsWhenFoldedThenShouldBreakTheTieOnArrival(t *testing.T) {
	fold, _ := rememberedIn(t, "agents", "support", "ana", nil,
		"deploy friday", "deploy friday again", "deploy friday as well")

	items := fold.Recall(laser.MemoryScope{}, laser.MemoryQuery{Query: "deploy friday"})

	wantTexts(t, items, "deploy friday as well", "deploy friday again", "deploy friday")
	if items[0].Score != items[1].Score || items[1].Score != items[2].Score {
		t.Fatal("the three cover the query equally, so none outranks")
	}
}

func TestGivenMoreItemsThanTheLimitWhenFoldedThenShouldTrimThem(t *testing.T) {
	bodies := make([]string, 0, laser.DefaultRecallLimit+5)
	for index := range laser.DefaultRecallLimit + 5 {
		bodies = append(bodies, string(rune('a'+index%26))+itoaForTest(index))
	}
	fold, _ := rememberedIn(t, "agents", "support", "ana", nil, bodies...)

	if got := len(fold.Recall(laser.MemoryScope{}, laser.MemoryQuery{Limit: 7})); got != 7 {
		t.Errorf("want seven items, got %d", got)
	}
	if got := len(fold.Recall(laser.MemoryScope{}, laser.MemoryQuery{})); got != laser.DefaultRecallLimit {
		t.Errorf("want the default limit, got %d", got)
	}
}

func itoaForTest(value int) string {
	if value == 0 {
		return "0"
	}
	var digits []byte
	for value > 0 {
		digits = append([]byte{byte('0' + value%10)}, digits...)
		value /= 10
	}
	return string(digits)
}

// The supersede scenario: a fact corrected in a later conversation must leave
// only the correction for that user in every conversation.
func TestGivenAFactCorrectedInAnotherConversationWhenFoldedThenShouldRecallTheCorrection(t *testing.T) {
	first := conversation(11)
	second := conversation(12)
	fold := laser.NewMemoryFold("agents", "prefs")
	scope := laser.MemoryScope{Stream: "agents", User: "ana"}
	old := laser.ContentMemoryID(scope, laser.MemoryFact, []byte("the user is allergic to peanuts"))
	fresh := laser.ContentMemoryID(scope, laser.MemoryFact, []byte("the user is allergic to shellfish"))

	fold.Observe(foldRecord(t, memoryWrite{
		record: wire.RememberItem(old.String(), string(laser.MemoryFact),
			[]byte("the user is allergic to peanuts")),
		namespace: "prefs", user: "ana", conversation: &first, at: 1,
	}))
	fold.Observe(foldRecord(t, memoryWrite{
		record:    wire.ForgetItem(old.String()),
		namespace: "prefs", user: "ana", conversation: &second, at: 2,
	}))
	fold.Observe(foldRecord(t, memoryWrite{
		record: wire.RememberItem(fresh.String(), string(laser.MemoryFact),
			[]byte("the user is allergic to shellfish")),
		namespace: "prefs", user: "ana", conversation: &second, at: 3,
	}))

	items := fold.Recall(laser.MemoryScope{Stream: "agents", User: "ana"}, laser.MemoryQuery{})

	wantTexts(t, items, "the user is allergic to shellfish")
}
