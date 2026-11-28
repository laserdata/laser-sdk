package laser_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func memoryHandle(server *streamServer) *laser.Laser {
	return streamHandle(server, laser.Options{DefaultStream: "agents"})
}

// sentMemory reads back the memory record that reached the server.
func sentMemory(t *testing.T, server *streamServer) wire.MemoryRecord {
	t.Helper()
	if len(server.sentMessages) == 0 {
		t.Fatal("nothing was sent")
	}
	var record wire.MemoryRecord
	if err := wire.Unmarshal(server.sentMessages[0].Payload, &record); err != nil {
		t.Fatalf("decode the memory record: %v", err)
	}
	return record
}

// Two users must never share an item, so the owner is part of the identity
// rather than only a filter applied afterwards.
func TestGivenTwoOwnersWhenRememberingOneFactThenShouldMintDistinctIds(t *testing.T) {
	body := []byte("the sky is blue")
	first := laser.ContentMemoryID(
		laser.MemoryScope{Stream: "agents", User: "ada"}, laser.MemoryFact, body)
	second := laser.ContentMemoryID(
		laser.MemoryScope{Stream: "agents", User: "grace"}, laser.MemoryFact, body)

	if first == second {
		t.Error("two users must not share an item id")
	}
}

// Remembering the same thing twice converges on one item rather than growing
// the store, which is what makes the id content-addressed.
func TestGivenOneFactWhenRememberedTwiceThenShouldMintTheSameId(t *testing.T) {
	scope := laser.MemoryScope{Stream: "agents", User: "ada"}
	body := []byte("the sky is blue")

	first := laser.ContentMemoryID(scope, laser.MemoryFact, body)
	second := laser.ContentMemoryID(scope, laser.MemoryFact, body)

	if first != second {
		t.Error("the same fact under the same owner is one item")
	}
}

// The kind is mixed into the id, so the same body under two kinds is two
// items rather than one that overwrites the other.
func TestGivenOneBodyUnderTwoKindsWhenRememberedThenShouldMintDistinctIds(t *testing.T) {
	scope := laser.MemoryScope{Stream: "agents", User: "ada"}
	body := []byte("deploy on friday")

	fact := laser.ContentMemoryID(scope, laser.MemoryFact, body)
	procedure := laser.ContentMemoryID(scope, laser.MemoryProcedure, body)

	if fact == procedure {
		t.Error("a fact and a procedure with one body are two items")
	}
}

// Every identity field separates owners, or one of them is a filter that does
// not actually isolate.
func TestGivenEachOwnerFieldWhenChangedThenShouldMintADistinctId(t *testing.T) {
	agent := agentID(t, "planner")
	other := agentID(t, "executor")
	session := conversation(1)
	another := conversation(2)
	body := []byte("fact")
	base := laser.MemoryScope{Stream: "agents", User: "ada", App: "support",
		Agent: &agent, Conversation: &session}

	for name, changed := range map[string]laser.MemoryScope{
		"stream":       {Stream: "other", User: "ada", App: "support", Agent: &agent, Conversation: &session},
		"user":         {Stream: "agents", User: "grace", App: "support", Agent: &agent, Conversation: &session},
		"app":          {Stream: "agents", User: "ada", App: "billing", Agent: &agent, Conversation: &session},
		"agent":        {Stream: "agents", User: "ada", App: "support", Agent: &other, Conversation: &session},
		"conversation": {Stream: "agents", User: "ada", App: "support", Agent: &agent, Conversation: &another},
	} {
		t.Run(name, func(t *testing.T) {
			if laser.ContentMemoryID(base, laser.MemoryFact, body) ==
				laser.ContentMemoryID(changed, laser.MemoryFact, body) {
				t.Errorf("changing the %s must change the identity", name)
			}
		})
	}
}

// A kind a newer build writes must degrade rather than fail a whole recall.
func TestGivenAnUnknownKindWhenReadThenShouldDegradeToAFact(t *testing.T) {
	if laser.MemoryKindFromWord("telepathy") != laser.MemoryFact {
		t.Error("an unrecognised kind must read as a fact")
	}
	if laser.MemoryKindFromWord("procedure") != laser.MemoryProcedure {
		t.Error("a known kind must read as itself")
	}
}

// The taxonomy is what a reader groups on, so each kind must land in its own
// class.
func TestGivenEachKindWhenClassifiedThenShouldLandInItsTaxonomy(t *testing.T) {
	for kind, class := range map[laser.MemoryKind]laser.MemoryClass{
		laser.MemoryMessage:      laser.ClassEpisodic,
		laser.MemoryProcedure:    laser.ClassProcedural,
		laser.MemoryFact:         laser.ClassSemantic,
		laser.MemorySummary:      laser.ClassSemantic,
		laser.MemoryEntity:       laser.ClassSemantic,
		laser.MemoryFeedbackKind: laser.ClassSemantic,
	} {
		if kind.Class() != class {
			t.Errorf("%s belongs to %s, got %s", kind, class, kind.Class())
		}
	}
}

func TestGivenARememberedItemWhenWrittenThenShouldRideTheMemoryTopic(t *testing.T) {
	server := &streamServer{}
	session := conversation(3)
	memory := memoryHandle(server).Memory("support").
		For(laser.MemoryScope{User: "ada", Conversation: &session})

	id, err := memory.Remember(context.Background(), laser.MemoryFact, []byte("the sky is blue"))
	if err != nil {
		t.Fatalf("remember: %v", err)
	}

	record := sentMemory(t, server)
	if record.Kind != wire.MemoryItem {
		t.Errorf("want an item record, got %d", record.Kind)
	}
	if record.ID != id.String() {
		t.Errorf("want the minted id written, got %q", record.ID)
	}
	if record.ItemKind != string(laser.MemoryFact) {
		t.Errorf("want the kind written, got %q", record.ItemKind)
	}
	if string(record.Body) != "the sky is blue" {
		t.Errorf("want the body written, got %q", record.Body)
	}
	// A session's memory stays ordered, so it is partitioned by conversation.
	if string(server.sentPartitions.Value) != session.String() {
		t.Errorf("want the conversation as the key, got %q", server.sentPartitions.Value)
	}
	headers := sentHeaders(t, server)
	if headers[wire.HeaderMemoryNamespace] != "stream:agents/support" {
		t.Errorf("want the namespace stamped, got %q", headers[wire.HeaderMemoryNamespace])
	}
}

// The provenance a scope carries is written with the item, so a reader can
// trace where a memory came from.
func TestGivenAScopeWithProvenanceWhenRememberingThenShouldWriteIt(t *testing.T) {
	server := &streamServer{}
	producer := wire.ProducerInfo{Name: "extractor", Version: "1.0.0"}
	memory := memoryHandle(server).Memory("support").
		For(laser.MemoryScope{User: "ada", Producer: &producer})

	if _, err := memory.Remember(
		context.Background(), laser.MemoryFact, []byte("fact")); err != nil {
		t.Fatalf("remember: %v", err)
	}

	record := sentMemory(t, server)
	if record.Producer == nil || record.Producer.Name != "extractor" {
		t.Errorf("want the producer written, got %+v", record.Producer)
	}
}

// Forgetting changes what is recalled rather than erasing what happened, so it
// is a record of its own.
func TestGivenAForgottenItemWhenWrittenThenShouldBeATombstone(t *testing.T) {
	server := &streamServer{}
	session := conversation(3)
	memory := memoryHandle(server).Memory("support").
		For(laser.MemoryScope{User: "ada", Conversation: &session})
	target := laser.ContentMemoryID(memory.Scope(), laser.MemoryFact, []byte("x"))

	if err := memory.Forget(context.Background(), target); err != nil {
		t.Fatalf("forget: %v", err)
	}

	record := sentMemory(t, server)
	if record.Kind != wire.MemoryForget {
		t.Errorf("want a tombstone, got %d", record.Kind)
	}
	if record.Target != target.String() {
		t.Errorf("want the target named, got %q", record.Target)
	}
	if record.Conversation == nil || *record.Conversation != session.String() {
		t.Errorf("want the tombstone scoped to its conversation, got %v", record.Conversation)
	}
}

// Feedback changes what an agent is shown later without anything being
// rewritten.
func TestGivenFeedbackWhenWrittenThenShouldCarryItsWeight(t *testing.T) {
	server := &streamServer{}
	memory := memoryHandle(server).Memory("support").
		For(laser.MemoryScope{User: "ada"})
	target := laser.ContentMemoryID(memory.Scope(), laser.MemoryFact, []byte("x"))

	if err := memory.Improve(context.Background(), laser.Feedback{
		Target: target, Weight: -0.5,
	}); err != nil {
		t.Fatalf("improve: %v", err)
	}

	record := sentMemory(t, server)
	if record.Kind != wire.MemoryFeedback {
		t.Errorf("want a feedback record, got %d", record.Kind)
	}
	if record.Weight != -0.5 {
		t.Errorf("want the demotion carried, got %v", record.Weight)
	}
}

func TestGivenNoStreamWhenRememberingThenShouldRefuseIt(t *testing.T) {
	server := &streamServer{}
	memory := streamHandle(server, laser.Options{}).Memory("support")

	_, err := memory.Remember(context.Background(), laser.MemoryFact, []byte("x"))

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want an InvalidError, got %T: %v", err, err)
	}
}

// The stream is the isolation boundary rather than a filter, so narrowing a
// scope must not drop it.
func TestGivenANarrowedScopeWhenSetThenShouldKeepTheStream(t *testing.T) {
	server := &streamServer{}
	memory := memoryHandle(server).Memory("support").
		For(laser.MemoryScope{User: "ada"})

	if memory.Scope().Stream != "agents" {
		t.Errorf("want the stream kept, got %q", memory.Scope().Stream)
	}
}

// Keyword recall keeps only what shares a word: an item matching nothing is
// not a weak answer but no answer.
func TestGivenAKeywordRecallWhenRankedThenShouldDropWhatSharesNothing(t *testing.T) {
	items := []laser.MemoryItem{
		{Body: []byte("the deploy runbook for friday"), Kind: laser.MemoryProcedure},
		{Body: []byte("unrelated trivia about penguins"), Kind: laser.MemoryFact},
		{Body: []byte("deploy checklist"), Kind: laser.MemoryFact},
	}

	ranked := laser.Rank(items, laser.MemoryQuery{Query: "deploy friday"})

	if len(ranked) != 2 {
		t.Fatalf("want only the matching items, got %d", len(ranked))
	}
	// The item covering both query words outranks the one covering half.
	if !strings.Contains(ranked[0].Text(), "friday") {
		t.Errorf("want the fuller match first, got %q", ranked[0].Text())
	}
	if ranked[0].Score <= ranked[1].Score {
		t.Error("want the ranks ordered")
	}
}

// The score counts query words found rather than item words matched, so a long
// item does not outrank a precise one simply by holding more words.
func TestGivenALongItemWhenRankedThenShouldNotOutrankAPreciseOne(t *testing.T) {
	precise := laser.MemoryItem{Body: []byte("deploy friday")}
	rambling := laser.MemoryItem{Body: []byte(
		"deploy friday and also many other unrelated words padding this out")}

	ranked := laser.Rank([]laser.MemoryItem{rambling, precise},
		laser.MemoryQuery{Query: "deploy friday"})

	if len(ranked) != 2 {
		t.Fatalf("want both, got %d", len(ranked))
	}
	if ranked[0].Score != ranked[1].Score {
		t.Errorf("both cover the query fully, so neither outranks: %v vs %v",
			ranked[0].Score, ranked[1].Score)
	}
}

// The score is the fraction of the query an item covers, so a caller can
// threshold on it rather than only compare two items.
func TestGivenAPartialMatchWhenRankedThenShouldScoreTheFractionCovered(t *testing.T) {
	full := laser.MemoryItem{Body: []byte("deploy on friday")}
	half := laser.MemoryItem{Body: []byte("deploy on monday")}

	ranked := laser.Rank([]laser.MemoryItem{half, full},
		laser.MemoryQuery{Query: "deploy friday"})

	if len(ranked) != 2 {
		t.Fatalf("want both, got %d", len(ranked))
	}
	if ranked[0].Score != 1 {
		t.Errorf("an item covering the whole query scores 1, got %v", ranked[0].Score)
	}
	if ranked[1].Score != 0.5 {
		t.Errorf("an item covering half the query scores 0.5, got %v", ranked[1].Score)
	}
}

// The code separates one kind from another inside an id, so two kinds sharing
// one would make the same body under both a single item.
func TestGivenEveryKindWhenCodedThenShouldBeDistinct(t *testing.T) {
	kinds := []laser.MemoryKind{
		laser.MemoryFact, laser.MemoryMessage, laser.MemorySummary,
		laser.MemoryEntity, laser.MemoryFeedbackKind, laser.MemoryProcedure,
	}
	seen := map[byte]laser.MemoryKind{}
	scope := laser.MemoryScope{Stream: "agents", User: "ada"}
	ids := map[laser.MemoryID]laser.MemoryKind{}

	for _, kind := range kinds {
		if other, clash := seen[kind.Code()]; clash {
			t.Errorf("%s and %s share the code %d", kind, other, kind.Code())
		}
		seen[kind.Code()] = kind

		id := laser.ContentMemoryID(scope, kind, []byte("one body"))
		if other, clash := ids[id]; clash {
			t.Errorf("%s and %s mint one id for one body", kind, other)
		}
		ids[id] = kind
	}
}

// Without a query there is nothing to rank against, so the most recent is what
// a reader wants.
func TestGivenNoQueryWhenRankedThenShouldReturnTheMostRecent(t *testing.T) {
	now := time.Now()
	items := []laser.MemoryItem{
		{Body: []byte("older"), At: now.Add(-time.Hour)},
		{Body: []byte("newest"), At: now},
		{Body: []byte("middle"), At: now.Add(-time.Minute)},
	}

	ranked := laser.Rank(items, laser.MemoryQuery{})

	if len(ranked) != 3 || ranked[0].Text() != "newest" || ranked[2].Text() != "older" {
		t.Errorf("want newest first, got %v", texts(ranked))
	}
}

// Asking for recent explicitly ignores the query rather than filtering by it.
func TestGivenRecentStrategyWhenRankedThenShouldIgnoreTheQuery(t *testing.T) {
	now := time.Now()
	items := []laser.MemoryItem{
		{Body: []byte("shares nothing"), At: now},
		{Body: []byte("deploy friday"), At: now.Add(-time.Hour)},
	}

	ranked := laser.Rank(items, laser.MemoryQuery{
		Query: "deploy friday", Strategy: laser.RecallRecent,
	})

	if len(ranked) != 2 {
		t.Fatalf("recent recall filters nothing, got %d", len(ranked))
	}
	if ranked[0].Text() != "shares nothing" {
		t.Errorf("want the most recent first, got %q", ranked[0].Text())
	}
}

func TestGivenAKindFilterWhenRankedThenShouldKeepOnlyThoseKinds(t *testing.T) {
	items := []laser.MemoryItem{
		{Body: []byte("a"), Kind: laser.MemoryFact},
		{Body: []byte("b"), Kind: laser.MemoryProcedure},
		{Body: []byte("c"), Kind: laser.MemoryMessage},
	}

	ranked := laser.Rank(items, laser.MemoryQuery{
		Kinds: []laser.MemoryKind{laser.MemoryProcedure, laser.MemoryMessage},
	})

	if len(ranked) != 2 {
		t.Fatalf("want only the named kinds, got %d", len(ranked))
	}
	for _, item := range ranked {
		if item.Kind == laser.MemoryFact {
			t.Error("a filtered-out kind must not survive")
		}
	}
}

func TestGivenMoreItemsThanTheLimitWhenRankedThenShouldTrimThem(t *testing.T) {
	var items []laser.MemoryItem
	for index := range 100 {
		items = append(items, laser.MemoryItem{
			Body: []byte("item"), At: time.Now().Add(-time.Duration(index) * time.Minute),
		})
	}

	if got := len(laser.Rank(items, laser.MemoryQuery{Limit: 7})); got != 7 {
		t.Errorf("want seven items, got %d", got)
	}
	if got := len(laser.Rank(items, laser.MemoryQuery{})); got != laser.DefaultRecallLimit {
		t.Errorf("want the default limit, got %d", got)
	}
}

// A block ordered by rank is what a model reads best, so filling stops at the
// first item that does not fit rather than skipping to a smaller one.
func TestGivenATokenBudgetWhenRenderedThenShouldFillInRankOrder(t *testing.T) {
	items := []laser.MemoryItem{
		{Body: []byte(strings.Repeat("a", 40))},
		{Body: []byte(strings.Repeat("b", 400))},
		{Body: []byte("tiny")},
	}

	block := laser.ToContextBlock(items, 20)

	if !strings.Contains(block, "aaa") {
		t.Error("want the first item included")
	}
	if strings.Contains(block, "bbb") {
		t.Error("an item past the budget must not be included")
	}
	if strings.Contains(block, "tiny") {
		t.Error("filling stops at the first item that does not fit, " +
			"rather than skipping ahead to a smaller one")
	}
}

func TestGivenNoBudgetWhenRenderedThenShouldIncludeEverything(t *testing.T) {
	items := []laser.MemoryItem{
		{Body: []byte("first")}, {Body: []byte("second")},
	}

	block := laser.ToContextBlock(items, 0)

	if !strings.Contains(block, "first") || !strings.Contains(block, "second") {
		t.Errorf("want both items, got %q", block)
	}
	if strings.Count(block, "\n") != 1 {
		t.Errorf("want one item per line, got %q", block)
	}
}

func texts(items []laser.MemoryItem) []string {
	var out []string
	for _, item := range items {
		out = append(out, item.Text())
	}
	return out
}
