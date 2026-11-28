package laser_test

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenMessagesAcrossSessionsWhenConsolidatedThenShouldKeepStableSummariesAndPruneOnlyTheirScope(t *testing.T) {
	ctx := context.Background()
	client := laser.FromClient(&memoryServer{}, laser.Options{DefaultStream: "agents"})
	base := client.Memory("notes")
	sessions := []wire.ConversationID{conversation(1), conversation(2)}
	write := func(user string, session wire.ConversationID, kind laser.MemoryKind, body string) laser.MemoryID {
		t.Helper()
		id, err := base.For(laser.MemoryScope{User: user, Conversation: &session}).Remember(ctx, kind, []byte(body))
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	older := write("alice", sessions[0], laser.MemoryMessage, "first")
	newer := write("alice", sessions[0], laser.MemoryMessage, "second")
	write("alice", sessions[1], laser.MemoryMessage, "third")
	write("alice", sessions[0], laser.MemoryFact, "keep me")
	write("bob", sessions[0], laser.MemoryMessage, "private")

	owner := base.For(laser.MemoryScope{User: "alice"})
	generator := &joiningSummarizer{}
	report, err := owner.ConsolidateWith(ctx, 10, generator, false)
	if err != nil || report.Summarized != 3 || report.Pruned != 0 || len(generator.calls) != 2 ||
		!slices.Equal(generator.calls[0], []string{"third"}) || !slices.Equal(generator.calls[1], []string{"second", "first"}) {
		t.Fatalf("unexpected first pass: %+v %v %v", report, generator.calls, err)
	}
	report, err = owner.ConsolidateWith(ctx, 10, generator, true)
	if err != nil || report.Summarized != 0 || report.Pruned != 3 || len(generator.calls) != 2 {
		t.Fatalf("repeat pass: %+v %v %v", report, generator.calls, err)
	}

	scope := laser.MemoryScope{Stream: "agents", User: "alice", Conversation: &sessions[0]}
	covered := [][]byte{older.ID[:], newer.ID[:]}
	slices.SortFunc(covered, bytes.Compare)
	want := laser.ContentMemoryID(scope, laser.MemorySummary, bytes.Join(covered, nil))
	items, err := base.For(scope).Recall(ctx, laser.MemoryQuery{Kinds: []laser.MemoryKind{laser.MemorySummary}})
	if err != nil || len(items) != 1 || items[0].ID != want || items[0].Text() != "second first" {
		t.Fatalf("unexpected durable summary: %+v %v", items, err)
	}
	scoped := client.Context(sessions[0]).Memory("notes").For(laser.MemoryScope{User: "alice", Conversation: &sessions[0]})
	report, err = scoped.Consolidate(ctx, 1)
	if err != nil || report.Pruned != 1 {
		t.Fatalf("expected oldest item pruned, got %+v %v", report, err)
	}
	rows, err := base.For(laser.MemoryScope{User: "bob"}).Recall(ctx, laser.MemoryQuery{})
	if err != nil || len(rows) != 1 || rows[0].Text() != "private" {
		t.Fatalf("another user's messages changed: %+v %v", rows, err)
	}
}

func TestGivenAFailedSummaryWhenPruningThenShouldLeaveTheSourceMessagesIntact(t *testing.T) {
	ctx := context.Background()
	memory := recallMemory(&memoryServer{}, laser.MemoryScope{})
	if _, err := memory.Remember(ctx, laser.MemoryMessage, []byte("work")); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("summary unavailable")
	report, err := laser.NewDefaultConsolidator(memory, 0).WithSummarizer(failingSummarizer{failure}).PruneSummarized().Consolidate(ctx, nil)
	if !errors.Is(err, failure) || report.Pruned != 0 {
		t.Fatalf("unexpected failure handling: %+v %v", report, err)
	}
	if rows := mustRecall(t, memory, laser.MemoryQuery{}); len(rows) != 1 || rows[0].Text() != "work" {
		t.Fatalf("source message was pruned: %+v", rows)
	}
}

type joiningSummarizer struct{ calls [][]string }

func (s *joiningSummarizer) Summarize(_ context.Context, bodies [][]byte) ([]byte, error) {
	var call []string
	for _, body := range bodies {
		call = append(call, string(body))
	}
	s.calls = append(s.calls, call)
	return bytes.Join(bodies, []byte(" ")), nil
}

type failingSummarizer struct{ err error }

func (s failingSummarizer) Summarize(context.Context, [][]byte) ([]byte, error) { return nil, s.err }
