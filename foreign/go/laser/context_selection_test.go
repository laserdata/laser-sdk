package laser_test

import (
	"context"
	"reflect"
	"slices"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenABuiltInPolicyWhenAskedItsVersionThenShouldBeOne(t *testing.T) {
	for _, policy := range []laser.ContextPolicy{laser.LastN(3), laser.RoleFilter{}, laser.Chain{}, laser.TokenBudget{}} {
		if got := laser.PolicyVersion(policy); got != "1" {
			t.Fatalf("%s: want version 1, got %q", policy.Name(), got)
		}
	}
}

func TestGivenAPolicyThatNamesItsVersionWhenAskedThenShouldReportIt(t *testing.T) {
	if got := laser.PolicyVersion(versionedPolicy{laser.LastN(1)}); got != "7" {
		t.Fatalf("want the policy's own version, got %q", got)
	}
}

func TestGivenAHistoryWhenSelectedThenShouldReportWhatWasKeptAndDropped(t *testing.T) {
	session := conversation(1)
	history := laser.Assemble(session).Add("agent.sessions",
		contextRecord(t, session, 10, 0, "one"),
		contextRecord(t, session, 20, 1, "two"),
		contextRecord(t, session, 30, 2, "three"),
	).Messages()

	selection := laser.SelectionOf(laser.LastN(2), history)

	want := laser.ContextSelection{Kept: history[1:], Dropped: history[:1], Reason: "last_n(2)"}
	if !reflect.DeepEqual(selection, want) {
		t.Fatalf("want kept %v dropped %v by %q, got kept %v dropped %v by %q",
			bodies(want.Kept), bodies(want.Dropped), want.Reason,
			bodies(selection.Kept), bodies(selection.Dropped), selection.Reason)
	}
}

// Two topics can hold a record at the same partition and offset, and they are not the same record.
func TestGivenTwoTopicsAtOnePositionWhenSelectedThenShouldTellThemApart(t *testing.T) {
	session := conversation(1)
	history := laser.Assemble(session).
		Add("agent.sessions", contextRecord(t, session, 10, 0, "session")).
		Add("agent.memory", contextRecord(t, session, 20, 0, "memory")).
		Messages()

	selection := laser.SelectionOf(laser.LastN(1), history)

	if !slices.Equal(bodies(selection.Kept), []string{"memory"}) || !slices.Equal(bodies(selection.Dropped), []string{"session"}) {
		t.Fatalf("kept %v dropped %v", bodies(selection.Kept), bodies(selection.Dropped))
	}
}

func TestGivenAPolicyThatReportsItsOwnSelectionWhenSelectedThenShouldUseIt(t *testing.T) {
	session := conversation(1)
	history := laser.Assemble(session).Add("agent.sessions", contextRecord(t, session, 10, 0, "one")).Messages()

	selection := laser.SelectionOf(versionedPolicy{laser.LastN(0)}, history)

	if selection.Reason != "own" || len(selection.Kept) != 1 {
		t.Fatalf("want the policy's own selection, got %+v", selection)
	}
}

func TestGivenSubconversationsWhenAssembledAcrossThemThenShouldKeepTheChildrenToo(t *testing.T) {
	root, child, grandchild, stranger := conversation(1), conversation(2), conversation(3), conversation(4)
	records := []*laser.Record{
		contextRecord(t, root, 10, 0, "root"),
		subconversationRecord(t, child, &root, &root, 20, 1, "child"),
		subconversationRecord(t, grandchild, &child, &root, 30, 2, "grandchild"),
		subconversationRecord(t, stranger, &stranger, &stranger, 40, 3, "stranger"),
	}

	narrow := laser.Assemble(root).Add("agent.sessions", records...).Messages()
	wide := laser.Assemble(root).AcrossSubconversations().Add("agent.sessions", records...).Messages()

	if !slices.Equal(bodies(narrow), []string{"root"}) {
		t.Fatalf("without the option only the conversation belongs, got %v", bodies(narrow))
	}
	if !slices.Equal(bodies(wide), []string{"root", "child", "grandchild"}) {
		t.Fatalf("want the conversation and what is rooted at it, got %v", bodies(wide))
	}
}

func TestGivenAScopeAcrossSubconversationsWhenFetchedThenShouldReadTheChildrenToo(t *testing.T) {
	server := newScope()
	root, child := conversation(1), conversation(2)
	server.appendRecord(t, root, "planner", "root turn")
	server.appendChild(t, child, root, "worker", "child turn")
	scope := scopeHandle(server).Context(root)
	ctx := context.Background()

	narrow, err := scope.Fetch(ctx, []string{wire.AgentSessions}, 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	wide, err := scope.AcrossSubconversations().Fetch(ctx, []string{wire.AgentSessions}, 10, 0)
	if err != nil {
		t.Fatal(err)
	}

	if !slices.Equal(bodies(narrow), []string{"root turn"}) || !slices.Equal(bodies(wide), []string{"root turn", "child turn"}) {
		t.Fatalf("narrow %v wide %v", bodies(narrow), bodies(wide))
	}
}

func TestGivenAScopeWhenMemoryIsOpenedOnABackendThenShouldCarryTheConversation(t *testing.T) {
	session := conversation(5)
	scope := scopeHandle(newScope()).Context(session)

	memory, err := scope.MemoryWith("notes", laser.MemoryBackendVector)

	if err != nil || memory.Backend() != "vector" {
		t.Fatalf("backend=%v err=%v", memory, err)
	}
	if got := memory.Scope(); got.Conversation == nil || *got.Conversation != session || got.Stream != "agents" {
		t.Fatalf("want the conversation and stream carried, got %+v", got)
	}
	if _, err := scope.MemoryWith("notes", laser.MemoryBackend("nope")); err == nil {
		t.Fatal("an unknown backend must be refused")
	}
}

// versionedPolicy names its own version and reports its own selection.
type versionedPolicy struct{ laser.LastN }

func (versionedPolicy) Version() string { return "7" }

func (policy versionedPolicy) Selection(history []laser.ContextMessage) laser.ContextSelection {
	return laser.ContextSelection{Kept: history, Reason: "own"}
}

// subconversationRecord is a record of a child conversation that names its parent and root.
func subconversationRecord(
	t *testing.T, child wire.ConversationID, parent, root *wire.ConversationID, timestamp, offset uint64, body string,
) *laser.Record {
	t.Helper()
	record := contextRecord(t, child, timestamp, offset, body)
	record.Provenance.ParentConversation, record.Provenance.RootConversation = parent, root
	return record
}
