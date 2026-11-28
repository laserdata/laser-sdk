package laser_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenAManagedMemoryViewWhenRecallingFromAnotherHandleThenShouldWaitForOwnWritesAndEnforceScope(t *testing.T) {
	server := &memoryViewServer{t: t}
	client := laser.FromClient(server, laser.Options{DefaultStream: "agents", Capabilities: kvCapabilities()})
	session := conversation(1)
	user, other, app := "a", "b", "desk"
	scope := laser.MemoryScope{User: user, App: app, Conversation: &session}
	id, err := client.Memory("ns").For(scope).Remember(context.Background(), laser.MemoryFact, []byte("ours"))
	if err != nil {
		t.Fatal(err)
	}
	conversationID := session.String()
	foreign := laser.ContentMemoryID(laser.MemoryScope{User: other}, laser.MemoryFact, []byte("theirs"))
	server.rows = []wire.KvEntry{
		{Key: []byte(id.String()), Value: []byte("ours"), Scope: &wire.MemoryRowScope{User: &user, App: &app, Conversation: &conversationID}},
		{Key: []byte(foreign.String()), Value: []byte("theirs"), Scope: &wire.MemoryRowScope{User: &other, App: &app, Conversation: &conversationID}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	items, err := client.Memory("ns").For(scope).Recall(ctx, laser.MemoryQuery{})
	if err != nil || len(items) != 1 || items[0].Text() != "ours" || len(server.scans) != 2 || len(server.polls) != 0 {
		t.Fatalf("recall=%+v err=%v scans=%d log polls=%d", items, err, len(server.scans), len(server.polls))
	}
	for _, scan := range server.scans {
		if scan.Namespace != "stream:agents/ns" || scan.Stream == nil || *scan.Stream != "agents" ||
			scan.User == nil || *scan.User != user || scan.App == nil || *scan.App != app ||
			scan.Conversation == nil || *scan.Conversation != conversationID {
			t.Fatalf("scope lenses absent from scan: %+v", scan)
		}
	}
}

// A view that has not reached a superseded fact yet shows it as absent, which
// must not be read as the correction having been applied.
func TestGivenAViewBehindASupersededFactWhenRecalledThenShouldWaitForTheForget(t *testing.T) {
	server := &scriptedViewServer{t: t}
	client := laser.FromClient(server, laser.Options{DefaultStream: "agents", Capabilities: kvCapabilities()})
	session := conversation(1)
	memory := client.Memory("ns").For(laser.MemoryScope{Conversation: &session})
	old, err := memory.Remember(context.Background(), laser.MemoryFact, []byte("I live in Lisbon"))
	if err != nil {
		t.Fatal(err)
	}
	corrected, err := memory.Supersede(context.Background(), old, laser.MemoryFact, []byte("I live in Porto"))
	if err != nil {
		t.Fatal(err)
	}
	conversationID := session.String()
	row := func(id laser.MemoryID, body string) wire.KvEntry {
		return wire.KvEntry{Key: []byte(id.String()), Value: []byte(body),
			Scope: &wire.MemoryRowScope{Conversation: &conversationID}}
	}
	stale, current := row(old, "I live in Lisbon"), row(corrected, "I live in Porto")
	// Nothing applied, then both items without the forget, then the forget.
	server.stages = [][]wire.KvEntry{nil, {stale, current}, {current}}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	items, err := memory.Recall(ctx, laser.MemoryQuery{Limit: 5})
	if err != nil {
		t.Fatalf("recall: %v", err)
	}
	if len(items) != 1 || items[0].ID != corrected {
		t.Fatalf("want only the corrected fact after %d scans, got %+v", server.scans, items)
	}
}

// A recall that gives up on its own writes reports a timeout, which a caller
// may retry, as the peer clients do.
func TestGivenAViewThatNeverShowsAWriteWhenTheWaitEndsThenShouldReportATimeout(t *testing.T) {
	server := &scriptedViewServer{t: t, stages: [][]wire.KvEntry{nil}}
	client := laser.FromClient(server, laser.Options{DefaultStream: "agents", Capabilities: kvCapabilities()})
	session := conversation(1)
	memory := client.Memory("ns").For(laser.MemoryScope{Conversation: &session})
	if _, err := memory.Remember(context.Background(), laser.MemoryFact, []byte("ours")); err != nil {
		t.Fatal(err)
	}
	laser.SetOwnWritesWaitForTest(t, 100*time.Millisecond)

	_, err := memory.Recall(context.Background(), laser.MemoryQuery{})
	var timedOut *laser.StageTimeoutError
	if !errors.As(err, &timedOut) || !laser.RetryableError(err) {
		t.Fatalf("want a retryable stage timeout, got %T: %v", err, err)
	}
}

// A forget is read after its own write: a recall waits while the view still
// shows the forgotten item, and returns once it is gone.
func TestGivenAForgottenItemStillInTheViewWhenRecalledThenShouldWaitUntilItIsGone(t *testing.T) {
	server := &scriptedViewServer{t: t}
	client := laser.FromClient(server, laser.Options{DefaultStream: "agents", Capabilities: kvCapabilities()})
	session := conversation(1)
	memory := client.Memory("ns").For(laser.MemoryScope{Conversation: &session})
	id, err := memory.Remember(context.Background(), laser.MemoryFact, []byte("ours"))
	if err != nil {
		t.Fatal(err)
	}
	if err := memory.Forget(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	conversationID := session.String()
	server.stages = [][]wire.KvEntry{
		{{Key: []byte(id.String()), Value: []byte("ours"), Scope: &wire.MemoryRowScope{Conversation: &conversationID}}},
		nil,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	items, err := memory.Recall(ctx, laser.MemoryQuery{})
	if err != nil || len(items) != 0 || server.scans != 2 {
		t.Fatalf("want no item after two scans, got %+v, %v after %d scans", items, err, server.scans)
	}
}

type memoryViewServer struct {
	memoryServer
	t     *testing.T
	scans []wire.KvScan
	rows  []wire.KvEntry
}

func (server *memoryViewServer) SendBinaryRequest(_ context.Context, code uint32, body []byte) ([]byte, error) {
	var scan wire.KvScan
	decodeManagedTestRequest(server.t, code, body, &scan)
	server.scans = append(server.scans, scan)
	page := wire.KvPage{Page: scan.Page, TotalPages: 1}
	if len(server.scans) > 1 {
		page.Items = server.rows
	}
	return wire.Marshal(wire.OkReply(wire.KvOutcome{Kind: wire.KvPageOutcome, Page: &page}))
}

// scriptedViewServer answers each scan with the next scripted set of rows, the
// last one repeating, the way a view catches up on the log.
type scriptedViewServer struct {
	memoryServer
	t      *testing.T
	stages [][]wire.KvEntry
	scans  int
}

func (server *scriptedViewServer) SendBinaryRequest(_ context.Context, code uint32, body []byte) ([]byte, error) {
	var scan wire.KvScan
	decodeManagedTestRequest(server.t, code, body, &scan)
	stage := min(server.scans, len(server.stages)-1)
	server.scans++
	page := wire.KvPage{Page: scan.Page, TotalPages: 1, Items: server.stages[stage]}
	return wire.Marshal(wire.OkReply(wire.KvOutcome{Kind: wire.KvPageOutcome, Page: &page}))
}
