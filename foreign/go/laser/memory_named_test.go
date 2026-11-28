package laser_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenANamedItemWhenSetThenShouldPublishAKeyedFactOnTheMemoryTopic(t *testing.T) {
	server := &memoryServer{}
	memory := laser.FromClient(server, laser.Options{DefaultStream: "agents"}).Memory("notes")

	if err := memory.Set(context.Background(), "plan", []byte(`{"step":1}`)); err != nil {
		t.Fatal(err)
	}

	if len(server.appended) != 1 {
		t.Fatalf("want one record, got %d", len(server.appended))
	}
	var record wire.MemoryRecord
	if err := wire.Unmarshal(server.appended[0].Payload, &record); err != nil {
		t.Fatal(err)
	}
	want := wire.RememberItem("notes/plan", "fact", []byte(`{"step":1}`))
	if record.Kind != want.Kind || record.ID != want.ID || record.ItemKind != want.ItemKind ||
		!bytes.Equal(record.Body, want.Body) || record.Origin != nil || record.Producer != nil {
		t.Fatalf("want %+v, got %+v", want, record)
	}
	headers := headerStrings(t, server.appended[0])
	if headers[wire.HeaderMemoryNamespace] != "stream:agents/notes" {
		t.Fatalf("want the scoped namespace header, got %q", headers[wire.HeaderMemoryNamespace])
	}
	if _, stamped := headers[wire.HeaderMemoryUser]; stamped {
		t.Fatal("a named item carries no owner")
	}
}

// Every write of one key shares a partition key, so a set, an update and a
// remove of that key fold in offset order.
func TestGivenOneKeyWhenWrittenTwiceThenShouldShareOneConversation(t *testing.T) {
	server := &memoryServer{}
	memory := laser.FromClient(server, laser.Options{DefaultStream: "agents"}).Memory("notes")
	ctx := context.Background()

	if err := memory.Set(ctx, "plan", []byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := memory.Remove(ctx, "plan"); err != nil {
		t.Fatal(err)
	}

	want := laser.DeriveConversationID("notes/plan")
	for index, message := range server.appended {
		record, err := laser.OpenRecord(message)
		if err != nil {
			t.Fatal(err)
		}
		if record.Provenance == nil || record.Provenance.Conversation != want {
			t.Fatalf("record %d rides another conversation: %+v", index, record.Provenance)
		}
	}
}

func TestGivenANamedItemWhenFetchedFoldedThenShouldReadWhatWasSet(t *testing.T) {
	server := &memoryServer{}
	client := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	ctx := context.Background()
	if err := client.Memory("notes").Set(ctx, "plan", []byte("ship it")); err != nil {
		t.Fatal(err)
	}

	// A fresh handle holds no fold, so the value has to come off the topic.
	value, found, err := client.Memory("notes").FetchFolded(ctx, "plan")

	if err != nil || !found || string(value) != "ship it" {
		t.Fatalf("value=%q found=%v err=%v", value, found, err)
	}
}

func TestGivenTwoNamespacesWhenOneKeyIsSetThenShouldKeepThemApart(t *testing.T) {
	server := &memoryServer{}
	client := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	ctx := context.Background()
	if err := client.Memory("notes").Set(ctx, "plan", []byte("ours")); err != nil {
		t.Fatal(err)
	}

	_, found, err := client.Memory("drafts").FetchFolded(ctx, "plan")

	if err != nil || found {
		t.Fatalf("another namespace read the key: found=%v err=%v", found, err)
	}
}

func TestGivenARemovedNamedItemWhenFetchedThenShouldBeAbsent(t *testing.T) {
	server := &memoryServer{}
	client := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	ctx := context.Background()
	memory := client.Memory("notes")
	if err := memory.Set(ctx, "plan", []byte("ship it")); err != nil {
		t.Fatal(err)
	}

	if err := memory.Remove(ctx, "plan"); err != nil {
		t.Fatal(err)
	}

	for name, handle := range map[string]*laser.Memory{"same": memory, "fresh": client.Memory("notes")} {
		if _, found, err := handle.FetchFolded(ctx, "plan"); err != nil || found {
			t.Fatalf("%s handle: found=%v err=%v", name, found, err)
		}
	}
	var tombstone wire.MemoryRecord
	if err := wire.Unmarshal(server.appended[1].Payload, &tombstone); err != nil {
		t.Fatal(err)
	}
	if tombstone.Kind != wire.MemoryForget || tombstone.Target != "notes/plan" || tombstone.Conversation != nil {
		t.Fatalf("want an unscoped keyed tombstone, got %+v", tombstone)
	}
}

func TestGivenAnAbsentKeyWhenRemovedThenShouldSucceed(t *testing.T) {
	server := &memoryServer{}
	memory := laser.FromClient(server, laser.Options{DefaultStream: "agents"}).Memory("notes")

	if err := memory.Remove(context.Background(), "never-set"); err != nil {
		t.Fatal(err)
	}
}

func TestGivenANamedItemWhenUpdatedThenShouldMergeThePatch(t *testing.T) {
	server := &memoryServer{}
	client := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	ctx := context.Background()
	memory := client.Memory("notes")
	if err := memory.Set(ctx, "user", []byte(`{"tier":"free","city":"Lisbon","tags":{"a":1,"b":2}}`)); err != nil {
		t.Fatal(err)
	}

	if err := memory.Update(ctx, "user", []byte(`{"tier":"pro","city":null,"tags":{"b":null,"c":3}}`)); err != nil {
		t.Fatal(err)
	}

	value, found, err := client.Memory("notes").FetchFolded(ctx, "user")
	if err != nil || !found || string(value) != `{"tags":{"a":1,"c":3},"tier":"pro"}` {
		t.Fatalf("value=%s found=%v err=%v", value, found, err)
	}
}

func TestGivenAnAbsentKeyWhenUpdatedThenShouldMergeOntoNothing(t *testing.T) {
	server := &memoryServer{}
	memory := laser.FromClient(server, laser.Options{DefaultStream: "agents"}).Memory("notes")
	ctx := context.Background()

	if err := memory.Update(ctx, "user", []byte(`{"tier":"pro","gone":null}`)); err != nil {
		t.Fatal(err)
	}

	value, found, err := memory.FetchFolded(ctx, "user")
	if err != nil || !found || string(value) != `{"tier":"pro"}` {
		t.Fatalf("value=%s found=%v err=%v", value, found, err)
	}
}

func TestGivenAPatchThatIsNotAnObjectWhenUpdatedThenShouldReplaceTheValue(t *testing.T) {
	server := &memoryServer{}
	memory := laser.FromClient(server, laser.Options{DefaultStream: "agents"}).Memory("notes")
	ctx := context.Background()
	if err := memory.Set(ctx, "count", []byte(`{"n":1}`)); err != nil {
		t.Fatal(err)
	}

	if err := memory.Update(ctx, "count", []byte(`7`)); err != nil {
		t.Fatal(err)
	}

	value, _, err := memory.FetchFolded(ctx, "count")
	if err != nil || string(value) != `7` {
		t.Fatalf("value=%s err=%v", value, err)
	}
}

func TestGivenAnInvalidPatchOrStoredValueWhenUpdatedThenShouldRefuseIt(t *testing.T) {
	server := &memoryServer{}
	memory := laser.FromClient(server, laser.Options{DefaultStream: "agents"}).Memory("notes")
	ctx := context.Background()
	if err := memory.Set(ctx, "text", []byte("not json")); err != nil {
		t.Fatal(err)
	}
	sent := len(server.appended)

	var codec *laser.CodecError
	if err := memory.Update(ctx, "text", []byte(`{"a":1}`)); !errors.As(err, &codec) {
		t.Fatalf("a stored value that is not JSON must be a codec error, got %v", err)
	}
	if err := memory.Update(ctx, "fresh", []byte(`{broken`)); !errors.As(err, &codec) {
		t.Fatalf("a patch that is not JSON must be a codec error, got %v", err)
	}
	if len(server.appended) != sent {
		t.Fatal("a refused update must not write")
	}
}

func TestGivenAManagedViewWhenFetchingANamedItemThenShouldPointReadTheView(t *testing.T) {
	server := &namedViewServer{t: t, entry: &wire.KvEntry{Key: []byte("notes/plan"), Value: []byte("from the view"), Version: 1}}
	memory := laser.FromClient(server, laser.Options{DefaultStream: "agents", Capabilities: kvCapabilities()}).Memory("notes")

	value, found, err := memory.Fetch(context.Background(), "plan")

	if err != nil || !found || string(value) != "from the view" {
		t.Fatalf("value=%q found=%v err=%v", value, found, err)
	}
	if len(server.gets) != 1 || server.gets[0].Namespace != "stream:agents/notes" || string(server.gets[0].Key) != "notes/plan" {
		t.Fatalf("want one read of notes/plan in the scoped namespace, got %+v", server.gets)
	}
	if len(server.polls) != 0 {
		t.Fatal("a managed fetch must not fold the topic")
	}
}

func TestGivenAManagedViewWithoutTheKeyWhenFetchingThenShouldReportAbsence(t *testing.T) {
	server := &namedViewServer{t: t}
	memory := laser.FromClient(server, laser.Options{DefaultStream: "agents", Capabilities: kvCapabilities()}).Memory("notes")

	value, found, err := memory.Fetch(context.Background(), "plan")

	if err != nil || found || value != nil {
		t.Fatalf("value=%q found=%v err=%v", value, found, err)
	}
}

func TestGivenNoManagedViewWhenFetchingANamedItemThenShouldFoldTheTopic(t *testing.T) {
	server := &memoryServer{}
	client := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	ctx := context.Background()
	if err := client.Memory("notes").Set(ctx, "plan", []byte("folded")); err != nil {
		t.Fatal(err)
	}

	value, found, err := client.Memory("notes").Fetch(ctx, "plan")

	if err != nil || !found || string(value) != "folded" {
		t.Fatalf("value=%q found=%v err=%v", value, found, err)
	}
}

// A named item is point state, so it never shows up as a recalled memory.
func TestGivenANamedItemWhenRecalledThenShouldNotBeAnItem(t *testing.T) {
	server := &memoryServer{}
	client := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	ctx := context.Background()
	memory := client.Memory("notes")
	if err := memory.Set(ctx, "plan", []byte("point state")); err != nil {
		t.Fatal(err)
	}
	if _, err := memory.Remember(ctx, laser.MemoryFact, []byte("a fact")); err != nil {
		t.Fatal(err)
	}

	items, err := client.Memory("notes").RecallFolded(ctx, laser.MemoryQuery{})

	if err != nil || len(items) != 1 || items[0].Text() != "a fact" {
		t.Fatalf("items=%v err=%v", texts(items), err)
	}
}

func TestGivenAGovernorWhenANamedItemIsSetThenShouldOfferTheEncodedRecord(t *testing.T) {
	server := &memoryServer{}
	governor := &scriptedGovernor{decision: laser.Allow()}
	client := laser.FromClient(server, laser.Options{DefaultStream: "agents"}).
		WithGovernor(governor, laser.GovernorEnforce)

	if err := client.Memory("notes").Set(context.Background(), "plan", []byte("body")); err != nil {
		t.Fatal(err)
	}

	asked := governor.asked()
	if len(asked) != 1 || asked[0].Kind != laser.ActionMemoryWrite ||
		!bytes.Equal(asked[0].Payload, server.appended[0].Payload) {
		t.Fatalf("the governor must see the encoded record once, got %+v", asked)
	}
}

func TestGivenNoStreamWhenSettingANamedItemThenShouldRefuseIt(t *testing.T) {
	memory := laser.FromClient(&memoryServer{}, laser.Options{}).Memory("notes")

	err := memory.Set(context.Background(), "plan", []byte("x"))

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want the missing stream refused, got %v", err)
	}
}

func TestGivenANamedKeyOverTheMemoryIDCapWhenWrittenThenShouldRefuseItBeforeSending(t *testing.T) {
	server := &memoryServer{}
	memory := laser.FromClient(server, laser.Options{DefaultStream: "agents"}).Memory("notes")
	key := strings.Repeat("k", wire.MaxMemoryIDBytes+1-len("notes/"))
	ctx := context.Background()

	var invalid *laser.InvalidError
	if err := memory.Set(ctx, key, []byte("value")); !errors.As(err, &invalid) {
		t.Fatalf("an over-long named id must be refused, got %v", err)
	}
	if err := memory.Remove(ctx, key); !errors.As(err, &invalid) {
		t.Fatalf("an over-long named target must be refused, got %v", err)
	}
	if len(server.appended) != 0 {
		t.Fatal("a refused key must not reach the topic")
	}
}

// A named item rides the memory topic like every other memory write, keyed
// under its namespace so two namespaces on one topic never collide.

// namedViewServer answers a point read of the managed view and records it.
type namedViewServer struct {
	memoryServer
	t     *testing.T
	gets  []wire.KvGet
	entry *wire.KvEntry
}

func (server *namedViewServer) SendBinaryRequest(_ context.Context, code uint32, body []byte) ([]byte, error) {
	if code != wire.AgdxKvGetCode {
		server.t.Fatalf("want a point read, got code %d", code)
	}
	var get wire.KvGet
	decodeManagedTestRequest(server.t, code, body, &get)
	server.gets = append(server.gets, get)
	return wire.Marshal(wire.OkReply(wire.KvOutcome{Kind: wire.KvValue, Entry: server.entry}))
}

func headerStrings(t *testing.T, message iggcon.IggyMessage) map[string]string {
	t.Helper()
	entries, err := iggcon.DeserializeHeaders(message.UserHeaders)
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]string{}
	for _, entry := range entries {
		if entry.Value.Kind == iggcon.String {
			found[string(entry.Key.Value)] = string(entry.Value.Value)
		}
	}
	return found
}
