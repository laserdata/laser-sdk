package laser_test

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenEachBackendWhenOpenedThenShouldNameWhatItResolvedTo(t *testing.T) {
	client := laser.FromClient(&memoryServer{}, laser.Options{DefaultStream: "agents"})
	custom, err := client.MemoryCustom(&recordingMemory{})
	if err != nil {
		t.Fatal(err)
	}
	want := map[laser.MemoryBackend]string{
		laser.MemoryBackendAuto: "log", laser.MemoryBackendLog: "log", laser.MemoryBackendVector: "vector",
	}
	for backend, name := range want {
		memory, err := client.MemoryWith("notes", backend)
		if err != nil || memory.Backend() != name {
			t.Fatalf("%s: backend=%q err=%v", backend, memory.Backend(), err)
		}
	}
	if client.Memory("notes").Backend() != "log" || custom.Backend() != "custom" {
		t.Fatalf("default=%q custom=%q", client.Memory("notes").Backend(), custom.Backend())
	}
}

func TestGivenAnUnknownBackendWhenOpenedThenShouldRefuseIt(t *testing.T) {
	client := laser.FromClient(&memoryServer{}, laser.Options{DefaultStream: "agents"})

	_, err := client.MemoryWith("notes", laser.MemoryBackend("graph"))

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want the backend refused, got %v", err)
	}
}

func TestGivenAVectorHandleWhenUsedThenShouldKeepItemsInProcessUnderItsScope(t *testing.T) {
	server := &memoryServer{}
	client := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	opened, err := client.MemoryWith("notes", laser.MemoryBackendVector)
	if err != nil {
		t.Fatal(err)
	}
	first, second := conversation(1), conversation(2)
	memory := opened.Embedder(&wordEmbedder{vocabulary: []string{"cats", "sleep", "invoice", "overdue"}})
	ctx := context.Background()
	ours := memory.For(laser.MemoryScope{Conversation: &first})
	theirs := memory.For(laser.MemoryScope{Conversation: &second})
	if _, err := ours.Remember(ctx, laser.MemoryFact, []byte("cats sleep")); err != nil {
		t.Fatal(err)
	}
	if _, err := theirs.Remember(ctx, laser.MemoryFact, []byte("invoice overdue")); err != nil {
		t.Fatal(err)
	}

	items, err := ours.Recall(ctx, laser.MemoryQuery{Query: "cats"})

	if err != nil || !slices.Equal(texts(items), []string{"cats sleep"}) {
		t.Fatalf("items=%v err=%v", texts(items), err)
	}
	if len(server.appended) != 0 {
		t.Fatal("a vector handle must not write to the log")
	}
}

func TestGivenAVectorHandleWithoutAnEmbedderWhenRememberingThenShouldRefuseIt(t *testing.T) {
	client := laser.FromClient(&memoryServer{}, laser.Options{DefaultStream: "agents"})
	memory, err := client.MemoryWith("notes", laser.MemoryBackendVector)
	if err != nil {
		t.Fatal(err)
	}

	_, err = memory.Remember(context.Background(), laser.MemoryFact, []byte("x"))

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want the missing embedder named, got %v", err)
	}
}

func TestGivenAVectorHandleWhenAnItemIsRememberedTwiceThenShouldStoreOneItem(t *testing.T) {
	memory, _ := vectorMemoryWith(t, "cats sleep", "cats sleep")

	items, err := memory.Recall(context.Background(), laser.MemoryScope{}, laser.MemoryQuery{})

	if err != nil || len(items) != 1 {
		t.Fatalf("want one item, got %v err=%v", texts(items), err)
	}
}

func TestGivenAVectorHandleWhenForgettingInAConversationThenShouldKeepTheItemElsewhere(t *testing.T) {
	client := laser.FromClient(&memoryServer{}, laser.Options{DefaultStream: "agents"})
	opened, err := client.MemoryWith("notes", laser.MemoryBackendVector)
	if err != nil {
		t.Fatal(err)
	}
	memory := opened.Embedder(&wordEmbedder{vocabulary: []string{"cats"}})
	first, second := conversation(1), conversation(2)
	ctx := context.Background()
	ours := memory.For(laser.MemoryScope{Conversation: &first})
	id, err := ours.Remember(ctx, laser.MemoryFact, []byte("cats"))
	if err != nil {
		t.Fatal(err)
	}

	if err := memory.For(laser.MemoryScope{Conversation: &second}).Forget(ctx, id); err != nil {
		t.Fatal(err)
	}
	kept, err := ours.Recall(ctx, laser.MemoryQuery{})
	if err != nil || len(kept) != 1 {
		t.Fatalf("a forget scoped elsewhere removed the item: %v err=%v", texts(kept), err)
	}
	if err := ours.Forget(ctx, id); err != nil {
		t.Fatal(err)
	}
	gone, err := ours.Recall(ctx, laser.MemoryQuery{})
	if err != nil || len(gone) != 0 {
		t.Fatalf("want the item gone, got %v err=%v", texts(gone), err)
	}
}

func TestGivenAGovernedVectorHandleWhenTheGovernorBlocksThenShouldNotKeepTheItem(t *testing.T) {
	governor := &scriptedGovernor{decision: laser.Block("no memory")}
	client := laser.FromClient(&memoryServer{}, laser.Options{DefaultStream: "agents"}).
		WithGovernor(governor, laser.GovernorEnforce)
	opened, err := client.MemoryWith("notes", laser.MemoryBackendVector)
	if err != nil {
		t.Fatal(err)
	}
	memory := opened.Embedder(&wordEmbedder{vocabulary: []string{"cats"}})
	ctx := context.Background()

	_, err = memory.Remember(ctx, laser.MemoryFact, []byte("cats"))

	var blocked *laser.PolicyBlockedError
	if !errors.As(err, &blocked) {
		t.Fatalf("want the write blocked, got %v", err)
	}
	asked := governor.asked()
	if len(asked) != 1 || asked[0].Kind != laser.ActionMemoryWrite || string(asked[0].Payload) != "cats" ||
		asked[0].Stream != "agents" || asked[0].Topic != wire.AgentMemory {
		t.Fatalf("want one memory write offered with the body, got %+v", asked)
	}
	if items, err := memory.Recall(ctx, laser.MemoryQuery{}); err != nil || len(items) != 0 {
		t.Fatalf("a blocked item was kept: %v err=%v", texts(items), err)
	}
}

func TestGivenAGovernorThatRetargetsAForgetWhenAVectorItemIsForgottenThenShouldDropTheItemItNamed(t *testing.T) {
	memory, governor := governedVectorMemory(t)
	ctx := context.Background()
	asked, err := memory.Remember(ctx, laser.MemoryFact, []byte("cats one"))
	if err != nil {
		t.Fatal(err)
	}
	named, err := memory.Remember(ctx, laser.MemoryFact, []byte("cats two"))
	if err != nil {
		t.Fatal(err)
	}
	governor.rewrite = func(record wire.MemoryRecord) *wire.MemoryRecord {
		rewritten := wire.ForgetItem(named.String())
		return &rewritten
	}

	if err := memory.Forget(ctx, asked); err != nil {
		t.Fatal(err)
	}

	kept, err := memory.Recall(ctx, laser.MemoryQuery{})
	if err != nil || len(kept) != 1 || kept[0].ID != asked {
		t.Fatalf("want only the item the governor named dropped, kept %v err=%v", texts(kept), err)
	}
}

func TestGivenAGovernorThatRetargetsFeedbackWhenAVectorItemIsImprovedThenShouldWeighTheItemItNamed(t *testing.T) {
	memory, governor := governedVectorMemory(t)
	ctx := context.Background()
	asked, err := memory.Remember(ctx, laser.MemoryFact, []byte("cats one"))
	if err != nil {
		t.Fatal(err)
	}
	named, err := memory.Remember(ctx, laser.MemoryFact, []byte("cats two"))
	if err != nil {
		t.Fatal(err)
	}
	governor.rewrite = func(record wire.MemoryRecord) *wire.MemoryRecord {
		rewritten := wire.FeedbackForItem(named.String(), record.Weight)
		return &rewritten
	}

	if err := memory.Improve(ctx, laser.Feedback{Target: asked, Weight: 1}); err != nil {
		t.Fatal(err)
	}

	ranked, err := memory.Recall(ctx, laser.MemoryQuery{Query: "cats"})
	if err != nil || len(ranked) != 2 || ranked[0].ID != named {
		t.Fatalf("want the item the governor named ranked first, got %v err=%v", texts(ranked), err)
	}
}

func TestGivenAGovernorThatTurnsAForgetIntoFeedbackWhenAVectorItemIsForgottenThenShouldRefuseIt(t *testing.T) {
	memory, governor := governedVectorMemory(t)
	ctx := context.Background()
	id, err := memory.Remember(ctx, laser.MemoryFact, []byte("cats one"))
	if err != nil {
		t.Fatal(err)
	}
	governor.rewrite = func(record wire.MemoryRecord) *wire.MemoryRecord {
		rewritten := wire.FeedbackForItem(record.Target, 1)
		return &rewritten
	}

	err = memory.Forget(ctx, id)

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want a modification of another kind refused, got %v", err)
	}
	if kept, err := memory.Recall(ctx, laser.MemoryQuery{}); err != nil || len(kept) != 1 {
		t.Fatalf("a refused forget dropped the item: %v err=%v", texts(kept), err)
	}
}

func TestGivenAStandaloneVectorMemoryWhenWritingThenShouldNotAskAGovernor(t *testing.T) {
	memory, _ := vectorMemoryWith(t, "cats sleep")

	items, err := memory.Recall(context.Background(), laser.MemoryScope{}, laser.MemoryQuery{})

	if err != nil || len(items) != 1 {
		t.Fatalf("items=%v err=%v", texts(items), err)
	}
}

func TestGivenACustomBackendWhenUsedThroughTheHandleThenShouldDelegateEveryVerb(t *testing.T) {
	server := &memoryServer{}
	backend := &recordingMemory{items: []laser.MemoryItem{{Body: []byte("theirs")}}}
	client := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	opened, err := client.MemoryCustom(backend)
	if err != nil {
		t.Fatal(err)
	}
	session := conversation(3)
	memory := opened.For(laser.MemoryScope{User: "u-1", Conversation: &session})
	ctx := context.Background()

	id, err := memory.Remember(ctx, laser.MemoryFact, []byte("a fact"))
	if err != nil || id != (laser.MemoryID{ID: wire.IDFromBytes([16]byte{15: 7})}) {
		t.Fatalf("the backend mints the id, got %v err=%v", id, err)
	}
	items, err := memory.Recall(ctx, laser.MemoryQuery{Query: "q"})
	if err != nil || !slices.Equal(texts(items), []string{"theirs"}) {
		t.Fatalf("items=%v err=%v", texts(items), err)
	}
	folded, err := memory.RecallFolded(ctx, laser.MemoryQuery{Query: "f"})
	if err != nil || len(folded) != 1 {
		t.Fatalf("a folded recall delegates too, got %v err=%v", texts(folded), err)
	}
	if err := memory.Improve(ctx, laser.Feedback{Target: id, Weight: 1}); err != nil {
		t.Fatal(err)
	}
	if err := memory.Forget(ctx, id); err != nil {
		t.Fatal(err)
	}

	want := []string{"remember:a fact", "recall:q", "recall:f", "improve", "forget"}
	if !slices.Equal(backend.calls, want) {
		t.Fatalf("want %v, got %v", want, backend.calls)
	}
	for index, scope := range backend.scopes {
		if scope.User != "u-1" || scope.Conversation == nil || *scope.Conversation != session {
			t.Fatalf("call %d lost the scope: %+v", index, scope)
		}
	}
	if len(server.appended) != 0 {
		t.Fatal("a custom backend must not write to the log")
	}
}

func TestGivenAHandleWithoutAKeySpaceWhenANamedVerbIsCalledThenShouldRefuseIt(t *testing.T) {
	client := laser.FromClient(&memoryServer{}, laser.Options{DefaultStream: "agents"})
	vector, err := client.MemoryWith("notes", laser.MemoryBackendVector)
	if err != nil {
		t.Fatal(err)
	}
	custom, err := client.MemoryCustom(&recordingMemory{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	for name, memory := range map[string]*laser.Memory{"vector": vector, "custom": custom} {
		var unsupported *laser.UnsupportedError
		if err := memory.Set(ctx, "k", []byte("v")); !errors.As(err, &unsupported) {
			t.Fatalf("%s set: %v", name, err)
		}
		if _, _, err := memory.Fetch(ctx, "k"); !errors.As(err, &unsupported) {
			t.Fatalf("%s fetch: %v", name, err)
		}
		if _, _, err := memory.FetchFolded(ctx, "k"); !errors.As(err, &unsupported) {
			t.Fatalf("%s fetch folded: %v", name, err)
		}
		if err := memory.Update(ctx, "k", []byte("{}")); !errors.As(err, &unsupported) {
			t.Fatalf("%s update: %v", name, err)
		}
		if err := memory.Remove(ctx, "k"); !errors.As(err, &unsupported) {
			t.Fatalf("%s remove: %v", name, err)
		}
		want := "unsupported: memory: Remove(key) addresses a named item and needs the durable memory handle, " +
			"this handle resolved to " + name
		if unsupported.Surface != "memory" || unsupported.Error() != want {
			t.Fatalf("%s: got surface %q and %q, want %q", name, unsupported.Surface, unsupported.Error(), want)
		}
	}
}

func TestGivenARerankedHandleWhenRecallingThenShouldApplyTheRerankerOnlyToAQuery(t *testing.T) {
	server := &memoryServer{}
	reranker := &recordingReranker{}
	memory := laser.FromClient(server, laser.Options{DefaultStream: "agents"}).Memory("notes").Reranker(reranker)
	ctx := context.Background()
	for _, body := range []string{"first fact", "second fact"} {
		if _, err := memory.Remember(ctx, laser.MemoryFact, []byte(body)); err != nil {
			t.Fatal(err)
		}
	}

	plain, err := memory.Recall(ctx, laser.MemoryQuery{})
	if err != nil {
		t.Fatal(err)
	}
	ranked, err := memory.Recall(ctx, laser.MemoryQuery{Query: "fact"})
	if err != nil {
		t.Fatal(err)
	}
	folded, err := memory.RecallFolded(ctx, laser.MemoryQuery{Query: "fact"})
	if err != nil {
		t.Fatal(err)
	}

	if !slices.Equal(texts(plain), []string{"second fact", "first fact"}) {
		t.Fatalf("a recall without a query keeps its order, got %v", texts(plain))
	}
	if !slices.Equal(texts(ranked), []string{"first fact", "second fact"}) ||
		!slices.Equal(texts(folded), []string{"first fact", "second fact"}) {
		t.Fatalf("want both recalls reranked, got %v and %v", texts(ranked), texts(folded))
	}
	if !slices.Equal(reranker.queries, []string{"fact", "fact"}) {
		t.Fatalf("want the reranker asked twice with the query, got %v", reranker.queries)
	}
}

func TestGivenALogHandleWhenAnEmbedderIsRegisteredThenShouldStayTheSameHandle(t *testing.T) {
	memory := laser.FromClient(&memoryServer{}, laser.Options{DefaultStream: "agents"}).Memory("notes")

	configured := memory.Embedder(&wordEmbedder{})

	if configured != memory || configured.Backend() != "log" {
		t.Fatalf("an embedder on a log handle is unused, got backend %q", configured.Backend())
	}
}

func TestGivenASearchWhenLimitedThenShouldCapTheItemsAndKeepOnlyTheSharers(t *testing.T) {
	server := &memoryServer{}
	memory := laser.FromClient(server, laser.Options{DefaultStream: "agents"}).Memory("notes")
	ctx := context.Background()
	for _, body := range []string{"node one is slow", "node two is slow", "node three is slow", "an unrelated invoice"} {
		if _, err := memory.Remember(ctx, laser.MemoryFact, []byte(body)); err != nil {
			t.Fatal(err)
		}
	}

	capped, err := memory.Search(ctx, "slow node", 2)
	if err != nil {
		t.Fatal(err)
	}
	every, err := memory.Search(ctx, "slow node", 0)
	if err != nil {
		t.Fatal(err)
	}

	if len(capped) != 2 || len(every) != 3 {
		t.Fatalf("want 2 capped and 3 sharing a word, got %d and %d", len(capped), len(every))
	}
}

func TestGivenAMemoryTopicBuilderWhenBuiltThenShouldCreateTheTopicWithItsPartitionsAndExpiry(t *testing.T) {
	server := &topicServer{}
	client := laser.FromClient(server, laser.Options{DefaultStream: "agents"})

	memory, err := client.MemoryTopic("team.memory").Stream("fleet").Partitions(4).TTL(2 * time.Hour).Build(context.Background())

	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(server.created, []string{"team.memory"}) || !slices.Equal(server.createdOn, []string{"fleet"}) {
		t.Fatalf("want team.memory created on fleet, got %v on %v", server.created, server.createdOn)
	}
	options := server.options[0]
	if options.PartitionsCount == nil || *options.PartitionsCount != 4 ||
		options.MessageExpiry == nil || *options.MessageExpiry != iggcon.Duration((2*time.Hour).Microseconds()) {
		t.Fatalf("want 4 partitions and a two hour expiry, got %+v", options)
	}
	if memory.Namespace() != "team.memory" || memory.Backend() != "log" || memory.Scope().Stream != "fleet" {
		t.Fatalf("namespace=%q backend=%q stream=%q", memory.Namespace(), memory.Backend(), memory.Scope().Stream)
	}
}

func TestGivenAMemoryTopicBuilderWhenDefaultedThenShouldUseOnePartitionAndThirtyDays(t *testing.T) {
	server := &topicServer{}
	client := laser.FromClient(server, laser.Options{DefaultStream: "agents"})

	if _, err := client.MemoryTopic("team.memory").Partitions(0).Build(context.Background()); err != nil {
		t.Fatal(err)
	}

	options := server.options[0]
	if *options.PartitionsCount != 1 || *options.MessageExpiry != iggcon.Duration(laser.DefaultMemoryTopicTTL.Microseconds()) ||
		!slices.Equal(server.createdOn, []string{"agents"}) {
		t.Fatalf("want one partition and thirty days on the default stream, got %+v on %v", options, server.createdOn)
	}
}

func TestGivenAMemoryTopicBuilderWithNoExpiryWhenBuiltThenShouldNeverExpire(t *testing.T) {
	server := &topicServer{}
	client := laser.FromClient(server, laser.Options{DefaultStream: "agents"})

	if _, err := client.MemoryTopic("team.memory").NoExpiry().Build(context.Background()); err != nil {
		t.Fatal(err)
	}

	if expiry := server.options[0].MessageExpiry; expiry == nil || *expiry != iggcon.IggyExpiryNeverExpire {
		t.Fatalf("want the never-expire marker, got %v", expiry)
	}
}

func TestGivenAnExistingMemoryTopicWhenBuiltThenShouldKeepItsSettings(t *testing.T) {
	server := &topicServer{existing: map[string]bool{"team.memory": true}}
	client := laser.FromClient(server, laser.Options{DefaultStream: "agents"})

	if _, err := client.MemoryTopic("team.memory").Partitions(8).Build(context.Background()); err != nil {
		t.Fatal(err)
	}

	if len(server.created) != 0 {
		t.Fatalf("an existing topic keeps its settings, created %v", server.created)
	}
}

func TestGivenMemoryOnATopicWhenWrittenThenShouldRideThatTopicAndStreamUnderItsOwnNamespace(t *testing.T) {
	server := &topicServer{existing: map[string]bool{"team.memory": true}}
	client := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	memory, err := client.MemoryOnTopic("team.memory", "fleet")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	if _, err := memory.Remember(ctx, laser.MemoryFact, []byte("a fact")); err != nil {
		t.Fatal(err)
	}
	if err := memory.Set(ctx, "plan", []byte("ship")); err != nil {
		t.Fatal(err)
	}

	if !slices.Equal(server.sentStreams, []string{"fleet", "fleet"}) ||
		!slices.Equal(server.sentTopics, []string{"team.memory", "team.memory"}) {
		t.Fatalf("want both writes on fleet/team.memory, got %v %v", server.sentStreams, server.sentTopics)
	}
	for index, message := range server.appended {
		if got := headerStrings(t, message)[wire.HeaderMemoryNamespace]; got != "stream:fleet/team.memory" {
			t.Fatalf("record %d carries namespace %q", index, got)
		}
	}
	var named wire.MemoryRecord
	if err := wire.Unmarshal(server.appended[1].Payload, &named); err != nil {
		t.Fatal(err)
	}
	if named.ID != "team.memory/plan" {
		t.Fatalf("the named key defaults to the topic's name, got %q", named.ID)
	}
}

func TestGivenMemoryOnATopicWithoutAStreamWhenOpenedThenShouldKeepTheDefaultStream(t *testing.T) {
	client := laser.FromClient(&memoryServer{}, laser.Options{DefaultStream: "agents"})

	memory, err := client.MemoryOnTopic("team.memory", "")

	if err != nil || memory.Scope().Stream != "agents" {
		t.Fatalf("stream=%q err=%v", memory.Scope().Stream, err)
	}
	if _, err := client.MemoryOnTopic("", ""); err == nil {
		t.Fatal("an empty topic name must be refused")
	}
}

func TestGivenACustomBackendThatAppendsWhenAnIdIsSuppliedThenShouldKeepThatId(t *testing.T) {
	backend := &appendingMemory{}
	client := laser.FromClient(&memoryServer{}, laser.Options{DefaultStream: "agents"})
	memory, err := client.MemoryCustom(backend)
	if err != nil {
		t.Fatal(err)
	}
	id := laser.MemoryID{ID: wire.IDFromBytes([16]byte{15: 9})}

	if err := memory.Append(context.Background(), id, laser.MemorySummary, []byte("s")); err != nil {
		t.Fatal(err)
	}

	want := []appended{{id: id, kind: laser.MemorySummary, body: "s"}}
	if !reflect.DeepEqual(backend.appended, want) {
		t.Fatalf("want %+v, got %+v", want, backend.appended)
	}
}

// recordingMemory is a custom backend that records what reached it.
type recordingMemory struct {
	calls  []string
	scopes []laser.MemoryScope
	items  []laser.MemoryItem
}

func (memory *recordingMemory) Remember(_ context.Context, scope laser.MemoryScope, body []byte) (laser.MemoryID, error) {
	memory.calls = append(memory.calls, "remember:"+string(body))
	memory.scopes = append(memory.scopes, scope)
	return laser.MemoryID{ID: wire.IDFromBytes([16]byte{15: 7})}, nil
}

func (memory *recordingMemory) Recall(_ context.Context, scope laser.MemoryScope, query laser.MemoryQuery) ([]laser.MemoryItem, error) {
	memory.calls = append(memory.calls, "recall:"+query.Query)
	memory.scopes = append(memory.scopes, scope)
	return memory.items, nil
}

func (memory *recordingMemory) Improve(_ context.Context, scope laser.MemoryScope, feedback laser.Feedback) (laser.MemoryID, error) {
	memory.calls = append(memory.calls, "improve")
	memory.scopes = append(memory.scopes, scope)
	return feedback.Target, nil
}

func (memory *recordingMemory) Forget(_ context.Context, scope laser.MemoryScope, _ laser.MemoryID) error {
	memory.calls = append(memory.calls, "forget")
	memory.scopes = append(memory.scopes, scope)
	return nil
}

// recordingReranker returns what it was given in reverse, recording the query.
type recordingReranker struct{ queries []string }

func (reranker *recordingReranker) Rerank(_ context.Context, query string, items []laser.MemoryItem) ([]laser.MemoryItem, error) {
	reranker.queries = append(reranker.queries, query)
	reversed := slices.Clone(items)
	slices.Reverse(reversed)
	return reversed, nil
}

// topicServer records topic creation and answers a topic as absent until it is created.
type topicServer struct {
	memoryServer
	created       []string
	createdOn     []string
	options       []*iggcon.TopicOptions
	sentStreams   []string
	sentTopics    []string
	existing      map[string]bool
	streamCreated []string
}

func (server *topicServer) GetTopic(_ context.Context, _, topic iggcon.Identifier) (*iggcon.TopicDetails, error) {
	name, err := topic.String()
	if err != nil {
		return nil, err
	}
	if !server.existing[name] {
		return nil, ierror.TopicNameNotFound{}
	}
	return &iggcon.TopicDetails{Topic: iggcon.Topic{Name: name, PartitionsCount: 1}}, nil
}

func (server *topicServer) CreateStream(_ context.Context, name string, _ *iggcon.StreamOptions) (*iggcon.StreamDetails, error) {
	server.streamCreated = append(server.streamCreated, name)
	return &iggcon.StreamDetails{Stream: iggcon.Stream{Name: name}}, nil
}

func (server *topicServer) CreateTopic(
	_ context.Context, stream iggcon.Identifier, name string, options *iggcon.TopicOptions,
) (*iggcon.TopicDetails, error) {
	on, err := stream.String()
	if err != nil {
		return nil, err
	}
	server.created = append(server.created, name)
	server.createdOn = append(server.createdOn, on)
	server.options = append(server.options, options)
	if server.existing == nil {
		server.existing = map[string]bool{}
	}
	server.existing[name] = true
	return &iggcon.TopicDetails{Topic: iggcon.Topic{Name: name}}, nil
}

func (server *topicServer) SendMessages(
	ctx context.Context, stream, topic iggcon.Identifier, partitioning iggcon.Partitioning, messages []iggcon.IggyMessage,
) (*iggcon.SendMessagesResponse, error) {
	on, _ := stream.String()
	named, _ := topic.String()
	server.sentStreams = append(server.sentStreams, on)
	server.sentTopics = append(server.sentTopics, named)
	return server.memoryServer.SendMessages(ctx, stream, topic, partitioning, messages)
}

type appended struct {
	id   laser.MemoryID
	kind laser.MemoryKind
	body string
}

// appendingMemory is a custom backend that also takes a caller-supplied id.
type appendingMemory struct {
	recordingMemory
	appended []appended
}

func (memory *appendingMemory) Append(
	_ context.Context, _ laser.MemoryScope, id laser.MemoryID, kind laser.MemoryKind, body []byte,
) (laser.MemoryID, error) {
	memory.appended = append(memory.appended, appended{id: id, kind: kind, body: string(body)})
	return id, nil
}

// recordRewriter allows item bodies and replaces a tombstone or a feedback record with what rewrite returns.
type recordRewriter struct {
	rewrite func(wire.MemoryRecord) *wire.MemoryRecord
}

func (governor *recordRewriter) Decide(
	_ context.Context, action *laser.GovernedAction,
) (*laser.ActionDecision, error) {
	var record wire.MemoryRecord
	if governor.rewrite == nil || wire.Unmarshal(action.Payload, &record) != nil || record.Kind == wire.MemoryItem {
		return laser.Allow(), nil
	}
	rewritten := governor.rewrite(record)
	if rewritten == nil {
		return laser.Allow(), nil
	}
	body, err := wire.Marshal(*rewritten)
	if err != nil {
		return nil, err
	}
	return laser.Modify(body), nil
}

func governedVectorMemory(t *testing.T) (*laser.Memory, *recordRewriter) {
	t.Helper()
	governor := &recordRewriter{}
	client := laser.FromClient(&memoryServer{}, laser.Options{DefaultStream: "agents"}).
		WithGovernor(governor, laser.GovernorEnforce)
	opened, err := client.MemoryWith("notes", laser.MemoryBackendVector)
	if err != nil {
		t.Fatal(err)
	}
	return opened.Embedder(&wordEmbedder{vocabulary: []string{"cats"}}), governor
}
