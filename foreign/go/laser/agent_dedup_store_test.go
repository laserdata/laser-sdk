package laser_test

import (
	"context"
	"slices"
	"sync"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
)

func TestGivenACallerSuppliedDedupStoreWhenARecordRepeatsThenShouldSkipTheRepeat(t *testing.T) {
	store := &recordingDedupStore{seen: map[string]bool{}}
	agent := laser.FromClient(&streamServer{}, laser.Options{}).
		Agent(agentID(t, "planner"), laser.AgentOptions{Dedup: store})
	record := func() *laser.Record {
		return recordWith(&laser.Provenance{
			Conversation:   conversation(1),
			IdempotencyKey: textPointer("order-7"),
		})
	}

	first := agent.Decide(record())
	second := agent.Decide(record())

	if first.Verdict != laser.Deliver || second.Verdict != laser.Skip {
		t.Fatalf("want deliver then skip, got %s then %s", first.Verdict, second.Verdict)
	}
	if want := []string{"order-7", "order-7"}; !slices.Equal(store.observed(), want) {
		t.Fatalf("want the store asked for %v, got %v", want, store.observed())
	}
	if agent.Dedup() != nil {
		t.Fatal("want no in-memory window behind a caller-supplied store")
	}
}

func TestGivenACallerSuppliedDedupStoreWhenARecordCarriesNoKeyThenShouldNotAskTheStore(t *testing.T) {
	store := &recordingDedupStore{seen: map[string]bool{}}
	agent := laser.FromClient(&streamServer{}, laser.Options{}).
		Agent(agentID(t, "planner"), laser.AgentOptions{Dedup: store})

	decision := agent.Decide(recordWith(&laser.Provenance{Conversation: conversation(1)}))

	if decision.Verdict != laser.Deliver || len(store.observed()) != 0 {
		t.Fatalf("want a keyless record delivered unasked, got %s after %v", decision.Verdict, store.observed())
	}
}

func TestGivenANilWindowWhenAnAgentIsOpenedThenShouldOpenTheDefaultWindow(t *testing.T) {
	var window *laser.Deduplicator
	agent := laser.FromClient(&streamServer{}, laser.Options{}).
		Agent(agentID(t, "planner"), laser.AgentOptions{Dedup: window})

	decision := agent.Decide(recordWith(&laser.Provenance{
		Conversation:   conversation(1),
		IdempotencyKey: textPointer("order-7"),
	}))

	if decision.Verdict != laser.Deliver || agent.Dedup() == nil || agent.Dedup().Remembered() != 1 {
		t.Fatalf("want the default window to remember the key, got %s", decision.Verdict)
	}
}

func TestGivenTheWindowWhenObservedThenShouldAnswerLikeAccept(t *testing.T) {
	var store laser.DedupStore = laser.NewDeduplicator(0)

	first := store.Observe(context.Background(), "order-7")
	second := store.Observe(context.Background(), "order-7")

	if !first || second {
		t.Fatalf("want new then seen, got %v then %v", first, second)
	}
}

type recordingDedupStore struct {
	mutex sync.Mutex
	seen  map[string]bool
	keys  []string
}

func (store *recordingDedupStore) Observe(_ context.Context, key string) bool {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	store.keys = append(store.keys, key)
	if store.seen[key] {
		return false
	}
	store.seen[key] = true
	return true
}

func (store *recordingDedupStore) observed() []string {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	return append([]string(nil), store.keys...)
}
