package laser

import (
	"context"
	"errors"
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// The lane is merged from the reconcile goroutine while the run goroutine
// decides records, so both must read the handled set under the same lock.
func TestGivenALaneMergeWhenARecordIsDecidedConcurrentlyThenShouldNotRace(t *testing.T) {
	pause := livePause()
	session := NewConversationID()
	merged := make(chan struct{})
	go func() {
		defer close(merged)
		for offset := uint64(0); offset < 500; offset++ {
			pause.merge(laneScan{session: {
				parked:  map[parkingKey]wire.SessionParking{},
				handled: map[parkingKey]bool{{topic: 3, offset: offset}: true},
			}})
		}
	}()
	for running := true; running; {
		select {
		case <-merged:
			running = false
		default:
		}
		if _, err := pause.hold(context.Background(), workAt(session, 499), nil); err != nil {
			t.Fatalf("decide the record: %v", err)
		}
	}
	decision, err := pause.hold(context.Background(), workAt(session, 499), nil)
	if err != nil || decision != holdCommit {
		t.Fatalf("want a record the lane lists as handled committed, got %v, %v", decision, err)
	}
}

// The live path hands back held work whatever partition it came from, and only
// the reconcile path leaves an unassigned partition to the member reading it.
func TestGivenHeldWorkOnAnUnassignedPartitionWhenAskedThenShouldFilterOnlyForReconcile(t *testing.T) {
	pause := livePause()
	session := NewConversationID()
	key := parkingKey{topic: 3, partition: 2, offset: 7}
	pause.merge(laneScan{session: {
		parked:  map[parkingKey]wire.SessionParking{key: {}},
		handled: map[parkingKey]bool{},
	}})
	// The request of an empty parking is a control position, so the record is
	// held by a pause rather than behind an approval.
	pause.book.controlSource = &wire.LogPosition{}
	pause.observeAssignment([]uint32{0})

	if _, _, found := pause.nextHeld(session, true); found {
		t.Fatal("want the reconcile path to leave an unassigned partition alone")
	}
	got, _, found := pause.nextHeld(session, false)
	if !found || got != key {
		t.Fatalf("want the live path to hand the record back, got %+v, %v", got, found)
	}
}

// A source whose ids were never read back would be named stream 0, topic 0,
// which are real ids, so a session lens names its origin only once resolved.
func TestGivenAnUnresolvedSourceWhenTheSessionIsOpenedThenShouldNotNameAnOrigin(t *testing.T) {
	handle := FromClient(nil, Options{DefaultStream: "agents"})
	identity, err := wire.NewAgentID("executor")
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := handle.AgentRuntime(
		handle.Agent(identity, AgentOptions{}), RuntimeOptions{Topic: wire.AgentSessions})
	if err != nil {
		t.Fatalf("open the runtime: %v", err)
	}
	agent := runtime.contextFor(workAt(NewConversationID(), 41))

	if origin := agent.Session().current; origin != nil {
		t.Fatalf("want no origin for an unresolved source, got %+v", origin)
	}
	runtime.source, runtime.sourceResolved = wire.LogPosition{StreamID: 7, TopicID: 3}, true
	origin := agent.Session().current
	if origin == nil || origin.Message == nil ||
		origin.Message.Stream != 7 || origin.Message.Topic != 3 || origin.Message.Offset != 41 {
		t.Fatalf("want the resolved origin named, got %+v", origin)
	}
}

// A control read that keeps failing leaves the book stale, so it is reported
// when the failures reach the budget, once.
func TestGivenAControlReadThatKeepsFailingWhenTheBudgetIsReachedThenShouldReportItOnce(t *testing.T) {
	var reported []error
	follower := &controlFollower{report: func(err error) { reported = append(reported, err) }}
	cause := &TransportError{Reason: "poll"}

	for failures := uint32(1); failures <= ConsumerFailureBudget+3; failures++ {
		follower.noteFailure(failures, cause)
	}
	if len(reported) != 1 {
		t.Fatalf("want one report for one outage, got %d", len(reported))
	}
	var transport *TransportError
	if !errors.As(reported[0], &transport) || !errors.Is(reported[0], cause) {
		t.Fatalf("want the read failure carried as the cause, got %v", reported[0])
	}
}

// livePause is a gate whose book is live and whose source ids are known, so a
// decision needs no server.
func livePause() *sessionPause {
	book := NewControlBook()
	book.Load(nil)
	book.SetLive(true)
	pause := newSessionPause(nil, nil, wire.AgentID{}, book, "agents", wire.AgentSessions)
	pause.ids = &sourceIDs{stream: 7, topic: 3, generation: 1}
	return pause
}

func workAt(session wire.ConversationID, offset uint64) *AgentMessage {
	record := &Record{}
	record.Message = iggcon.IggyMessage{}
	record.Message.Header.Offset = offset
	return &AgentMessage{Record: record, Envelope: &wire.AgentEnvelope{Conversation: session}}
}
