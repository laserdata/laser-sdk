package laser_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// A memory remembered through a session says which agent produced it and
// which record it came from, so a reader can walk the fact back to its cause.
func TestGivenASessionActingOnARecordWhenItRemembersThenShouldStampTheProducerAndOrigin(t *testing.T) {
	server := &streamServer{streamExists: true, topicExists: true}
	handle := streamHandle(server, laser.Options{DefaultStream: "agents"})
	session := handle.Sessions().Open(conversation(1), "").As(agentID(t, "executor"))
	origin := actedOn(conversation(1))

	if _, err := session.ActingOn(origin).LinkedMemory().
		Remember(context.Background(), laser.MemoryFact, []byte("node-7 is hot")); err != nil {
		t.Fatalf("remember: %v", err)
	}

	var record wire.MemoryRecord
	if err := wire.Unmarshal(server.sent()[0].Payload, &record); err != nil {
		t.Fatalf("decode the memory record: %v", err)
	}
	wantProducer := &wire.ProducerInfo{Name: "sdk:executor", Version: laser.ClientVersion}
	if !reflect.DeepEqual(record.Producer, wantProducer) {
		t.Errorf("want the producer %+v, got %+v", wantProducer, record.Producer)
	}
	if !reflect.DeepEqual(record.Origin, &origin) {
		t.Errorf("want the origin %+v, got %+v", origin, record.Origin)
	}
}

// A session that names no record still names its producer, and leaves the
// origin out rather than inventing one.
func TestGivenASessionActingOnNothingWhenItRemembersThenShouldLeaveTheOriginOut(t *testing.T) {
	server := &streamServer{streamExists: true, topicExists: true}
	handle := streamHandle(server, laser.Options{DefaultStream: "agents"})
	session := handle.Sessions().Open(conversation(1), "").As(agentID(t, "executor"))

	if _, err := session.LinkedMemory().
		Remember(context.Background(), laser.MemoryFact, []byte("node-7 is hot")); err != nil {
		t.Fatalf("remember: %v", err)
	}

	var record wire.MemoryRecord
	if err := wire.Unmarshal(server.sent()[0].Payload, &record); err != nil {
		t.Fatalf("decode the memory record: %v", err)
	}
	if record.Origin != nil || record.Producer == nil {
		t.Fatalf("want a producer and no origin, got %+v and %+v", record.Producer, record.Origin)
	}
}

// A graph fact written through a session carries the session, the producer
// and the record it came from.
func TestGivenASessionActingOnARecordWhenItLinksAGraphFactThenShouldStampSessionProducerAndSource(t *testing.T) {
	server := &graphServer{}
	handle := graphHandle(server, laser.Options{DefaultStream: "agents"})
	session := handle.Sessions().Open(conversation(1), "").As(agentID(t, "executor"))
	origin := actedOn(conversation(1))

	if err := session.ActingOn(origin).LinkedGraph("knowledge").
		Link(context.Background(), "host:node-7", "runs", "service:api"); err != nil {
		t.Fatalf("link: %v", err)
	}

	var upsert wire.GraphUpsert
	decodeGraphRequest(t, server, 0, &upsert)
	wantSession := &wire.SessionRef{Stream: "agents", Session: conversation(1)}
	if !reflect.DeepEqual(upsert.Session, wantSession) {
		t.Errorf("want the session %+v, got %+v", wantSession, upsert.Session)
	}
	wantProducer := &wire.ProducerInfo{Name: "sdk:executor", Version: laser.ClientVersion}
	if len(upsert.Edges) != 1 || !reflect.DeepEqual(upsert.Edges[0].Producer, wantProducer) ||
		!reflect.DeepEqual(upsert.Edges[0].Source, &origin) {
		t.Errorf("want the edge stamped with %+v and %+v, got %+v", wantProducer, origin, upsert.Edges)
	}
}

// The items a recall put into the context are recorded on the lane with the
// query that found them, so a reader sees what the model was shown.
func TestGivenRecalledItemsWhenTheRetrievalIsRecordedThenShouldWriteThemWithTheirScores(t *testing.T) {
	server := &streamServer{streamExists: true, topicExists: true}
	handle := streamHandle(server, laser.Options{DefaultStream: "agents"})
	session := handle.Sessions().Open(conversation(1), "").As(agentID(t, "executor"))
	item := laser.MemoryItem{
		ID:    laser.ContentMemoryID(laser.MemoryScope{}, laser.MemoryFact, []byte("node-7 is hot")),
		Score: 0.5,
	}
	query := "which node is hot"

	if _, err := session.RecordRetrieval(context.Background(), &query, []laser.MemoryItem{item}); err != nil {
		t.Fatalf("record the retrieval: %v", err)
	}

	var envelope wire.AgentEnvelope
	if err := wire.Unmarshal(server.sent()[0].Payload, &envelope); err != nil {
		t.Fatalf("decode the lane record: %v", err)
	}
	if envelope.Operation == nil || *envelope.Operation != wire.OperationContextRetrieved {
		t.Fatalf("want a context retrieval event, got %+v", envelope.Operation)
	}
	var body wire.ContextRetrieval
	if err := wire.Unmarshal(envelope.Body, &body); err != nil {
		t.Fatalf("decode the retrieval: %v", err)
	}
	want := wire.ContextRetrieval{Query: &query, Items: []wire.RetrievedItem{{ID: item.ID.String(), Score: 0.5}}}
	if !reflect.DeepEqual(body, want) {
		t.Fatalf("want %+v, got %+v", want, body)
	}
}

// The idle timeout a session names for itself is what its start records.
func TestGivenASessionIdleTimeoutWhenBegunThenShouldRecordIt(t *testing.T) {
	server := &streamServer{streamExists: true, topicExists: true}
	handle := streamHandle(server, laser.Options{DefaultStream: "agents"})
	session := handle.Sessions().Start().As(agentID(t, "executor")).WithIdleTimeout(90 * time.Second)

	_, lease, err := session.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer lease.Release()

	var envelope wire.AgentEnvelope
	if err := wire.Unmarshal(server.sent()[0].Payload, &envelope); err != nil {
		t.Fatalf("decode the start record: %v", err)
	}
	var start wire.SessionStart
	if err := wire.Unmarshal(envelope.Body, &start); err != nil {
		t.Fatalf("decode the session start: %v", err)
	}
	if start.IdleTimeoutMicros == nil || *start.IdleTimeoutMicros != 90_000_000 {
		t.Fatalf("want an idle timeout of ninety seconds recorded, got %v", start.IdleTimeoutMicros)
	}
}

// actedOn is the record a linked write names as its origin.
func actedOn(session wire.ConversationID) wire.SourceRef {
	conversation := session.String()
	return wire.SourceRef{Kind: wire.SourceMessage, Message: &wire.MessageSource{
		Stream: 7, Topic: 3, Partition: 1, Offset: 41, Conversation: &conversation,
	}}
}
