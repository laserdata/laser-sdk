package laser_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// cursorServer serves a fixed set of partitions, handing back the records at
// or after the offset each poll asks for.
type cursorServer struct {
	clusterOfOne
	partitions  uint32
	byPartition map[uint32][]iggcon.IggyMessage
	topicAbsent bool
	polls       []uint64
	pollErrAt   *uint32
	maxCount    uint32
}

func (server *cursorServer) Close() error { return nil }

func (server *cursorServer) GetTopic(
	_ context.Context, _, _ iggcon.Identifier,
) (*iggcon.TopicDetails, error) {
	if server.topicAbsent {
		return nil, ierror.TopicNameNotFound{}
	}
	return &iggcon.TopicDetails{Topic: iggcon.Topic{PartitionsCount: server.partitions}}, nil
}

func (server *cursorServer) PollMessages(
	_ context.Context, _, _ iggcon.Identifier, partitionID uint32,
	_ iggcon.Consumer, strategy iggcon.PollingStrategy, options iggcon.PollOptions,
) (*iggcon.PolledMessage, error) {
	if server.pollErrAt != nil && partitionID == *server.pollErrAt {
		return nil, errors.New("injected partition error")
	}
	server.polls = append(server.polls, strategy.Value)
	if options.Count > server.maxCount {
		server.maxCount = options.Count
	}
	var out []iggcon.IggyMessage
	for _, message := range server.byPartition[partitionID] {
		if message.Header.Offset >= strategy.Value && uint32(len(out)) < options.Count {
			out = append(out, message)
		}
	}
	return &iggcon.PolledMessage{Messages: out}, nil
}

// recordAt builds a record that sits at one position with one timestamp.
func recordAt(t *testing.T, offset, timestamp uint64, body string) iggcon.IggyMessage {
	t.Helper()
	message, err := iggcon.NewIggyMessage([]byte(body))
	if err != nil {
		t.Fatalf("build a record: %v", err)
	}
	message.Header.Offset = offset
	message.Header.Timestamp = timestamp
	return message
}

func cursorHandle(server iggcon.Client) *laser.Laser {
	return laser.FromClient(server, laser.Options{DefaultStream: "agents"})
}

// A merge across partitions has to be ordered by the server's single clock, or
// two readers of one topic see different orders.
func TestGivenRecordsAcrossPartitionsWhenPolledThenShouldMergeByTimestamp(t *testing.T) {
	server := &cursorServer{
		partitions: 2,
		byPartition: map[uint32][]iggcon.IggyMessage{
			0: {recordAt(t, 0, 30, "third"), recordAt(t, 1, 40, "fourth")},
			1: {recordAt(t, 0, 10, "first"), recordAt(t, 1, 20, "second")},
		},
	}
	cursor, err := cursorHandle(server).Topic("inferences").Replay()
	if err != nil {
		t.Fatalf("replay: %v", err)
	}

	records, err := cursor.Poll(context.Background())
	if err != nil {
		t.Fatalf("poll: %v", err)
	}

	var order []string
	for _, record := range records {
		order = append(order, string(record.Payload()))
	}
	want := []string{"first", "second", "third", "fourth"}
	if strings.Join(order, ",") != strings.Join(want, ",") {
		t.Errorf("want %v, got %v", want, order)
	}
}

// The cursor owns its offsets, so persisting them and resuming must not
// re-deliver the last record.
func TestGivenAPolledCursorWhenResumedThenShouldNotRedeliver(t *testing.T) {
	messages := []iggcon.IggyMessage{
		recordAt(t, 0, 10, "one"), recordAt(t, 1, 20, "two"),
	}
	server := &cursorServer{
		partitions:  1,
		byPartition: map[uint32][]iggcon.IggyMessage{0: messages},
	}
	cursor, err := cursorHandle(server).Topic("inferences").Replay()
	if err != nil {
		t.Fatalf("replay: %v", err)
	}

	first, err := cursor.Poll(context.Background())
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if len(first) != 2 {
		t.Fatalf("want both records, got %d", len(first))
	}
	offsets := cursor.Offsets()
	if len(offsets) != 1 || offsets[0] != 2 {
		t.Errorf("want the next offset past the last record, got %v", offsets)
	}

	// A fresh cursor seeded from the persisted offsets resumes, rather than
	// reading the same records again.
	resumed, err := cursorHandle(server).Topic("inferences").Replay()
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	again, err := resumed.FromOffsets(offsets).Poll(context.Background())
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if len(again) != 0 {
		t.Errorf("a resumed cursor must not redeliver, got %d records", len(again))
	}
}

// A list shorter than the topic has partitions is padded with zero, so those
// partitions are read from the start rather than skipped.
func TestGivenFewerOffsetsThanPartitionsWhenPolledThenShouldReadTheRestFromTheStart(t *testing.T) {
	server := &cursorServer{
		partitions: 2,
		byPartition: map[uint32][]iggcon.IggyMessage{
			0: {recordAt(t, 0, 10, "partition zero")},
			1: {recordAt(t, 0, 20, "partition one")},
		},
	}
	cursor, err := cursorHandle(server).Topic("inferences").Replay()
	if err != nil {
		t.Fatalf("replay: %v", err)
	}

	records, err := cursor.FromOffsets([]uint64{1}).Poll(context.Background())
	if err != nil {
		t.Fatalf("poll: %v", err)
	}

	if len(records) != 1 || string(records[0].Payload()) != "partition one" {
		t.Errorf("want the unread partition read from the start, got %d records", len(records))
	}
}

// A reader started before its writer is not an error: the topic simply has
// nothing in it yet.
func TestGivenAnAbsentTopicWhenPolledThenShouldReportNothingRatherThanFail(t *testing.T) {
	server := &cursorServer{topicAbsent: true}
	cursor, err := cursorHandle(server).Topic("inferences").Replay()
	if err != nil {
		t.Fatalf("replay: %v", err)
	}

	records, err := cursor.Poll(context.Background())
	if err != nil {
		t.Fatalf("an absent topic must read as empty: %v", err)
	}
	if len(records) != 0 {
		t.Errorf("want nothing, got %d records", len(records))
	}
}

// A corrupt partition count sizes a loop and an allocation, so it is clamped
// before either.
func TestGivenAnAbsurdPartitionCountWhenPolledThenShouldClampIt(t *testing.T) {
	server := &cursorServer{partitions: 1_000_000, byPartition: map[uint32][]iggcon.IggyMessage{}}
	cursor, err := cursorHandle(server).Topic("inferences").Replay()
	if err != nil {
		t.Fatalf("replay: %v", err)
	}

	if _, err := cursor.Poll(context.Background()); err != nil {
		t.Fatalf("poll: %v", err)
	}

	if uint32(len(cursor.Offsets())) > 1024 {
		t.Errorf("want the partition count clamped, got %d", len(cursor.Offsets()))
	}
}

// Each stops when the visitor says so, without draining the rest.
func TestGivenACursorWhenStoppedEarlyThenShouldVisitNoFurtherRecords(t *testing.T) {
	server := &cursorServer{
		partitions: 1,
		byPartition: map[uint32][]iggcon.IggyMessage{
			0: {recordAt(t, 0, 10, "one"), recordAt(t, 1, 20, "two")},
		},
	}
	cursor, err := cursorHandle(server).Topic("inferences").Replay()
	if err != nil {
		t.Fatalf("replay: %v", err)
	}

	seen := 0
	if err := cursor.Each(context.Background(), func(*laser.Record) bool {
		seen++
		return false
	}); err != nil {
		t.Fatalf("each: %v", err)
	}
	if seen != 1 {
		t.Errorf("want one record visited, got %d", seen)
	}
}

func TestGivenNoStreamWhenOpeningACursorThenShouldRefuseWithTheRemedy(t *testing.T) {
	server := &cursorServer{}

	_, err := laser.FromClient(server, laser.Options{}).Topic("inferences").Replay()

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want an InvalidError, got %T: %v", err, err)
	}
	if !strings.Contains(err.Error(), "DefaultStream") {
		t.Errorf("want the remedy named, got %v", err)
	}
}

// A batch of zero would ask the server for nothing and loop forever.
func TestGivenAZeroBatchWhenSetThenShouldStillReadSomething(t *testing.T) {
	server := &cursorServer{
		partitions:  1,
		byPartition: map[uint32][]iggcon.IggyMessage{0: {recordAt(t, 0, 10, "one")}},
	}
	cursor, err := cursorHandle(server).Topic("inferences").Replay()
	if err != nil {
		t.Fatalf("replay: %v", err)
	}

	records, err := cursor.Batch(0).Poll(context.Background())
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if len(records) != 1 {
		t.Errorf("want the record read, got %d", len(records))
	}
	if server.maxCount == 0 {
		t.Error("a read must ask for at least one record")
	}
}

// A record with a spine arrives through the cursor with it already read, the
// same as through a consumer.
func TestGivenARecordWithProvenanceWhenReplayedThenShouldReadItOff(t *testing.T) {
	headers, err := (&laser.Provenance{Conversation: conversation(4)}).Headers()
	if err != nil {
		t.Fatalf("encode the provenance: %v", err)
	}
	message, err := iggcon.NewIggyMessage([]byte("body"), iggcon.WithUserHeaders(headers))
	if err != nil {
		t.Fatalf("build a record: %v", err)
	}
	server := &cursorServer{
		partitions:  1,
		byPartition: map[uint32][]iggcon.IggyMessage{0: {message}},
	}
	cursor, err := cursorHandle(server).Topic("inferences").Replay()
	if err != nil {
		t.Fatalf("replay: %v", err)
	}

	records, err := cursor.Poll(context.Background())
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if len(records) != 1 || records[0].Provenance == nil {
		t.Fatalf("want the provenance read, got %+v", records)
	}
	if records[0].Provenance.Conversation != conversation(4) {
		t.Errorf("want the conversation, got %v", records[0].Provenance.Conversation)
	}
}

func TestGivenALaterPartitionFailureWhenRetryingThenShouldDeliverEarlierRecords(t *testing.T) {
	second := uint32(1)
	server := &cursorServer{partitions: 2, pollErrAt: &second,
		byPartition: map[uint32][]iggcon.IggyMessage{0: {recordAt(t, 0, 10, "first")}}}
	cursor, err := cursorHandle(server).Topic("events").Replay()
	if err != nil {
		t.Fatal(err)
	}
	cursor.FromOffsets([]uint64{0, 0})
	if _, err := cursor.Poll(context.Background()); err == nil {
		t.Fatal("poll must surface partition failure")
	}
	server.pollErrAt = nil
	records, err := cursor.Poll(context.Background())
	if err != nil || len(records) != 1 || string(records[0].Payload()) != "first" {
		t.Fatalf("retry returned %+v, %v", records, err)
	}
}

func TestGivenARecordWithABrokenSpineWhenPolledThenShouldReportItOnceAndReadOn(t *testing.T) {
	headers, err := (&laser.Provenance{Conversation: conversation(5)}).Headers()
	if err != nil {
		t.Fatalf("encode the spine: %v", err)
	}
	fence, err := iggcon.NewHeaderKeyString(wire.HeaderFence)
	if err != nil {
		t.Fatalf("header key: %v", err)
	}
	headers = append(headers, iggcon.HeaderEntry{
		Key:   fence,
		Value: iggcon.HeaderValue{Kind: iggcon.String, Value: []byte("not a number")},
	})
	broken, err := iggcon.NewIggyMessage([]byte("broken"), iggcon.WithUserHeaders(headers))
	if err != nil {
		t.Fatalf("build a record: %v", err)
	}
	broken.Header.Offset, broken.Header.Timestamp = 1, 20
	server := &cursorServer{
		partitions: 1,
		byPartition: map[uint32][]iggcon.IggyMessage{
			0: {recordAt(t, 0, 10, "before"), broken, recordAt(t, 2, 30, "after")},
		},
	}
	cursor, err := cursorHandle(server).Topic("inferences").Replay()
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	ctx := context.Background()

	before, err := cursor.Poll(ctx)
	if err != nil || len(before) != 1 || string(before[0].Payload()) != "before" {
		t.Fatalf("want the record ahead of the broken one first, got %d records and %v", len(before), err)
	}
	_, err = cursor.Poll(ctx)
	var unreadable *laser.UndecodableRecord
	if !errors.As(err, &unreadable) || unreadable.PartitionID != 0 || unreadable.Message.Header.Offset != 1 {
		t.Fatalf("want the broken record reported with its position, got %v", err)
	}
	after, err := cursor.Poll(ctx)
	if err != nil || len(after) != 1 || string(after[0].Payload()) != "after" {
		t.Fatalf("want the read to go on past the broken record, got %d records and %v", len(after), err)
	}
}
