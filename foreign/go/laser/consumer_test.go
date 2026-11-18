package laser_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// consumerServer is a client the partition reader can read through. The reader
// resolves its fetch path by type assertion, so this stand-in implements that
// seam as well as the admin calls a consumer makes.
type consumerServer struct {
	iggcon.Client
	mutex  sync.Mutex
	joined string
	stored []iggcon.ConsumerPosition
}

func (server *consumerServer) Close() error { return nil }

func (server *consumerServer) FetchMessages(
	_ context.Context, _, _ iggcon.Identifier, _ iggcon.Consumer,
	_ []iggcon.FetchMessagesTarget, _ uint32, _ bool, _ iggcon.DeferredPollOptions,
) ([]iggcon.FetchMessagesSection, error) {
	return nil, nil
}

func (server *consumerServer) ReadinessGroups(
	_ context.Context, _, _ iggcon.Identifier, partitions []uint32,
) ([][]uint32, error) {
	return [][]uint32{partitions}, nil
}

func (server *consumerServer) JoinConsumerGroup(
	_ context.Context, _, _ iggcon.Identifier, groupID iggcon.Identifier,
) error {
	name, _ := groupID.String()
	server.mutex.Lock()
	defer server.mutex.Unlock()
	server.joined = name
	return nil
}

func (server *consumerServer) LeaveConsumerGroup(
	_ context.Context, _, _, _ iggcon.Identifier,
) error {
	return nil
}

func (server *consumerServer) SyncConsumerGroup(
	_ context.Context, _, _, _ iggcon.Identifier,
) (*iggcon.ConsumerGroupAssignment, error) {
	return &iggcon.ConsumerGroupAssignment{}, nil
}

func (server *consumerServer) StoreConsumerPosition(
	_ context.Context, _ iggcon.Consumer, _, _ iggcon.Identifier,
	position iggcon.ConsumerPosition,
) error {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	server.stored = append(server.stored, position)
	return nil
}

func (server *consumerServer) DeleteConsumerPosition(
	_ context.Context, _ iggcon.Consumer, _, _ iggcon.Identifier, _ iggcon.ConsumerPosition,
) error {
	return nil
}

// A standalone consumer has nothing to assign it partitions, so one that names
// none would read nothing at all. The refusal says what to do instead.
func TestGivenAStandaloneConsumerWithNoPartitionsThenShouldRefuseWithTheRemedy(t *testing.T) {
	server := &consumerServer{}

	_, err := streamHandle(server, laser.Options{DefaultStream: "agents"}).
		Topic("inferences").Consumer(context.Background(), "worker", laser.ConsumerOptions{})

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want an InvalidError, got %T: %v", err, err)
	}
	if !strings.Contains(err.Error(), "join a group") {
		t.Errorf("want the alternative named, got %v", err)
	}
}

// A group member is assigned its partitions by the server and rebalanced as
// members come and go, so naming its own would fight the assignment.
func TestGivenAGroupMemberNamingPartitionsThenShouldRefuseIt(t *testing.T) {
	server := &consumerServer{}

	_, err := streamHandle(server, laser.Options{DefaultStream: "agents"}).
		Topic("inferences").ConsumerGroup(context.Background(), "workers",
		laser.ConsumerOptions{Partitions: []uint32{0}})

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want an InvalidError, got %T: %v", err, err)
	}
	if !strings.Contains(err.Error(), "assigned") {
		t.Errorf("want the reason named, got %v", err)
	}
}

func TestGivenNoStreamWhenOpeningAConsumerThenShouldRefuseWithTheRemedy(t *testing.T) {
	server := &consumerServer{}

	_, err := streamHandle(server, laser.Options{}).Topic("inferences").
		Consumer(context.Background(), "worker", laser.ConsumerOptions{Partitions: []uint32{0}})

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want an InvalidError, got %T: %v", err, err)
	}
	if !strings.Contains(err.Error(), "DefaultStream") {
		t.Errorf("want the remedy named, got %v", err)
	}
}

func TestGivenAConsumerGroupWhenOpenedThenShouldJoinIt(t *testing.T) {
	server := &consumerServer{}

	consumer, err := streamHandle(server, laser.Options{DefaultStream: "agents"}).
		Topic("inferences").ConsumerGroup(context.Background(), "workers",
		laser.ConsumerOptions{})
	if err != nil {
		t.Fatalf("join: %v", err)
	}
	defer consumer.Close(context.Background())

	server.mutex.Lock()
	joined := server.joined
	server.mutex.Unlock()
	if joined != "workers" {
		t.Errorf("want the group joined, got %q", joined)
	}
}

// A record with no spine is the normal case on a plain data topic, so it is
// opened without one rather than refused.
func TestGivenARecordWithNoProvenanceWhenOpenedThenShouldCarryNone(t *testing.T) {
	message, err := iggcon.NewIggyMessage([]byte("plain"))
	if err != nil {
		t.Fatalf("build a record: %v", err)
	}

	record, err := laser.OpenRecord(message)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if record.Provenance != nil {
		t.Errorf("want no provenance, got %+v", record.Provenance)
	}
	if string(record.Payload()) != "plain" {
		t.Errorf("want the body kept, got %q", record.Payload())
	}
}

// A record carrying a spine arrives with it already read, so a handler does
// not parse headers itself.
func TestGivenARecordWithProvenanceWhenOpenedThenShouldReadItOff(t *testing.T) {
	provenance := &laser.Provenance{
		Conversation: conversation(9),
		FenceToken:   uint64Pointer(3),
	}
	headers, err := provenance.Headers()
	if err != nil {
		t.Fatalf("encode the provenance: %v", err)
	}
	message, err := iggcon.NewIggyMessage([]byte("body"), iggcon.WithUserHeaders(headers))
	if err != nil {
		t.Fatalf("build a record: %v", err)
	}

	record, err := laser.OpenRecord(message)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if record.Provenance == nil {
		t.Fatal("want the provenance read off the record")
	}
	if record.Provenance.Conversation != conversation(9) {
		t.Errorf("want the conversation, got %v", record.Provenance.Conversation)
	}
	if record.Provenance.FenceToken == nil || *record.Provenance.FenceToken != 3 {
		t.Errorf("want the fence, got %v", record.Provenance.FenceToken)
	}
}

// A spine that is present but broken is a different thing from one that is
// absent: handing the record over with the fence silently missing would let
// through what the gate exists to stop.
func TestGivenARecordWithABrokenSpineWhenOpenedThenShouldRefuseIt(t *testing.T) {
	headers, err := (&laser.Provenance{Conversation: conversation(9)}).Headers()
	if err != nil {
		t.Fatalf("encode the provenance: %v", err)
	}
	fence, err := iggcon.NewHeaderKeyString(wire.HeaderFence)
	if err != nil {
		t.Fatalf("header key: %v", err)
	}
	headers = append(headers, iggcon.HeaderEntry{
		Key:   fence,
		Value: iggcon.HeaderValue{Kind: iggcon.String, Value: []byte("not a number")},
	})
	message, err := iggcon.NewIggyMessage([]byte("body"), iggcon.WithUserHeaders(headers))
	if err != nil {
		t.Fatalf("build a record: %v", err)
	}

	if _, err := laser.OpenRecord(message); err == nil {
		t.Error("a record with a broken spine must be refused")
	}
}

// A record past its deadline is one nobody is waiting for, so a consumer can
// tell without re-reading the headers.
func TestGivenARecordPastItsDeadlineWhenCheckedThenShouldReportItExpired(t *testing.T) {
	past := time.Now().Add(-time.Minute)
	future := time.Now().Add(time.Hour)

	expired := &laser.Record{Provenance: &laser.Provenance{
		Conversation: conversation(1), DeadlineAt: &past}}
	if !expired.Expired(time.Now()) {
		t.Error("want the record reported expired")
	}

	live := &laser.Record{Provenance: &laser.Provenance{
		Conversation: conversation(1), DeadlineAt: &future}}
	if live.Expired(time.Now()) {
		t.Error("a record inside its deadline is not expired")
	}

	// A record with no deadline never expires, and neither does one with no
	// spine at all.
	none := &laser.Record{Provenance: &laser.Provenance{Conversation: conversation(1)}}
	if none.Expired(time.Now()) {
		t.Error("a record with no deadline never expires")
	}
	if (&laser.Record{}).Expired(time.Now()) {
		t.Error("a record with no provenance never expires")
	}
}

// The poll options a caller sets have to reach the read, or a tuned consumer
// quietly runs on the defaults.
func TestGivenConsumerOptionsWhenOpenedThenShouldApplyThem(t *testing.T) {
	server := &consumerServer{}

	consumer, err := streamHandle(server, laser.Options{DefaultStream: "agents"}).
		Topic("inferences").Consumer(context.Background(), "worker",
		laser.ConsumerOptions{
			Partitions: []uint32{0, 1},
			PollWait:   250 * time.Millisecond,
			Count:      10,
		})
	if err != nil {
		t.Fatalf("open the consumer: %v", err)
	}
	defer consumer.Close(context.Background())

	partitions := consumer.Partitions()
	if len(partitions) != 2 {
		t.Errorf("want both partitions read, got %v", partitions)
	}
	if consumer.Topic().Name() != "inferences" {
		t.Errorf("want the topic kept, got %q", consumer.Topic().Name())
	}
}

// A tuned consumer must actually run tuned, or it quietly reads on the
// defaults while the caller believes otherwise.
func TestGivenConsumerLimitsWhenResolvedThenShouldCarryThemToTheRead(t *testing.T) {
	tuned := laser.ConsumerOptions{PollWait: 250 * time.Millisecond, Count: 10}.PollOptions()
	if tuned.MaxWait != 250*time.Millisecond {
		t.Errorf("want the wait carried, got %v", tuned.MaxWait)
	}
	if tuned.Count != 10 {
		t.Errorf("want the count carried, got %d", tuned.Count)
	}
	if tuned.MinCount > tuned.Count {
		t.Errorf("the batch bounds must not cross, got min %d of %d",
			tuned.MinCount, tuned.Count)
	}
	if tuned.Commit.Kind != iggcon.CommitManual {
		t.Error("nothing is committed unless the caller asks, so a handler with " +
			"side effects re-reads after a crash rather than skipping")
	}

	// The smallest useful read still leaves the bounds consistent.
	smallest := laser.ConsumerOptions{Count: 1}.PollOptions()
	if smallest.MinCount > smallest.Count {
		t.Errorf("want consistent bounds at a count of one, got min %d of %d",
			smallest.MinCount, smallest.Count)
	}

	// With no wait named, a read still bounds itself rather than blocking.
	plain := laser.ConsumerOptions{}.PollOptions()
	if plain.MaxWait != laser.DefaultConsumerPollWait {
		t.Errorf("want the default wait, got %v", plain.MaxWait)
	}

	auto := iggcon.DefaultAutoCommit()
	committing := laser.ConsumerOptions{AutoCommit: &auto}.PollOptions()
	if committing.Commit.Kind != iggcon.CommitAuto {
		t.Errorf("want auto-commit carried, got %v", committing.Commit.Kind)
	}
}
