package laser_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// consumerServer is a client the partition reader can read through. The reader
// resolves its fetch path by type assertion, so this stand-in implements that
// seam as well as the admin calls a consumer makes.
type consumerServer struct {
	clusterOfOne
	mutex  sync.Mutex
	joined string
	stored []iggcon.ConsumerPosition
	// created is the consumer groups the client asked the server to make,
	// so a test can see that joining a group creates it when absent.
	created []string
}

func (server *consumerServer) Close() error { return nil }

// CreateConsumerGroup records the create a join performs when the group may
// not be there yet.
func (server *consumerServer) CreateConsumerGroup(
	_ context.Context, _, _ iggcon.Identifier, name string,
) (*iggcon.ConsumerGroupDetails, error) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	server.created = append(server.created, name)
	return &iggcon.ConsumerGroupDetails{}, nil
}

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

// storedOffsetServer answers the three stored-offset calls and records them.
type storedOffsetServer struct {
	consumerServer
	guard   sync.Mutex
	stores  []uint64
	deletes int
	held    *iggcon.ConsumerOffsetInfo
	missing bool
}

func (server *storedOffsetServer) StoreConsumerOffset(
	_ context.Context, _ iggcon.Consumer, _, _ iggcon.Identifier,
	offset uint64, _ *uint32,
) error {
	server.guard.Lock()
	defer server.guard.Unlock()
	server.stores = append(server.stores, offset)
	return nil
}

func (server *storedOffsetServer) GetConsumerOffset(
	_ context.Context, _ iggcon.Consumer, _, _ iggcon.Identifier, _ *uint32,
) (*iggcon.ConsumerOffsetInfo, error) {
	if server.missing {
		return nil, ierror.ConsumerOffsetNotFound{}
	}
	return server.held, nil
}

func (server *storedOffsetServer) DeleteConsumerOffset(
	_ context.Context, _ iggcon.Consumer, _, _ iggcon.Identifier, _ *uint32,
) error {
	server.guard.Lock()
	defer server.guard.Unlock()
	if server.missing {
		return ierror.ConsumerOffsetNotFound{}
	}
	server.deletes++
	return nil
}

func storedOffsetConsumer(t *testing.T, server iggcon.Client) *laser.Consumer {
	t.Helper()
	consumer, err := streamHandle(server, laser.Options{DefaultStream: "agents"}).
		Topic("inferences").Consumer(context.Background(), "worker",
		laser.ConsumerOptions{Partitions: []uint32{1}})
	if err != nil {
		t.Fatalf("open the consumer: %v", err)
	}
	t.Cleanup(func() { _ = consumer.Close(context.Background()) })
	return consumer
}

func TestGivenAStoredOffsetWhenReadThenShouldReportItWithThePartitionHead(t *testing.T) {
	server := &storedOffsetServer{held: &iggcon.ConsumerOffsetInfo{
		PartitionId: 1, StoredOffset: 40, CurrentOffset: 57}}
	consumer := storedOffsetConsumer(t, server)

	stored, err := consumer.StoredOffset(context.Background(), 1)
	if err != nil {
		t.Fatalf("read the stored offset: %v", err)
	}

	// The head comes back with it, or a caller cannot tell a consumer that
	// is caught up from one that is far behind.
	if stored == nil || stored.Offset != 40 || stored.CurrentOffset != 57 {
		t.Fatalf("want 40 of 57, got %+v", stored)
	}
}

func TestGivenNoStoredOffsetWhenReadThenShouldReportAbsenceRatherThanZero(t *testing.T) {
	consumer := storedOffsetConsumer(t, &storedOffsetServer{missing: true})

	stored, err := consumer.StoredOffset(context.Background(), 1)
	if err != nil {
		t.Fatalf("read the stored offset: %v", err)
	}

	// Offset zero is a stored position like any other, so absence cannot be
	// reported as a zero a caller would resume from.
	if stored != nil {
		t.Fatalf("want absence, got %+v", stored)
	}
}

func TestGivenAnExplicitOffsetWhenStoredThenShouldSendItToTheServer(t *testing.T) {
	server := &storedOffsetServer{}
	consumer := storedOffsetConsumer(t, server)

	if err := consumer.StoreOffset(context.Background(), 40, nil); err != nil {
		t.Fatalf("store the offset: %v", err)
	}

	server.guard.Lock()
	defer server.guard.Unlock()
	if len(server.stores) != 1 || server.stores[0] != 40 {
		t.Fatalf("want offset 40 stored, got %v", server.stores)
	}
}

func TestGivenAStoredOffsetWhenDeletedThenShouldRemoveIt(t *testing.T) {
	server := &storedOffsetServer{}
	consumer := storedOffsetConsumer(t, server)

	if err := consumer.DeleteOffset(context.Background(), nil); err != nil {
		t.Fatalf("delete the offset: %v", err)
	}

	server.guard.Lock()
	defer server.guard.Unlock()
	if server.deletes != 1 {
		t.Fatalf("want one delete, got %d", server.deletes)
	}
}

func TestGivenNoStoredOffsetWhenDeletedThenShouldReadAsAlreadyGone(t *testing.T) {
	consumer := storedOffsetConsumer(t, &storedOffsetServer{missing: true})

	// Deleting what is not there is the state the caller asked for, which is
	// what lets a cleanup run twice without being special-cased.
	if err := consumer.DeleteOffset(context.Background(), nil); err != nil {
		t.Fatalf("want absence read as done, got %v", err)
	}
}

// A server holds no consumer group until someone makes one, so joining has
// to create it: a join that only joined answered "consumer group not found"
// against a real deployment, which no in-process fake ever showed.
func TestGivenAnAbsentConsumerGroupWhenJoinedThenShouldCreateItFirst(t *testing.T) {
	server := &consumerServer{}
	handle := streamHandle(server, laser.Options{DefaultStream: "agents"})

	if _, err := handle.Stream("agents").Topic("jobs").
		ConsumerGroup(context.Background(), "workers", laser.ConsumerOptions{}); err != nil {
		t.Fatalf("join the group: %v", err)
	}
	if len(server.created) != 1 || server.created[0] != "workers" {
		t.Errorf("want the group created before joining, got %v", server.created)
	}
}

// Where only an operator may define the group, a member joins without
// bringing one into being.
func TestGivenSkipGroupCreateWhenJoinedThenShouldNotCreateIt(t *testing.T) {
	server := &consumerServer{}
	handle := streamHandle(server, laser.Options{DefaultStream: "agents"})

	if _, err := handle.Stream("agents").Topic("jobs").
		ConsumerGroup(context.Background(), "workers", laser.ConsumerOptions{
			SkipGroupCreate: true,
		}); err != nil {
		t.Fatalf("join the group: %v", err)
	}
	if len(server.created) != 0 {
		t.Errorf("want no group created, got %v", server.created)
	}
}
