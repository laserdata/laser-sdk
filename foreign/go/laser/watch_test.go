package laser_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// feedServer is a change-feed topic: records per partition, pollable by
// offset or from the end.
type feedServer struct {
	clusterOfOne
	guard      sync.Mutex
	partitions uint32
	// records are what each partition holds, keyed by partition.
	records map[uint32][]iggcon.IggyMessage
	// floor is the lowest offset each partition still holds, so a test can
	// model what retention removed.
	floor   map[uint32]uint64
	missing bool
	polls   int
	topics  []string
	streams []string
}

func newFeed(partitions uint32) *feedServer {
	return &feedServer{
		partitions: partitions,
		records:    map[uint32][]iggcon.IggyMessage{},
		floor:      map[uint32]uint64{},
	}
}

func (server *feedServer) Close() error { return nil }

// append puts one payload at the next offset of a partition.
func (server *feedServer) append(partition uint32, payload []byte) {
	server.guard.Lock()
	defer server.guard.Unlock()
	offset := server.floor[partition] + uint64(len(server.records[partition]))
	message, err := iggcon.NewIggyMessage(payload)
	if err != nil {
		panic(err)
	}
	message.Header.Offset = offset
	message.Header.Timestamp = uint64(len(server.records[partition]) + 1)
	// A retried batch republishes its change under the same id, so a feed
	// record's id has to be its own rather than the zero the builder leaves.
	message.Header.Id[0] = byte(partition)
	message.Header.Id[1] = byte(offset)
	server.records[partition] = append(server.records[partition], message)
}

// restamp replaces the server timestamps of a partition's records, so a test
// can model a clock that did not advance monotonically.
func (server *feedServer) restamp(partition uint32, timestamps []uint64) {
	server.guard.Lock()
	defer server.guard.Unlock()
	for index := range server.records[partition] {
		if index < len(timestamps) {
			server.records[partition][index].Header.Timestamp = timestamps[index]
		}
	}
}

// expireBelow models retention removing everything under an offset.
func (server *feedServer) expireBelow(partition uint32, offset uint64) {
	server.guard.Lock()
	defer server.guard.Unlock()
	kept := server.records[partition][:0]
	for _, message := range server.records[partition] {
		if message.Header.Offset >= offset {
			kept = append(kept, message)
		}
	}
	server.records[partition] = append([]iggcon.IggyMessage(nil), kept...)
	server.floor[partition] = offset
}

func (server *feedServer) GetTopic(
	ctx context.Context, stream iggcon.Identifier, topic iggcon.Identifier,
) (*iggcon.TopicDetails, error) {
	// A real client stops on a cancelled caller, so the fake does too.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	named, err := topic.String()
	if err != nil {
		return nil, err
	}
	on, err := stream.String()
	if err != nil {
		return nil, err
	}
	server.guard.Lock()
	defer server.guard.Unlock()
	server.topics = append(server.topics, named)
	server.streams = append(server.streams, on)
	if server.missing {
		return nil, nil
	}
	return &iggcon.TopicDetails{
		Topic: iggcon.Topic{Id: feedTopicID, PartitionsCount: server.partitions},
	}, nil
}

// feedTopicID is the numeric id the feed fake gives every topic.
const feedTopicID uint32 = 3

func (server *feedServer) PollMessages(
	ctx context.Context, _ iggcon.Identifier, _ iggcon.Identifier, partition uint32,
	_ iggcon.Consumer, strategy iggcon.PollingStrategy, options iggcon.PollOptions,
) (*iggcon.PolledMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	server.guard.Lock()
	defer server.guard.Unlock()
	server.polls++
	held := server.records[partition]
	if strategy.Kind == iggcon.POLLING_LAST {
		if len(held) == 0 {
			return &iggcon.PolledMessage{PartitionId: partition}, nil
		}
		return &iggcon.PolledMessage{
			PartitionId: partition,
			Messages:    []iggcon.IggyMessage{held[len(held)-1]},
		}, nil
	}
	var from []iggcon.IggyMessage
	for _, message := range held {
		if message.Header.Offset >= strategy.Value {
			from = append(from, message)
		}
	}
	if uint32(len(from)) > options.Count {
		from = from[:options.Count]
	}
	return &iggcon.PolledMessage{PartitionId: partition, Messages: from}, nil
}

// visited is every topic the reader looked up.
func (server *feedServer) visited() []string {
	server.guard.Lock()
	defer server.guard.Unlock()
	return append([]string(nil), server.topics...)
}

// visitedStreams is the stream of every topic the reader looked up.
func (server *feedServer) visitedStreams() []string {
	server.guard.Lock()
	defer server.guard.Unlock()
	return append([]string(nil), server.streams...)
}

func watchCapabilities() *laser.Capabilities {
	capabilities := laser.OpenCapabilities()
	capabilities.Managed = true
	capabilities.Hello = laser.HelloAnswered
	capabilities.Watch = true
	return &capabilities
}

func watchHandle(server iggcon.Client, options laser.Options) *laser.Laser {
	if options.Capabilities == nil {
		options.Capabilities = watchCapabilities()
	}
	if options.DefaultStream == "" {
		options.DefaultStream = "agents"
	}
	return laser.FromClient(server, options)
}

// changeBytes is one change record on the feed.
func changeBytes(t *testing.T, index string, to uint64, version uint32) []byte {
	t.Helper()
	encoded, err := wire.Marshal(wire.ChangeRecord{
		V: version, Index: index, PartitionID: 0,
		FromOffset: to, ToOffset: to, Rows: 1,
	})
	if err != nil {
		t.Fatalf("encode a change: %v", err)
	}
	return encoded
}

// A change record says a projector batch landed, which turns "query after my
// data arrives" from sleep-and-retry into await-then-query.
func TestGivenChangesOnTheFeedWhenPolledThenShouldReturnThemInOrder(t *testing.T) {
	server := newFeed(1)
	server.append(0, changeBytes(t, "stream:agents/readings", 10, wire.ChangeOpVersion))
	server.append(0, changeBytes(t, "stream:agents/readings", 20, wire.ChangeOpVersion))
	reader, err := watchHandle(server, laser.Options{}).Watch().
		Records(context.Background())
	if err != nil {
		t.Fatalf("records: %v", err)
	}

	changes, err := reader.Poll(context.Background())
	if err != nil {
		t.Fatalf("poll: %v", err)
	}

	if len(changes) != 2 {
		t.Fatalf("want both changes, got %d", len(changes))
	}
	if changes[0].Change.ToOffset != 10 || changes[1].Change.ToOffset != 20 {
		t.Errorf("want them in feed order, got %d then %d",
			changes[0].Change.ToOffset, changes[1].Change.ToOffset)
	}
	// The feed position is what a reader resumes from, and the message id is
	// how it drops a change a retried batch published twice.
	if changes[0].Offset != 0 || changes[1].Offset != 1 {
		t.Errorf("want the feed offsets, got %d and %d",
			changes[0].Offset, changes[1].Offset)
	}
	if changes[0].MessageID == changes[1].MessageID {
		t.Error("two feed records must not share an id")
	}
	// Caught up, so a second poll returns nothing rather than repeating.
	again, err := reader.Poll(context.Background())
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if len(again) != 0 {
		t.Errorf("want nothing new, got %d", len(again))
	}
}

// A reader watching one index does not want another's advancements, and the
// narrowing is by the name every other managed resource takes.
func TestGivenAnIndexFilterWhenPolledThenShouldKeepOnlyThatIndex(t *testing.T) {
	server := newFeed(1)
	server.append(0, changeBytes(t, "stream:agents/readings", 10, wire.ChangeOpVersion))
	server.append(0, changeBytes(t, "stream:agents/elsewhere", 11, wire.ChangeOpVersion))
	server.append(0, changeBytes(t, "stream:agents/readings", 12, wire.ChangeOpVersion))
	reader, err := watchHandle(server, laser.Options{}).Watch().
		Index("readings").Records(context.Background())
	if err != nil {
		t.Fatalf("records: %v", err)
	}

	changes, err := reader.Poll(context.Background())
	if err != nil {
		t.Fatalf("poll: %v", err)
	}

	if len(changes) != 2 {
		t.Fatalf("want this index alone, got %d", len(changes))
	}
	for _, change := range changes {
		if change.Change.Index != "stream:agents/readings" {
			t.Errorf("want only the watched index, got %q", change.Change.Index)
		}
	}
	// Another index is not other traffic: it was read and understood.
	if reader.Skipped() != 0 {
		t.Errorf("want nothing skipped, got %d", reader.Skipped())
	}
}

// A reader that starts at the end receives only what is published after it
// opens, for a caller waiting on its own write rather than catching up.
func TestGivenAReaderFromTheEndWhenPolledThenShouldSeeOnlyLaterChanges(t *testing.T) {
	server := newFeed(1)
	server.append(0, changeBytes(t, "stream:agents/readings", 1, wire.ChangeOpVersion))
	server.append(0, changeBytes(t, "stream:agents/readings", 2, wire.ChangeOpVersion))
	reader, err := watchHandle(server, laser.Options{}).Watch().
		FromEnd().Records(context.Background())
	if err != nil {
		t.Fatalf("records: %v", err)
	}

	before, err := reader.Poll(context.Background())
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if len(before) != 0 {
		t.Fatalf("want nothing from before it opened, got %d", len(before))
	}

	server.append(0, changeBytes(t, "stream:agents/readings", 3, wire.ChangeOpVersion))
	after, err := reader.Poll(context.Background())
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if len(after) != 1 || after[0].Change.ToOffset != 3 {
		t.Fatalf("want the later change alone, got %+v", after)
	}
	// Nothing was missed: the records before it opened were never its to
	// read.
	if reader.Missed() != 0 {
		t.Errorf("want nothing missed, got %d", reader.Missed())
	}
}

// A partition that held nothing when the reader opened at its end has no
// earlier records, so its first record says nothing about records missed.
func TestGivenAnEmptyPartitionAtTheEndWhenItFillsThenShouldMissNothing(t *testing.T) {
	server := newFeed(1)
	reader, err := watchHandle(server, laser.Options{}).Watch().
		FromEnd().Records(context.Background())
	if err != nil {
		t.Fatalf("records: %v", err)
	}

	server.append(0, changeBytes(t, "stream:agents/readings", 1, wire.ChangeOpVersion))
	changes, err := reader.Poll(context.Background())
	if err != nil {
		t.Fatalf("poll: %v", err)
	}

	if len(changes) != 1 {
		t.Fatalf("want the first change, got %d", len(changes))
	}
	if reader.Missed() != 0 {
		t.Errorf("an empty partition misses nothing, got %d", reader.Missed())
	}
}

// A partition that was empty when the reader opened at its end, but whose
// earlier records retention had already removed, still misses nothing: its
// first record is the first record it was ever going to get.
func TestGivenAnEmptiedPartitionAtTheEndWhenItFillsThenShouldMissNothing(t *testing.T) {
	server := newFeed(1)
	// Retention removed everything, so the partition holds nothing and will
	// write next at three.
	server.expireBelow(0, 3)
	reader, err := watchHandle(server, laser.Options{}).Watch().
		FromEnd().Records(context.Background())
	if err != nil {
		t.Fatalf("records: %v", err)
	}

	server.append(0, changeBytes(t, "stream:agents/readings", 1, wire.ChangeOpVersion))
	changes, err := reader.Poll(context.Background())
	if err != nil {
		t.Fatalf("poll: %v", err)
	}

	if len(changes) != 1 || changes[0].Offset != 3 {
		t.Fatalf("want the record at three, got %+v", changes)
	}
	// Without anchoring, the gap between zero and three would read as three
	// changes this reader missed. It missed none: they were gone before it
	// existed.
	if reader.Missed() != 0 {
		t.Errorf("want nothing missed, got %d", reader.Missed())
	}
}

// Seeding a reader with stored offsets replaces where it starts, so a
// partition it had anchored at the end is measured from the stored offset
// instead.
func TestGivenStoredOffsetsAfterStartingAtTheEndWhenResumedThenShouldMeasureFromThem(t *testing.T) {
	server := newFeed(1)
	server.expireBelow(0, 3)
	reader, err := watchHandle(server, laser.Options{}).Watch().
		FromEnd().Records(context.Background())
	if err != nil {
		t.Fatalf("records: %v", err)
	}

	// The caller asked to resume from zero, so the records retention removed
	// are records it asked for and did not get.
	server.append(0, changeBytes(t, "stream:agents/readings", 1, wire.ChangeOpVersion))
	if _, err := reader.FromOffsets([]uint64{0}).
		Poll(context.Background()); err != nil {
		t.Fatalf("poll: %v", err)
	}

	if reader.Missed() != 3 {
		t.Errorf("want the gap counted from the stored offset, got %d",
			reader.Missed())
	}
}

// Retention removing records before a resumed reader reached them means
// changes were missed, so the reader says so rather than reporting a clean
// catch-up.
func TestGivenExpiredRecordsWhenAResumedReaderPollsThenShouldCountThemMissed(t *testing.T) {
	server := newFeed(1)
	for offset := range 5 {
		server.append(0, changeBytes(t, "stream:agents/readings",
			uint64(offset), wire.ChangeOpVersion))
	}
	server.expireBelow(0, 3)
	reader, err := watchHandle(server, laser.Options{}).Watch().
		Records(context.Background())
	if err != nil {
		t.Fatalf("records: %v", err)
	}

	changes, err := reader.FromOffsets([]uint64{0}).Poll(context.Background())
	if err != nil {
		t.Fatalf("poll: %v", err)
	}

	if len(changes) != 2 {
		t.Fatalf("want what survived, got %d", len(changes))
	}
	// It resumed at zero and the first record it got was at three, so three
	// were removed before it reached them.
	if reader.Missed() != 3 {
		t.Errorf("want three missed, got %d", reader.Missed())
	}
}

// The changes topic is overridable, so other traffic on it must not wedge the
// reader: it is counted and stepped over.
func TestGivenOtherTrafficOnTheFeedWhenPolledThenShouldSkipIt(t *testing.T) {
	server := newFeed(1)
	server.append(0, []byte("not a change record at all"))
	noVersion, err := wire.Marshal(map[string]string{"note": "also not one"})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	server.append(0, noVersion)
	server.append(0, changeBytes(t, "stream:agents/readings", 10, wire.ChangeOpVersion))
	reader, err := watchHandle(server, laser.Options{}).Watch().
		Records(context.Background())
	if err != nil {
		t.Fatalf("records: %v", err)
	}

	changes, err := reader.Poll(context.Background())
	if err != nil {
		t.Fatalf("poll: %v", err)
	}

	if len(changes) != 1 || changes[0].Change.ToOffset != 10 {
		t.Fatalf("want the change past the noise, got %+v", changes)
	}
	if reader.Skipped() != 2 {
		t.Errorf("want both non-records counted, got %d", reader.Skipped())
	}
}

// A change record of another version is one this build cannot read, so the
// partition stops at it rather than stepping over it: the next poll resumes
// there once the build understands it.
func TestGivenANewerChangeRecordWhenPolledThenShouldStopAtIt(t *testing.T) {
	server := newFeed(1)
	server.append(0, changeBytes(t, "stream:agents/readings", 10, wire.ChangeOpVersion))
	server.append(0, changeBytes(t, "stream:agents/readings", 11, wire.ChangeOpVersion+1))
	server.append(0, changeBytes(t, "stream:agents/readings", 12, wire.ChangeOpVersion))
	reader, err := watchHandle(server, laser.Options{}).Watch().
		Records(context.Background())
	if err != nil {
		t.Fatalf("records: %v", err)
	}

	// The changes before it come back.
	changes, err := reader.Poll(context.Background())
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if len(changes) != 1 || changes[0].Change.ToOffset != 10 {
		t.Fatalf("want the readable change alone, got %+v", changes)
	}
	// It stopped at the record rather than past it.
	if offsets := reader.Offsets(); len(offsets) != 1 || offsets[0] != 1 {
		t.Errorf("want the reader parked at the newer record, got %v", offsets)
	}

	// A poll with nothing else to return says why, so a build never misreads
	// a newer record as readable.
	_, err = reader.Poll(context.Background())
	var skew *laser.VersionError
	if !errors.As(err, &skew) {
		t.Fatalf("want a VersionError, got %T: %v", err, err)
	}
	if skew.Served != wire.ChangeOpVersion+1 {
		t.Errorf("want the version it found, got %d", skew.Served)
	}
}

// A change record claiming version zero is still a change record, so it stops
// its partition rather than being skipped as other traffic.
func TestGivenAVersionZeroChangeWhenPolledThenShouldStopRatherThanSkip(t *testing.T) {
	server := newFeed(1)
	server.append(0, changeBytes(t, "stream:agents/readings", 10, 0))
	reader, err := watchHandle(server, laser.Options{}).Watch().
		Records(context.Background())
	if err != nil {
		t.Fatalf("records: %v", err)
	}

	_, err = reader.Poll(context.Background())

	var skew *laser.VersionError
	if !errors.As(err, &skew) {
		t.Fatalf("want a VersionError, got %T: %v", err, err)
	}
	if reader.Skipped() != 0 {
		t.Error("a change record is not other traffic, whatever version it claims")
	}
}

// A deployment that does not publish the feed is refused rather than left
// polling a topic nothing writes to: waiting on silence looks identical to
// waiting on work that never comes.
func TestGivenNoFeedWhenWatchingThenShouldRefuseIt(t *testing.T) {
	capabilities := watchCapabilities()
	capabilities.Watch = false
	server := newFeed(1)

	_, err := watchHandle(server, laser.Options{Capabilities: capabilities}).
		Watch().Records(context.Background())

	var unsupported *laser.UnsupportedError
	if !errors.As(err, &unsupported) {
		t.Fatalf("want an UnsupportedError, got %T: %v", err, err)
	}
}

// Under stream tenancy each stream's feed rides its own ops topic, so a
// scoped handle reads its own rather than a shared one it may not see.
func TestGivenStreamTenancyWhenWatchingThenShouldReadItsOwnFeedTopic(t *testing.T) {
	capabilities := watchCapabilities()
	capabilities.StreamTenancy = true
	server := newFeed(1)
	reader, err := watchHandle(server, laser.Options{Capabilities: capabilities}).
		Watch().FromEnd().Records(context.Background())
	if err != nil {
		t.Fatalf("records: %v", err)
	}
	_ = reader

	visited := server.visited()
	if len(visited) == 0 {
		t.Fatal("want the feed topic looked up")
	}
	want := wire.StreamOpsTopic("agents", wire.ChangesTopic)
	if visited[0] != want {
		t.Errorf("want %q, got %q", want, visited[0])
	}

	// Without tenancy it is the shared topic.
	shared := newFeed(1)
	if _, err := watchHandle(shared, laser.Options{}).Watch().
		FromEnd().Records(context.Background()); err != nil {
		t.Fatalf("records: %v", err)
	}
	if shared.visited()[0] != wire.ChangesTopic {
		t.Errorf("want the shared topic, got %q", shared.visited()[0])
	}
}

func TestGivenAnOverriddenOpsStreamAndChangesTopicWhenWatchingThenShouldReadTheFeedThere(t *testing.T) {
	server := newFeed(1)
	handle := watchHandle(server, laser.Options{OpsStream: "acme-ops"}).WithChangesTopic("acme-changes")

	if _, err := handle.Watch().FromEnd().Records(context.Background()); err != nil {
		t.Fatalf("records: %v", err)
	}

	got := [2]string{server.visitedStreams()[0], server.visited()[0]}
	want := [2]string{"acme-ops", "acme-changes"}
	if got != want {
		t.Errorf("want the feed read on %v, got %v", want, got)
	}
}

func TestGivenStreamTenancyAndAnOverriddenTopologyWhenWatchingThenShouldReadTheStreamTopicOnTheOverriddenOpsStream(t *testing.T) {
	capabilities := watchCapabilities()
	capabilities.StreamTenancy = true
	server := newFeed(1)
	handle := watchHandle(server, laser.Options{
		Capabilities: capabilities, OpsStream: "acme-ops", ChangesTopic: "acme-changes",
	})

	if _, err := handle.Watch().FromEnd().Records(context.Background()); err != nil {
		t.Fatalf("records: %v", err)
	}

	got := [2]string{server.visitedStreams()[0], server.visited()[0]}
	want := [2]string{"acme-ops", wire.StreamOpsTopic("agents", wire.ChangesTopic)}
	if got != want {
		t.Errorf("want the feed read on %v, got %v", want, got)
	}
}

// A feed nobody has written to yet is not an error: a reader started before
// its writer starts at zero, which is both the end and the beginning.
func TestGivenAFeedThatDoesNotExistWhenWatchingThenShouldStartAtZero(t *testing.T) {
	server := newFeed(1)
	server.missing = true

	reader, err := watchHandle(server, laser.Options{}).Watch().
		FromEnd().Records(context.Background())
	if err != nil {
		t.Fatalf("records: %v", err)
	}

	if offsets := reader.Offsets(); len(offsets) != 0 {
		t.Errorf("want no seeded offsets, got %v", offsets)
	}
	changes, err := reader.Poll(context.Background())
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if len(changes) != 0 {
		t.Errorf("want nothing, got %d", len(changes))
	}
}

// Offsets are the resume state, so a fresh reader seeded with them picks up
// exactly where the last one stopped.
func TestGivenStoredOffsetsWhenResumedThenShouldContinueWhereItStopped(t *testing.T) {
	server := newFeed(1)
	server.append(0, changeBytes(t, "stream:agents/readings", 10, wire.ChangeOpVersion))
	server.append(0, changeBytes(t, "stream:agents/readings", 20, wire.ChangeOpVersion))
	handle := watchHandle(server, laser.Options{})
	first, err := handle.Watch().Records(context.Background())
	if err != nil {
		t.Fatalf("records: %v", err)
	}
	if _, err := first.Poll(context.Background()); err != nil {
		t.Fatalf("poll: %v", err)
	}
	stored := first.Offsets()

	server.append(0, changeBytes(t, "stream:agents/readings", 30, wire.ChangeOpVersion))
	resumed, err := handle.Watch().Records(context.Background())
	if err != nil {
		t.Fatalf("records: %v", err)
	}
	changes, err := resumed.FromOffsets(stored).Poll(context.Background())
	if err != nil {
		t.Fatalf("poll: %v", err)
	}

	if len(changes) != 1 || changes[0].Change.ToOffset != 30 {
		t.Fatalf("want only what landed since, got %+v", changes)
	}
	if resumed.Missed() != 0 {
		t.Errorf("a clean resume misses nothing, got %d", resumed.Missed())
	}
}

// A bounded wait returns as soon as the first change lands, and an empty
// batch when none does, so a caller can await its own write.
func TestGivenABoundedWaitWhenAChangeLandsThenShouldReturnAtOnce(t *testing.T) {
	server := newFeed(1)
	reader, err := watchHandle(server, laser.Options{}).Watch().
		Records(context.Background())
	if err != nil {
		t.Fatalf("records: %v", err)
	}

	empty, err := reader.PollWithin(context.Background(), 60*time.Millisecond)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	if len(empty) != 0 {
		t.Errorf("want nothing in time, got %d", len(empty))
	}

	go func() {
		time.Sleep(20 * time.Millisecond)
		server.append(0, changeBytes(t, "stream:agents/readings", 10,
			wire.ChangeOpVersion))
	}()
	changes, err := reader.PollWithin(context.Background(), 5*time.Second)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	if len(changes) != 1 {
		t.Errorf("want the change that landed, got %d", len(changes))
	}
}

// A caller that gives up stops the wait rather than running out its bound.
func TestGivenACancelledCallerWhenWaitingOnTheFeedThenShouldStop(t *testing.T) {
	server := newFeed(1)
	reader, err := watchHandle(server, laser.Options{}).Watch().
		Records(context.Background())
	if err != nil {
		t.Fatalf("records: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	_, err = reader.PollWithin(ctx, time.Hour)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want the caller's deadline, got %v", err)
	}
}

// Each walks the feed until it is caught up, and stops where the caller says.
func TestGivenTheFeedWhenWalkedThenShouldStopWhereTheCallerSays(t *testing.T) {
	server := newFeed(1)
	for offset := range 3 {
		server.append(0, changeBytes(t, "stream:agents/readings",
			uint64(offset), wire.ChangeOpVersion))
	}
	reader, err := watchHandle(server, laser.Options{}).Watch().
		Records(context.Background())
	if err != nil {
		t.Fatalf("records: %v", err)
	}

	seen := 0
	if err := reader.Each(context.Background(), func(laser.WatchedChange) bool {
		seen++
		return seen < 2
	}); err != nil {
		t.Fatalf("each: %v", err)
	}
	if seen != 2 {
		t.Errorf("want the walk stopped at two, got %d", seen)
	}

	// Stopping early leaves the reader at the change it did not take, so
	// the rest of the feed is still there to read.
	caught := 0
	if err := reader.Each(context.Background(), func(laser.WatchedChange) bool {
		caught++
		return true
	}); err != nil {
		t.Fatalf("each: %v", err)
	}
	if caught != 1 {
		t.Errorf("want the rest of the feed, got %d", caught)
	}
}

// A rewind positions at the earliest change left, not at whichever one the
// remainder happens to list first, so a feed whose timestamps do not advance
// monotonically cannot make a stop skip a change.
func TestGivenUnorderedTimestampsWhenAStopRewindsThenShouldTakeTheEarliest(t *testing.T) {
	server := newFeed(1)
	for offset := range 3 {
		server.append(0, changeBytes(t, "stream:agents/readings",
			uint64(offset), wire.ChangeOpVersion))
	}
	// The server's clock went backwards across the batch, so feed order and
	// offset order disagree.
	server.restamp(0, []uint64{3, 2, 1})
	reader, err := watchHandle(server, laser.Options{}).Watch().
		Records(context.Background())
	if err != nil {
		t.Fatalf("records: %v", err)
	}

	// Stop after the first change the walk hands over, which is the one at
	// the highest offset because the clock ran backwards.
	var taken laser.WatchedChange
	if err := reader.Each(context.Background(), func(change laser.WatchedChange) bool {
		taken = change
		return false
	}); err != nil {
		t.Fatalf("each: %v", err)
	}
	if taken.Offset != 2 {
		t.Fatalf("want the walk to start at the newest stamp, got %d", taken.Offset)
	}

	// Everything it did not take is still there. A change it already took may
	// come back with it, which is what the feed's message ids are for: the
	// alternative is rewinding past a change nobody read.
	rest, err := reader.Poll(context.Background())
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	left := map[uint64]bool{}
	for _, change := range rest {
		left[change.Offset] = true
	}
	for _, offset := range []uint64{0, 1} {
		if !left[offset] {
			t.Errorf("want the change at %d still there, got %v", offset, left)
		}
	}
}

// Several partitions are drained together, so a reader sees every
// partition's changes rather than only the first one's.
func TestGivenSeveralPartitionsWhenPolledThenShouldDrainThemAll(t *testing.T) {
	server := newFeed(3)
	for partition := uint32(0); partition < 3; partition++ {
		server.append(partition, changeBytes(t, "stream:agents/readings",
			uint64(partition), wire.ChangeOpVersion))
	}
	reader, err := watchHandle(server, laser.Options{}).Watch().
		Records(context.Background())
	if err != nil {
		t.Fatalf("records: %v", err)
	}

	changes, err := reader.Poll(context.Background())
	if err != nil {
		t.Fatalf("poll: %v", err)
	}

	if len(changes) != 3 {
		t.Fatalf("want every partition drained, got %d", len(changes))
	}
	seen := map[uint32]bool{}
	for _, change := range changes {
		seen[change.PartitionID] = true
	}
	if len(seen) != 3 {
		t.Errorf("want one change per partition, got %v", seen)
	}
}

// A newer record on one partition must not stop the others, because each
// partition's progress is its own.
func TestGivenANewerRecordOnOnePartitionWhenPolledThenShouldDrainTheOthers(t *testing.T) {
	server := newFeed(2)
	server.append(0, changeBytes(t, "stream:agents/readings", 1, wire.ChangeOpVersion+1))
	server.append(1, changeBytes(t, "stream:agents/readings", 2, wire.ChangeOpVersion))
	reader, err := watchHandle(server, laser.Options{}).Watch().
		Records(context.Background())
	if err != nil {
		t.Fatalf("records: %v", err)
	}

	changes, err := reader.Poll(context.Background())
	if err != nil {
		t.Fatalf("poll: %v", err)
	}

	if len(changes) != 1 || changes[0].PartitionID != 1 {
		t.Fatalf("want the readable partition drained, got %+v", changes)
	}
	// The stopped partition stays parked where it is, and the other moved on.
	offsets := reader.Offsets()
	if len(offsets) != 2 || offsets[0] != 0 || offsets[1] != 1 {
		t.Errorf("want one parked and one advanced, got %v", offsets)
	}
}
