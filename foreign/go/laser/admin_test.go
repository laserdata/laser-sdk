package laser_test

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
)

// adminServer answers the catalogue reads and records the patches.
type adminServer struct {
	clusterOfOne
	mutex sync.Mutex

	streams []iggcon.Stream
	topics  []iggcon.Topic
	groups  []iggcon.ConsumerGroup
	// details is what a topic read answers with, nil for a topic that is not
	// there.
	details *iggcon.TopicDetails
	// stream is what a stream read answers with, nil for one that is gone.
	stream *iggcon.StreamDetails
	// blank makes the stream read answer with neither details nor an error,
	// which a client is free to do and is not the same as a read that failed.
	blank bool

	streamUpdate *iggcon.StreamUpdate
	topicUpdate  *iggcon.TopicUpdate
	// calls records the partition and segment verbs with their counts.
	calls []string
	fail  error
}

func (server *adminServer) Close() error { return nil }

func (server *adminServer) GetStreams(_ context.Context) ([]iggcon.Stream, error) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	if server.fail != nil {
		return nil, server.fail
	}
	return server.streams, nil
}

func (server *adminServer) GetStream(
	_ context.Context, _ iggcon.Identifier,
) (*iggcon.StreamDetails, error) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	if server.fail != nil {
		return nil, server.fail
	}
	if server.blank {
		return nil, nil
	}
	if server.stream == nil {
		return nil, ierror.StreamNameNotFound{}
	}
	return server.stream, nil
}

func (server *adminServer) UpdateStream(
	_ context.Context, _ iggcon.Identifier, update iggcon.StreamUpdate,
) error {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	if server.fail != nil {
		return server.fail
	}
	server.streamUpdate = &update
	return nil
}

func (server *adminServer) GetTopics(
	_ context.Context, _ iggcon.Identifier,
) ([]iggcon.Topic, error) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	if server.fail != nil {
		return nil, server.fail
	}
	return server.topics, nil
}

func (server *adminServer) GetTopic(
	_ context.Context, _, _ iggcon.Identifier,
) (*iggcon.TopicDetails, error) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	if server.fail != nil {
		return nil, server.fail
	}
	if server.details == nil {
		return nil, ierror.TopicNameNotFound{}
	}
	return server.details, nil
}

func (server *adminServer) UpdateTopic(
	_ context.Context, _, _ iggcon.Identifier, update iggcon.TopicUpdate,
) error {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	if server.fail != nil {
		return server.fail
	}
	server.topicUpdate = &update
	return nil
}

func (server *adminServer) GetConsumerGroups(
	_ context.Context, _, _ iggcon.Identifier,
) ([]iggcon.ConsumerGroup, error) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	if server.fail != nil {
		return nil, server.fail
	}
	return server.groups, nil
}

func (server *adminServer) CreatePartitions(
	_ context.Context, _, _ iggcon.Identifier, count uint32,
) error {
	return server.verb("create-partitions", count)
}

func (server *adminServer) DeletePartitions(
	_ context.Context, _, _ iggcon.Identifier, count uint32,
) error {
	return server.verb("delete-partitions", count)
}

func (server *adminServer) DeleteSegments(
	_ context.Context, _, _ iggcon.Identifier, partition, count uint32,
) error {
	return server.verb("delete-segments", count)
}

func (server *adminServer) verb(name string, count uint32) error {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	if server.fail != nil {
		return server.fail
	}
	server.calls = append(server.calls, name+":"+itoaUint(count))
	return nil
}

func itoaUint(value uint32) string {
	if value == 0 {
		return "0"
	}
	var digits [10]byte
	position := len(digits)
	for value > 0 {
		position--
		digits[position] = byte('0' + value%10)
		value /= 10
	}
	return string(digits[position:])
}

func (server *adminServer) verbs() []string {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	return append([]string(nil), server.calls...)
}

func adminHandle(server *adminServer) *laser.Laser {
	return laser.FromClient(server, laser.Options{DefaultStream: "agents"})
}

func TestGivenAConnectionWhenStreamsAreListedThenShouldReportWhatTheServerKnows(t *testing.T) {
	server := &adminServer{streams: []iggcon.Stream{{
		Id: 3, Name: "agents", TopicsCount: 2, SizeBytes: 2048,
		MessagesCount: 12, CreatedAt: 1_700_000_000_123_456,
	}}}

	listed, err := adminHandle(server).Streams(context.Background())
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	want := laser.StreamInfo{
		ID: 3, Name: "agents", TopicsCount: 2, SizeBytes: 2048,
		MessagesCount: 12, CreatedAtMicros: 1_700_000_000_123_456,
	}
	if len(listed) != 1 || listed[0] != want {
		t.Fatalf("got %+v, want %+v", listed, want)
	}
}

func TestGivenAStreamWhenReadThenShouldReportItOrNothing(t *testing.T) {
	server := &adminServer{stream: &iggcon.StreamDetails{
		Stream: iggcon.Stream{Id: 3, Name: "agents", TopicsCount: 1},
	}}
	handle := adminHandle(server)

	info, err := handle.Stream("agents").Get(context.Background())
	if err != nil || info == nil || info.ID != 3 {
		t.Fatalf("got %+v %v", info, err)
	}
	exists, err := handle.Stream("agents").Exists(context.Background())
	if err != nil || !exists {
		t.Fatalf("got %v %v", exists, err)
	}

	// A stream that is not there is not an error: the caller asked whether
	// it exists, and the answer is no.
	server.stream = nil
	info, err = handle.Stream("agents").Get(context.Background())
	if err != nil || info != nil {
		t.Fatalf("got %+v %v", info, err)
	}
	exists, err = handle.Stream("agents").Exists(context.Background())
	if err != nil || exists {
		t.Fatalf("got %v %v", exists, err)
	}
}

func TestGivenAStreamWhenRenamedThenShouldSendTheNewName(t *testing.T) {
	server := &adminServer{}
	handle := adminHandle(server)

	if err := handle.Stream("agents").Rename(
		context.Background(), "workers"); err != nil {
		t.Fatalf("rename: %v", err)
	}
	if server.streamUpdate == nil || server.streamUpdate.Name == nil ||
		*server.streamUpdate.Name != "workers" {
		t.Fatalf("got %+v", server.streamUpdate)
	}
	// A rename to nothing is refused before it is sent: the server would
	// either refuse it or leave the stream unnameable.
	if err := handle.Stream("agents").Rename(context.Background(), ""); err == nil {
		t.Fatal("a stream needs a name")
	}
}

func TestGivenAStreamThatIsGoneWhenItsTopicsAreListedThenShouldSaySo(t *testing.T) {
	// The server lists a missing stream exactly the way it lists one with no
	// topics, and those are not the same answer.
	server := &adminServer{}
	_, err := adminHandle(server).Stream("agents").Topics(context.Background())
	if err == nil {
		t.Fatal("a stream that is not there must not read as empty")
	}

	server.stream = &iggcon.StreamDetails{Stream: iggcon.Stream{Name: "agents"}}
	listed, err := adminHandle(server).Stream("agents").Topics(context.Background())
	if err != nil || len(listed) != 0 {
		t.Fatalf("an existing stream with no topics is empty, got %+v %v", listed, err)
	}
}

func TestGivenATopicWhenListedThenShouldReportItsRetention(t *testing.T) {
	server := &adminServer{stream: &iggcon.StreamDetails{}, topics: []iggcon.Topic{
		{
			Id: 7, Name: "metrics", PartitionsCount: 3, Size: 2048,
			MessagesCount: 12, CreatedAt: 1_700_000_000_123_456,
			MessageExpiry: iggcon.Duration(time.Hour.Microseconds()),
			MaxTopicSize:  4096,
		},
		{Id: 8, Name: "events", MessageExpiry: iggcon.IggyExpiryNeverExpire,
			MaxTopicSize: math.MaxUint64},
		{Id: 9, Name: "audit", MessageExpiry: iggcon.IggyExpiryServerDefault},
	}}

	listed, err := adminHandle(server).Stream("agents").Topics(context.Background())
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(listed) != 3 {
		t.Fatalf("got %d topics", len(listed))
	}
	if listed[0].MessageExpiry != time.Hour || listed[0].MaxSizeBytes != 4096 ||
		listed[0].ID != 7 || listed[0].PartitionsCount != 3 ||
		listed[0].SizeBytes != 2048 || listed[0].MessagesCount != 12 ||
		listed[0].CreatedAtMicros != 1_700_000_000_123_456 {
		t.Fatalf("got %+v", listed[0])
	}
	// Never-expire and unlimited are not durations or caps, so neither reads
	// as one. Reporting the largest number this build can hold as a cap
	// would have a caller plan around a limit that is not there.
	if listed[1].MessageExpiry != 0 || listed[1].MaxSizeBytes != 0 {
		t.Fatalf("an unlimited topic reported %+v", listed[1])
	}
	if listed[2].MessageExpiry != 0 {
		t.Fatalf("a server default is not a duration, got %+v", listed[2])
	}
}

func TestGivenATopicWhenReadThenShouldReportItOrNothing(t *testing.T) {
	server := &adminServer{details: &iggcon.TopicDetails{
		Topic: iggcon.Topic{Id: 7, Name: "metrics", PartitionsCount: 2},
	}}
	handle := adminHandle(server)

	info, err := handle.Topic("metrics").Get(context.Background())
	if err != nil || info == nil || info.ID != 7 {
		t.Fatalf("got %+v %v", info, err)
	}
	exists, err := handle.Topic("metrics").Exists(context.Background())
	if err != nil || !exists {
		t.Fatalf("got %v %v", exists, err)
	}

	server.details = nil
	info, err = handle.Topic("metrics").Get(context.Background())
	if err != nil || info != nil {
		t.Fatalf("got %+v %v", info, err)
	}
	exists, err = handle.Topic("metrics").Exists(context.Background())
	if err != nil || exists {
		t.Fatalf("got %v %v", exists, err)
	}
}

func TestGivenATopicPatchWhenAppliedThenShouldSendOnlyWhatChanged(t *testing.T) {
	for _, probe := range []struct {
		name    string
		options laser.UpdateTopicOptions
		check   func(*testing.T, iggcon.TopicUpdate)
	}{
		{"a rename", laser.UpdateTopicOptions{Name: "renamed"},
			func(t *testing.T, update iggcon.TopicUpdate) {
				if update.Name == nil || *update.Name != "renamed" {
					t.Fatalf("got %+v", update)
				}
				if update.MessageExpiry != nil || update.MaxTopicSize != nil {
					t.Fatalf("an unset field must stay unset, got %+v", update)
				}
			}},
		{"an expiry", laser.UpdateTopicOptions{ExpireAfter: time.Hour},
			func(t *testing.T, update iggcon.TopicUpdate) {
				if update.MessageExpiry == nil ||
					*update.MessageExpiry != iggcon.Duration(time.Hour.Microseconds()) {
					t.Fatalf("got %+v", update)
				}
			}},
		{"never expiring", laser.UpdateTopicOptions{NeverExpire: true},
			func(t *testing.T, update iggcon.TopicUpdate) {
				if update.MessageExpiry == nil ||
					*update.MessageExpiry != iggcon.IggyExpiryNeverExpire {
					t.Fatalf("got %+v", update)
				}
			}},
		{"a size cap", laser.UpdateTopicOptions{MaxTopicBytes: 4096},
			func(t *testing.T, update iggcon.TopicUpdate) {
				if update.MaxTopicSize == nil || *update.MaxTopicSize != 4096 {
					t.Fatalf("got %+v", update)
				}
			}},
		{"an unlimited size", laser.UpdateTopicOptions{UnlimitedTopicSize: true},
			func(t *testing.T, update iggcon.TopicUpdate) {
				if update.MaxTopicSize == nil ||
					*update.MaxTopicSize != math.MaxUint64 {
					t.Fatalf("got %+v", update)
				}
			}},
	} {
		t.Run(probe.name, func(t *testing.T) {
			server := &adminServer{}
			if err := adminHandle(server).Topic("metrics").Update(
				context.Background(), probe.options); err != nil {
				t.Fatalf("update: %v", err)
			}
			if server.topicUpdate == nil {
				t.Fatal("nothing was sent")
			}
			probe.check(t, *server.topicUpdate)
		})
	}
}

func TestGivenATopicPatchThatContradictsItselfWhenAppliedThenShouldRefuseIt(t *testing.T) {
	// The caller asked for two different things and only it knows which it
	// meant, so neither is picked for it.
	for _, options := range []laser.UpdateTopicOptions{
		{ExpireAfter: time.Hour, NeverExpire: true},
		{MaxTopicBytes: 4096, UnlimitedTopicSize: true},
	} {
		server := &adminServer{}
		if err := adminHandle(server).Topic("metrics").Update(
			context.Background(), options); err == nil {
			t.Fatalf("%+v must be refused", options)
		}
		if server.topicUpdate != nil {
			t.Fatal("nothing must be sent")
		}
	}
}

func TestGivenATopicWhenItsPartitionsAreListedThenShouldReportThem(t *testing.T) {
	server := &adminServer{details: &iggcon.TopicDetails{
		Topic: iggcon.Topic{Id: 7, Name: "metrics"},
		Partitions: []iggcon.PartitionContract{{
			Id: 1, SegmentsCount: 2, CurrentOffset: 99, SizeBytes: 512,
			MessagesCount: 100, CreatedAt: 1_700_000_000_000_001,
		}},
	}}

	listed, err := adminHandle(server).Topic("metrics").Partitions(context.Background())
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	want := laser.PartitionInfo{
		ID: 1, SegmentsCount: 2, CurrentOffset: 99, SizeBytes: 512,
		MessagesCount: 100, CreatedAtMicros: 1_700_000_000_000_001,
	}
	if len(listed) != 1 || listed[0] != want {
		t.Fatalf("got %+v, want %+v", listed, want)
	}

	server.details = nil
	if _, err := adminHandle(server).Topic("metrics").Partitions(
		context.Background()); err == nil {
		t.Fatal("a topic that is not there must not read as empty")
	}
}

func TestGivenATopicWhenItsGroupsAreListedThenShouldReportThem(t *testing.T) {
	server := &adminServer{
		details: &iggcon.TopicDetails{Topic: iggcon.Topic{Name: "metrics"}},
		groups: []iggcon.ConsumerGroup{
			{Id: 1, Name: "readers", PartitionsCount: 3, MembersCount: 2},
		},
	}

	listed, err := adminHandle(server).Topic("metrics").ConsumerGroups(
		context.Background())
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	want := laser.ConsumerGroupSummary{
		ID: 1, Name: "readers", PartitionsCount: 3, MembersCount: 2,
	}
	if len(listed) != 1 || listed[0] != want {
		t.Fatalf("got %+v, want %+v", listed, want)
	}

	// An existing topic with no groups is empty; one that is gone is not.
	server.groups = nil
	listed, err = adminHandle(server).Topic("metrics").ConsumerGroups(
		context.Background())
	if err != nil || len(listed) != 0 {
		t.Fatalf("got %+v %v", listed, err)
	}
	server.details = nil
	if _, err := adminHandle(server).Topic("metrics").ConsumerGroups(
		context.Background()); err == nil {
		t.Fatal("a topic that is not there must not read as empty")
	}
}

func TestGivenACountOfZeroWhenPartitionsChangeThenShouldRefuseIt(t *testing.T) {
	server := &adminServer{}
	topic := adminHandle(server).Topic("metrics")

	// Zero would either do nothing or be refused by the server, and neither
	// tells the caller it asked for nothing.
	if err := topic.AddPartitions(context.Background(), 0); err == nil {
		t.Fatal("adding no partitions must be refused")
	}
	if err := topic.DeletePartitions(context.Background(), 0); err == nil {
		t.Fatal("deleting no partitions must be refused")
	}
	if err := topic.DeleteSegments(context.Background(), 0, 0); err == nil {
		t.Fatal("deleting no segments must be refused")
	}
	if verbs := server.verbs(); len(verbs) != 0 {
		t.Fatalf("nothing must be sent, got %v", verbs)
	}
}

func TestGivenACountWhenPartitionsChangeThenShouldSendIt(t *testing.T) {
	server := &adminServer{}
	topic := adminHandle(server).Topic("metrics")

	if err := topic.AddPartitions(context.Background(), 4); err != nil {
		t.Fatalf("add: %v", err)
	}
	if err := topic.DeletePartitions(context.Background(), 2); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := topic.DeleteSegments(context.Background(), 1, 3); err != nil {
		t.Fatalf("segments: %v", err)
	}
	want := []string{"create-partitions:4", "delete-partitions:2", "delete-segments:3"}
	verbs := server.verbs()
	if len(verbs) != 3 {
		t.Fatalf("got %v", verbs)
	}
	for index, verb := range want {
		if verbs[index] != verb {
			t.Fatalf("got %v, want %v", verbs, want)
		}
	}
}

func TestGivenAServerThatRefusesWhenAdministeringThenShouldReportIt(t *testing.T) {
	server := &adminServer{fail: ierror.Disconnected{}}
	handle := adminHandle(server)
	topic := handle.Topic("metrics")

	for name, call := range map[string]func() error{
		"streams": func() error {
			_, err := handle.Streams(context.Background())
			return err
		},
		"stream": func() error {
			_, err := handle.Stream("agents").Get(context.Background())
			return err
		},
		"rename": func() error {
			return handle.Stream("agents").Rename(context.Background(), "x")
		},
		"topics": func() error {
			_, err := handle.Stream("agents").Topics(context.Background())
			return err
		},
		"topic": func() error {
			_, err := topic.Get(context.Background())
			return err
		},
		"update": func() error {
			return topic.Update(context.Background(),
				laser.UpdateTopicOptions{Name: "x"})
		},
		"partitions": func() error {
			_, err := topic.Partitions(context.Background())
			return err
		},
		"add partitions":    func() error { return topic.AddPartitions(context.Background(), 1) },
		"delete partitions": func() error { return topic.DeletePartitions(context.Background(), 1) },
		"delete segments": func() error {
			return topic.DeleteSegments(context.Background(), 0, 1)
		},
		"groups": func() error {
			_, err := topic.ConsumerGroups(context.Background())
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := call()
			var transport *laser.TransportError
			if !errors.As(err, &transport) {
				t.Fatalf("want a TransportError, got %T: %v", err, err)
			}
		})
	}
}

func TestGivenNoStreamWhenAdministeringATopicThenShouldSaySo(t *testing.T) {
	handle := laser.FromClient(&adminServer{}, laser.Options{})
	if _, err := handle.Topic("metrics").Get(context.Background()); err == nil {
		t.Fatal("a topic needs a stream")
	}
}

func TestGivenAStreamReadThatAnswersWithNothingThenShouldReadAsAbsent(t *testing.T) {
	// A client is free to answer a missing stream with neither details nor
	// an error. Reading past that would dereference nothing at all.
	server := &adminServer{blank: true}
	handle := adminHandle(server)

	info, err := handle.Stream("agents").Get(context.Background())
	if err != nil || info != nil {
		t.Fatalf("got %+v %v", info, err)
	}
	if _, err := handle.Stream("agents").Topics(context.Background()); err == nil {
		t.Fatal("a stream that is not there must not read as empty")
	}
}
