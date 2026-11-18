package laser_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// streamServer records the admin and publish calls a facade makes, so a test
// can state what reached the server rather than only that nothing failed.
type streamServer struct {
	iggcon.Client
	// mutex guards what a concurrent test reads while a send is in flight.
	// A fake that is not safe makes the race detector point at the test
	// rather than at the code under test, which hides a real one.
	mutex        sync.Mutex
	streamExists bool
	topicExists  bool
	createFails  bool
	// appearsAfterCreate makes a failed create behave like a concurrent one:
	// the create loses, and the resource is there on the next read.
	appearsAfterCreate bool

	calls          []string
	createdStream  string
	createdTopic   string
	topicOptions   *iggcon.TopicOptions
	sentMessages   []iggcon.IggyMessage
	sentPartitions iggcon.Partitioning
}

func (server *streamServer) Close() error { return nil }

func (server *streamServer) record(call string) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	server.calls = append(server.calls, call)
}

func (server *streamServer) GetStream(
	_ context.Context, _ iggcon.Identifier,
) (*iggcon.StreamDetails, error) {
	server.record("get-stream")
	if server.streamExists {
		return &iggcon.StreamDetails{}, nil
	}
	return nil, ierror.StreamNameNotFound{}
}

func (server *streamServer) CreateStream(
	_ context.Context, name string, _ *iggcon.StreamOptions,
) (*iggcon.StreamDetails, error) {
	server.record("create-stream")
	server.createdStream = name
	if server.createFails {
		server.streamExists = server.appearsAfterCreate
		return nil, errors.New("already exists")
	}
	server.streamExists = true
	return &iggcon.StreamDetails{}, nil
}

func (server *streamServer) DeleteStream(_ context.Context, _ iggcon.Identifier) error {
	server.record("delete-stream")
	if !server.streamExists {
		return ierror.StreamNameNotFound{}
	}
	server.streamExists = false
	return nil
}

func (server *streamServer) GetTopic(
	_ context.Context, _, _ iggcon.Identifier,
) (*iggcon.TopicDetails, error) {
	server.record("get-topic")
	if server.topicExists {
		return &iggcon.TopicDetails{}, nil
	}
	return nil, ierror.TopicNameNotFound{}
}

func (server *streamServer) CreateTopic(
	_ context.Context, _ iggcon.Identifier, name string, options *iggcon.TopicOptions,
) (*iggcon.TopicDetails, error) {
	server.record("create-topic")
	server.createdTopic = name
	server.topicOptions = options
	if server.createFails {
		server.topicExists = server.appearsAfterCreate
		return nil, errors.New("already exists")
	}
	server.topicExists = true
	return &iggcon.TopicDetails{}, nil
}

func (server *streamServer) DeleteTopic(_ context.Context, _, _ iggcon.Identifier) error {
	server.record("delete-topic")
	if !server.topicExists {
		return ierror.TopicNameNotFound{}
	}
	server.topicExists = false
	return nil
}

func (server *streamServer) SendMessages(
	_ context.Context, _, _ iggcon.Identifier,
	partitioning iggcon.Partitioning, messages []iggcon.IggyMessage,
) (*iggcon.SendMessagesResponse, error) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	server.calls = append(server.calls, "send")
	server.sentPartitions = partitioning
	server.sentMessages = messages
	return &iggcon.SendMessagesResponse{}, nil
}

// sent is what reached the server, copied so a caller reading it while another
// send is in flight does not race the write.
func (server *streamServer) sent() []iggcon.IggyMessage {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	return append([]iggcon.IggyMessage(nil), server.sentMessages...)
}

func streamHandle(server iggcon.Client, options laser.Options) *laser.Laser {
	return laser.FromClient(server, options)
}

// sentHeaders reads back the user headers of the record that reached the
// server.
func sentHeaders(t *testing.T, server *streamServer) map[string]string {
	t.Helper()
	if len(server.sentMessages) == 0 {
		t.Fatal("nothing was sent")
	}
	entries, err := iggcon.DeserializeHeaders(server.sentMessages[0].UserHeaders)
	if err != nil {
		t.Fatalf("decode the user headers: %v", err)
	}
	headers := map[string]string{}
	for _, entry := range entries {
		headers[string(entry.Key.Value)] = string(entry.Value.Value)
	}
	return headers
}

func TestGivenAnAbsentStreamWhenEnsuredThenShouldCreateIt(t *testing.T) {
	server := &streamServer{}

	if err := streamHandle(server, laser.Options{}).Stream("agents").
		Ensure(context.Background()); err != nil {
		t.Fatalf("ensure: %v", err)
	}

	if server.createdStream != "agents" {
		t.Errorf("want the stream created, got %q", server.createdStream)
	}
}

// An existing stream is left exactly as it is, so ensuring is safe to call on
// every start.
func TestGivenAnExistingStreamWhenEnsuredThenShouldLeaveItAlone(t *testing.T) {
	server := &streamServer{streamExists: true}

	if err := streamHandle(server, laser.Options{}).Stream("agents").
		Ensure(context.Background()); err != nil {
		t.Fatalf("ensure: %v", err)
	}

	for _, call := range server.calls {
		if call == "create-stream" {
			t.Error("an existing stream must not be created again")
		}
	}
}

// Two callers racing to create one stream both succeed: the loser's create
// fails, and the stream it wanted is then there, which is the outcome it asked
// for.
func TestGivenAConcurrentCreateWhenEnsuredThenShouldSucceedAnyway(t *testing.T) {
	server := &streamServer{createFails: true, appearsAfterCreate: true}

	if err := streamHandle(server, laser.Options{}).Stream("agents").
		Ensure(context.Background()); err != nil {
		t.Fatalf("a lost race must still succeed: %v", err)
	}
}

// A create that fails and leaves nothing behind is a real failure, not a race.
func TestGivenAFailedCreateWhenNothingAppearsThenShouldReportIt(t *testing.T) {
	server := &streamServer{createFails: true, appearsAfterCreate: false}

	err := streamHandle(server, laser.Options{}).Stream("agents").
		Ensure(context.Background())

	var transport *laser.TransportError
	if !errors.As(err, &transport) {
		t.Fatalf("want a TransportError, got %T: %v", err, err)
	}
}

// A topic needs its stream, so ensuring one ensures the other first rather
// than failing on a stream the caller never made.
func TestGivenAnAbsentTopicWhenEnsuredThenShouldCreateTheStreamFirst(t *testing.T) {
	server := &streamServer{}
	partitions := uint32(4)

	if err := streamHandle(server, laser.Options{DefaultStream: "agents"}).
		Topic("inferences").Ensure(context.Background(), partitions, nil); err != nil {
		t.Fatalf("ensure: %v", err)
	}

	order := strings.Join(server.calls, ",")
	if !strings.Contains(order, "create-stream") || !strings.Contains(order, "create-topic") {
		t.Fatalf("want both created, got %s", order)
	}
	if strings.Index(order, "create-stream") > strings.Index(order, "create-topic") {
		t.Errorf("want the stream created first, got %s", order)
	}
	if server.topicOptions == nil || server.topicOptions.PartitionsCount == nil ||
		*server.topicOptions.PartitionsCount != 4 {
		t.Errorf("want four partitions, got %+v", server.topicOptions)
	}
	// Nil leaves the server's default, which is what a single-node deployment
	// needs, so it must not be invented here.
	if server.topicOptions.ReplicationFactor != nil {
		t.Errorf("want no replication factor, got %d", *server.topicOptions.ReplicationFactor)
	}
}

func TestGivenNoPartitionsWhenEnsuringATopicThenShouldRefuseIt(t *testing.T) {
	server := &streamServer{}

	err := streamHandle(server, laser.Options{DefaultStream: "agents"}).
		Topic("inferences").Ensure(context.Background(), 0, nil)

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want an InvalidError, got %T: %v", err, err)
	}
	if len(server.calls) != 0 {
		t.Error("a refused ensure must not reach the socket")
	}
}

// Deleting something that was already gone is not an error, and the result
// says which happened.
func TestGivenADeleteWhenTheResourceIsAbsentThenShouldReportItWasNotThere(t *testing.T) {
	server := &streamServer{}

	existed, err := streamHandle(server, laser.Options{}).Stream("agents").
		Delete(context.Background())
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if existed {
		t.Error("want the stream reported absent")
	}

	present := &streamServer{streamExists: true}
	existed, err = streamHandle(present, laser.Options{}).Stream("agents").
		Delete(context.Background())
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if !existed {
		t.Error("want the stream reported removed")
	}
}

// Without a stream there is nothing to address, so the refusal says how to
// give one rather than guessing.
func TestGivenNoStreamWhenPublishingThenShouldRefuseWithTheRemedy(t *testing.T) {
	server := &streamServer{}

	_, err := streamHandle(server, laser.Options{}).Topic("inferences").
		Publish().Payload([]byte("x")).Send(context.Background())

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want an InvalidError, got %T: %v", err, err)
	}
	if !strings.Contains(err.Error(), "DefaultStream") {
		t.Errorf("want the remedy named, got %v", err)
	}
	if len(server.calls) != 0 {
		t.Error("a refused publish must not reach the socket")
	}
}

// Records sharing a key land on one partition, which is the only ordering
// Iggy gives.
func TestGivenAPartitionKeyWhenPublishingThenShouldPartitionByIt(t *testing.T) {
	server := &streamServer{}

	if _, err := streamHandle(server, laser.Options{DefaultStream: "agents"}).
		Topic("inferences").Publish().Payload([]byte("body")).Key("conversation-7").
		Send(context.Background()); err != nil {
		t.Fatalf("publish: %v", err)
	}

	if server.sentPartitions.Kind != iggcon.MessageKey {
		t.Errorf("want key partitioning, got %v", server.sentPartitions.Kind)
	}
	if string(server.sentPartitions.Value) != "conversation-7" {
		t.Errorf("want the key carried, got %q", server.sentPartitions.Value)
	}
}

// Without a key the producer balances across partitions and no order is
// promised, which is a different thing from a key that happens to be empty.
func TestGivenNoPartitionKeyWhenPublishingThenShouldBalance(t *testing.T) {
	server := &streamServer{}

	if _, err := streamHandle(server, laser.Options{DefaultStream: "agents"}).
		Topic("inferences").Publish().Payload([]byte("body")).
		Send(context.Background()); err != nil {
		t.Fatalf("publish: %v", err)
	}

	if server.sentPartitions.Kind != iggcon.Balanced {
		t.Errorf("want balanced partitioning, got %v", server.sentPartitions.Kind)
	}
}

// An empty key would hash every record onto one partition while reading as "no
// key given", silently serializing the topic.
func TestGivenAnEmptyPartitionKeyWhenPublishingThenShouldRefuseIt(t *testing.T) {
	server := &streamServer{}

	_, err := streamHandle(server, laser.Options{DefaultStream: "agents"}).
		Topic("inferences").Publish().Payload([]byte("body")).Key("").
		Send(context.Background())

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want an InvalidError, got %T: %v", err, err)
	}
	// The transport refuses an empty key too, but only as a size error, which
	// does not tell the caller that omitting the key is the alternative.
	if !strings.Contains(err.Error(), "omit the key") {
		t.Errorf("want the alternative named, got %v", err)
	}
	if len(server.calls) != 0 {
		t.Error("a refused publish must not reach the socket")
	}
}

// A typed body stamps its content type, so a reader knows how to decode it
// without being told out of band.
func TestGivenATypedPayloadWhenPublishedThenShouldStampItsContentType(t *testing.T) {
	for name, publish := range map[string]struct {
		send func(*laser.PublishRequest) *laser.PublishRequest
		want string
	}{
		"json": {func(r *laser.PublishRequest) *laser.PublishRequest {
			return r.JSON(map[string]int{"n": 1})
		}, string(wire.ContentJSON)},
		"cbor": {func(r *laser.PublishRequest) *laser.PublishRequest {
			return r.CBOR(wire.KvGet{Namespace: "ns", Key: []byte("k")})
		}, string(wire.ContentCBOR)},
	} {
		t.Run(name, func(t *testing.T) {
			server := &streamServer{}
			request := streamHandle(server, laser.Options{DefaultStream: "agents"}).
				Topic("inferences").Publish()

			if _, err := publish.send(request).Send(context.Background()); err != nil {
				t.Fatalf("publish: %v", err)
			}

			headers := sentHeaders(t, server)
			if headers[wire.HeaderContentType] != publish.want {
				t.Errorf("want content type %q, got %q",
					publish.want, headers[wire.HeaderContentType])
			}
			if len(server.sentMessages[0].Payload) == 0 {
				t.Error("want an encoded body")
			}
		})
	}
}

func TestGivenUserHeadersWhenPublishedThenShouldCarryThem(t *testing.T) {
	server := &streamServer{}

	if _, err := streamHandle(server, laser.Options{DefaultStream: "agents"}).
		Topic("inferences").Publish().Payload([]byte("body")).
		Header("trace", "abc").Header("tenant", "acme").
		Send(context.Background()); err != nil {
		t.Fatalf("publish: %v", err)
	}

	headers := sentHeaders(t, server)
	if headers["trace"] != "abc" || headers["tenant"] != "acme" {
		t.Errorf("want both headers, got %+v", headers)
	}
}

// An empty batch touches no socket, so a caller can hand one over without
// checking first.
func TestGivenAnEmptyBatchWhenSentThenShouldTouchNoSocket(t *testing.T) {
	server := &streamServer{}

	response, err := streamHandle(server, laser.Options{DefaultStream: "agents"}).
		Topic("inferences").Batch(context.Background(), nil, "")
	if err != nil {
		t.Fatalf("batch: %v", err)
	}
	if response == nil {
		t.Error("want an empty response rather than nil")
	}
	if len(server.calls) != 0 {
		t.Errorf("an empty batch must send nothing, got %v", server.calls)
	}
}

func TestGivenABatchWhenSentThenShouldShareOnePartitioning(t *testing.T) {
	server := &streamServer{}
	first, err := iggcon.NewIggyMessage([]byte("one"))
	if err != nil {
		t.Fatalf("build a record: %v", err)
	}
	second, err := iggcon.NewIggyMessage([]byte("two"))
	if err != nil {
		t.Fatalf("build a record: %v", err)
	}

	if _, err := streamHandle(server, laser.Options{DefaultStream: "agents"}).
		Topic("inferences").Batch(context.Background(),
		[]iggcon.IggyMessage{first, second}, "key-1"); err != nil {
		t.Fatalf("batch: %v", err)
	}

	if len(server.sentMessages) != 2 {
		t.Errorf("want both records in one call, got %d", len(server.sentMessages))
	}
	if string(server.sentPartitions.Value) != "key-1" {
		t.Errorf("want one shared key, got %q", server.sentPartitions.Value)
	}
}

// A builder reports the first mistake in a chain, not the last.
func TestGivenAnUnencodablePayloadWhenPublishedThenShouldReportIt(t *testing.T) {
	server := &streamServer{}

	_, err := streamHandle(server, laser.Options{DefaultStream: "agents"}).
		Topic("inferences").Publish().JSON(make(chan int)).Key("k").
		Send(context.Background())

	var codec *laser.CodecError
	if !errors.As(err, &codec) {
		t.Fatalf("want a CodecError, got %T: %v", err, err)
	}
	if len(server.calls) != 0 {
		t.Error("a refused publish must not reach the socket")
	}
}

func TestGivenAStreamHandleWhenAddressingATopicThenShouldUseThatStream(t *testing.T) {
	server := &streamServer{streamExists: true, topicExists: true}
	handle := streamHandle(server, laser.Options{DefaultStream: "agents"})

	topic := handle.Stream("billing").Topic("invoices")
	if topic.StreamName() != "billing" || topic.Name() != "invoices" {
		t.Errorf("want billing/invoices, got %s/%s", topic.StreamName(), topic.Name())
	}
	if _, err := topic.Publish().Payload([]byte("x")).Send(context.Background()); err != nil {
		t.Fatalf("publish: %v", err)
	}
}
