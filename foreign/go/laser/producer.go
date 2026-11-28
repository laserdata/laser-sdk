package laser

import (
	"context"
	"encoding/binary"
	"math"
	"slices"
	"sync"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// defaultProducerBatchLength is how many records one send carries when the caller names none.
const defaultProducerBatchLength = 1000

// Routing selects a partition for a whole direct send. Its zero value balances sends.
type Routing struct {
	kind      iggcon.PartitioningKind
	key       string
	partition uint32
}

// BalancedRouting lets the server spread sends across the partitions.
func BalancedRouting() Routing { return Routing{} }

// KeyRouting sends every record of a key to the same partition.
func KeyRouting(key []byte) Routing { return Routing{kind: iggcon.MessageKey, key: string(key)} }

// PartitionRouting sends to one named partition.
func PartitionRouting(partition uint32) Routing {
	return Routing{kind: iggcon.PartitionIdKind, partition: partition}
}

func (routing Routing) native() (iggcon.Partitioning, error) {
	switch routing.kind {
	case iggcon.MessageKey:
		key, err := iggcon.EntityIdString(routing.key)
		if err != nil {
			return key, &InvalidError{Reason: "producer partition key: " + err.Error()}
		}
		return key, nil
	case iggcon.PartitionIdKind:
		return iggcon.PartitionId(routing.partition), nil
	default:
		return iggcon.None(), nil
	}
}

// ProducerMessage retains the payload and native Iggy headers of a direct publish.
type ProducerMessage = iggcon.IggyMessage

// NewProducerMessage builds a record from a payload and optional native headers.
func NewProducerMessage(payload []byte, headers ...iggcon.HeaderEntry) (ProducerMessage, error) {
	message, err := iggcon.NewIggyMessage(payload, iggcon.WithUserHeaders(headers))
	if err != nil {
		return ProducerMessage{}, &InvalidError{Reason: "build the producer message: " + err.Error()}
	}
	return message, nil
}

// ProducerHeaderValue is what a native header carries: text, bytes, a boolean or a fixed-width number.
type ProducerHeaderValue interface {
	string | []byte | bool | uint8 | uint16 | uint32 | uint64 | int8 | int16 | int32 | int64 | float32 | float64
}

// ProducerHeader builds one native header that keeps the kind and width of value on the log.
func ProducerHeader[V ProducerHeaderValue](key string, value V) (iggcon.HeaderEntry, error) {
	name, err := iggcon.NewHeaderKeyString(key)
	if err != nil {
		return iggcon.HeaderEntry{}, &InvalidError{Reason: "producer header key: " + err.Error()}
	}
	var encoded iggcon.HeaderValue
	switch typed := any(value).(type) {
	case string:
		encoded = iggcon.HeaderValue{Kind: iggcon.String, Value: []byte(typed)}
	case []byte:
		encoded = iggcon.HeaderValue{Kind: iggcon.Raw, Value: slices.Clone(typed)}
	case bool:
		encoded = iggcon.HeaderValue{Kind: iggcon.Bool, Value: []byte{0}}
		if typed {
			encoded.Value[0] = 1
		}
	case uint8:
		encoded = iggcon.HeaderValue{Kind: iggcon.Uint8, Value: []byte{typed}}
	case uint16:
		encoded = iggcon.HeaderValue{Kind: iggcon.Uint16, Value: binary.LittleEndian.AppendUint16(nil, typed)}
	case uint32:
		encoded = iggcon.HeaderValue{Kind: iggcon.Uint32, Value: binary.LittleEndian.AppendUint32(nil, typed)}
	case uint64:
		encoded = iggcon.HeaderValue{Kind: iggcon.Uint64, Value: binary.LittleEndian.AppendUint64(nil, typed)}
	case int8:
		encoded = iggcon.HeaderValue{Kind: iggcon.Int8, Value: []byte{uint8(typed)}}
	case int16:
		encoded = iggcon.HeaderValue{Kind: iggcon.Int16, Value: binary.LittleEndian.AppendUint16(nil, uint16(typed))}
	case int32:
		encoded = iggcon.HeaderValue{Kind: iggcon.Int32, Value: binary.LittleEndian.AppendUint32(nil, uint32(typed))}
	case int64:
		encoded = iggcon.HeaderValue{Kind: iggcon.Int64, Value: binary.LittleEndian.AppendUint64(nil, uint64(typed))}
	case float32:
		encoded = iggcon.HeaderValue{Kind: iggcon.Float, Value: binary.LittleEndian.AppendUint32(nil, math.Float32bits(typed))}
	case float64:
		encoded = iggcon.HeaderValue{Kind: iggcon.Double, Value: binary.LittleEndian.AppendUint64(nil, math.Float64bits(typed))}
	}
	return iggcon.HeaderEntry{Key: name, Value: encoded}, nil
}

// ProducerBuilder configures synchronous publishing and optional resource creation.
type ProducerBuilder struct {
	topic                     *Topic
	batchLength               int
	createStream, createTopic bool
	partitions                uint32
	replication               *uint32
	expireAfter               *time.Duration
	neverExpire               bool
	maxBytes                  *uint64
	unlimited                 bool
	routing                   Routing
	publish                   PublishOptions
}

// Producer configures a synchronous producer. It creates a missing topic by default.
func (topic *Topic) Producer() *ProducerBuilder {
	return &ProducerBuilder{topic: topic, batchLength: defaultProducerBatchLength, createTopic: true, partitions: 1, publish: topic.laser.publishSchedule()}
}

// BatchLength caps the records one send carries. Zero keeps the default of 1000.
func (b *ProducerBuilder) BatchLength(n int) *ProducerBuilder {
	b.batchLength = n
	return b
}

// CreateStream creates the stream when it is absent.
func (b *ProducerBuilder) CreateStream(create bool) *ProducerBuilder {
	b.createStream = create
	return b
}

// CreateTopic creates the topic when it is absent, which is the default.
func (b *ProducerBuilder) CreateTopic(create bool) *ProducerBuilder {
	b.createTopic = create
	return b
}

// Partitions is the partition count of a topic the producer creates.
func (b *ProducerBuilder) Partitions(n uint32) *ProducerBuilder {
	b.partitions = n
	return b
}

// ReplicationFactor is the replication factor of a topic the producer creates.
func (b *ProducerBuilder) ReplicationFactor(n uint32) *ProducerBuilder {
	b.replication = &n
	return b
}

// ExpireAfter is the record retention of a topic the producer creates.
func (b *ProducerBuilder) ExpireAfter(age time.Duration) *ProducerBuilder {
	b.expireAfter = &age
	return b
}

// NeverExpire keeps the records of a topic the producer creates.
func (b *ProducerBuilder) NeverExpire() *ProducerBuilder {
	b.neverExpire = true
	return b
}

// MaxTopicBytes caps the size of a topic the producer creates.
func (b *ProducerBuilder) MaxTopicBytes(n uint64) *ProducerBuilder {
	b.maxBytes = &n
	return b
}

// UnlimitedTopicSize leaves a topic the producer creates without a size cap.
func (b *ProducerBuilder) UnlimitedTopicSize() *ProducerBuilder {
	b.unlimited = true
	return b
}

// Routing selects the partition of every send that names no routing of its own.
func (b *ProducerBuilder) Routing(r Routing) *ProducerBuilder {
	b.routing = r
	return b
}

// Retries is how many times a failed send is repeated.
func (b *ProducerBuilder) Retries(n uint32) *ProducerBuilder {
	b.publish.MaxRetries = n
	return b
}

// RetryBackoff is the first wait between repeated sends.
func (b *ProducerBuilder) RetryBackoff(d time.Duration) *ProducerBuilder {
	b.publish.RetryBackoff = d
	return b
}

// Encryption seals every record with this key.
func (b *ProducerBuilder) Encryption(key *Encryption) *ProducerBuilder {
	b.topic = b.topic.WithEncryption(key)
	return b
}

// Build ensures the stream and topic the builder asked for and returns the producer.
func (b *ProducerBuilder) Build(ctx context.Context) (*Producer, error) {
	options, err := b.topicOptions()
	if err != nil {
		return nil, err
	}
	routing, err := b.routing.native()
	if err != nil {
		return nil, err
	}
	scoped, err := b.topic.laser.WithPublishOptions(b.publish)
	if err != nil {
		return nil, err
	}
	topic := *b.topic
	topic.laser = scoped
	if topic.stream == "" {
		return nil, errNoStream()
	}
	streamID, topicID, err := topic.identifiers()
	if err != nil {
		return nil, err
	}
	_, err = runWithRecovery(ctx, b.publish, func(attempt context.Context) (struct{}, error) {
		if b.createStream {
			if err := scoped.Stream(topic.stream).Ensure(attempt); err != nil {
				return struct{}{}, err
			}
		}
		return struct{}{}, topic.ensureOptions(attempt, streamID, topicID, b.createTopic, options)
	}, scoped.reconnect)
	if err != nil {
		return nil, err
	}
	batchLength := b.batchLength
	if batchLength == 0 {
		batchLength = defaultProducerBatchLength
	}
	p := &Producer{topic: topic, streamID: streamID, topicID: topicID, routing: routing, batchLength: batchLength}
	p.recorder = newProducerRecorder(scoped, topic.stream, topic.name)
	return p, nil
}

func (b *ProducerBuilder) topicOptions() (*iggcon.TopicOptions, error) {
	if b.batchLength < 0 || b.createTopic && b.partitions == 0 {
		return nil, &InvalidError{Reason: "producer batch length must not be negative and topic partition count must be positive"}
	}
	if err := validateTopicReplication(b.replication); err != nil {
		return nil, err
	}
	options := &iggcon.TopicOptions{PartitionsCount: &b.partitions, ReplicationFactor: b.replication, MaxTopicSize: b.maxBytes}
	if b.expireAfter != nil {
		if b.neverExpire || b.expireAfter.Microseconds() <= 0 {
			return nil, &InvalidError{Reason: "expire_after requires a positive retention and excludes never_expire"}
		}
		expiry := iggcon.Duration(b.expireAfter.Microseconds())
		options.MessageExpiry = &expiry
	} else if b.neverExpire {
		expiry := iggcon.IggyExpiryNeverExpire
		options.MessageExpiry = &expiry
	}
	if b.maxBytes != nil && (b.unlimited || *b.maxBytes == 0) {
		return nil, &InvalidError{Reason: "max_topic_bytes requires a positive limit and excludes unlimited_topic_size"}
	}
	if b.unlimited {
		size := uint64(math.MaxUint64)
		options.MaxTopicSize = &size
	}
	return options, nil
}

// Producer synchronously publishes records and returns every confirmation supplied by Iggy.
type Producer struct {
	topic             Topic
	streamID, topicID iggcon.Identifier
	batchLength       int
	routing           iggcon.Partitioning
	recorder          *producerRecorder
	mu                sync.Mutex
	active            int
	closed            bool
	drained           chan struct{}
}

// Send publishes one payload under the producer's routing.
func (p *Producer) Send(ctx context.Context, payload []byte) (*iggcon.SendMessagesResponse, error) {
	m, err := NewProducerMessage(payload)
	if err != nil {
		return nil, err
	}
	return p.SendMessage(ctx, m)
}

// SendMessage publishes one record under the producer's routing.
func (p *Producer) SendMessage(ctx context.Context, m ProducerMessage) (*iggcon.SendMessagesResponse, error) {
	return p.send(ctx, []ProducerMessage{m}, p.routing)
}

// SendWithRouting publishes one record under the routing given.
func (p *Producer) SendWithRouting(ctx context.Context, m ProducerMessage, r Routing) (*iggcon.SendMessagesResponse, error) {
	native, err := r.native()
	if err != nil {
		return nil, err
	}
	return p.send(ctx, []ProducerMessage{m}, native)
}

// SendKeyed publishes one record to the partition of its key.
func (p *Producer) SendKeyed(ctx context.Context, m ProducerMessage, key []byte) (*iggcon.SendMessagesResponse, error) {
	return p.SendWithRouting(ctx, m, KeyRouting(key))
}

// SendToPartition publishes one record to a named partition.
func (p *Producer) SendToPartition(ctx context.Context, m ProducerMessage, id uint32) (*iggcon.SendMessagesResponse, error) {
	return p.SendWithRouting(ctx, m, PartitionRouting(id))
}

// SendBatch publishes records in sends of at most the batch length. A failed
// send returns a PublishFailure with the confirmed and the unconfirmed records.
func (p *Producer) SendBatch(ctx context.Context, messages []ProducerMessage) (*iggcon.SendMessagesResponse, error) {
	return p.send(ctx, slices.Clone(messages), p.routing)
}

// SendBatchWithRouting publishes records like SendBatch under the routing given.
func (p *Producer) SendBatchWithRouting(ctx context.Context, messages []ProducerMessage, r Routing) (*iggcon.SendMessagesResponse, error) {
	if len(messages) == 0 {
		return &iggcon.SendMessagesResponse{}, nil
	}
	native, err := r.native()
	if err != nil {
		return nil, err
	}
	return p.send(ctx, slices.Clone(messages), native)
}

// Flush waits for the sends already in flight. A closed producer refuses it.
func (p *Producer) Flush(ctx context.Context) error { return p.wait(ctx, false) }

// Close waits for the sends already in flight and stops the producer. Later
// sends fail, and a second close returns at once.
func (p *Producer) Close(ctx context.Context) error {
	err := p.wait(ctx, true)
	if err == nil {
		p.recorder.unregister()
	}
	return err
}

// Statistics is the producer's send counters and latency percentiles so far.
func (p *Producer) Statistics() wire.ProducerStatistics { return p.recorder.snapshot() }

func (p *Producer) send(ctx context.Context, messages []ProducerMessage, routing iggcon.Partitioning) (reply *iggcon.SendMessagesResponse, err error) {
	if len(messages) == 0 {
		return &iggcon.SendMessagesResponse{}, nil
	}
	for i := range messages {
		m := &messages[i]
		if len(m.Payload) == 0 || len(m.Payload) > iggcon.MaxPayloadSize || uint64(m.Header.PayloadLength) != uint64(len(m.Payload)) ||
			len(m.UserHeaders) > iggcon.MaxUserHeadersSize || uint64(m.Header.UserHeaderLength) != uint64(len(m.UserHeaders)) {
			return nil, &InvalidError{Reason: "producer message has invalid payload or header lengths"}
		}
	}
	if err = p.enter(ctx); err != nil {
		size := producerPayloadBytes(messages)
		started := p.recorder.begin(uint64(len(messages)), size)
		p.recorder.finish(started, uint64(len(messages)), size, false)
		return nil, err
	}
	defer p.leave()
	if err := p.topic.laser.governMessages(ctx, p.topic.stream, p.topic.name, messages); err != nil {
		return nil, err
	}
	size := producerPayloadBytes(messages)
	started := p.recorder.begin(uint64(len(messages)), size)
	defer func() { p.recorder.finish(started, uint64(len(messages)), size, err == nil) }()
	for i := range messages {
		if messages[i].Header.Id == (iggcon.MessageID{}) {
			messages[i].Header.Id = iggcon.MessageID(NewOperationID())
		}
	}
	reply = &iggcon.SendMessagesResponse{}
	for start := 0; start < len(messages); {
		end := start + min(len(messages)-start, p.batchLength)
		batch := messages[start:end]
		if p.topic.encryption != nil {
			batch, err = p.topic.encryption.seal(batch)
		}
		var sent *iggcon.SendMessagesResponse
		if err == nil {
			sent, err = p.topic.publishOnce(ctx, p.streamID, p.topicID, routing, batch)
		}
		if err != nil {
			fail := &PublishFailure{Cause: err, Stream: p.topic.stream, Topic: p.topic.name, Unconfirmed: messages[start:]}
			if len(reply.Confirmations) > 0 {
				fail.Committed = []iggcon.SendMessagesResponse{*reply}
			}
			return nil, fail
		}
		if sent != nil {
			reply.Confirmations = append(reply.Confirmations, sent.Confirmations...)
		}
		start = end
	}
	return reply, nil
}

func producerPayloadBytes(messages []ProducerMessage) uint64 {
	var bytes uint64
	for i := range messages {
		bytes += uint64(len(messages[i].Payload))
	}
	return bytes
}

func (p *Producer) enter(ctx context.Context) error {
	for {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return errProducerClosed()
		}
		wait := p.drained
		if wait == nil {
			p.active++
			p.mu.Unlock()
			return nil
		}
		p.mu.Unlock()
		select {
		case <-wait:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func errProducerClosed() error {
	return &InvalidError{Reason: "the producer is shut down, build a new one with Topic.Producer"}
}

func (p *Producer) leave() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.active--
	if p.active == 0 && p.drained != nil {
		close(p.drained)
		p.drained = nil
	}
}

func (p *Producer) wait(ctx context.Context, shutdown bool) error {
	p.mu.Lock()
	if !shutdown && p.closed {
		p.mu.Unlock()
		return errProducerClosed()
	}
	if shutdown {
		p.closed = true
	}
	if p.active == 0 {
		p.mu.Unlock()
		return nil
	}
	if p.drained == nil {
		p.drained = make(chan struct{})
	}
	wait := p.drained
	p.mu.Unlock()
	select {
	case <-wait:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
