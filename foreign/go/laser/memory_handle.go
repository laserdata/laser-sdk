package laser

import (
	"context"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// DefaultMemoryTopicTTL is how long a memory topic keeps its records unless told otherwise.
const DefaultMemoryTopicTTL = 30 * 24 * time.Hour

// DefaultSearchLimit is how many items a keyword search returns when it names no limit.
const DefaultSearchLimit = 50

// MemoryBackend selects the store a memory handle runs on.
type MemoryBackend string

const (
	// MemoryBackendAuto is memory over the stream, the default.
	MemoryBackendAuto MemoryBackend = "auto"
	// MemoryBackendLog is the same durable stream model, named explicitly.
	MemoryBackendLog MemoryBackend = "log"
	// MemoryBackendVector is the in-process similarity index, gone when the process exits.
	MemoryBackendVector MemoryBackend = "vector"
)

// CustomMemory is an external store plugged in as a memory backend. It owns its scoping and its ids.
type CustomMemory interface {
	// Remember keeps body under scope and returns the id it was stored under.
	Remember(ctx context.Context, scope MemoryScope, body []byte) (MemoryID, error)
	// Recall returns the items the query asks for under scope.
	Recall(ctx context.Context, scope MemoryScope, query MemoryQuery) ([]MemoryItem, error)
	// Improve records feedback on an item and returns the feedback record's id.
	Improve(ctx context.Context, scope MemoryScope, feedback Feedback) (MemoryID, error)
	// Forget removes an item under scope.
	Forget(ctx context.Context, scope MemoryScope, id MemoryID) error
}

// MemoryAppender is a custom backend that also stores an item under a caller-supplied id and kind.
type MemoryAppender interface {
	Append(ctx context.Context, scope MemoryScope, id MemoryID, kind MemoryKind, body []byte) (MemoryID, error)
}

// MemoryWith opens memory in namespace on an explicit backend. The vector backend needs Embedder before its first write.
func (laser *Laser) MemoryWith(namespace string, backend MemoryBackend) (*Memory, error) {
	switch backend {
	case MemoryBackendAuto, MemoryBackendLog, "":
		return laser.Memory(namespace), nil
	case MemoryBackendVector:
		memory := laser.Memory(namespace)
		memory.vector = NewGovernedVectorMemory(laser, nil)
		return memory, nil
	default:
		return nil, &InvalidError{Reason: "unknown memory backend " + string(backend) + ": use auto, log or vector"}
	}
}

// MemoryCustom opens memory over an external backend, which every verb routes straight to.
func (laser *Laser) MemoryCustom(backend CustomMemory) (*Memory, error) {
	if backend == nil {
		return nil, &InvalidError{Reason: "a custom memory needs a backend"}
	}
	memory := laser.Memory(wire.AgentMemory)
	memory.custom = backend
	return memory, nil
}

// MemoryOnTopic opens memory in an existing caller-named topic. An empty stream keeps the default stream.
func (laser *Laser) MemoryOnTopic(topic, stream string) (*Memory, error) {
	if _, err := iggcon.NewIdentifier(topic); err != nil {
		return nil, &InvalidError{Reason: "memory topic name: " + err.Error()}
	}
	owner := laser
	if stream != "" && stream != laser.defaultStream {
		owner = laser.WithDefaultStream(stream)
	}
	// The named items key on the namespace, which defaults to the topic's name.
	return owner.logMemory(topic, topic), nil
}

// MemoryTopicBuilder configures a memory topic. Build creates it when missing and returns the handle.
type MemoryTopicBuilder struct {
	laser      *Laser
	stream     string
	topic      string
	partitions uint32
	ttl        time.Duration
}

// MemoryTopic begins a memory topic with one partition and thirty days of retention.
func (laser *Laser) MemoryTopic(topic string) *MemoryTopicBuilder {
	return &MemoryTopicBuilder{laser: laser, topic: topic, partitions: 1, ttl: DefaultMemoryTopicTTL}
}

// Stream names the stream the memory topic lives on, the default stream otherwise.
func (builder *MemoryTopicBuilder) Stream(stream string) *MemoryTopicBuilder {
	builder.stream = stream
	return builder
}

// Partitions sets the partition count, at least one. One scope's records always share a partition.
func (builder *MemoryTopicBuilder) Partitions(partitions uint32) *MemoryTopicBuilder {
	builder.partitions = max(partitions, 1)
	return builder
}

// TTL expires records on the topic after this long.
func (builder *MemoryTopicBuilder) TTL(ttl time.Duration) *MemoryTopicBuilder {
	builder.ttl = ttl
	return builder
}

// NoExpiry keeps records on the topic until its size retention rotates them out.
func (builder *MemoryTopicBuilder) NoExpiry() *MemoryTopicBuilder {
	builder.ttl = 0
	return builder
}

// Build creates the stream and topic when missing and returns memory on the topic.
func (builder *MemoryTopicBuilder) Build(ctx context.Context) (*Memory, error) {
	memory, err := builder.laser.MemoryOnTopic(builder.topic, builder.stream)
	if err != nil {
		return nil, err
	}
	if memory.stream == "" {
		return nil, errNoStream()
	}
	if err := memory.laser.Stream(memory.stream).Ensure(ctx); err != nil {
		return nil, err
	}
	retention := TopicRetention{}
	if builder.ttl > 0 {
		retention = ExpireAfter(builder.ttl)
	}
	options := retention.options(builder.partitions)
	if builder.ttl <= 0 {
		expiry := iggcon.IggyExpiryNeverExpire
		options.MessageExpiry = &expiry
	}
	if err := memory.laser.ensureRetained(ctx, memory.topic, options); err != nil {
		return nil, err
	}
	return memory, nil
}

// Backend names the store this handle resolved to: log, vector or custom.
func (memory *Memory) Backend() string {
	switch {
	case memory.custom != nil:
		return "custom"
	case memory.vector != nil:
		return "vector"
	default:
		return "log"
	}
}

// Embedder registers the embedding seam of a vector handle. The log and custom backends ignore it.
func (memory *Memory) Embedder(embedder Embedder) *Memory {
	if memory.vector == nil {
		return memory
	}
	configured := *memory
	configured.vector = &VectorMemory{embedder: embedder, governed: memory.vector.governed}
	return &configured
}

// Reranker wraps recall with a second stage that reorders what a query found.
func (memory *Memory) Reranker(reranker Reranker) *Memory {
	wrapped := *memory
	wrapped.reranker = reranker
	return &wrapped
}

// Search is keyword recall for query within this handle's scope. A limit of zero takes DefaultSearchLimit.
func (memory *Memory) Search(ctx context.Context, query string, limit int) ([]MemoryItem, error) {
	if limit <= 0 {
		limit = DefaultSearchLimit
	}
	return memory.Recall(ctx, MemoryQuery{Query: query, Strategy: RecallKeyword, Limit: limit})
}
