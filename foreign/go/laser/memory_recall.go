package laser

import (
	"context"
	"sync"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// memoryReaderName is how a recall names itself to the server. The offsets
// stay client-owned, so the name only identifies the reader.
const memoryReaderName = "laser-log-memory"

// memoryReader is the folded view one namespace recalls from, together with
// the place on the topic the fold has reached.
//
// It is held on the handle rather than rebuilt per call, so a recall folds
// only what was appended since the last one rather than rescanning the topic
// from its start. Nothing is checkpointed beside it, because the log already
// holds the truth: a fresh handle rebuilds the view from the topic.
type memoryReader struct {
	// mutex serialises the fold, so two recalls in flight never absorb one
	// record twice and double a feedback weight.
	mutex  sync.Mutex
	fold   *MemoryFold
	cursor *Cursor
}

func newMemoryReader(stream, namespace string) *memoryReader {
	return &memoryReader{fold: NewMemoryFold(stream, namespace)}
}

// Recall reads this namespace's memory back and returns the items the query
// asks for, most relevant first.
//
// A managed KV deployment serves its memory view. Apache Iggy reads the log.
//
// A topic nobody has written to yet recalls nothing rather than failing: a
// reader started before its writer is not an error.
func (memory *Memory) Recall(
	ctx context.Context, query MemoryQuery,
) ([]MemoryItem, error) {
	items, err := memory.recallStored(ctx, query, false)
	return memory.rerank(ctx, query, items, err)
}

// RecallFolded reads the log directly, including on a deployment with a managed view.
func (memory *Memory) RecallFolded(ctx context.Context, query MemoryQuery) ([]MemoryItem, error) {
	items, err := memory.recallStored(ctx, query, true)
	return memory.rerank(ctx, query, items, err)
}

// recallStored reads from the handle's store. Only the log has a fold apart from its view.
func (memory *Memory) recallStored(ctx context.Context, query MemoryQuery, folded bool) ([]MemoryItem, error) {
	if err := query.Strategy.validate(); err != nil {
		return nil, err
	}
	switch {
	case memory.custom != nil:
		return memory.custom.Recall(ctx, memory.scope, query)
	case memory.vector != nil:
		return memory.vector.Recall(ctx, memory.scope, query)
	}
	if memory.stream == "" {
		return nil, errNoStream()
	}
	// The log filters by the query's words for graph and temporal, as for keyword.
	if query.Strategy == RecallGraph || query.Strategy == RecallTemporal {
		query.Strategy = RecallKeyword
	}
	if !folded && memory.laser.Negotiated().Kv.Available {
		return memory.recallView(ctx, query)
	}
	return memory.recallFold(ctx, query)
}

// rerank hands a recall that carries a query to the reranker, when the handle has one.
func (memory *Memory) rerank(ctx context.Context, query MemoryQuery, items []MemoryItem, err error) ([]MemoryItem, error) {
	if err != nil || memory.reranker == nil || query.Query == "" {
		return items, err
	}
	return memory.reranker.Rerank(ctx, query.Query, items)
}

func (memory *Memory) recallFold(ctx context.Context, query MemoryQuery) ([]MemoryItem, error) {
	memory.reader.mutex.Lock()
	defer memory.reader.mutex.Unlock()
	if err := memory.catchUp(ctx); err != nil {
		return nil, err
	}
	return memory.reader.fold.Recall(memory.scope, query), nil
}

// catchUp folds in everything appended since the last recall. The reader's
// mutex is already held.
func (memory *Memory) catchUp(ctx context.Context) error {
	reader := memory.reader
	if reader.cursor == nil {
		cursor, err := memory.laser.Stream(memory.stream).
			Topic(memory.topic).ReplayAs(memoryReaderName)
		if err != nil {
			return err
		}
		reader.cursor = cursor
	}
	// One poll drains each partition only as far as the drain ceiling, so a
	// long topic takes several. Reading until there is nothing left is what
	// makes a recall see every write that reached the log before it.
	for {
		records, err := reader.cursor.Poll(ctx)
		if err != nil {
			return err
		}
		if len(records) == 0 {
			return nil
		}
		for _, record := range records {
			reader.fold.Observe(record)
		}
	}
}

// memoryHeaders is the header block a memory record rides under: the
// provenance spine, then the namespace and the owner the fold filters on.
//
// The scope rides as headers rather than only inside the record, because
// recall folds the topic back and narrows on them. A record naming neither a
// conversation nor an owner can only ever be recalled by everyone.
func memoryHeaders(scope MemoryScope, namespace, idempotencyKey string) ([]iggcon.HeaderEntry, error) {
	provenance := &Provenance{Agent: scope.Agent, IdempotencyKey: &idempotencyKey}
	if scope.Conversation != nil {
		provenance.Conversation = *scope.Conversation
	}
	headers, err := provenance.Headers()
	if err != nil {
		return nil, err
	}
	// Written in a fixed order rather than from a map, so one scope always
	// produces one header block and two records of a scope compare equal.
	for _, header := range [...][2]string{
		{wire.HeaderMemoryNamespace, namespace},
		{wire.HeaderMemoryUser, scope.User},
		{wire.HeaderMemoryApp, scope.App},
	} {
		if header[1] == "" {
			// An unset owner widens recall rather than narrowing it to the
			// empty string, so there is nothing to stamp.
			continue
		}
		key, err := iggcon.NewHeaderKeyString(header[0])
		if err != nil {
			return nil, &InvalidError{
				Reason: "the memory header " + header[0] + ": " + err.Error()}
		}
		headers = append(headers, iggcon.HeaderEntry{
			Key:   key,
			Value: iggcon.HeaderValue{Kind: iggcon.String, Value: []byte(header[1])},
		})
	}
	return headers, nil
}
