package laser

import (
	"bytes"
	"cmp"
	"container/list"
	"context"
	"slices"
	"sync"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// ownWritesWait is how long a recall waits for the view to show this
// connection's writes.
var ownWritesWait = 10 * time.Second

// ownWritesStage names the wait a recall gives up on, for its timeout error.
const ownWritesStage = "the memory view to apply this connection's writes: a folded recall reads the topic directly"

type memoryKey struct{ stream, topic, namespace, id string }

type memoryWrite struct {
	key                            memoryKey
	present                        bool
	user, app, agent, conversation string
	// unseen marks a forget of an item this connection remembered and the view
	// never showed, whose absence may only mean the view has not reached it.
	unseen bool
}

type memoryWrites struct {
	mu    sync.Mutex
	order list.List
	byKey map[memoryKey]*list.Element
}

func (memory *Memory) trackWrite(record wire.MemoryRecord, conversation *wire.ConversationID) {
	write := memoryWrite{key: memoryKey{stream: memory.stream, topic: memory.topic, namespace: memory.laser.ResourceName(memory.namespace)},
		user: memory.scope.User, app: memory.scope.App}
	if memory.scope.Agent != nil {
		write.agent = memory.scope.Agent.String()
	}
	if memory.scope.Conversation != nil {
		write.conversation = memory.scope.Conversation.String()
	}
	switch record.Kind {
	case wire.MemoryItem:
		write.key.id, write.present = record.ID, true
		write.conversation = conversation.String()
	case wire.MemoryForget:
		write.key.id = record.Target
	default:
		return
	}
	writes := &memory.laser.state().memoryWrites
	writes.mu.Lock()
	defer writes.mu.Unlock()
	if writes.byKey == nil {
		writes.byKey = make(map[memoryKey]*list.Element)
	}
	if previous := writes.byKey[write.key]; previous != nil {
		replaced := previous.Value.(*memoryWrite)
		write.unseen = !write.present && (replaced.present || replaced.unseen)
		writes.order.Remove(previous)
	}
	writes.byKey[write.key] = writes.order.PushBack(&write)
	if writes.order.Len() > 4096 {
		old := writes.order.Front()
		delete(writes.byKey, old.Value.(*memoryWrite).key)
		writes.order.Remove(old)
	}
}

func (memory *Memory) pendingWrites(namespace string) map[string]*memoryWrite {
	writes := &memory.laser.state().memoryWrites
	writes.mu.Lock()
	defer writes.mu.Unlock()
	pending := make(map[string]*memoryWrite)
	scope := memory.scope
	for key, element := range writes.byKey {
		if key.stream != memory.stream || key.topic != memory.topic || key.namespace != namespace {
			continue
		}
		w := element.Value.(*memoryWrite)
		if w.present && (scope.Conversation != nil && scope.Conversation.String() != w.conversation ||
			scope.User != "" && scope.User != w.user || scope.App != "" && scope.App != w.app ||
			scope.Agent != nil && scope.Agent.String() != w.agent) {
			continue
		}
		pending[key.id] = w
	}
	return pending
}

func (writes *memoryWrites) settle(write *memoryWrite) {
	writes.mu.Lock()
	defer writes.mu.Unlock()
	if current := writes.byKey[write.key]; current != nil && current.Value.(*memoryWrite) == write {
		writes.order.Remove(current)
		delete(writes.byKey, write.key)
	}
}

func (memory *Memory) recallView(ctx context.Context, query MemoryQuery) ([]MemoryItem, error) {
	if memory.scope.Stream != "" && memory.stream != memory.scope.Stream {
		return []MemoryItem{}, nil
	}
	namespace := memory.laser.ResourceName(memory.namespace)
	until := time.Now().Add(ownWritesWait)
	backoff := 50 * time.Millisecond
	for {
		pending := memory.pendingWrites(namespace)
		reflected := make(map[string]bool, len(pending))
		items, err := memory.scanView(ctx, query, func(entry *wire.KvEntry) {
			w := pending[string(entry.Key)]
			if w == nil {
				return
			}
			if w.present || w.conversation == "" || entry.Scope != nil && entry.Scope.Conversation != nil &&
				*entry.Scope.Conversation == w.conversation {
				reflected[w.key.id] = true
			}
		})
		if err != nil {
			return nil, err
		}
		expired := !time.Now().Before(until)
		behind := false
		for id, write := range pending {
			behind = behind || write.present && !reflected[id]
		}
		waiting := false
		for id, write := range pending {
			// A view still behind on this connection's items has not shown that an unseen one is gone.
			if reflected[id] != write.present || write.unseen && behind {
				waiting = true
				if !expired {
					continue
				}
			}
			memory.laser.state().memoryWrites.settle(write)
		}
		if !waiting {
			return items, nil
		}
		if expired {
			return nil, &StageTimeoutError{Stage: ownWritesStage}
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(min(time.Until(until), backoff)):
		}
		backoff = min(2*backoff, 500*time.Millisecond)
	}
}

type scoredMemoryRow struct {
	item             MemoryItem
	topic, partition uint32
	offset           uint64
}

func newestMemoryRows(a, b scoredMemoryRow) int {
	return cmp.Or(cmp.Compare(b.item.Score, a.item.Score), b.item.At.Compare(a.item.At),
		cmp.Compare(b.topic, a.topic), cmp.Compare(b.partition, a.partition), cmp.Compare(b.offset, a.offset),
		bytes.Compare(b.item.ID.ID[:], a.item.ID.ID[:]))
}

func (memory *Memory) scanView(ctx context.Context, query MemoryQuery, observe func(*wire.KvEntry)) ([]MemoryItem, error) {
	kv := memory.laser.Kv(memory.namespace)
	scan := wire.KvScan{V: wire.KvOpVersion, Namespace: kv.ResourceNamespace(), Page: 1}
	scope := memory.scope
	if scope.Conversation != nil {
		conversation := scope.Conversation.String()
		scan.Conversation = &conversation
		if stream := memory.laser.resourceStream(); stream != "" {
			scan.Stream = &stream
		}
	}
	if scope.User != "" {
		scan.User = &scope.User
	}
	if scope.App != "" {
		scan.App = &scope.App
	}
	if scope.Agent != nil {
		agent := scope.Agent.String()
		scan.Agent = &agent
	}
	words := newQueryWords(query)
	limit := recallLimit(query)
	var selected []scoredMemoryRow
	for {
		outcome, err := kv.execute(ctx, wire.AgdxKvScanCode, scan)
		if err != nil {
			return nil, err
		}
		if outcome.Kind != wire.KvPageOutcome || outcome.Page == nil {
			return nil, unexpectedKvOutcome("memory scan", outcome.Kind)
		}
		page := outcome.Page
		if page.Page != scan.Page {
			return nil, &ProtocolError{Reason: "memory scan returned a different page than requested"}
		}
		for i := range page.Items {
			entry := &page.Items[i]
			observe(entry)
			row, keep := viewMemoryRow(entry, scope)
			if !keep || !matchesKind(row.item, query.Kinds) || !words.keeps(row.item.Body) {
				continue
			}
			row.item.Score = words.overlap(row.item.Body)
			if entry.Scope.Weight != nil && query.Strategy != RecallRecent {
				row.item.Score += float64(*entry.Scope.Weight)
			}
			selected = append(selected, row)
		}
		slices.SortFunc(selected, newestMemoryRows)
		if len(selected) > limit {
			clear(selected[limit:])
			selected = selected[:limit]
		}
		if len(page.Items) == 0 || scan.Page >= page.TotalPages {
			break
		}
		scan.Page++
	}
	items := make([]MemoryItem, len(selected))
	for i := range selected {
		items[i] = selected[i].item
	}
	return items, nil
}

func viewMemoryRow(entry *wire.KvEntry, requested MemoryScope) (scoredMemoryRow, bool) {
	var row scoredMemoryRow
	stored := entry.Scope
	id, err := wire.ParseID(string(entry.Key))
	if err != nil || stored == nil {
		return row, false
	}
	scope := MemoryScope{Stream: requested.Stream, Conversation: requested.Conversation}
	if stored.Conversation != nil {
		parsed, err := wire.ParseID(*stored.Conversation)
		if err != nil {
			return row, false
		}
		scope.Conversation = &wire.ConversationID{ID: parsed}
	}
	if scope.Conversation == nil {
		return row, false
	}
	if stored.User != nil {
		scope.User = *stored.User
	}
	if stored.App != nil {
		scope.App = *stored.App
	}
	if stored.Agent != nil {
		if agent, err := wire.NewAgentID(*stored.Agent); err == nil {
			scope.Agent = &agent
		}
	}
	if !memoryScopeMatches(scope, requested) {
		return row, false
	}
	kind := MemoryFact
	if stored.Kind != nil {
		kind = MemoryKindFromWord(*stored.Kind)
	}
	row.item = MemoryItem{ID: MemoryID{ID: id}, Scope: scope, Body: entry.Value, Source: stored.Source, Kind: kind}
	if stored.TimestampMicros != nil {
		row.item.At = time.UnixMicro(int64(*stored.TimestampMicros)).UTC()
	}
	if src := stored.Source; src != nil && src.Message != nil {
		row.topic, row.partition, row.offset = src.Message.Topic, src.Message.Partition, src.Message.Offset
	}
	return row, true
}
