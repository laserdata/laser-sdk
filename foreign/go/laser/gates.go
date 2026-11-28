package laser

import (
	"context"
	"sync"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// Tuning for the gates a running agent applies. The caps bound memory by the
// recently active working set rather than by every record ever seen.
const (
	// FenceMapSoftCap is how many tasks a fence gate tracks before it
	// considers sweeping idle entries.
	FenceMapSoftCap = 16_384
	// FenceEntryTTL is how long an untouched entry survives a sweep. The gate
	// is kept for any task active within the window, so only tasks long idle,
	// where a stale holder's replay is no longer plausible, are dropped.
	FenceEntryTTL = 10 * time.Minute
	// FenceSweepInterval is the least time between sweeps, so the walk over
	// every entry runs at most this often under load rather than on each
	// accepted fence.
	FenceSweepInterval = time.Second
	// DedupWindow is how many dedup keys a consumer remembers.
	DedupWindow = 16_384
	// dedupScopeSeparator joins the principal and the key. It is the ASCII
	// unit separator, which cannot appear in an agent id, so two agents'
	// keys cannot collide by concatenation.
	dedupScopeSeparator = "\x1f"
)

// FenceGate drops a record whose fence is below the highest already accepted
// for its task.
//
// A holder that lost its lease and came back with stale work is the case this
// exists for: its fence is behind, so its effect is refused. An equal fence is
// the same holder's legitimate retry and is accepted, which dedup then handles.
//
// It is safe for concurrent use.
type FenceGate struct {
	mutex     sync.Mutex
	tasks     map[wire.ConversationID]fenceEntry
	lastSweep time.Time
}

type fenceEntry struct {
	fence   uint64
	touched time.Time
}

// NewFenceGate opens an empty gate.
func NewFenceGate() *FenceGate {
	return &FenceGate{tasks: map[wire.ConversationID]fenceEntry{}}
}

// Accept reports whether a fenced effect may be applied, advancing the task's
// high water when it may.
//
// A record carrying no fence is not fenced work and passes: the gate refuses
// stale holders, it does not require everything to be fenced.
func (gate *FenceGate) Accept(
	task wire.ConversationID, fence *uint64, now time.Time,
) bool {
	if fence == nil {
		return true
	}
	gate.mutex.Lock()
	defer gate.mutex.Unlock()
	gate.sweepLocked(now)

	entry, seen := gate.tasks[task]
	if seen && *fence < entry.fence {
		return false
	}
	gate.tasks[task] = fenceEntry{fence: *fence, touched: now}
	return true
}

// HighWater is the highest fence accepted for a task, and whether any was.
func (gate *FenceGate) HighWater(task wire.ConversationID) (uint64, bool) {
	gate.mutex.Lock()
	defer gate.mutex.Unlock()
	entry, seen := gate.tasks[task]
	return entry.fence, seen
}

// Tracked is how many tasks the gate currently holds.
func (gate *FenceGate) Tracked() int {
	gate.mutex.Lock()
	defer gate.mutex.Unlock()
	return len(gate.tasks)
}

// sweepLocked drops long-idle entries once the map is over its cap, bounding
// memory without reopening the gate for any recently active task.
func (gate *FenceGate) sweepLocked(now time.Time) {
	if len(gate.tasks) <= FenceMapSoftCap {
		return
	}
	if now.Sub(gate.lastSweep) <= FenceSweepInterval {
		return
	}
	gate.lastSweep = now
	for task, entry := range gate.tasks {
		if now.Sub(entry.touched) >= FenceEntryTTL {
			delete(gate.tasks, task)
		}
	}
}

// DedupStore decides whether a dedup key was seen before. A durable store plugs in here.
type DedupStore interface {
	// Observe records the key and reports whether it is new.
	Observe(ctx context.Context, key string) bool
}

// Deduplicator remembers the operations a consumer has already handled, so a
// redelivery is not applied twice.
//
// Delivery is at least once and handlers are expected to tolerate redelivery,
// so this is a best-effort window rather than a guarantee. The fence is the
// real at-most-once gate.
type Deduplicator struct {
	mutex sync.Mutex
	seen  map[string]struct{}
	order []string
	limit int
}

// NewDeduplicator opens a window remembering at most limit keys. A limit of
// zero or less takes DedupWindow.
func NewDeduplicator(limit int) *Deduplicator {
	if limit <= 0 {
		limit = DedupWindow
	}
	return &Deduplicator{seen: make(map[string]struct{}, limit), limit: limit}
}

// Accept reports whether this is the first time the key has been seen,
// remembering it when it is. A record with no dedup key always passes, since
// there is nothing to compare it against.
func (dedup *Deduplicator) Accept(key string) bool {
	if key == "" {
		return true
	}
	dedup.mutex.Lock()
	defer dedup.mutex.Unlock()
	if _, seen := dedup.seen[key]; seen {
		return false
	}
	dedup.seen[key] = struct{}{}
	dedup.order = append(dedup.order, key)
	// The oldest key leaves when the window is full, so memory stays bounded
	// by the window rather than by the traffic through it.
	if len(dedup.order) > dedup.limit {
		oldest := dedup.order[0]
		dedup.order = dedup.order[1:]
		delete(dedup.seen, oldest)
	}
	return true
}

// Observe records the key and reports whether it is new, as Accept does.
func (dedup *Deduplicator) Observe(_ context.Context, key string) bool {
	return dedup.Accept(key)
}

// Remembered is how many keys the window currently holds.
func (dedup *Deduplicator) Remembered() int {
	dedup.mutex.Lock()
	defer dedup.mutex.Unlock()
	return len(dedup.seen)
}

// DedupKey is the key a record is deduplicated under, empty when it carries
// none.
//
// It is scoped to the producing agent, so one producer cannot suppress or
// replay another's key. The agent is publisher-asserted, so this is a
// namespace against accidental reuse rather than a security boundary: the
// fence is the gate that holds against a hostile producer.
func DedupKey(provenance *Provenance) string {
	if provenance == nil || provenance.IdempotencyKey == nil {
		return ""
	}
	key := *provenance.IdempotencyKey
	if provenance.Agent == nil {
		return key
	}
	return provenance.Agent.String() + dedupScopeSeparator + key
}
