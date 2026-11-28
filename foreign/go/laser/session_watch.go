package laser

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// SessionChange identifies the sessions changed since the previous read.
// Resync asks the caller to list sessions again after a retention gap or an index replacement.
type SessionChange struct {
	Sessions []wire.ConversationID
	Resync   bool
}

// SessionWatch follows changes to a stream's managed session index.
type SessionWatch struct {
	sessions  *Sessions
	pollEvery time.Duration
	next      uint64
	results   uint32
	series    uint64
	reading   chan struct{}
}

// Watch starts after the newest change already visible to this read.
func (sessions *Sessions) Watch(ctx context.Context, pollEvery time.Duration) (*SessionWatch, error) {
	if pollEvery <= 0 {
		return nil, &InvalidError{Reason: "session watch poll interval must be above zero"}
	}
	start, err := sessions.Changes(ctx, 1, 0)
	if err != nil {
		return nil, err
	}
	return &SessionWatch{sessions: sessions, pollEvery: pollEvery, next: nextChangeSequence(start.TotalResults), results: start.Results, series: start.Series, reading: make(chan struct{}, 1)}, nil
}

func nextChangeSequence(last uint64) uint64 {
	if last == math.MaxUint64 {
		return last
	}
	return last + 1
}

// Next waits for an index change or context cancellation.
func (watch *SessionWatch) Next(ctx context.Context) (SessionChange, error) {
	select {
	case watch.reading <- struct{}{}:
		defer func() { <-watch.reading }()
	case <-ctx.Done():
		return SessionChange{}, ctx.Err()
	}
	for {
		index := (watch.next - 1) / uint64(max(watch.results, 1))
		page := uint32(min(index, uint64(math.MaxUint32-1))) + 1
		changes, err := watch.sessions.Changes(ctx, page, watch.results)
		if err != nil {
			var coded interface{ ResultCode() wire.ResultCode }
			if !errors.As(err, &coded) || coded.ResultCode() != wire.ResultStale {
				return SessionChange{}, err
			}
		} else if change, ok := watch.observe(changes); ok {
			return change, nil
		}
		if !sleepFor(ctx, watch.pollEvery) {
			return SessionChange{}, ctx.Err()
		}
	}
}

func (watch *SessionWatch) observe(page *wire.SessionChanges) (SessionChange, bool) {
	watch.results = page.Results
	if page.Series != watch.series || page.Floor > watch.next {
		watch.series = page.Series
		watch.next = nextChangeSequence(page.TotalResults)
		return SessionChange{Resync: true}, true
	}
	minSequence := watch.next
	seen := make(map[wire.ConversationID]struct{})
	var change SessionChange
	advanced := false
	for _, row := range page.Items {
		if row.Seq < minSequence {
			continue
		}
		advanced = true
		watch.next = nextChangeSequence(row.Seq)
		change.Resync = change.Resync || row.Truncated
		for _, id := range row.Sessions {
			if _, ok := seen[id]; !ok {
				seen[id] = struct{}{}
				change.Sessions = append(change.Sessions, id)
			}
		}
	}
	if change.Resync {
		change.Sessions = nil
	}
	return change, advanced
}
