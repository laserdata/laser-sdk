package laser

import (
	"context"
	"errors"
	"slices"
	"time"

	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// GroupReaderOptions configure a group reader. The zero value takes every default.
type GroupReaderOptions struct {
	// Start is where each partition held at the open starts. The default
	// resumes after the group's stored offset, as every partition gained
	// later does.
	Start *wire.FilteredStart
	// Partitions restricts reads to these partitions when the group assigns
	// them to this member. Empty reads the whole assignment.
	Partitions []uint32
	// Count is the most matching records one page returns.
	Count uint32
	// MaxExamined is the most source records one page examines. Zero leaves
	// the scan to the server's own budget.
	MaxExamined uint32
	// MaxReplyBytes is the most record bytes one page carries.
	MaxReplyBytes uint32
	// LocalGuard evaluates every returned record again with the shared
	// evaluator and fails the page on a disagreement.
	LocalGuard bool
	// IdleInterval is how long the reader waits when nothing is new on a
	// server that holds no filtered wait, and before a blocked or failed
	// partition is read again.
	IdleInterval time.Duration
	// MaxWait is how long the server may hold one filtered wait, at most
	// what it announces. A negative value paces idle rounds by IdleInterval.
	MaxWait time.Duration
	// MaxUnackedPages is the most pages with records one partition holds
	// unacknowledged.
	MaxUnackedPages int
}

func (options GroupReaderOptions) resolved() (GroupReaderOptions, error) {
	if options.MaxUnackedPages < 0 || options.IdleInterval < 0 {
		return options, &InvalidError{Reason: "group reader max unacknowledged pages and idle interval must not be negative"}
	}
	if options.Count == 0 {
		options.Count = DefaultFilteredCount
	}
	if options.MaxReplyBytes == 0 {
		options.MaxReplyBytes = DefaultFilteredReplyBytes
	}
	if options.IdleInterval == 0 {
		options.IdleInterval = filteredIdleInterval
	}
	switch {
	case options.MaxWait == 0:
		options.MaxWait = defaultFilteredMaxWait
	case options.MaxWait < 0:
		options.MaxWait = 0
	}
	if options.MaxUnackedPages == 0 {
		options.MaxUnackedPages = defaultMaxUnackedPages
	}
	options.Partitions = slices.Clone(options.Partitions)
	return options, nil
}

// NextPage returns the next page with at least one matching record across
// every partition this member reads. It waits while nothing is new, so bound
// it with the context.
func (reader *GroupReader) NextPage(ctx context.Context) (*MatchedPage, error) {
	for {
		page, more, err := reader.ReadRound(ctx)
		if err != nil || page != nil {
			return page, err
		}
		if more {
			continue
		}
		reader.awaitRecords(ctx)
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
}

// TryNextPage reads each partition in turn until a page matches or nothing
// is new, without the idle wait. It returns nil when nothing is new.
func (reader *GroupReader) TryNextPage(ctx context.Context) (*MatchedPage, error) {
	for {
		page, more, err := reader.ReadRound(ctx)
		if err != nil || page != nil || !more {
			return page, err
		}
	}
}

// ReadRound performs one bounded round without an idle wait. The flag
// reports unmatched work that remains, and ExaminedInRound counts what the
// round examined.
func (reader *GroupReader) ReadRound(ctx context.Context) (*MatchedPage, bool, error) {
	reader.roundMu.Lock()
	defer reader.roundMu.Unlock()
	return reader.round(ctx)
}

// ExaminedInRound is the number of source records the last round examined, pages that matched nothing included.
func (reader *GroupReader) ExaminedInRound() uint64 {
	reader.roundMu.Lock()
	defer reader.roundMu.Unlock()
	return reader.examined
}

// IdleInterval is how long this reader waits when nothing is new on a server that holds no filtered wait.
func (reader *GroupReader) IdleInterval() time.Duration { return reader.idle }

// NextRecord returns the next matching record, one at a time over NextPage.
func (reader *GroupReader) NextRecord(ctx context.Context) (*MatchedRecord, error) {
	for {
		reader.roundMu.Lock()
		for len(reader.buffered) > 0 {
			record := reader.buffered[0]
			reader.buffered = reader.buffered[1:]
			if reader.Owns(record.page) {
				reader.roundMu.Unlock()
				return &record, nil
			}
		}
		reader.roundMu.Unlock()
		page, err := reader.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		reader.roundMu.Lock()
		reader.buffered = append(reader.buffered, page.Records...)
		reader.roundMu.Unlock()
	}
}

// Owns reports whether the page came from this reader in its current membership, so its records can still be acknowledged.
func (reader *GroupReader) Owns(page *MatchedPage) bool {
	reader.progressMu.Lock()
	defer reader.progressMu.Unlock()
	_, err := reader.pageProgress(page)
	return err == nil
}

// awaitRecords waits like WaitForRecords and keeps what became ready for the next round.
func (reader *GroupReader) awaitRecords(ctx context.Context) []uint32 {
	ready, moved := reader.waitForRecords(ctx, reader.idle)
	reader.roundMu.Lock()
	for _, partition := range ready {
		reader.ready[partition] = struct{}{}
	}
	reader.roundMu.Unlock()
	if moved {
		reader.mu.Lock()
		reader.syncDue = true
		reader.mu.Unlock()
	}
	return ready
}

// round reads each readable partition once, from where the last round stopped, until one returns records.
func (reader *GroupReader) round(ctx context.Context) (*MatchedPage, bool, error) {
	reader.examined = 0
	if err := reader.deferred; err != nil {
		reader.deferred = nil
		return nil, false, err
	}
	reader.mu.Lock()
	due := reader.syncDue || time.Since(reader.syncedAt) >= reader.refresh
	reader.mu.Unlock()
	if due {
		if _, err := reader.Partitions(ctx); err != nil {
			return nil, false, err
		}
	}
	readable := reader.readable()
	if len(readable) == 0 {
		return nil, false, nil
	}
	now := time.Now()
	var busy bool
	var failed error
	var saturated []uint32
	for step := range readable {
		index := (reader.cursor + step) % len(readable)
		partition := readable[index]
		start, full, waiting := reader.partitionStart(partition, now)
		if waiting {
			continue
		}
		if full {
			saturated = append(saturated, partition)
			continue
		}
		page, more, err := reader.readPartition(ctx, partition, start)
		if err != nil {
			reader.deferPartition(partition, now.Add(reader.retry))
			if failed == nil {
				failed = err
			}
			continue
		}
		if page != nil {
			reader.cursor = index + 1
			// A page is delivered first and the failure of another partition follows it.
			reader.deferred = failed
			return page, false, nil
		}
		busy = busy || more
	}
	if failed != nil {
		return nil, false, failed
	}
	if len(saturated) > 0 && !busy {
		return nil, false, &InvalidError{Reason: "partitions " + joinPartitions(saturated) + " each have " +
			itoa(reader.maxUnacked) + " unacknowledged pages, acknowledge before reading more"}
	}
	return nil, busy, nil
}

// readable is the assignment this reader may read, narrowed to the partitions a wait found ready.
func (reader *GroupReader) readable() []uint32 {
	reader.mu.Lock()
	readable := slices.Clone(reader.assigned)
	reader.mu.Unlock()
	if selected := reader.options.Partitions; len(selected) > 0 {
		readable = slices.DeleteFunc(readable, func(partition uint32) bool { return !slices.Contains(selected, partition) })
	}
	woken := slices.DeleteFunc(slices.Clone(readable), func(partition uint32) bool {
		_, ready := reader.ready[partition]
		return !ready
	})
	clear(reader.ready)
	if len(woken) > 0 {
		return woken
	}
	return readable
}

// partitionStart is where the next read of a partition starts, with whether it is full or waits out a delay.
func (reader *GroupReader) partitionStart(partition uint32, now time.Time) (start wire.FilteredStart, full, waiting bool) {
	reader.progressMu.Lock()
	defer reader.progressMu.Unlock()
	progress := reader.progress[partition]
	if progress == nil {
		first := wire.StartAtNext()
		if _, unread := reader.unread[partition]; unread && reader.options.Start != nil {
			first = *reader.options.Start
		}
		progress = newPartitionProgress(first)
		reader.progress[partition] = progress
	}
	delete(reader.unread, partition)
	return progress.readStart(), progress.inFlight() >= reader.maxUnacked, progress.waiting(now)
}

// readPartition reads one page. It returns nil without an error for a page without records, with whether more work remains.
func (reader *GroupReader) readPartition(ctx context.Context, partition uint32, start wire.FilteredStart) (*MatchedPage, bool, error) {
	request := reader.Read(partition, wire.GroupPolicy()).From(start).
		Count(reader.options.Count).MaxReplyBytes(reader.options.MaxReplyBytes)
	if reader.options.MaxExamined > 0 {
		request.MaxExamined(reader.options.MaxExamined)
	}
	page, err := request.Fetch(ctx)
	if err != nil {
		again, reported := reader.afterFailure(partition, err)
		return nil, again, reported
	}
	delete(reader.lost, partition)
	reader.examined += uint64(page.Examined())
	reader.progressMu.Lock()
	var blocked *blockedRecord
	if progress := reader.progress[partition]; progress != nil {
		if blocked = progress.blockedAt(); blocked != nil {
			progress.deferUntil(time.Now().Add(reader.idle))
		}
	}
	reader.progressMu.Unlock()
	if len(page.Records) > 0 {
		return page, false, nil
	}
	// A failed store of an empty page is sent again with the next acknowledgment.
	_ = reader.ack(ctx, page, nil)
	if blocked != nil {
		return nil, false, &FilterFaultError{PartitionID: partition, Offset: blocked.Offset, Reason: blocked.Reason,
			Oversized: blocked.Kind == blockOversized, Page: page}
	}
	return nil, page.Page.Stop == wire.StopFilled || page.Page.Stop == wire.StopBudget, nil
}

// afterFailure restarts a partition whose history, policy or owner changed
// under the read. It reports whether the partition is read again at once, and
// the error the caller gets otherwise.
func (reader *GroupReader) afterFailure(partition uint32, err error) (bool, error) {
	var guarded *guardRefusal
	fromGuard := errors.As(err, &guarded)
	if fromGuard {
		err = guarded.cause
	}
	switch readRefusal(err) {
	case wire.ReasonMembershipStale:
		reader.forgetLeader(partition)
		// Only a second loss in a row reaches the caller, the first is answered by a sync.
		if _, again := reader.lost[partition]; again {
			return false, err
		}
		reader.lost[partition] = struct{}{}
		reader.expireAssignment()
		return true, nil
	case wire.ReasonConflict:
		reader.restart(partition)
		return !fromGuard, guardedError(fromGuard, err)
	case wire.ReasonSourceChanged:
		reader.restart(partition)
		reader.expireAssignment()
		return false, err
	default:
		return false, err
	}
}

func guardedError(fromGuard bool, err error) error {
	if fromGuard {
		return err
	}
	return nil
}

// restart reads a partition again from its stored offset, under the policy the group holds now.
func (reader *GroupReader) restart(partition uint32) {
	reader.forgetLeader(partition)
	reader.resetPartition(partition)
	// Buffered records of the partition belong to the dropped pages.
	reader.buffered = slices.DeleteFunc(reader.buffered, func(record MatchedRecord) bool { return record.PartitionID == partition })
	reader.guardMu.Lock()
	reader.guard = nil
	reader.guardMu.Unlock()
}

func (reader *GroupReader) expireAssignment() {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	reader.syncDue = true
}

func (reader *GroupReader) forgetLeader(partition uint32) {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	delete(reader.leaders, partition)
}

// guardPage runs the local guard over a filtered page before it is tracked.
func (reader *GroupReader) guardPage(ctx context.Context, page wire.FilteredPage, records []MatchedRecord) error {
	if !reader.guarded || !page.Policy.Mode.IsFiltered() {
		return nil
	}
	reader.guardMu.Lock()
	defer reader.guardMu.Unlock()
	if reader.guard == nil {
		guard, err := loadLocalGuard(ctx, reader.laser, page, reader.source)
		if err != nil {
			return err
		}
		reader.guard = guard
	}
	if err := reader.guard.check(page, records); err != nil {
		return &guardRefusal{cause: err}
	}
	return nil
}

// guardRefusal marks a page the local guard refused, which reaches the caller even when it restarts the partition.
type guardRefusal struct{ cause error }

func (refusal *guardRefusal) Error() string { return refusal.cause.Error() }

func (refusal *guardRefusal) Unwrap() error { return refusal.cause }

// readRefusal classifies a failed read, reading a history the server no longer holds as a changed source.
func readRefusal(err error) wire.FilterErrorReason {
	var refusal *FilterError
	if errors.As(err, &refusal) {
		return refusal.Reason()
	}
	if errors.Is(err, ierror.HistoryUnavailable{}) {
		return wire.ReasonSourceChanged
	}
	return ""
}

func joinPartitions(partitions []uint32) string {
	text := ""
	for index, partition := range partitions {
		if index > 0 {
			text += ", "
		}
		text += itoa(int(partition))
	}
	return text
}
