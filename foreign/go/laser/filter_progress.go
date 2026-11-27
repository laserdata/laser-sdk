package laser

import (
	"bytes"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// partitionProgress is one partition's read position and acknowledgment
// state for an advanced filtered read.
//
// Pages are read ahead of acknowledgments. A page's safe offset is stored
// only once it and every earlier page are fully handled, so an
// acknowledgment never covers a record the caller has not finished.
//
// It is the reader's own bookkeeping rather than part of the client's
// surface, so it stays unexported, as the Rust peer stays crate-private.
type partitionProgress struct {
	original wire.FilteredStart
	start    wire.FilteredStart
	// pages are the read but not fully handled pages, oldest first.
	pages    []*pendingPage
	unstored *ackTarget
	// storedOffset is the newest offset this reader stored, nil before it
	// stored any. Offset zero is a stored offset like any other, so it is a
	// pointer rather than a zero that would read as unset.
	storedOffset *uint64
	// durable is the offset the last receipt reported for a source
	// generation, which another consumer can have moved past this reader's
	// own progress.
	durable           *uint64
	durableGeneration wire.SourceGeneration
	blocked           *blockedRecord
	// skipTarget is the acknowledgment that skips the blocking record,
	// fenced like the page that reported the block.
	skipTarget *ackTarget
	// retryAt is when a deferred partition may be read again, zero when it
	// is not waiting.
	retryAt time.Time
	// firstSequence is the first page sequence this partition saw, nil until
	// it read one.
	firstSequence   *uint64
	settledSequence uint64
	// waitFrom is the first offset no page examined yet, as the last page
	// reported it.
	waitFrom *uint64
}

// ackTarget is the offset a completed prefix of pages makes safe to store,
// with the group, the history and the partition owner it was read under.
type ackTarget struct {
	GroupID    *uint64
	Offset     uint64
	Generation wire.SourceGeneration
	AckContext wire.AckContext
	Policy     wire.AppliedPolicy
}

// sameAs compares two targets by value. The policy carries pointers and a
// digest held as bytes, so identity would call two equal fences different
// and store an offset twice.
func (target *ackTarget) sameAs(other *ackTarget) bool {
	switch {
	case target == nil || other == nil:
		return target == other
	case target.Offset != other.Offset:
		return false
	case target.Generation != other.Generation:
		return false
	case target.AckContext != other.AckContext:
		return false
	case !sameOptionalUint64(target.GroupID, other.GroupID):
		return false
	}
	return samePolicy(target.Policy, other.Policy)
}

// blockKind is why a partition cannot be read past its current position.
type blockKind uint8

const (
	// blockFault is a record that failed to decode under the stop fault
	// policy.
	blockFault blockKind = iota
	// blockOversized is one matching record that alone exceeds the reply
	// cap.
	blockOversized
)

// blockedRecord is the record a partition stopped at.
type blockedRecord struct {
	Kind   blockKind
	Offset uint64
	// Reason is why the record could not be evaluated, empty for an
	// oversized record, which decoded fine and only did not fit.
	Reason wire.FaultReason
}

// pendingPage is one read page and the returned records still outstanding.
type pendingPage struct {
	sequence uint64
	target   *ackTarget
	// outstanding are the offsets of the returned records the caller has not
	// finished.
	outstanding map[uint64]struct{}
}

func newPartitionProgress(start wire.FilteredStart) *partitionProgress {
	return &partitionProgress{original: start, start: start}
}

// waiting reports whether the partition waits out a delay after an error or
// a block, so one failing partition neither starves the others nor spins the
// reader.
func (progress *partitionProgress) waiting(now time.Time) bool {
	return !progress.retryAt.IsZero() && now.Before(progress.retryAt)
}

func (progress *partitionProgress) deferUntil(until time.Time) {
	progress.retryAt = until
}

// start is where the next read starts.
func (progress *partitionProgress) readStart() wire.FilteredStart {
	return progress.start
}

func (progress *partitionProgress) blockedAt() *blockedRecord {
	return progress.blocked
}

// waitOffset is the offset a filtered wait holds this partition from: the
// first record no page examined, else the start offset of an unread
// partition.
func (progress *partitionProgress) waitOffset() uint64 {
	switch {
	case progress.waitFrom != nil:
		return *progress.waitFrom
	case progress.original.Kind == wire.StartAtOffset:
		return progress.original.Offset
	default:
		return 0
	}
}

// inFlight is the number of pages read but not yet fully handled.
func (progress *partitionProgress) inFlight() int {
	return len(progress.pages)
}

// knows reports whether a page sequence belongs to this partition, whether
// it is still in flight or already settled.
func (progress *partitionProgress) knows(sequence uint64) bool {
	for _, page := range progress.pages {
		if page.sequence == sequence {
			return true
		}
	}
	return progress.firstSequence != nil &&
		sequence >= *progress.firstSequence && sequence <= progress.settledSequence
}

// record tracks a page and its returned offsets, and continues after it. A
// blocked partition is read again after its delay, so a block ends once the
// record ages out of the partition or the policy stops faulting.
//
// A page without records waits only for the pages before it, so behind an
// unfinished page it folds its safe offset into the newest one instead of
// queueing. Only pages that returned records hold a slot, and a caller
// holding one page on a quiet partition never fills the queue.
func (progress *partitionProgress) record(
	sequence uint64, page wire.FilteredPage, offsets []uint64,
) {
	progress.retryAt = time.Time{}
	if progress.firstSequence == nil {
		progress.firstSequence = ptrOf(sequence)
	}
	progress.start = page.NextStart(progress.original)
	// A page that examined nothing saw the head as its frontier, so the next
	// record lands after it. A head of zero may also be an empty partition,
	// whose next record is zero.
	switch {
	case page.NextScanOffset != nil:
		progress.waitFrom = ptrOf(*page.NextScanOffset)
	case page.Frontier == 0:
		progress.waitFrom = ptrOf(uint64(0))
	default:
		progress.waitFrom = ptrOf(saturatingAdd(page.Frontier, 1))
	}
	progress.blocked = nil
	if page.Fault != nil {
		switch page.Stop {
		case wire.StopFault:
			progress.blocked = &blockedRecord{
				Kind: blockFault, Offset: page.Fault.Offset, Reason: page.Fault.Reason}
		case wire.StopOversizedRecord:
			progress.blocked = &blockedRecord{
				Kind: blockOversized, Offset: page.Fault.Offset}
		}
	}
	progress.skipTarget = nil
	if progress.blocked != nil {
		progress.skipTarget = targetOf(page, progress.blocked.Offset)
	}
	var target *ackTarget
	if page.SafeAckOffset != nil {
		target = targetOf(page, *page.SafeAckOffset)
	}
	outstanding := make(map[uint64]struct{}, len(offsets))
	for _, offset := range offsets {
		outstanding[offset] = struct{}{}
	}
	newest := progress.newestPage()
	if newest != nil && len(outstanding) == 0 {
		if target != nil {
			newest.target = target
		}
	} else {
		progress.pages = append(progress.pages, &pendingPage{
			sequence: sequence, target: target, outstanding: outstanding,
		})
	}
	progress.settle()
}

// completeRecord marks one returned record handled. A record already settled
// is a no-op.
func (progress *partitionProgress) completeRecord(sequence, offset uint64) {
	if page := progress.pageOf(sequence); page != nil {
		delete(page.outstanding, offset)
	}
	progress.settle()
}

// completeThrough marks every record of the page up to and including offset
// handled, and every record of every earlier page. It reports false for an
// offset past what the page made safe.
func (progress *partitionProgress) completeThrough(sequence, offset uint64) bool {
	page := progress.pageOf(sequence)
	if page == nil || page.target == nil {
		return true
	}
	target := *page.target
	if offset > target.Offset {
		return false
	}
	for _, pending := range progress.pages {
		switch {
		case pending.sequence < sequence:
			pending.outstanding = map[uint64]struct{}{}
		case pending.sequence == sequence:
			for held := range pending.outstanding {
				if held <= offset {
					delete(pending.outstanding, held)
				}
			}
		}
	}
	progress.settle()
	target.Offset = offset
	if progress.unstored == nil || progress.unstored.Offset < offset {
		progress.pend(&target)
	}
	return true
}

// skip passes the record the partition is blocked at, as a native consumer
// seeks past it. The skip is acknowledged under the fence of the page that
// reported the block and, like an empty page, only once every earlier page
// is handled. It reports false for an offset the partition is not blocked
// at.
func (progress *partitionProgress) skip(sequence, offset uint64) bool {
	if progress.skipTarget == nil || progress.skipTarget.Offset != offset {
		return false
	}
	target := progress.skipTarget
	progress.skipTarget = nil
	next := saturatingAdd(offset, 1)
	progress.blocked = nil
	progress.retryAt = time.Time{}
	progress.waitFrom = ptrOf(next)
	if progress.start.Kind == wire.StartContinue && progress.start.Continuation != nil {
		continuation := *progress.start.Continuation
		continuation.NextScanOffset = next
		progress.start = wire.StartFromContinuation(continuation)
	} else {
		progress.start = wire.StartFromOffset(next)
	}
	if newest := progress.newestPage(); newest != nil {
		newest.target = target
	} else {
		progress.pages = append(progress.pages, &pendingPage{
			sequence: sequence, target: target,
			outstanding: map[uint64]struct{}{},
		})
	}
	progress.settle()
	return true
}

// completePage marks every record of a page handled. A page already settled
// is a no-op.
func (progress *partitionProgress) completePage(sequence uint64) {
	if page := progress.pageOf(sequence); page != nil {
		page.outstanding = map[uint64]struct{}{}
	}
	progress.settle()
}

// unstoredTarget is the newest offset the completed prefix makes safe and
// that is not yet stored.
func (progress *partitionProgress) unstoredTarget() *ackTarget {
	return progress.unstored
}

// acknowledged reports whether receipt answered the store of target: the
// same source generation and an offset at least the target's.
//
// The receipt's offset is the durable progress, which only covers later
// acknowledgments and never marks a local record handled. A newer target
// completed meanwhile stays pending unless the durable progress covers it.
func (progress *partitionProgress) acknowledged(
	target *ackTarget, receipt wire.AckReceipt,
) bool {
	if receipt.Generation != target.Generation || receipt.Offset < target.Offset {
		return false
	}
	if progress.storedOffset == nil || *progress.storedOffset < target.Offset {
		progress.storedOffset = ptrOf(target.Offset)
	}
	durable := receipt.Offset
	if progress.durable != nil && progress.durableGeneration == receipt.Generation &&
		*progress.durable > durable {
		durable = *progress.durable
	}
	progress.durable = ptrOf(durable)
	progress.durableGeneration = receipt.Generation
	if progress.unstored != nil &&
		(progress.unstored.sameAs(target) || progress.covered(progress.unstored)) {
		progress.unstored = nil
	}
	return true
}

// storedThrough is the newest offset this reader stored for the partition,
// nil before it stored any.
func (progress *partitionProgress) storedThrough() *uint64 {
	return progress.storedOffset
}

// pend keeps target to store unless stored or durable progress covers it. A
// covered target also covers an older one still pending.
func (progress *partitionProgress) pend(target *ackTarget) {
	if !progress.covered(target) {
		progress.unstored = target
		return
	}
	if progress.unstored != nil && progress.unstored.Offset <= target.Offset {
		progress.unstored = nil
	}
}

func (progress *partitionProgress) covered(target *ackTarget) bool {
	if progress.storedOffset != nil && *progress.storedOffset >= target.Offset {
		return true
	}
	return progress.durable != nil &&
		progress.durableGeneration == target.Generation &&
		*progress.durable >= target.Offset
}

// settle retires the completed prefix. Its newest safe offset supersedes any
// older unstored one, because a stored offset covers everything before it.
func (progress *partitionProgress) settle() {
	for len(progress.pages) > 0 && len(progress.pages[0].outstanding) == 0 {
		page := progress.pages[0]
		progress.pages = progress.pages[1:]
		progress.settledSequence = page.sequence
		if page.target != nil {
			progress.pend(page.target)
		}
	}
}

func (progress *partitionProgress) pageOf(sequence uint64) *pendingPage {
	for _, page := range progress.pages {
		if page.sequence == sequence {
			return page
		}
	}
	return nil
}

func (progress *partitionProgress) newestPage() *pendingPage {
	if len(progress.pages) == 0 {
		return nil
	}
	return progress.pages[len(progress.pages)-1]
}

// targetOf is the acknowledgment one page fences at offset.
func targetOf(page wire.FilteredPage, offset uint64) *ackTarget {
	return &ackTarget{
		GroupID:    page.Policy.GroupID,
		Offset:     offset,
		Generation: page.Generation,
		AckContext: page.AckContext,
		Policy:     page.Policy,
	}
}

// samePolicy compares two applied policies by value.
func samePolicy(left, right wire.AppliedPolicy) bool {
	switch {
	case left.Mode != right.Mode:
		return false
	case left.PolicyGeneration != right.PolicyGeneration:
		return false
	case !sameOptionalUint64(left.GroupID, right.GroupID):
		return false
	case !sameOptionalUint32(left.FilterID, right.FilterID):
		return false
	case !sameOptionalUint32(left.Revision, right.Revision):
		return false
	}
	return sameOptionalDigest(left.Digest, right.Digest)
}

func sameOptionalUint64(left, right *uint64) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func sameOptionalUint32(left, right *uint32) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func sameOptionalDigest(left, right *wire.Digest32) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return bytes.Equal(left.Bytes, right.Bytes)
}
