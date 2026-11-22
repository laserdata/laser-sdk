package laser

import (
	"testing"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func progressGeneration() wire.SourceGeneration {
	return wire.SourceGeneration{
		StreamID: 1, StreamCreatedAtMicros: 10,
		TopicID: 2, TopicCreatedAtMicros: 20,
		PartitionID: 0, PartitionIncarnation: 30,
	}
}

func progressDigest() wire.Digest32 {
	var digest [32]byte
	for index := range digest {
		digest[index] = 7
	}
	return wire.NewDigest32(digest)
}

// progressPage is a page that examined up to nextScanOffset, whose last
// examined offset is therefore the one before it.
func progressPage(nextScanOffset *uint64, stop wire.StopReason) wire.FilteredPage {
	digest := progressDigest()
	page := wire.FilteredPage{
		V:           wire.FilterOpVersion,
		PartitionID: 0,
		Policy: wire.AppliedPolicy{
			Digest: &digest, Mode: wire.ModeFiltered,
		},
		Generation:     progressGeneration(),
		NextScanOffset: nextScanOffset,
		Frontier:       100,
		Examined:       10,
		Stop:           stop,
	}
	if nextScanOffset != nil && *nextScanOffset > 0 {
		page.SafeAckOffset = ptrOf(*nextScanOffset - 1)
	}
	return page
}

// faultPage is a group page that stopped in front of a malformed record,
// read under another owner than progressPage.
func faultPage(nextScanOffset *uint64, offset uint64) wire.FilteredPage {
	page := progressPage(nextScanOffset, wire.StopFault)
	page.Policy.GroupID = ptrOf(uint64(3))
	page.Policy.PolicyGeneration = 2
	page.AckContext = wire.AckContext{OwnerGeneration: 5, MetadataOp: 9}
	page.Fault = &wire.RecordFault{Offset: offset, Reason: wire.FaultMalformed}
	return page
}

func progressReceipt(target *ackTarget, offset uint64) wire.AckReceipt {
	return wire.AckReceipt{PartitionID: 0, Offset: offset, Generation: target.Generation}
}

// unstoredOffset reads the pending offset, or nil when nothing is pending.
func unstoredOffset(progress *partitionProgress) *uint64 {
	if target := progress.unstoredTarget(); target != nil {
		return ptrOf(target.Offset)
	}
	return nil
}

func wantUnstored(t *testing.T, progress *partitionProgress, want uint64) {
	t.Helper()
	offset := unstoredOffset(progress)
	if offset == nil || *offset != want {
		t.Fatalf("got %v, want %d", offset, want)
	}
}

func wantNothingUnstored(t *testing.T, progress *partitionProgress, why string) {
	t.Helper()
	if offset := unstoredOffset(progress); offset != nil {
		t.Fatalf("%s, got %d", why, *offset)
	}
}

func TestGivenOutOfOrderAcksWhenSettledThenShouldStoreOnlyTheCompletedPrefix(t *testing.T) {
	progress := newPartitionProgress(wire.StartAtNext())
	progress.record(1, progressPage(ptrOf(uint64(10)), wire.StopFilled), []uint64{3, 7})
	progress.record(2, progressPage(ptrOf(uint64(20)), wire.StopFilled), []uint64{15})

	progress.completePage(2)
	wantNothingUnstored(t, progress, "the first page is still open")
	progress.completeRecord(1, 3)
	wantNothingUnstored(t, progress, "offset 7 is still open")
	progress.completeRecord(1, 7)

	wantUnstored(t, progress, 19)
	if progress.inFlight() != 0 {
		t.Fatalf("got %d pages still in flight", progress.inFlight())
	}
}

func TestGivenACheckpointInsideAPageWhenAcknowledgedThroughThenShouldLeaveLaterRecordsPending(t *testing.T) {
	progress := newPartitionProgress(wire.StartAtFirst())
	progress.record(1, progressPage(ptrOf(uint64(10)), wire.StopFilled), []uint64{3, 7})
	progress.record(2, progressPage(ptrOf(uint64(20)), wire.StopFilled), []uint64{11, 15})

	if !progress.completeThrough(2, 11) {
		t.Fatal("11 is inside the page")
	}
	wantUnstored(t, progress, 11)
	if progress.inFlight() != 1 {
		t.Fatalf("got %d pages in flight", progress.inFlight())
	}
	stored := progress.unstoredTarget()
	if !progress.acknowledged(stored, progressReceipt(stored, stored.Offset)) {
		t.Fatal("the checkpoint can be stored")
	}
	if !progress.completeThrough(2, 11) {
		t.Fatal("11 is still inside the page")
	}
	wantNothingUnstored(t, progress,
		"an acknowledgment the stored offset covers is not sent again")
	progress.completeRecord(2, 15)
	wantUnstored(t, progress, 19)
}

func TestGivenAnOffsetPastThePageWhenAcknowledgedThroughThenShouldKeepProgressUnchanged(t *testing.T) {
	progress := newPartitionProgress(wire.StartAtFirst())
	progress.record(1, progressPage(ptrOf(uint64(10)), wire.StopFilled), []uint64{3, 7})

	// The page made offsets up to 9 safe, so 10 is past what it examined.
	if progress.completeThrough(1, 10) {
		t.Fatal("an offset past the page must be refused")
	}
	wantNothingUnstored(t, progress, "nothing was completed")
	if progress.inFlight() != 1 {
		t.Fatalf("got %d pages in flight", progress.inFlight())
	}
}

func TestGivenAnEmptyPageWhenRecordedThenShouldMakeItsScanSafeAtOnce(t *testing.T) {
	progress := newPartitionProgress(wire.StartAtNext())
	progress.record(1, progressPage(ptrOf(uint64(500)), wire.StopBudget), nil)

	target := progress.unstoredTarget()
	if target == nil || target.Offset != 499 {
		t.Fatalf("an examined range is safe, got %+v", target)
	}
	if !progress.acknowledged(target, progressReceipt(target, 499)) {
		t.Fatal("the scan can be stored")
	}
	wantNothingUnstored(t, progress, "the scan was stored")
}

func TestGivenAnUnreadPartitionWhenWaitingThenShouldHoldFromItsStartOffset(t *testing.T) {
	if offset := newPartitionProgress(wire.StartFromOffset(7)).waitOffset(); offset != 7 {
		t.Fatalf("got %d", offset)
	}
	// Every other start resolves on the server, so the wait holds from the
	// beginning of the partition rather than from a position nobody knows.
	if offset := newPartitionProgress(wire.StartAtNext()).waitOffset(); offset != 0 {
		t.Fatalf("got %d", offset)
	}
}

func TestGivenAScannedPageWhenWaitingThenShouldHoldFromTheFirstUnexaminedRecord(t *testing.T) {
	progress := newPartitionProgress(wire.StartAtNext())
	progress.record(1, progressPage(ptrOf(uint64(42)), wire.StopEndOfVisible), nil)

	if offset := progress.waitOffset(); offset != 42 {
		t.Fatalf("got %d", offset)
	}
}

func TestGivenAPageThatExaminedNothingWhenWaitingThenShouldHoldPastTheHeadItSaw(t *testing.T) {
	progress := newPartitionProgress(wire.StartAtNext())
	caughtUp := progressPage(nil, wire.StopEndOfVisible)
	progress.record(1, caughtUp, nil)
	if offset := progress.waitOffset(); offset != 101 {
		t.Fatalf("got %d", offset)
	}

	empty := progressPage(nil, wire.StopEndOfVisible)
	empty.Frontier = 0
	progress.record(2, empty, nil)
	if offset := progress.waitOffset(); offset != 0 {
		t.Fatalf("an empty partition and one holding only offset 0 share a head of 0, got %d",
			offset)
	}
}

func TestGivenAPageThatExaminedNothingWhenRecordedThenShouldRepeatTheOriginalStart(t *testing.T) {
	progress := newPartitionProgress(wire.StartAtNext())
	progress.record(1, progressPage(nil, wire.StopEndOfVisible), nil)

	if progress.readStart().Kind != wire.StartNext {
		t.Fatalf("got %+v", progress.readStart())
	}
	wantNothingUnstored(t, progress, "nothing examined, nothing to store")
}

func TestGivenAScannedPageWhenRecordedThenShouldContinueAfterIt(t *testing.T) {
	progress := newPartitionProgress(wire.StartAtFirst())
	progress.record(1, progressPage(ptrOf(uint64(42)), wire.StopBudget), nil)

	start := progress.readStart()
	if start.Kind != wire.StartContinue || start.Continuation == nil {
		t.Fatalf("got %+v", start)
	}
	continuation := *start.Continuation
	if continuation.NextScanOffset != 42 ||
		continuation.Generation != progressGeneration() ||
		continuation.Mode != wire.ModeFiltered ||
		continuation.PolicyGeneration != 0 || continuation.GroupID != nil {
		t.Fatalf("got %+v", continuation)
	}
	if !sameOptionalDigest(continuation.Digest, ptrOf(progressDigest())) {
		t.Fatalf("got %+v", continuation.Digest)
	}
}

func TestGivenAFaultStopWhenRecordedThenShouldBlockThePartitionAtTheFault(t *testing.T) {
	progress := newPartitionProgress(wire.StartAtFirst())
	faulted := progressPage(ptrOf(uint64(8)), wire.StopFault)
	faulted.Fault = &wire.RecordFault{Offset: 8, Reason: wire.FaultMalformed}
	progress.record(1, faulted, []uint64{2})

	blocked := progress.blockedAt()
	if blocked == nil || blocked.Kind != blockFault || blocked.Offset != 8 ||
		blocked.Reason != wire.FaultMalformed {
		t.Fatalf("got %+v", blocked)
	}
	progress.completeRecord(1, 2)
	// The fault itself stays unacknowledged, so a later read sees it again.
	wantUnstored(t, progress, 7)
}

func TestGivenAnOversizedStopWhenRecordedThenShouldBlockWithoutAFaultReason(t *testing.T) {
	// The record decoded fine and only did not fit, so it carries no reason
	// a reader could act on.
	progress := newPartitionProgress(wire.StartAtFirst())
	oversized := progressPage(ptrOf(uint64(8)), wire.StopOversizedRecord)
	oversized.Fault = &wire.RecordFault{Offset: 8}
	progress.record(1, oversized, nil)

	blocked := progress.blockedAt()
	if blocked == nil || blocked.Kind != blockOversized || blocked.Offset != 8 ||
		blocked.Reason != "" {
		t.Fatalf("got %+v", blocked)
	}
	if !progress.skip(2, 8) {
		t.Fatal("an oversized record can be skipped like a fault")
	}
}

func TestGivenAStopWithoutAFaultWhenRecordedThenShouldNotBlock(t *testing.T) {
	// A stop reason names the record only when the page reports one, so a
	// page that named none blocks nothing.
	progress := newPartitionProgress(wire.StartAtFirst())
	progress.record(1, progressPage(ptrOf(uint64(8)), wire.StopFault), nil)

	if progress.blockedAt() != nil {
		t.Fatalf("got %+v", progress.blockedAt())
	}
	if progress.skip(2, 8) {
		t.Fatal("nothing is blocked, so nothing can be skipped")
	}
}

func TestGivenAFaultWhenSkippedThenShouldStoreItUnderTheFaultPageFence(t *testing.T) {
	progress := newPartitionProgress(wire.StartAtFirst())
	progress.record(1, progressPage(ptrOf(uint64(5)), wire.StopFilled), []uint64{4})
	faulted := faultPage(ptrOf(uint64(8)), 8)
	progress.record(2, faulted, nil)

	if !progress.skip(3, 8) {
		t.Fatal("the partition is blocked at 8")
	}
	if progress.blockedAt() != nil {
		t.Fatalf("got %+v", progress.blockedAt())
	}
	wantNothingUnstored(t, progress, "record 4 is still open, so the skip waits for it")
	progress.completeRecord(1, 4)
	want := &ackTarget{
		GroupID: faulted.Policy.GroupID, Offset: 8,
		Generation: faulted.Generation, AckContext: faulted.AckContext,
		Policy: faulted.Policy,
	}
	if !progress.unstoredTarget().sameAs(want) {
		t.Fatalf("got %+v, want %+v", progress.unstoredTarget(), want)
	}
}

func TestGivenASkippedFaultWhenReadAgainThenShouldStartAfterIt(t *testing.T) {
	continued := newPartitionProgress(wire.StartAtFirst())
	continued.record(1, faultPage(ptrOf(uint64(8)), 8), nil)
	continued.deferUntil(time.Now().Add(time.Minute))

	if !continued.skip(2, 8) {
		t.Fatal("the partition is blocked at 8")
	}
	if continued.waiting(time.Now()) {
		t.Fatal("a skip reads the partition at once")
	}
	if offset := continued.waitOffset(); offset != 9 {
		t.Fatalf("got %d", offset)
	}
	start := continued.readStart()
	if start.Kind != wire.StartContinue || start.Continuation == nil {
		t.Fatalf("got %+v", start)
	}
	continuation := *start.Continuation
	if continuation.NextScanOffset != 9 || continuation.PolicyGeneration != 2 ||
		continuation.GroupID == nil || *continuation.GroupID != 3 {
		t.Fatalf("got %+v", continuation)
	}

	// A page that examined nothing left no continuation to carry, so the
	// skip names the offset outright.
	unresolved := newPartitionProgress(wire.StartAtNext())
	unresolved.record(1, faultPage(nil, 8), nil)
	if !unresolved.skip(2, 8) {
		t.Fatal("the partition is blocked at 8")
	}
	start = unresolved.readStart()
	if start.Kind != wire.StartAtOffset || start.Offset != 9 {
		t.Fatalf("got %+v", start)
	}
}

func TestGivenABlockWhenSkippedAtAnotherOffsetOrAgainThenShouldRefuse(t *testing.T) {
	progress := newPartitionProgress(wire.StartAtFirst())
	progress.record(1, faultPage(ptrOf(uint64(8)), 8), nil)

	if progress.skip(2, 7) {
		t.Fatal("the partition is blocked at 8, not 7")
	}
	if progress.blockedAt() == nil {
		t.Fatal("a refused skip leaves the block standing")
	}
	wantUnstored(t, progress, 7)
	if !progress.skip(3, 8) {
		t.Fatal("the partition is blocked at 8")
	}
	if progress.skip(4, 8) {
		t.Fatal("the block is gone")
	}

	cleared := newPartitionProgress(wire.StartAtFirst())
	cleared.record(1, faultPage(ptrOf(uint64(8)), 8), nil)
	cleared.record(2, progressPage(ptrOf(uint64(20)), wire.StopFilled), nil)
	if cleared.skip(3, 8) {
		t.Fatal("a page past the record ended the block")
	}
}

func TestGivenEmptyPagesBehindAHeldPageWhenRecordedThenShouldNotGrowTheQueue(t *testing.T) {
	progress := newPartitionProgress(wire.StartAtNext())
	progress.record(1, progressPage(ptrOf(uint64(10)), wire.StopFilled), []uint64{4})
	for sequence := uint64(2); sequence < 200; sequence++ {
		progress.record(sequence,
			progressPage(ptrOf(10+sequence), wire.StopBudget), nil)
	}

	if progress.inFlight() != 1 {
		t.Fatalf("idle rounds hold no slot, got %d", progress.inFlight())
	}
	wantNothingUnstored(t, progress, "the held record keeps the offset")
	progress.completeRecord(1, 4)
	// The newest scan becomes safe with the held page.
	wantUnstored(t, progress, 208)
}

func TestGivenANewerCompletionWhileStoringWhenStoredThenShouldKeepTheNewerTarget(t *testing.T) {
	progress := newPartitionProgress(wire.StartAtNext())
	progress.record(1, progressPage(ptrOf(uint64(10)), wire.StopBudget), nil)
	first := progress.unstoredTarget()
	if first == nil {
		t.Fatal("the scan is safe")
	}
	copied := *first
	progress.record(2, progressPage(ptrOf(uint64(20)), wire.StopBudget), nil)

	if !progress.acknowledged(&copied, progressReceipt(&copied, copied.Offset)) {
		t.Fatal("the first scan was stored")
	}
	wantUnstored(t, progress, 19)
}

func TestGivenAStoredCheckpointWhenAnEarlierOneCompletesThenShouldSendNothing(t *testing.T) {
	progress := newPartitionProgress(wire.StartAtFirst())
	progress.record(1, progressPage(ptrOf(uint64(20)), wire.StopFilled), []uint64{11, 15, 18})
	if !progress.completeThrough(1, 15) {
		t.Fatal("15 is inside the page")
	}
	target := progress.unstoredTarget()
	if target == nil {
		t.Fatal("the checkpoint can be stored")
	}
	copied := *target
	if !progress.acknowledged(&copied, progressReceipt(&copied, 15)) {
		t.Fatal("the checkpoint was stored")
	}

	if !progress.completeThrough(1, 11) {
		t.Fatal("11 is inside the page")
	}
	wantNothingUnstored(t, progress, "15 is stored, so 11 needs no acknowledgment")
}

func TestGivenAReceiptAboveTheTargetWhenAcknowledgedThenShouldCoverOnlyWhatItReports(t *testing.T) {
	progress := newPartitionProgress(wire.StartAtFirst())
	progress.record(1, progressPage(ptrOf(uint64(20)), wire.StopFilled), []uint64{11, 15, 18})
	if !progress.completeThrough(1, 11) {
		t.Fatal("11 is inside the page")
	}
	target := progress.unstoredTarget()
	copied := *target

	if !progress.acknowledged(&copied, progressReceipt(&copied, 15)) {
		t.Fatal("a higher durable offset answers the target")
	}
	// The reader's own progress stays its own: another consumer's durable
	// offset is not something this reader stored.
	stored := progress.storedThrough()
	if stored == nil || *stored != 11 {
		t.Fatalf("got %v", stored)
	}
	if progress.inFlight() != 1 {
		t.Fatalf("another consumer's progress marks no local record handled, got %d",
			progress.inFlight())
	}
	if !progress.completeThrough(1, 15) {
		t.Fatal("15 is inside the page")
	}
	wantNothingUnstored(t, progress, "the durable offset covers 15")
	progress.completeRecord(1, 18)
	wantUnstored(t, progress, 19)
}

func TestGivenAReceiptBelowTheTargetOrForAnotherHistoryThenShouldRefuseIt(t *testing.T) {
	progress := newPartitionProgress(wire.StartAtFirst())
	progress.record(1, progressPage(ptrOf(uint64(20)), wire.StopFilled), []uint64{11, 15})
	if !progress.completeThrough(1, 11) {
		t.Fatal("11 is inside the page")
	}
	target := progress.unstoredTarget()
	copied := *target

	if progress.acknowledged(&copied, progressReceipt(&copied, 10)) {
		t.Fatal("a receipt below the target does not answer it")
	}
	recreated := progressReceipt(&copied, 11)
	recreated.Generation.PartitionIncarnation = 31
	if progress.acknowledged(&copied, recreated) {
		t.Fatal("a receipt from another incarnation does not answer the target")
	}
	if !progress.unstoredTarget().sameAs(&copied) {
		t.Fatalf("got %+v", progress.unstoredTarget())
	}
	if progress.storedThrough() != nil {
		t.Fatalf("got %v", progress.storedThrough())
	}
}

func TestGivenARetiredAssignmentWhenNewPagesArriveThenShouldRefuseTheOldSequence(t *testing.T) {
	original := newPartitionProgress(wire.StartAtNext())
	original.record(1, progressPage(ptrOf(uint64(10)), wire.StopFilled), []uint64{3})
	original.completePage(1)
	if !original.knows(1) {
		t.Fatal("an already completed delivery is retryable in its assignment")
	}

	reassigned := newPartitionProgress(wire.StartAtNext())
	if reassigned.knows(1) {
		t.Fatal("a fresh assignment knows no earlier page")
	}
	reassigned.record(2, progressPage(ptrOf(uint64(20)), wire.StopFilled), []uint64{15})
	if reassigned.knows(1) {
		t.Fatal("a page from the retired assignment is not this one's")
	}
	if !reassigned.knows(2) {
		t.Fatal("its own page is its own")
	}
}

func TestGivenADeferredPartitionWhenItsDelayPassesThenShouldReadAgain(t *testing.T) {
	// One failing partition neither starves the others nor spins the reader,
	// so it waits out a delay and then reads again.
	progress := newPartitionProgress(wire.StartAtNext())
	now := time.Now()
	if progress.waiting(now) {
		t.Fatal("a fresh partition waits for nothing")
	}
	progress.deferUntil(now.Add(time.Second))
	if !progress.waiting(now) {
		t.Fatal("the delay has not passed")
	}
	if progress.waiting(now.Add(2 * time.Second)) {
		t.Fatal("the delay passed")
	}
	// A page clears the delay, because the partition answered.
	progress.record(1, progressPage(ptrOf(uint64(10)), wire.StopFilled), nil)
	if progress.waiting(now) {
		t.Fatal("a page that arrived ends the wait")
	}
}

func TestGivenTwoFencesWhenComparedThenShouldDifferOnEveryPartOfThem(t *testing.T) {
	// The fence carries pointers and a digest held as bytes, so comparing by
	// identity would call two equal fences different and store an offset
	// twice, or call two different ones equal and drop an acknowledgment.
	page := faultPage(ptrOf(uint64(8)), 8)
	base := targetOf(page, 7)
	same := targetOf(page, 7)
	if !base.sameAs(same) {
		t.Fatal("two fences read from one page are the same fence")
	}
	if base.sameAs(nil) || (*ackTarget)(nil).sameAs(base) {
		t.Fatal("no fence is not some fence")
	}
	if !(*ackTarget)(nil).sameAs(nil) {
		t.Fatal("no fence is no fence")
	}

	for name, differ := range map[string]func(*ackTarget){
		"the offset":        func(target *ackTarget) { target.Offset = 8 },
		"the history":       func(target *ackTarget) { target.Generation.PartitionIncarnation = 31 },
		"the owner":         func(target *ackTarget) { target.AckContext.OwnerGeneration = 6 },
		"the group":         func(target *ackTarget) { target.GroupID = nil },
		"the policy filter": func(target *ackTarget) { target.Policy.FilterID = ptrOf(uint32(4)) },
		"the policy revision": func(target *ackTarget) {
			target.Policy.Revision = ptrOf(uint32(2))
		},
		"the policy mode": func(target *ackTarget) {
			target.Policy.Mode = wire.ModeUnfiltered
		},
		"the policy generation": func(target *ackTarget) { target.Policy.PolicyGeneration = 3 },
		"the policy digest": func(target *ackTarget) {
			var other [32]byte
			other[0] = 9
			target.Policy.Digest = ptrOf(wire.NewDigest32(other))
		},
		"a missing policy digest": func(target *ackTarget) { target.Policy.Digest = nil },
	} {
		t.Run(name, func(t *testing.T) {
			changed := targetOf(page, 7)
			differ(changed)
			if base.sameAs(changed) {
				t.Fatalf("%s makes it another fence", name)
			}
		})
	}
}
