package laser

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenRecordsOnTwoPartitionsWhenReadingNextPagesThenShouldWalkTheWholeAssignment(t *testing.T) {
	server := newPolicyServer(0, 1)
	server.append(0, 2)
	server.append(1, 3)
	reader := openPolicyTestReader(t, server)

	seen := map[uint32]int{}
	for range 2 {
		page := nextRoundPage(t, reader)
		seen[page.PartitionID] = len(page.Records)
		if err := reader.Ack(context.Background(), page); err != nil {
			t.Fatalf("acknowledge partition %d: %v", page.PartitionID, err)
		}
	}

	if seen[0] != 2 || seen[1] != 3 {
		t.Fatalf("want two records of partition 0 and three of partition 1, got %v", seen)
	}
	if stored, ok := server.storedOffset(1); !ok || stored != 2 {
		t.Fatalf("want partition 1 stored through offset 2, stored %d (%v)", stored, ok)
	}
}

func TestGivenNothingNewWhenTryingTheNextPageThenShouldAnswerNoneWithoutWaiting(t *testing.T) {
	server := newPolicyServer(0, 1)
	reader := openPolicyTestReader(t, server)

	page, err := reader.TryNextPage(context.Background())

	if err != nil || page != nil {
		t.Fatalf("want no page and no error, got %v and %v", page, err)
	}
	if waits := server.waitCount(); waits != 0 {
		t.Fatalf("a try must not hold a wait, saw %d", waits)
	}
}

func TestGivenAnIdleReaderWhenWaitingForTheNextPageThenShouldHoldAWaitAndReadWhatBecameReady(t *testing.T) {
	server := newPolicyServer(0, 1)
	reader := openPolicyTestReader(t, server)
	type answer struct {
		page *MatchedPage
		err  error
	}
	answered := make(chan answer, 1)
	go func() {
		page, err := reader.NextPage(context.Background())
		answered <- answer{page, err}
	}()
	server.awaitWait(t, 1)
	polls := server.pollCount()

	server.append(1, 1)

	select {
	case got := <-answered:
		if got.err != nil || got.page.PartitionID != 1 || len(got.page.Records) != 1 {
			t.Fatalf("want the record of partition 1, got %+v and %v", got.page, got.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the reader never returned the record that ended its wait")
	}
	if read := server.pollCount() - polls; read != 1 {
		t.Fatalf("a woken reader reads only the ready partition, saw %d reads", read)
	}
}

func TestGivenACancelledContextWhenWaitingForTheNextPageThenShouldReturnItsError(t *testing.T) {
	server := newPolicyServer(0)
	reader := openPolicyTestReader(t, server)
	ctx, cancel := context.WithCancel(context.Background())
	answered := make(chan error, 1)
	go func() {
		_, err := reader.NextPage(ctx)
		answered <- err
	}()
	server.awaitWait(t, 1)

	cancel()

	select {
	case err := <-answered:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("want the context error, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a cancelled read never returned")
	}
}

func TestGivenASparsePolicyWhenReadingOneRoundThenShouldReportMoreWorkAndWhatItExamined(t *testing.T) {
	server := newPolicyServer(0)
	server.selects = func(_ uint32, offset uint64) bool { return offset == 5 }
	server.digest = roundTestDigest(t)
	server.append(0, 6)
	reader := openPolicyTestReaderWith(t, server, GroupReaderOptions{Count: 2, MaxExamined: 4})

	page, more, err := reader.ReadRound(context.Background())

	if err != nil || page != nil || !more {
		t.Fatalf("want an empty round with more work, got page %v more %v err %v", page, more, err)
	}
	if examined := reader.ExaminedInRound(); examined != 4 {
		t.Fatalf("want four source records examined, got %d", examined)
	}
	if stored, ok := server.storedOffset(0); !ok || stored != 3 {
		t.Fatalf("an empty page stores its scan, stored %d (%v)", stored, ok)
	}

	page, more, err = reader.ReadRound(context.Background())

	if err != nil || page == nil || more || len(page.Records) != 1 || page.Records[0].Offset() != 5 {
		t.Fatalf("want the match at offset 5, got page %+v more %v err %v", page, more, err)
	}
	if examined := reader.ExaminedInRound(); examined != 2 {
		t.Fatalf("want two source records examined, got %d", examined)
	}
}

func TestGivenNamedPartitionsWhenReadingThenShouldReadOnlyThoseItIsAssigned(t *testing.T) {
	server := newPolicyServer(0, 1)
	server.append(0, 1)
	server.append(1, 1)
	reader := openPolicyTestReaderWith(t, server, GroupReaderOptions{Partitions: []uint32{1, 9}})

	page := nextRoundPage(t, reader)
	if page.PartitionID != 1 {
		t.Fatalf("want partition 1, got %d", page.PartitionID)
	}
	if err := reader.Ack(context.Background(), page); err != nil {
		t.Fatalf("acknowledge: %v", err)
	}
	again, err := reader.TryNextPage(context.Background())

	if err != nil || again != nil {
		t.Fatalf("want nothing more, got %v and %v", again, err)
	}
	server.mutex.Lock()
	defer server.mutex.Unlock()
	if server.pollsOf[0] != 0 {
		t.Fatalf("a partition left out must not be read, saw %d reads", server.pollsOf[0])
	}
}

func TestGivenAPageLimitWhenItIsReachedThenShouldNameTheRemedyUntilAcknowledged(t *testing.T) {
	server := newPolicyServer(0)
	server.append(0, 2)
	reader := openPolicyTestReaderWith(t, server, GroupReaderOptions{Count: 1, MaxUnackedPages: 1, IdleInterval: time.Hour})
	first := nextRoundPage(t, reader)

	_, err := reader.TryNextPage(context.Background())

	var invalid *InvalidError
	if !errors.As(err, &invalid) || !strings.Contains(err.Error(), "unacknowledged") {
		t.Fatalf("want the limit named, got %v", err)
	}
	if err := reader.Ack(context.Background(), first); err != nil {
		t.Fatalf("acknowledge: %v", err)
	}
	second, err := reader.TryNextPage(context.Background())
	if err != nil || second == nil || second.Records[0].Offset() != 1 {
		t.Fatalf("a full partition is read at once after the acknowledgment, got %+v and %v", second, err)
	}
	if polls := server.pollCount(); polls != 2 {
		t.Fatalf("a full partition must not be read, saw %d reads", polls)
	}
}

func TestGivenAnAcknowledgmentThatFindsNoPrimaryWhenStoredThenShouldSendTheSameTargetAgainUntilItIsTaken(t *testing.T) {
	server := newPolicyServer(0)
	server.append(0, 2)
	reader := openPolicyTestReaderWith(t, server, GroupReaderOptions{Count: 2, IdleInterval: time.Hour})
	page := nextRoundPage(t, reader)
	server.refuseAck(wire.ReasonUnavailable, wire.ReasonUnavailable, wire.ReasonUnavailable)

	err := reader.Ack(context.Background(), page)

	if err != nil {
		t.Fatalf("want the acknowledgment taken once a primary answers, got %v", err)
	}
	server.mutex.Lock()
	offsets := slices.Clone(server.ackOffsets)
	server.mutex.Unlock()
	if !slices.Equal(offsets, []uint64{1, 1, 1, 1}) {
		t.Fatalf("want four sends of the same target offset 1, got %v", offsets)
	}
}

func TestGivenAnAcknowledgmentThatNeverFindsAPrimaryWhenStoredThenShouldReportItAfterTheDeadline(t *testing.T) {
	server := newPolicyServer(0)
	server.append(0, 1)
	reader := openPolicyTestReaderWith(t, server, GroupReaderOptions{Count: 1, IdleInterval: time.Hour})
	reader.storeDeadline = 250 * time.Millisecond
	page := nextRoundPage(t, reader)
	refusals := make([]wire.FilterErrorReason, 64)
	for index := range refusals {
		refusals[index] = wire.ReasonUnavailable
	}
	server.refuseAck(refusals...)

	err := reader.Ack(context.Background(), page)

	var refusal *FilterError
	if !errors.As(err, &refusal) || refusal.Reason() != wire.ReasonUnavailable {
		t.Fatalf("want the refusal reported after the deadline, got %v", err)
	}
	server.mutex.Lock()
	sends := len(server.ackOffsets)
	server.mutex.Unlock()
	if sends < 4 {
		t.Fatalf("want the same acknowledgment sent again while the deadline held, saw %d sends", sends)
	}
}

func TestGivenANegativePageLimitWhenOpeningThenShouldRefuseIt(t *testing.T) {
	server := newPolicyServer(0)

	_, err := policyTestGroup(server).OpenReaderWith(context.Background(), GroupReaderOptions{MaxUnackedPages: -1})

	var invalid *InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want an InvalidError, got %v", err)
	}
	if server.joins != 0 {
		t.Fatal("a refused reader must not join the group")
	}
}

func TestGivenPagesOfSeveralMembershipsWhenAskedWhoOwnsThemThenShouldKeepOnlyTheCurrent(t *testing.T) {
	server := newPolicyServer(0)
	server.append(0, 1)
	reader := openPolicyTestReader(t, server)
	other := openPolicyTestReader(t, server)
	page := nextRoundPage(t, reader)

	if !reader.Owns(page) {
		t.Fatal("a reader owns the page it just read")
	}
	if other.Owns(page) {
		t.Fatal("another reader must not own the page")
	}
	if reader.Owns(nil) {
		t.Fatal("no reader owns a missing page")
	}

	reader.resetPartition(0)

	if reader.Owns(page) {
		t.Fatal("a page of a restarted partition is retired")
	}
}

func TestGivenRecordsWhenReadOneAtATimeThenShouldHandOverEachWithThePageThatAcknowledgesIt(t *testing.T) {
	server := newPolicyServer(0)
	server.append(0, 3)
	reader := openPolicyTestReaderWith(t, server, GroupReaderOptions{Count: 2})

	for offset := range uint64(3) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		record, err := reader.NextRecord(ctx)
		cancel()
		if err != nil {
			t.Fatalf("read record %d: %v", offset, err)
		}
		if record.Offset() != offset || record.Page() == nil {
			t.Fatalf("want offset %d with its page, got %d and %v", offset, record.Offset(), record.Page())
		}
		if err := reader.AckRecord(context.Background(), record.Page(), record.Offset()); err != nil {
			t.Fatalf("acknowledge offset %d: %v", offset, err)
		}
	}

	if stored, ok := server.storedOffset(0); !ok || stored != 2 {
		t.Fatalf("want every record stored, stored %d (%v)", stored, ok)
	}
}

func TestGivenAChangedPolicyWhenReadingARoundThenShouldRestartThePartitionAndReadOn(t *testing.T) {
	server := newPolicyServer(0)
	server.append(0, 1)
	server.refusePoll(0, wire.ReasonConflict)
	reader := openPolicyTestReader(t, server)

	page := nextRoundPage(t, reader)

	if len(page.Records) != 1 || page.Records[0].Offset() != 0 {
		t.Fatalf("want the record read again under the new policy, got %+v", page.Records)
	}
}

func TestGivenALostPartitionWhenReadingARoundThenShouldSyncAndReportOnlyTheSecondLoss(t *testing.T) {
	server := newPolicyServer(0)
	server.append(0, 1)
	server.refusePoll(0, wire.ReasonMembershipStale)
	reader := openPolicyTestReader(t, server)

	if page := nextRoundPage(t, reader); len(page.Records) != 1 {
		t.Fatalf("want the record after one lost read, got %+v", page.Records)
	}

	server.append(0, 1)
	server.refusePoll(0, wire.ReasonMembershipStale, wire.ReasonMembershipStale)
	_, err := reader.NextPage(context.Background())

	var refusal *FilterError
	if !errors.As(err, &refusal) || refusal.Reason() != wire.ReasonMembershipStale {
		t.Fatalf("want the second loss in a row reported, got %v", err)
	}
}

func TestGivenARecordThePolicyStopsOnWhenReadingARoundThenShouldReportItWithThePageThatSkipsIt(t *testing.T) {
	server := newPolicyServer(0)
	server.append(0, 2)
	server.fault(0, 0)
	reader := openPolicyTestReaderWith(t, server, GroupReaderOptions{IdleInterval: time.Hour})

	_, err := reader.TryNextPage(context.Background())

	var fault *FilterFaultError
	if !errors.As(err, &fault) || fault.PartitionID != 0 || fault.Offset != 0 || fault.Page == nil {
		t.Fatalf("want the fault at partition 0 offset 0 with its page, got %v", err)
	}
	if again, err := reader.TryNextPage(context.Background()); again != nil || err != nil {
		t.Fatalf("a blocked partition waits out its delay, got %v and %v", again, err)
	}
	if polls := server.pollCount(); polls != 1 {
		t.Fatalf("the blocked partition was read %d times before its delay passed", polls)
	}

	server.fault(0, noFault)
	if err := reader.Skip(context.Background(), fault.Page, fault.Offset); err != nil {
		t.Fatalf("skip the blocking record: %v", err)
	}

	if page := nextRoundPage(t, reader); page.Records[0].Offset() != 1 {
		t.Fatalf("want the record after the skipped one, got offset %d", page.Records[0].Offset())
	}
}

func TestGivenAFailedPartitionBesideOneWithRecordsWhenReadingThenShouldDeliverThePageBeforeTheFailure(t *testing.T) {
	server := newPolicyServer(0, 1)
	server.append(1, 1)
	server.refusePoll(0, wire.ReasonBackend)
	reader := openPolicyTestReader(t, server)

	page, err := reader.TryNextPage(context.Background())
	if err != nil || page == nil || page.PartitionID != 1 {
		t.Fatalf("want the page of partition 1 first, got %+v and %v", page, err)
	}
	_, err = reader.TryNextPage(context.Background())

	var refusal *FilterError
	if !errors.As(err, &refusal) || refusal.Reason() != wire.ReasonBackend {
		t.Fatalf("want the failure of partition 0 next, got %v", err)
	}
}

func TestGivenAnIdleIntervalWhenAskedThenShouldReportTheOneInUse(t *testing.T) {
	server := newPolicyServer(0)

	if idle := openPolicyTestReader(t, server).IdleInterval(); idle != 250*time.Millisecond {
		t.Fatalf("want the default of 250 ms, got %v", idle)
	}
	named := openPolicyTestReaderWith(t, server, GroupReaderOptions{IdleInterval: 40 * time.Millisecond})
	if idle := named.IdleInterval(); idle != 40*time.Millisecond {
		t.Fatalf("want the named 40 ms, got %v", idle)
	}
}

func TestGivenAStartWhenOpeningThenShouldReadTheFirstPagesFromIt(t *testing.T) {
	server := newPolicyServer(0)
	server.append(0, 5)
	start := wire.StartFromOffset(3)
	reader := openPolicyTestReaderWith(t, server, GroupReaderOptions{Start: &start})

	page := nextRoundPage(t, reader)

	if len(page.Records) != 2 || page.Records[0].Offset() != 3 {
		t.Fatalf("want the records from offset 3, got %+v", page.Records)
	}
}

func TestGivenALocalGuardWhenTheServerReturnsARecordTheFilterRejectsThenShouldFailThePageBeforeItIsSeen(t *testing.T) {
	filter := roundTestFilter()
	server := newPolicyServer(0)
	server.digest = roundTestDigest(t)
	server.selects = func(uint32, uint64) bool { return true }
	server.payload = func(_ uint32, offset uint64) []byte {
		if offset == 1 {
			return []byte(`{"table":"ground_stations"}`)
		}
		return []byte(`{"table":"satellites"}`)
	}
	server.append(0, 1)
	reader := openPolicyTestReader(t, server)
	guard, err := newLocalGuard(filter, nil)
	if err != nil {
		t.Fatalf("compile the guard: %v", err)
	}
	reader.guard, reader.guarded = guard, true

	page := nextRoundPage(t, reader)
	if err := reader.Ack(context.Background(), page); err != nil {
		t.Fatalf("acknowledge the agreed page: %v", err)
	}
	server.append(0, 1)
	_, err = reader.TryNextPage(context.Background())

	var refusal *FilterError
	if !errors.As(err, &refusal) || refusal.Reason() != wire.ReasonBackend {
		t.Fatalf("want the disagreement reported, got %v", err)
	}
	if reader.progress[0].inFlight() != 0 {
		t.Fatal("a page the guard refused must not be tracked as delivered")
	}
}

func TestGivenRecordsTheFilterSelectsWhenTheGuardChecksThenShouldPass(t *testing.T) {
	guard, page := roundTestGuard(t)

	if err := guard.check(page, guardRecords(`{"table":"satellites"}`)); err != nil {
		t.Fatalf("the local evaluation agrees: %v", err)
	}
}

func TestGivenARecordTheFilterRejectsWhenTheGuardChecksThenShouldFailBeforeExposure(t *testing.T) {
	guard, page := roundTestGuard(t)

	err := guard.check(page, guardRecords(`{"table":"ground_stations"}`))

	var refusal *FilterError
	if !errors.As(err, &refusal) || refusal.Reason() != wire.ReasonBackend {
		t.Fatalf("want a backend disagreement, got %v", err)
	}
}

func TestGivenAPageRunUnderAnotherFilterWhenTheGuardChecksThenShouldReportAConflict(t *testing.T) {
	guard, page := roundTestGuard(t)
	other, err := wire.NewConsumerFilter(mustPresent(t, "kind"), wire.CodecJSON).Digest()
	if err != nil {
		t.Fatalf("digest: %v", err)
	}
	page.Policy.Digest = &other

	err = guard.check(page, nil)

	var refusal *FilterError
	if !errors.As(err, &refusal) || refusal.Reason() != wire.ReasonConflict {
		t.Fatalf("want a conflict, got %v", err)
	}
}

func TestGivenAForgedUnevaluatedOffsetWithoutAPassPolicyWhenTheGuardChecksThenShouldReject(t *testing.T) {
	guard, page := roundTestGuard(t)
	page.Unevaluated = []wire.RecordFault{{Offset: 0, Reason: wire.FaultMalformed}}

	if err := guard.check(page, guardRecords(`{"table":"ground_stations"}`)); err == nil {
		t.Fatal("an unevaluated record without a pass policy must be refused")
	}
}

func TestGivenAMatchedRecordWhenDecodedAsJSONThenShouldFillTheValueOrSayWhyNot(t *testing.T) {
	record := MatchedRecord{Message: iggcon.IggyMessage{Payload: []byte(`{"table":"satellites"}`)}}
	var row struct {
		Table string `json:"table"`
	}

	if err := record.JSON(&row); err != nil || row.Table != "satellites" {
		t.Fatalf("want the table decoded, got %q and %v", row.Table, err)
	}
	record.Message.Payload = []byte("{")
	var codec *CodecError
	if err := record.JSON(&row); !errors.As(err, &codec) {
		t.Fatalf("want a CodecError for a broken body, got %v", err)
	}
}

func TestGivenAMatchedRecordWhenItsHeaderBlockIsBrokenThenShouldSaySo(t *testing.T) {
	sound := MatchedRecord{Message: iggcon.IggyMessage{Payload: []byte("x")}}
	broken := MatchedRecord{Message: iggcon.IggyMessage{Payload: []byte("x"), UserHeaders: []byte{0xff, 0xff, 0xff}}}

	if sound.HeadersMalformed() {
		t.Fatal("a record without headers has no malformed block")
	}
	if !broken.HeadersMalformed() {
		t.Fatal("a truncated header block is malformed")
	}
}

func roundTestFilter() wire.ConsumerFilter {
	return wire.NewConsumerFilter(wire.ExprPredicate("table", wire.CmpEq, wire.StringTyped("satellites")), wire.CodecJSON)
}

func roundTestDigest(t *testing.T) wire.Digest32 {
	t.Helper()
	digest, err := roundTestFilter().Digest()
	if err != nil {
		t.Fatalf("digest: %v", err)
	}
	return digest
}

func roundTestGuard(t *testing.T) (*localGuard, wire.FilteredPage) {
	t.Helper()
	guard, err := newLocalGuard(roundTestFilter(), nil)
	if err != nil {
		t.Fatalf("compile the guard: %v", err)
	}
	digest := roundTestDigest(t)
	return guard, wire.FilteredPage{V: wire.FilterOpVersion, Policy: wire.FilteredPolicy(nil, digest), Examined: 1, Matched: 1}
}

func guardRecords(payloads ...string) []MatchedRecord {
	records := make([]MatchedRecord, len(payloads))
	for offset, payload := range payloads {
		records[offset].Message.Payload = []byte(payload)
		records[offset].Message.Header.Offset = uint64(offset)
	}
	return records
}

func openPolicyTestReaderWith(t *testing.T, server *policyServer, options GroupReaderOptions) *GroupReader {
	t.Helper()
	reader, err := policyTestGroup(server).OpenReaderWith(context.Background(), options)
	if err != nil {
		t.Fatalf("open the group reader: %v", err)
	}
	t.Cleanup(func() { _ = reader.Close(context.Background()) })
	return reader
}

func nextRoundPage(t *testing.T, reader *GroupReader) *MatchedPage {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	page, err := reader.NextPage(ctx)
	if err != nil {
		t.Fatalf("read the next page: %v", err)
	}
	return page
}

func mustPresent(t *testing.T, path string) wire.FilterExpr {
	t.Helper()
	expr, err := wire.ExprPresentPath(path)
	if err != nil {
		t.Fatalf("present %q: %v", path, err)
	}
	return expr
}
