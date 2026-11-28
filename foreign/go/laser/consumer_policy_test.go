package laser

import (
	"context"
	"encoding/binary"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenALostPartitionMidPageWhenSeekingAnotherThenShouldReadOnWithoutThePage(t *testing.T) {
	server := newPolicyServer(0, 1)
	server.append(0, 3)
	server.append(1, 2)
	consumer := openPolicyTestConsumer(t, server, ConsumerOptions{})
	first := nextRecord(t, consumer)
	if first.PartitionID != 0 || first.Offset() != 0 {
		t.Fatalf("first delivery came from partition %d offset %d", first.PartitionID, first.Offset())
	}

	server.assign(1)
	if err := consumer.Seek(context.Background(), 1, iggcon.FirstPollingStrategy()); err != nil {
		t.Fatalf("seek a partition still held: %v", err)
	}

	for _, offset := range []uint64{0, 1} {
		record := nextRecord(t, consumer)
		if record.PartitionID != 1 || record.Offset() != offset {
			t.Fatalf("want partition 1 offset %d, got partition %d offset %d", offset, record.PartitionID, record.Offset())
		}
	}
}

func TestGivenAHandlerFailureWhenConsumingAgainThenShouldStoreLaterRecords(t *testing.T) {
	server := newPolicyServer(0)
	server.append(0, 3)
	consumer := openPolicyTestConsumer(t, server, ConsumerOptions{})
	failure := errors.New("handler failed")
	finished := errors.New("finished")

	err := consumer.Each(context.Background(), func(_ context.Context, record *Record) error {
		if record.Offset() == 1 {
			return failure
		}
		return nil
	})
	if !errors.Is(err, failure) {
		t.Fatalf("want the handler failure, got %v", err)
	}
	err = consumer.Each(context.Background(), func(_ context.Context, record *Record) error {
		if record.Offset() != 2 {
			t.Errorf("want the record after the failed one, got offset %d", record.Offset())
		}
		return finished
	})
	if !errors.Is(err, finished) {
		t.Fatalf("want the second handler result, got %v", err)
	}
}

func TestGivenAFailedRecordWhenALaterOneCountsThenShouldStoreThroughTheLaterOne(t *testing.T) {
	server := newPolicyServer(0)
	server.append(0, 3)
	consumer := openPolicyTestConsumer(t, server, ConsumerOptions{})
	failure := errors.New("handler failed")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := consumer.Each(ctx, func(_ context.Context, record *Record) error {
		if record.Offset() == 1 {
			return failure
		}
		return nil
	})
	if !errors.Is(err, failure) {
		t.Fatalf("want the handler failure, got %v", err)
	}
	_ = consumer.Each(ctx, func(_ context.Context, record *Record) error {
		if record.Offset() == 2 {
			cancel()
		}
		return nil
	})

	if stored, ok := server.storedOffset(0); !ok || stored != 2 {
		t.Fatalf("the batch end must be stored past the failed record, stored %d (%v)", stored, ok)
	}
}

func TestGivenAChangedPolicyWhenReadingThenShouldRestartThePartitionAndReadOn(t *testing.T) {
	server := newPolicyServer(0)
	server.append(0, 1)
	server.refusePoll(0, wire.ReasonConflict)
	consumer := openPolicyTestConsumer(t, server, ConsumerOptions{})

	if record := nextRecord(t, consumer); record.Offset() != 0 {
		t.Fatalf("want offset 0 after the policy change, got %d", record.Offset())
	}
}

func TestGivenALostPartitionWhenStoringItsProgressThenShouldDropTheStoreAndReadOn(t *testing.T) {
	server := newPolicyServer(0)
	server.append(0, 2)
	every := iggcon.AutoCommit{Every: iggcon.CommitEvery{Kind: iggcon.CommitEveryMessage}, After: iggcon.CommitAfterProcessing}
	consumer := openPolicyTestConsumer(t, server, ConsumerOptions{AutoCommit: &every})
	if record := nextRecord(t, consumer); record.Offset() != 0 {
		t.Fatalf("want offset 0 first, got %d", record.Offset())
	}

	server.refuseAck(wire.ReasonMembershipStale)

	// Nothing was stored, so the partition restarts and its owner reads the record again.
	if record := nextRecord(t, consumer); record.Offset() != 0 {
		t.Fatalf("want the unstored record again, got offset %d", record.Offset())
	}
}

func TestGivenAPartitionLostTwiceInARowWhenReadingThenShouldReportTheSecondLoss(t *testing.T) {
	server := newPolicyServer(0)
	server.append(0, 1)
	server.refusePoll(0, wire.ReasonMembershipStale, wire.ReasonMembershipStale)
	consumer := openPolicyTestConsumer(t, server, ConsumerOptions{})

	_, err := consumer.Next(context.Background())
	var refusal *FilterError
	if !errors.As(err, &refusal) || refusal.Reason() != wire.ReasonMembershipStale {
		t.Fatalf("want the second membership refusal, got %v", err)
	}
	if polls := server.pollCount(); polls != 2 {
		t.Fatalf("the first loss must be read again once, saw %d reads", polls)
	}
}

func TestGivenASeekWhenReadingThenShouldDeliverFromTheNewPosition(t *testing.T) {
	server := newPolicyServer(0)
	server.append(0, 3)
	consumer := openPolicyTestConsumer(t, server, ConsumerOptions{})
	nextRecord(t, consumer)
	nextRecord(t, consumer)

	if err := consumer.Seek(context.Background(), 0, iggcon.OffsetPollingStrategy(1)); err != nil {
		t.Fatalf("seek: %v", err)
	}

	if record := nextRecord(t, consumer); record.Offset() != 1 {
		t.Fatalf("want offset 1 after the seek, got %d", record.Offset())
	}
}

func TestGivenAnIdleConsumerWhenTheServerHoldsWaitsThenShouldWaitInsteadOfPolling(t *testing.T) {
	server := newPolicyServer(0)
	consumer := openPolicyTestConsumer(t, server, ConsumerOptions{})
	delivered := make(chan *Record, 1)
	go func() {
		record, err := consumer.Next(context.Background())
		if err == nil {
			delivered <- record
		}
	}()

	held := server.awaitWait(t, 1)[0]
	if len(held.Partitions) != 1 || held.Partitions[0].PartitionID != 0 || held.Partitions[0].Offset != 0 ||
		held.MaxWaitMs != 5000 || held.WaitID == 0 {
		t.Fatalf("unexpected filtered wait %+v", held)
	}
	polls := server.pollCount()
	server.append(0, 1)

	select {
	case record := <-delivered:
		if record.Offset() != 0 {
			t.Fatalf("want offset 0, got %d", record.Offset())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the ready wait did not deliver the record")
	}
	if after := server.pollCount(); polls != 1 || after != 2 {
		t.Fatalf("want one read before the wait and one after it, saw %d then %d", polls, after)
	}
}

func TestGivenWaitsOnSeveralNodesWhenOneAnswersReadyThenShouldCancelTheOthers(t *testing.T) {
	server := newPolicyServer(0, 1)
	server.route(0, 1)
	server.route(1, 2)
	consumer := openPolicyTestConsumer(t, server, ConsumerOptions{})
	delivered := make(chan *Record, 1)
	go func() {
		record, err := consumer.Next(context.Background())
		if err == nil {
			delivered <- record
		}
	}()

	waits := server.awaitWait(t, 2)
	if waits[0].WaitID == waits[1].WaitID {
		t.Fatal("each node's wait needs its own id")
	}
	quiet := waits[0]
	if quiet.Partitions[0].PartitionID == 0 {
		quiet = waits[1]
	}
	server.append(0, 1)

	select {
	case record := <-delivered:
		if record.PartitionID != 0 {
			t.Fatalf("want the ready partition, got %d", record.PartitionID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the ready wait did not deliver the record")
	}
	if cancelled := server.cancelled(); !slices.Contains(cancelled, quiet.WaitID) {
		t.Fatalf("the wait still held on the other node must be cancelled, cancelled %v", cancelled)
	}
}

func TestGivenAHeldWaitWhenTheAssignmentChangesThenShouldCancelItAndReadTheNewPartition(t *testing.T) {
	server := newPolicyServer(0)
	consumer := openPolicyTestConsumer(t, server, ConsumerOptions{})
	consumer.inner.(*policyConsumer).member.refresh = 5 * time.Millisecond
	delivered := make(chan *Record, 1)
	go func() {
		record, err := consumer.Next(context.Background())
		if err == nil {
			delivered <- record
		}
	}()
	held := server.awaitWait(t, 1)[0]

	server.append(1, 1)
	server.assign(0, 1)

	select {
	case record := <-delivered:
		if record.PartitionID != 1 {
			t.Fatalf("want the gained partition, got %d", record.PartitionID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the gained partition was not read")
	}
	if cancelled := server.cancelled(); !slices.Contains(cancelled, held.WaitID) {
		t.Fatalf("the wait of the old assignment must be cancelled, cancelled %v", cancelled)
	}
}

func TestGivenAHeldWaitWhenClosingThenShouldCancelIt(t *testing.T) {
	server := newPolicyServer(0)
	consumer := openPolicyTestConsumer(t, server, ConsumerOptions{})
	finished := make(chan error, 1)
	go func() {
		_, err := consumer.Next(context.Background())
		finished <- err
	}()
	held := server.awaitWait(t, 1)[0]

	if err := consumer.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}

	select {
	case err := <-finished:
		if err == nil {
			t.Fatal("a read on a closed consumer must end with an error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the read did not end when the consumer closed")
	}
	if cancelled := server.cancelled(); !slices.Contains(cancelled, held.WaitID) {
		t.Fatalf("closing must cancel the held wait, cancelled %v", cancelled)
	}
}

func TestGivenAServerWithoutFilteredWaitsWhenIdleThenShouldPaceByTheIdleInterval(t *testing.T) {
	server := newPolicyServer(0)
	server.maxWait = 0
	consumer := openPolicyTestConsumer(t, server, ConsumerOptions{})
	consumer.inner.(*policyConsumer).member.idle = time.Millisecond
	delivered := make(chan *Record, 1)
	go func() {
		record, err := consumer.Next(context.Background())
		if err == nil {
			delivered <- record
		}
	}()
	server.awaitPolls(t, 3)
	server.append(0, 1)

	select {
	case <-delivered:
	case <-time.After(5 * time.Second):
		t.Fatal("the paced read did not deliver the record")
	}
	if waits := server.waitCount(); waits != 0 {
		t.Fatalf("a server that announces no wait must not be sent one, saw %d", waits)
	}
}

func TestGivenABlockedPartitionWhenReadAgainThenShouldDeferItUntilSkipped(t *testing.T) {
	server := newPolicyServer(0)
	server.append(0, 2)
	server.fault(0, 0)
	consumer := openPolicyTestConsumer(t, server, ConsumerOptions{})
	consumer.inner.(*policyConsumer).member.retry = time.Hour

	_, err := consumer.Next(context.Background())
	var fault *FilterFaultError
	if !errors.As(err, &fault) || fault.PartitionID != 0 || fault.Offset != 0 {
		t.Fatalf("want the fault at partition 0 offset 0, got %v", err)
	}
	brief, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	_, err = consumer.Next(brief)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a deferred partition must wait out its delay, got %v", err)
	}
	if polls := server.pollCount(); polls != 1 {
		t.Fatalf("the blocked partition was read %d times before its delay passed", polls)
	}

	server.fault(0, noFault)
	if err := consumer.Skip(context.Background(), 0, 0); err != nil {
		t.Fatalf("skip the blocking record: %v", err)
	}
	if record := nextRecord(t, consumer); record.Offset() != 1 {
		t.Fatalf("want the record after the skipped one, got offset %d", record.Offset())
	}
}

func TestGivenCommitAfterDeliveryWhenTheStoreFailsThenShouldStillHandOverTheRecord(t *testing.T) {
	server := newPolicyServer(0)
	server.append(0, 2)
	delivery := iggcon.AutoCommit{Every: iggcon.CommitEvery{Kind: iggcon.CommitEveryMessage}, After: iggcon.CommitAfterDelivery}
	consumer := openPolicyTestConsumer(t, server, ConsumerOptions{AutoCommit: &delivery})
	server.refuseAck(wire.ReasonBackend)

	if record := nextRecord(t, consumer); record.Offset() != 0 {
		t.Fatalf("the delivered record must reach the caller, got offset %d", record.Offset())
	}
	_, err := consumer.Next(context.Background())
	var refusal *FilterError
	if !errors.As(err, &refusal) || refusal.Reason() != wire.ReasonBackend {
		t.Fatalf("the failed store must be reported by the next read, got %v", err)
	}
	if record := nextRecord(t, consumer); record.Offset() != 1 {
		t.Fatalf("want offset 1 after the report, got %d", record.Offset())
	}
}

func TestGivenACommitInFlightWhenClosingWithADeadlineThenShouldHonorIt(t *testing.T) {
	server := newPolicyServer(0)
	server.append(0, 1)
	consumer := openPolicyTestConsumer(t, server, ConsumerOptions{ManualCommit: true})
	record := nextRecord(t, consumer)
	release := server.holdAcks()
	committed := make(chan error, 1)
	go func() { committed <- consumer.Commit(context.Background(), record.Checkpoint) }()
	server.awaitAcks(t, 1)

	brief, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	err := consumer.Close(brief)
	cancel()

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("close must return when its context ends, got %v", err)
	}
	close(release)
	if err := <-committed; err != nil {
		t.Fatalf("the held commit: %v", err)
	}
	if err := consumer.Close(context.Background()); err != nil {
		t.Fatalf("close after the commit: %v", err)
	}
}

func TestGivenAReadInFlightWhenAnotherPartitionIsReadThenShouldNotWaitForIt(t *testing.T) {
	server := newPolicyServer(0, 1)
	server.append(0, 1)
	server.append(1, 1)
	reader := openPolicyTestReader(t, server)
	release := server.holdPolls(0)
	blocked := make(chan error, 1)
	go func() {
		_, err := reader.Read(0, wire.GroupPolicy()).Fetch(context.Background())
		blocked <- err
	}()
	server.awaitPolls(t, 1)

	fetched := make(chan error, 1)
	go func() {
		_, err := reader.Read(1, wire.GroupPolicy()).Fetch(context.Background())
		fetched <- err
	}()

	select {
	case err := <-fetched:
		if err != nil {
			t.Fatalf("read partition 1: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a read of one partition waited for a read of another")
	}
	close(release)
	if err := <-blocked; err != nil {
		t.Fatalf("read partition 0: %v", err)
	}
}

func TestGivenAPageWhenEachRecordIsAcknowledgedThenShouldStoreOnceTheLastOneIs(t *testing.T) {
	server := newPolicyServer(0)
	server.append(0, 2)
	reader := openPolicyTestReader(t, server)
	page, err := reader.Read(0, wire.GroupPolicy()).Fetch(context.Background())
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	if err := reader.AckRecord(context.Background(), page, 1); err != nil {
		t.Fatalf("acknowledge the second record: %v", err)
	}
	if _, stored := server.storedOffset(0); stored {
		t.Fatal("a page with an unfinished record must not be stored")
	}
	if err := reader.AckRecord(context.Background(), page, 0); err != nil {
		t.Fatalf("acknowledge the first record: %v", err)
	}

	if stored, ok := server.storedOffset(0); !ok || stored != 1 {
		t.Fatalf("want the page stored through offset 1, stored %d (%v)", stored, ok)
	}
}

func TestGivenTooManyUnacknowledgedPagesWhenReadingThenShouldNameTheRemedy(t *testing.T) {
	server := newPolicyServer(0)
	reader := openPolicyTestReader(t, server)
	progress := newPartitionProgress(wire.StartAtNext())
	for sequence := range uint64(defaultMaxUnackedPages) {
		progress.pages = append(progress.pages, &pendingPage{sequence: sequence, outstanding: map[uint64]struct{}{0: {}}})
	}
	reader.progress[0] = progress

	_, err := reader.Read(0, wire.GroupPolicy()).Fetch(context.Background())

	var invalid *InvalidError
	if !errors.As(err, &invalid) || !strings.Contains(err.Error(), "commit or acknowledge") {
		t.Fatalf("want the refusal to name the remedy, got %v", err)
	}
	if polls := server.pollCount(); polls != 0 {
		t.Fatalf("a saturated partition must not be read, saw %d reads", polls)
	}
}

func TestGivenRecordsHandedOverAndStoredWhenAskedForLocalOffsetsThenShouldReportTheLastOfEach(t *testing.T) {
	server := newPolicyServer(0)
	server.append(0, 2)
	consumer := openPolicyTestConsumer(t, server, ConsumerOptions{ManualCommit: true})
	if _, known := consumer.LastConsumedOffset(0); known {
		t.Fatal("a consumer that handed nothing over knows no consumed offset")
	}
	first := nextRecord(t, consumer)
	second := nextRecord(t, consumer)

	if consumed, known := consumer.LastConsumedOffset(0); !known || consumed != second.Offset() {
		t.Fatalf("want the last handed over offset 1, got %d (%v)", consumed, known)
	}
	if _, known := consumer.LastStoredOffset(0); known {
		t.Fatal("nothing was stored yet")
	}
	if err := consumer.Commit(context.Background(), first.Checkpoint); err != nil {
		t.Fatalf("commit: %v", err)
	}

	if stored, known := consumer.LastStoredOffset(0); !known || stored != 0 {
		t.Fatalf("want the committed offset 0, got %d (%v)", stored, known)
	}
}

func TestGivenAnAutomaticStoreWhenAskedForTheLastStoredOffsetThenShouldReportIt(t *testing.T) {
	server := newPolicyServer(0)
	server.append(0, 1)
	delivery := iggcon.AutoCommit{Every: iggcon.CommitEvery{Kind: iggcon.CommitEveryMessage}, After: iggcon.CommitAfterDelivery}
	consumer := openPolicyTestConsumer(t, server, ConsumerOptions{AutoCommit: &delivery})

	nextRecord(t, consumer)

	if stored, known := consumer.LastStoredOffset(0); !known || stored != 0 {
		t.Fatalf("want the acknowledged offset 0, got %d (%v)", stored, known)
	}
}

func TestGivenAJoinThatFailsWhenRetriesAreAllowedThenShouldOpenAfterThem(t *testing.T) {
	server := newPolicyServer(0)
	server.joinFails = 2

	consumer, err := policyTestGroup(server).Consumer(context.Background(),
		ConsumerOptions{SkipGroupCreate: true, InitRetries: 2, InitRetryInterval: time.Millisecond})

	if err != nil {
		t.Fatalf("want the open to succeed on its third try, got %v", err)
	}
	_ = consumer.Close(context.Background())
	if server.joins != 3 {
		t.Fatalf("want three joins, saw %d", server.joins)
	}
}

func TestGivenAJoinThatFailsWhenNoRetryIsAllowedThenShouldReportTheFirstFailure(t *testing.T) {
	server := newPolicyServer(0)
	server.joinFails = 1

	_, err := policyTestGroup(server).Consumer(context.Background(), ConsumerOptions{SkipGroupCreate: true})

	var transport *TransportError
	if !errors.As(err, &transport) || server.joins != 1 {
		t.Fatalf("want the join failure after one try, got %v after %d joins", err, server.joins)
	}
}

func TestGivenRetrySettingsNoOpenCanRunUnderWhenOpeningThenShouldRefuseThem(t *testing.T) {
	for name, options := range map[string]ConsumerOptions{
		"a negative polling retry interval": {PollRetryInterval: -time.Second},
		"retries without an interval":       {InitRetries: 1},
	} {
		server := newPolicyServer(0)
		options.SkipGroupCreate = true

		_, err := policyTestGroup(server).Consumer(context.Background(), options)

		var invalid *InvalidError
		if !errors.As(err, &invalid) || server.joins != 0 {
			t.Fatalf("%s: want an InvalidError before any join, got %v after %d joins", name, err, server.joins)
		}
	}
}

func TestGivenAFailedReadWhenReadAgainThenShouldWaitThePollingRetryInterval(t *testing.T) {
	server := newPolicyServer(0)
	server.append(0, 1)
	server.refusePoll(0, wire.ReasonBackend)
	consumer := openPolicyTestConsumer(t, server, ConsumerOptions{PollRetryInterval: time.Hour})

	_, err := consumer.Next(context.Background())
	var refusal *FilterError
	if !errors.As(err, &refusal) {
		t.Fatalf("want the read failure, got %v", err)
	}
	brief, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	_, err = consumer.Next(brief)
	cancel()

	if !errors.Is(err, context.DeadlineExceeded) || server.pollCount() != 1 {
		t.Fatalf("a failed partition waits out the retry interval, got %v after %d reads", err, server.pollCount())
	}
	if retry := openPolicyTestConsumer(t, server, ConsumerOptions{}).inner.(*policyConsumer).member.retry; retry != time.Second {
		t.Fatalf("want the default retry interval of one second, got %v", retry)
	}
}

const noFault = ^uint64(0)

// policyServer serves group policy reads, fenced acknowledgments and filtered
// waits over an in-memory log, as an unbound group on one or more nodes.
type policyServer struct {
	iggcon.Client
	mutex       sync.Mutex
	changed     chan struct{}
	maxWait     time.Duration
	assigned    []uint32
	heads       map[uint32]uint64
	stored      map[uint32]uint64
	faults      map[uint32]uint64
	routes      map[uint32]uint8
	pollRefusal map[uint32][]wire.FilterErrorReason
	ackRefusal  []wire.FilterErrorReason
	pollGate    map[uint32]chan struct{}
	ackGate     chan struct{}
	polls, acks int
	ackOffsets  []uint64
	pollsOf     map[uint32]int
	// selects makes the group run a filter that keeps only the offsets it accepts.
	selects func(partition uint32, offset uint64) bool
	digest  wire.Digest32
	// payload is the body of one record, "record" when unset.
	payload   func(partition uint32, offset uint64) []byte
	joinFails int
	joins     int
	waits     []wire.FilteredWaitRequest
	held      map[uint64]chan []uint32
	cancels   []uint64
}

func newPolicyServer(assigned ...uint32) *policyServer {
	return &policyServer{
		changed: make(chan struct{}), maxWait: 5 * time.Second, assigned: assigned,
		heads: map[uint32]uint64{}, stored: map[uint32]uint64{}, faults: map[uint32]uint64{},
		routes: map[uint32]uint8{}, pollRefusal: map[uint32][]wire.FilterErrorReason{},
		pollGate: map[uint32]chan struct{}{}, held: map[uint64]chan []uint32{}, pollsOf: map[uint32]int{},
	}
}

func (server *policyServer) Close() error { return nil }

func (server *policyServer) JoinConsumerGroup(context.Context, iggcon.Identifier, iggcon.Identifier, iggcon.Identifier) error {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	server.joins++
	if server.joinFails > 0 {
		server.joinFails--
		return ierror.Disconnected{}
	}
	return nil
}

func (server *policyServer) LeaveConsumerGroup(context.Context, iggcon.Identifier, iggcon.Identifier, iggcon.Identifier) error {
	return nil
}

func (server *policyServer) SyncConsumerGroup(context.Context, iggcon.Identifier, iggcon.Identifier, iggcon.Identifier) (*iggcon.ConsumerGroupAssignment, error) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	return &iggcon.ConsumerGroupAssignment{Partitions: slices.Clone(server.assigned)}, nil
}

func (server *policyServer) SendBinaryRequest(ctx context.Context, code uint32, payload []byte) ([]byte, error) {
	if code == getPartitionRouteCode {
		partition := binary.LittleEndian.Uint32(payload[len(payload)-4:])
		server.mutex.Lock()
		replica, routed := server.routes[partition]
		server.mutex.Unlock()
		if !routed {
			return nil, ierror.InvalidCommand{}
		}
		route := make([]byte, iggcon.PartitionRouteSize)
		route[24] = replica
		return route, nil
	}
	return server.SendBinaryRequestToReplica(ctx, code, payload, 0)
}

func (server *policyServer) SendBinaryRequestToReplica(ctx context.Context, code uint32, payload []byte, _ uint8) ([]byte, error) {
	switch code {
	case wire.AgdxFilteredPollCode:
		return server.poll(payload)
	case wire.AgdxFilteredAckCode:
		return server.ack(payload)
	case wire.AgdxFilteredWaitCode:
		return server.wait(ctx, payload)
	case wire.AgdxFilteredWaitCancelCode:
		return server.cancel(payload)
	default:
		return nil, ierror.InvalidCommand{}
	}
}

func (server *policyServer) poll(payload []byte) ([]byte, error) {
	var request wire.FilteredPollRequest
	if err := wire.Unmarshal(payload, &request); err != nil {
		return nil, err
	}
	id := request.PartitionID
	server.mutex.Lock()
	server.polls++
	server.pollsOf[id]++
	server.signal()
	gate := server.pollGate[id]
	server.mutex.Unlock()
	if gate != nil {
		<-gate
	}
	server.mutex.Lock()
	defer server.mutex.Unlock()
	if refusals := server.pollRefusal[id]; len(refusals) > 0 {
		server.pollRefusal[id] = refusals[1:]
		return refused(refusals[0])
	}
	start, known := uint64(0), true
	switch request.Start.Kind {
	case wire.StartContinue:
		start = request.Start.Continuation.NextScanOffset
	case wire.StartAtOffset:
		start = request.Start.Offset
	case wire.StartFirst:
	default:
		known = false
		if stored, ok := server.stored[id]; ok {
			start = stored + 1
		}
	}
	head := server.heads[id]
	group := uint64(7)
	policy := wire.AppliedPolicy{Mode: wire.ModeUnfiltered, GroupID: &group}
	end := min(head, start+uint64(request.Count))
	stop := wire.StopEndOfVisible
	if end < head {
		stop = wire.StopFilled
	}
	if server.selects != nil {
		policy = wire.FilteredPolicy(&group, server.digest)
		budget := uint64(wire.MaxFilteredPageExamined)
		if request.MaxExamined != nil {
			budget = uint64(*request.MaxExamined)
		}
		end = min(head, start+budget)
		stop = wire.StopEndOfVisible
		if end < head {
			stop = wire.StopBudget
		}
	}
	end = max(end, start)
	var fault *wire.RecordFault
	if at, faulted := server.faults[id]; faulted && at >= start && at < end {
		end, stop = at, wire.StopFault
		fault = &wire.RecordFault{Offset: at, Reason: wire.FaultMalformed}
	}
	var offsets []uint64
	for offset := start; offset < end; offset++ {
		if server.selects != nil && !server.selects(id, offset) {
			continue
		}
		offsets = append(offsets, offset)
		if server.selects != nil && uint32(len(offsets)) == request.Count && offset+1 < end {
			end, stop, fault = offset+1, wire.StopFilled, nil
			break
		}
	}
	frontier := uint64(0)
	if head > 0 {
		frontier = head - 1
	}
	page := wire.FilteredPage{
		V: wire.FilterOpVersion, PartitionID: id,
		Policy:     policy,
		Generation: wire.SourceGeneration{StreamID: 1, TopicID: 2, PartitionID: id, PartitionIncarnation: 3},
		AckContext: wire.AckContext{MetadataOp: 9},
		Frontier:   frontier, Examined: uint32(end - start), Matched: uint32(len(offsets)), Stop: stop, Fault: fault,
		Records: policyRecords(id, frontier, offsets, server.payload),
	}
	if end > start {
		next, safe := end, end-1
		page.NextScanOffset, page.SafeAckOffset = &next, &safe
	} else if known {
		page.NextScanOffset = &start
	}
	return wire.Marshal(wire.FilterReply{Outcome: &wire.FilterOutcome{Kind: wire.FilterPageOutcome, Page: &page}})
}

func (server *policyServer) ack(payload []byte) ([]byte, error) {
	var request wire.FilteredAck
	if err := wire.Unmarshal(payload, &request); err != nil {
		return nil, err
	}
	server.mutex.Lock()
	server.acks++
	server.ackOffsets = append(server.ackOffsets, request.Offset)
	server.signal()
	gate := server.ackGate
	server.mutex.Unlock()
	if gate != nil {
		<-gate
	}
	server.mutex.Lock()
	defer server.mutex.Unlock()
	if len(server.ackRefusal) > 0 {
		reason := server.ackRefusal[0]
		server.ackRefusal = server.ackRefusal[1:]
		return refused(reason)
	}
	server.stored[request.PartitionID] = request.Offset
	receipt := wire.AckReceipt{PartitionID: request.PartitionID, Offset: request.Offset, Generation: request.Generation}
	return wire.Marshal(wire.FilterReply{Outcome: &wire.FilterOutcome{Kind: wire.FilterAcknowledged, Receipt: &receipt}})
}

func (server *policyServer) wait(ctx context.Context, payload []byte) ([]byte, error) {
	var request wire.FilteredWaitRequest
	if err := wire.Unmarshal(payload, &request); err != nil {
		return nil, err
	}
	answer := make(chan []uint32, 1)
	server.mutex.Lock()
	server.waits = append(server.waits, request)
	server.held[request.WaitID] = answer
	server.wake()
	server.signal()
	server.mutex.Unlock()
	var ready []uint32
	select {
	case ready = <-answer:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(time.Duration(request.MaxWaitMs) * time.Millisecond):
	}
	server.mutex.Lock()
	delete(server.held, request.WaitID)
	server.mutex.Unlock()
	result := wire.FilteredWaitResult{Ready: ready}
	return wire.Marshal(wire.FilterReply{Outcome: &wire.FilterOutcome{Kind: wire.FilterReady, WaitResult: &result}})
}

func (server *policyServer) cancel(payload []byte) ([]byte, error) {
	var request wire.FilteredWaitCancel
	if err := wire.Unmarshal(payload, &request); err != nil {
		return nil, err
	}
	server.mutex.Lock()
	defer server.mutex.Unlock()
	var ended []uint64
	for _, id := range request.WaitIDs {
		server.cancels = append(server.cancels, id)
		if answer := server.held[id]; answer != nil {
			delete(server.held, id)
			answer <- nil
			ended = append(ended, id)
		}
	}
	server.signal()
	result := wire.FilteredWaitCancelled{Cancelled: ended}
	return wire.Marshal(wire.FilterReply{Outcome: &wire.FilterOutcome{Kind: wire.FilterCancelled, Cancelled: &result}})
}

// wake answers every held wait that names a partition with a record at its offset.
func (server *policyServer) wake() {
	for _, request := range server.waits {
		answer := server.held[request.WaitID]
		if answer == nil {
			continue
		}
		var ready []uint32
		for _, partition := range request.Partitions {
			if server.heads[partition.PartitionID] > partition.Offset {
				ready = append(ready, partition.PartitionID)
			}
		}
		if len(ready) > 0 {
			delete(server.held, request.WaitID)
			answer <- ready
		}
	}
}

// signal wakes every test goroutine waiting for the server to change.
func (server *policyServer) signal() {
	close(server.changed)
	server.changed = make(chan struct{})
}

func (server *policyServer) await(t *testing.T, what string, reached func() bool) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		server.mutex.Lock()
		done, changed := reached(), server.changed
		server.mutex.Unlock()
		if done {
			return
		}
		select {
		case <-changed:
		case <-deadline:
			t.Fatalf("the server never saw %s", what)
		}
	}
}

func (server *policyServer) awaitWait(t *testing.T, count int) []wire.FilteredWaitRequest {
	t.Helper()
	server.await(t, "the filtered waits", func() bool { return len(server.held) >= count })
	server.mutex.Lock()
	defer server.mutex.Unlock()
	return slices.Clone(server.waits[len(server.waits)-count:])
}

func (server *policyServer) awaitPolls(t *testing.T, count int) {
	t.Helper()
	server.await(t, "the filtered reads", func() bool { return server.polls >= count })
}

func (server *policyServer) awaitAcks(t *testing.T, count int) {
	t.Helper()
	server.await(t, "the acknowledgments", func() bool { return server.acks >= count })
}

func (server *policyServer) append(partition uint32, records uint64) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	server.heads[partition] += records
	server.wake()
}

func (server *policyServer) assign(partitions ...uint32) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	server.assigned = partitions
}

func (server *policyServer) route(partition uint32, replica uint8) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	server.routes[partition] = replica
}

func (server *policyServer) fault(partition uint32, offset uint64) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	if offset == noFault {
		delete(server.faults, partition)
		return
	}
	server.faults[partition] = offset
}

func (server *policyServer) refusePoll(partition uint32, reasons ...wire.FilterErrorReason) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	server.pollRefusal[partition] = reasons
}

func (server *policyServer) refuseAck(reasons ...wire.FilterErrorReason) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	server.ackRefusal = reasons
}

func (server *policyServer) holdPolls(partition uint32) chan struct{} {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	gate := make(chan struct{})
	server.pollGate[partition] = gate
	return gate
}

func (server *policyServer) holdAcks() chan struct{} {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	server.ackGate = make(chan struct{})
	return server.ackGate
}

func (server *policyServer) storedOffset(partition uint32) (uint64, bool) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	offset, stored := server.stored[partition]
	return offset, stored
}

func (server *policyServer) pollCount() int {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	return server.polls
}

func (server *policyServer) waitCount() int {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	return len(server.waits)
}

func (server *policyServer) cancelled() []uint64 {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	return slices.Clone(server.cancels)
}

func refused(reason wire.FilterErrorReason) ([]byte, error) {
	failure := wire.FilterError{Code: reason.Code(), Reason: reason, Message: "scripted refusal"}
	return wire.Marshal(wire.FilterReply{Err: &failure})
}

// policyRecords frames records the way a filtered page carries them.
func policyRecords(partition uint32, frontier uint64, offsets []uint64, bodies func(uint32, uint64) []byte) []byte {
	body := make([]byte, pollPrefixSize)
	binary.LittleEndian.PutUint32(body[0:4], partition)
	binary.LittleEndian.PutUint64(body[4:12], frontier)
	binary.LittleEndian.PutUint32(body[12:16], uint32(len(offsets)))
	binary.LittleEndian.PutUint64(body[16:24], 3)
	binary.LittleEndian.PutUint64(body[32:40], 9)
	for _, offset := range offsets {
		payload := []byte("record")
		if bodies != nil {
			payload = bodies(partition, offset)
		}
		record := make([]byte, batchHeaderSize+batchMessageHeaderSize+len(payload))
		binary.LittleEndian.PutUint64(record[8:16], offset)
		binary.LittleEndian.PutUint64(record[32:40], uint64(len(record)))
		binary.LittleEndian.PutUint32(record[48:52], 1)
		frame := record[batchHeaderSize:]
		binary.LittleEndian.PutUint32(frame[36:40], uint32(len(payload)))
		copy(frame[batchMessageHeaderSize:], payload)
		body = append(body, record...)
	}
	return body
}

func policyTestGroup(server *policyServer) *ConsumerGroup {
	capabilities := OpenCapabilities()
	capabilities.Managed = true
	capabilities.Hello = HelloAnswered
	capabilities.Filters = FilterCaps{Native: true, GroupPolicyReads: true, MaxWait: server.maxWait}
	handle := FromClient(server, Options{DefaultStream: "agents", Capabilities: &capabilities})
	return handle.Topic("work").Group("workers")
}

func openPolicyTestConsumer(t *testing.T, server *policyServer, options ConsumerOptions) *Consumer {
	t.Helper()
	options.SkipGroupCreate = true
	consumer, err := policyTestGroup(server).Consumer(context.Background(), options)
	if err != nil {
		t.Fatalf("open the policy consumer: %v", err)
	}
	t.Cleanup(func() { _ = consumer.Close(context.Background()) })
	return consumer
}

func openPolicyTestReader(t *testing.T, server *policyServer) *GroupReader {
	t.Helper()
	reader, err := policyTestGroup(server).OpenReader(context.Background())
	if err != nil {
		t.Fatalf("open the group reader: %v", err)
	}
	t.Cleanup(func() { _ = reader.Close(context.Background()) })
	return reader
}

func nextRecord(t *testing.T, consumer *Consumer) *Record {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	record, err := consumer.Next(ctx)
	if err != nil {
		t.Fatalf("read the next record: %v", err)
	}
	return record
}
