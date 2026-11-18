package laser_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
)

// batchServer records each batch that reached the log and can be told to
// refuse the next ones.
type batchServer struct {
	iggcon.Client
	guard   sync.Mutex
	batches [][]iggcon.IggyMessage
	topics  []string
	keys    []iggcon.Partitioning
	refuse  int
	err     error
	// attempts counts every append the producer tried, refused ones
	// included, so a test waits for the timer rather than racing it.
	attempts int
	tried    chan struct{}
	// gate holds the first append until a test releases it, so a test can
	// have an append in flight while it closes the handle.
	gate chan struct{}
}

func (server *batchServer) Close() error { return nil }

func (server *batchServer) SendMessages(
	_ context.Context, _ iggcon.Identifier, topic iggcon.Identifier,
	partitioning iggcon.Partitioning, messages []iggcon.IggyMessage,
) (*iggcon.SendMessagesResponse, error) {
	named, err := topic.String()
	if err != nil {
		return nil, err
	}
	server.guard.Lock()
	server.attempts++
	gate, tried := server.gate, server.tried
	server.gate = nil
	server.guard.Unlock()
	if tried != nil {
		select {
		case tried <- struct{}{}:
		default:
		}
	}
	if gate != nil {
		<-gate
	}
	server.guard.Lock()
	defer server.guard.Unlock()
	if server.refuse > 0 {
		server.refuse--
		failure := server.err
		if failure == nil {
			failure = errors.New("the partition is unavailable")
		}
		return nil, failure
	}
	server.batches = append(server.batches, messages)
	server.topics = append(server.topics, named)
	server.keys = append(server.keys, partitioning)
	return &iggcon.SendMessagesResponse{}, nil
}

// appended is every batch that reached the topic under test, leaving out the
// policy evidence a governed send writes to the audit topic.
func (server *batchServer) appended() [][]iggcon.IggyMessage {
	server.guard.Lock()
	defer server.guard.Unlock()
	batches := make([][]iggcon.IggyMessage, 0, len(server.batches))
	for index, topic := range server.topics {
		if topic == "events" {
			batches = append(batches, server.batches[index])
		}
	}
	return batches
}

// refuseNext tells the fake to refuse the next appends.
func (server *batchServer) refuseNext(count int) {
	server.guard.Lock()
	defer server.guard.Unlock()
	server.refuse = count
	server.tried = make(chan struct{}, 16)
}

// awaitAttempt waits for the next append the producer tries, so a test acts
// after the timer rather than racing it.
func (server *batchServer) awaitAttempt(t *testing.T) {
	t.Helper()
	server.guard.Lock()
	tried := server.tried
	server.guard.Unlock()
	select {
	case <-tried:
	case <-time.After(5 * time.Second):
		t.Fatal("the producer never tried to append")
	}
}

func batchingHandle(server iggcon.Client) *laser.Laser {
	return laser.FromClient(server, laser.Options{DefaultStream: "agents"})
}

// Many records become one append, which is the whole point: the per-call
// overhead is paid once rather than per record.
func TestGivenManyRecordsWhenBatchedThenShouldAppendThemAsOneCall(t *testing.T) {
	server := &batchServer{}
	producer := batchingHandle(server).Topic("events").Batching().
		MaxRecords(3).Linger(time.Hour).Build()
	defer producer.Close(context.Background())

	for _, body := range []string{"one", "two", "three"} {
		if err := producer.Send(context.Background(), []byte(body), nil); err != nil {
			t.Fatalf("send: %v", err)
		}
	}

	appended := server.appended()
	if len(appended) != 1 {
		t.Fatalf("want one append, got %d", len(appended))
	}
	if len(appended[0]) != 3 {
		t.Errorf("want three records in it, got %d", len(appended[0]))
	}
	if string(appended[0][0].Payload) != "one" {
		t.Errorf("want the records in order, got %q", appended[0][0].Payload)
	}
	// The queue is empty again, so the next record starts a fresh batch.
	if producer.Queued() != 0 {
		t.Errorf("want an empty queue, got %d", producer.Queued())
	}
}

// A batch flushes on whichever bound trips first, so a caller sets the one it
// cares about and the others stay out of the way.
func TestGivenEachBoundWhenTrippedThenShouldFlushOnIt(t *testing.T) {
	t.Run("the record count", func(t *testing.T) {
		server := &batchServer{}
		producer := batchingHandle(server).Topic("events").Batching().
			MaxRecords(2).MaxBytes(1 << 20).Linger(time.Hour).Build()
		defer producer.Close(context.Background())

		for range 2 {
			if err := producer.Send(
				context.Background(), []byte("x"), nil); err != nil {
				t.Fatalf("send: %v", err)
			}
		}

		if len(server.appended()) != 1 {
			t.Errorf("want the count to trip a flush, got %d appends",
				len(server.appended()))
		}
	})

	t.Run("the payload bytes", func(t *testing.T) {
		server := &batchServer{}
		producer := batchingHandle(server).Topic("events").Batching().
			MaxRecords(1000).MaxBytes(8).Linger(time.Hour).Build()
		defer producer.Close(context.Background())

		if err := producer.Send(
			context.Background(), []byte("0123456789"), nil); err != nil {
			t.Fatalf("send: %v", err)
		}

		if len(server.appended()) != 1 {
			t.Errorf("want the bytes to trip a flush, got %d appends",
				len(server.appended()))
		}
	})

	t.Run("the linger", func(t *testing.T) {
		server := &batchServer{}
		producer := batchingHandle(server).Topic("events").Batching().
			MaxRecords(1000).Linger(5 * time.Millisecond).Build()
		defer producer.Close(context.Background())

		if err := producer.Send(
			context.Background(), []byte("alone"), nil); err != nil {
			t.Fatalf("send: %v", err)
		}

		// A trickle of records must not wait for a batch that never fills.
		deadline := time.Now().Add(2 * time.Second)
		for len(server.appended()) == 0 && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if len(server.appended()) != 1 {
			t.Error("want the linger to flush a queue that never fills")
		}
	})
}

// A bound of zero would flush on nothing or on everything, so each is floored
// at the smallest value that still means something.
func TestGivenAZeroBoundWhenBuiltThenShouldFloorIt(t *testing.T) {
	server := &batchServer{}
	producer := batchingHandle(server).Topic("events").Batching().
		MaxRecords(0).MaxBytes(0).Linger(0).Build()
	defer producer.Close(context.Background())

	records, bytes, linger := producer.Bounds()
	if records != 1 || bytes != 1 {
		t.Errorf("want both bounds floored at one, got %d records and %d bytes",
			records, bytes)
	}
	if linger != laser.MinLinger {
		t.Errorf("want the linger floored, got %v", linger)
	}

	if err := producer.Send(context.Background(), []byte("x"), nil); err != nil {
		t.Fatalf("send: %v", err)
	}
	// One record trips a floored count of one.
	if len(server.appended()) != 1 {
		t.Errorf("want a floored count to flush at one, got %d appends",
			len(server.appended()))
	}

	// What a caller asked for reads back unchanged when it means something.
	asked := batchingHandle(&batchServer{}).Topic("events").Batching().
		MaxRecords(7).MaxBytes(99).Linger(time.Second).Build()
	defer asked.Close(context.Background())
	records, bytes, linger = asked.Bounds()
	if records != 7 || bytes != 99 || linger != time.Second {
		t.Errorf("want the bounds as asked, got %d, %d, %v", records, bytes, linger)
	}
}

// Flush is the guaranteed path, so a caller that needs the last batch on the
// log awaits it rather than hoping the timer ran.
func TestGivenAPartialBatchWhenFlushedThenShouldAppendIt(t *testing.T) {
	server := &batchServer{}
	producer := batchingHandle(server).Topic("events").Batching().
		MaxRecords(1000).Linger(time.Hour).Build()
	if err := producer.Send(context.Background(), []byte("x"), nil); err != nil {
		t.Fatalf("send: %v", err)
	}

	if err := producer.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}

	if len(server.appended()) != 1 {
		t.Fatalf("want the partial batch appended, got %d", len(server.appended()))
	}
	// Flushing nothing appends nothing rather than an empty batch.
	if err := producer.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if len(server.appended()) != 1 {
		t.Error("an empty queue appends nothing")
	}
	if err := producer.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// A close drains rather than cancelling, because a cancelled flush would drop
// a batch already taken out of the queue and lose it silently.
func TestGivenAQueuedRecordWhenClosedThenShouldDrainIt(t *testing.T) {
	server := &batchServer{}
	producer := batchingHandle(server).Topic("events").Batching().
		MaxRecords(1000).Linger(time.Hour).Build()
	if err := producer.Send(context.Background(), []byte("x"), nil); err != nil {
		t.Fatalf("send: %v", err)
	}

	if err := producer.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}

	if len(server.appended()) != 1 {
		t.Errorf("want the queue drained, got %d appends", len(server.appended()))
	}
	// Closing twice is safe, because a caller cannot always tell whether it
	// already did.
	if err := producer.Close(context.Background()); err != nil {
		t.Errorf("closing twice must succeed: %v", err)
	}
}

// A failed append says what landed and what did not, because a caller that
// replayed everything would double what is already on the log and one that
// replayed nothing would lose what never arrived.
func TestGivenAFailedAppendWhenSentThenShouldListTheUnconfirmedRecords(t *testing.T) {
	server := &batchServer{}
	server.refuseNext(1)
	producer := batchingHandle(server).Topic("events").Batching().
		MaxRecords(2).Linger(time.Hour).Build()
	defer producer.Close(context.Background())

	err := producer.Send(context.Background(), []byte("one"), nil)
	if err != nil {
		t.Fatalf("the first record only queues: %v", err)
	}
	err = producer.Send(context.Background(), []byte("two"), nil)

	var failure *laser.PublishFailure
	if !errors.As(err, &failure) {
		t.Fatalf("want a PublishFailure, got %T: %v", err, err)
	}
	if len(failure.Unconfirmed) != 2 {
		t.Fatalf("want both records unconfirmed, got %d", len(failure.Unconfirmed))
	}
	if string(failure.Unconfirmed[0].Payload) != "one" {
		t.Errorf("want the queued records listed, got %q",
			failure.Unconfirmed[0].Payload)
	}
	if failure.Stream != "agents" || failure.Topic != "events" {
		t.Errorf("want the destination named, got %s/%s",
			failure.Stream, failure.Topic)
	}
	// The ids the attempt used are kept, so a resend stays deduplicated on a
	// server that deduplicates.
	if failure.Unconfirmed[0].Header.Id == (iggcon.MessageID{}) {
		t.Error("an unconfirmed record keeps the id its attempt used")
	}
	if failure.Unconfirmed[0].Header.Id == failure.Unconfirmed[1].Header.Id {
		t.Error("two records must not share an id")
	}
}

// A failed linger flush has nobody to tell, so it keeps its failure and the
// next caller hears it exactly once.
func TestGivenAFailedLingerFlushWhenFlushedThenShouldReportItOnce(t *testing.T) {
	server := &batchServer{}
	server.refuseNext(1)
	producer := batchingHandle(server).Topic("events").Batching().
		MaxRecords(1000).Linger(2 * time.Millisecond).Build()
	if err := producer.Send(context.Background(), []byte("x"), nil); err != nil {
		t.Fatalf("send: %v", err)
	}
	server.awaitAttempt(t)

	reported := producer.Flush(context.Background())

	var failure *laser.PublishFailure
	if !errors.As(reported, &failure) {
		t.Fatalf("want the kept failure, got %T: %v", reported, reported)
	}
	if len(failure.Unconfirmed) != 1 ||
		string(failure.Unconfirmed[0].Payload) != "x" {
		t.Errorf("want the lost record listed, got %+v", failure.Unconfirmed)
	}
	// Reported once: the next flush is clean, so a caller is not told twice
	// about one lost batch.
	if err := producer.Flush(context.Background()); err != nil {
		t.Errorf("a kept failure is reported once, got it again: %v", err)
	}
	// The timer kept running, so a record sent after the failure still lands.
	if err := producer.Send(context.Background(), []byte("after"), nil); err != nil {
		t.Fatalf("send: %v", err)
	}
	if err := producer.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}
	appended := server.appended()
	if len(appended) != 1 || string(appended[0][0].Payload) != "after" {
		t.Errorf("want the later record on the log, got %+v", appended)
	}
}

// Nothing queues behind a failed batch, so a sender that finds a kept failure
// is refused its own record rather than left adding to a queue nobody has
// told it about.
func TestGivenAKeptFailureWhenSendingThenShouldRefuseTheRecordAndReportBoth(t *testing.T) {
	server := &batchServer{}
	server.refuseNext(1)
	producer := batchingHandle(server).Topic("events").Batching().
		MaxRecords(1000).Linger(2 * time.Millisecond).Build()
	defer producer.Close(context.Background())
	if err := producer.Send(context.Background(), []byte("queued"), nil); err != nil {
		t.Fatalf("send: %v", err)
	}
	server.awaitAttempt(t)

	refused := producer.Send(context.Background(), []byte("refused"), nil)

	var failure *laser.PublishFailure
	if !errors.As(refused, &failure) {
		t.Fatalf("want the kept failure, got %T: %v", refused, refused)
	}
	// Both the batch that failed and the record that was refused are listed,
	// so a caller knows everything it has to decide about.
	if len(failure.Unconfirmed) != 2 {
		t.Fatalf("want both records listed, got %+v", failure.Unconfirmed)
	}
	if string(failure.Unconfirmed[0].Payload) != "queued" ||
		string(failure.Unconfirmed[1].Payload) != "refused" {
		t.Errorf("want the lost batch then the refused record, got %+v",
			failure.Unconfirmed)
	}
	// The refused record was never queued, so nothing is waiting behind it.
	if producer.Queued() != 0 {
		t.Errorf("want nothing queued behind a failure, got %d", producer.Queued())
	}
}

// Several batches failing before anyone asks is one report listing all their
// records, rather than one report per batch the caller has to stitch
// together.
func TestGivenSeveralFailedBatchesWhenFlushedThenShouldReportThemTogether(t *testing.T) {
	server := &batchServer{}
	server.refuseNext(2)
	producer := batchingHandle(server).Topic("events").Batching().
		MaxRecords(1000).Linger(2 * time.Millisecond).Build()
	defer producer.Close(context.Background())

	if err := producer.Send(context.Background(), []byte("one"), nil); err != nil {
		t.Fatalf("send: %v", err)
	}
	server.awaitAttempt(t)
	// The first failure is kept rather than reported, so a sender that
	// bypasses it queues into the next batch. Flushing clears it first.
	_ = producer.Send(context.Background(), []byte("two"), nil)
	if err := producer.Send(context.Background(), []byte("two"), nil); err != nil {
		t.Fatalf("send: %v", err)
	}
	server.awaitAttempt(t)

	reported := producer.Flush(context.Background())

	var failure *laser.PublishFailure
	if !errors.As(reported, &failure) {
		t.Fatalf("want one report, got %T: %v", reported, reported)
	}
	if len(failure.Unconfirmed) != 1 {
		t.Errorf("want the second batch's record, got %+v", failure.Unconfirmed)
	}
}

// A sender hears its own inline flush, because it is the one being pushed
// back on.
func TestGivenAnInlineFlushThatFailsWhenSentThenShouldTellTheSender(t *testing.T) {
	server := &batchServer{}
	server.refuseNext(2)
	producer := batchingHandle(server).Topic("events").Batching().
		MaxRecords(1).Linger(time.Hour).Build()
	defer producer.Close(context.Background())

	// Each send trips the count, so each is its own failed batch.
	first := producer.Send(context.Background(), []byte("one"), nil)
	second := producer.Send(context.Background(), []byte("two"), nil)

	var failure *laser.PublishFailure
	if !errors.As(first, &failure) {
		t.Fatalf("want the first sender told, got %T: %v", first, first)
	}
	if !errors.As(second, &failure) {
		t.Fatalf("want the second sender told, got %T: %v", second, second)
	}
}

// A close waits for an append already in flight, because a batch the timer
// took out of the queue is not in the queue for the closing flush to find.
func TestGivenAnAppendInFlightWhenClosedThenShouldWaitForIt(t *testing.T) {
	gate := make(chan struct{})
	server := &batchServer{gate: gate, tried: make(chan struct{}, 4)}
	producer := batchingHandle(server).Topic("events").Batching().
		MaxRecords(1000).Linger(2 * time.Millisecond).Build()
	if err := producer.Send(context.Background(), []byte("in flight"), nil); err != nil {
		t.Fatalf("send: %v", err)
	}
	server.awaitAttempt(t)

	closed := make(chan error, 1)
	go func() { closed <- producer.Close(context.Background()) }()

	// The close cannot finish while the append it has to wait for is held.
	select {
	case err := <-closed:
		t.Fatalf("close returned while an append was in flight: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(gate)
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("close: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("close never finished")
	}
	if len(server.appended()) != 1 {
		t.Errorf("want the in-flight batch on the log, got %d",
			len(server.appended()))
	}
}

// One key per handle by construction, so ordering within a key is never
// silently interleaved by records that went out under another.
func TestGivenAPinnedKeyWhenBatchedThenShouldSendEveryBatchUnderIt(t *testing.T) {
	server := &batchServer{}
	producer := batchingHandle(server).Topic("events").Batching().
		MaxRecords(1).Linger(time.Hour).PartitionKey("tenant-a").Build()
	defer producer.Close(context.Background())

	if err := producer.Send(context.Background(), []byte("x"), nil); err != nil {
		t.Fatalf("send: %v", err)
	}

	server.guard.Lock()
	keys := append([]iggcon.Partitioning(nil), server.keys...)
	server.guard.Unlock()
	if len(keys) != 1 {
		t.Fatalf("want one append, got %d", len(keys))
	}
	if string(keys[0].Value) != "tenant-a" {
		t.Errorf("want the pinned key, got %q", keys[0].Value)
	}

	// Without a key the server's balanced partitioner spreads each batch.
	balanced := &batchServer{}
	spread := batchingHandle(balanced).Topic("events").Batching().
		MaxRecords(1).Linger(time.Hour).Build()
	defer spread.Close(context.Background())
	if err := spread.Send(context.Background(), []byte("x"), nil); err != nil {
		t.Fatalf("send: %v", err)
	}
	balanced.guard.Lock()
	defer balanced.guard.Unlock()
	if balanced.keys[0].Kind != iggcon.Balanced {
		t.Errorf("want balanced partitioning, got %v", balanced.keys[0].Kind)
	}
}

// A batched record is governed before it is queued, because a policy that
// refused it must refuse it before it is batched with records that were
// allowed.
func TestGivenAGovernedProducerWhenSendingThenShouldDecideBeforeQueueing(t *testing.T) {
	governor := &scriptedGovernor{decision: laser.Block("no")}
	server := &batchServer{}
	producer := batchingHandle(server).
		WithGovernor(governor, laser.GovernorEnforce).
		Topic("events").Batching().MaxRecords(1).Linger(time.Hour).Build()
	defer producer.Close(context.Background())

	err := producer.Send(context.Background(), []byte("secrets"), nil)

	var blocked *laser.PolicyBlockedError
	if !errors.As(err, &blocked) {
		t.Fatalf("want a PolicyBlockedError, got %T: %v", err, err)
	}
	if producer.Queued() != 0 {
		t.Error("a refused record must not be queued")
	}
	if len(server.appended()) != 0 {
		t.Error("a refused record must not reach the log")
	}
}

// A governor replacing the body puts the replacement in the batch, so what is
// appended is what the policy allowed.
func TestGivenAGovernedReplacementWhenBatchedThenShouldQueueTheReplacement(t *testing.T) {
	governor := &scriptedGovernor{decision: laser.Modify([]byte("redacted"))}
	server := &batchServer{}
	producer := batchingHandle(server).
		WithGovernor(governor, laser.GovernorEnforce).
		Topic("events").Batching().MaxRecords(1).Linger(time.Hour).Build()
	defer producer.Close(context.Background())

	if err := producer.Send(
		context.Background(), []byte("the card number"), nil); err != nil {
		t.Fatalf("send: %v", err)
	}

	appended := server.appended()
	if len(appended) != 1 || string(appended[0][0].Payload) != "redacted" {
		t.Fatalf("want the replacement appended, got %q", appended[0][0].Payload)
	}
}

// Concurrent senders must not lose or duplicate a record, and every batch
// must reach the log whole.
func TestGivenConcurrentSendersWhenBatchedThenShouldAppendEveryRecordOnce(t *testing.T) {
	server := &batchServer{}
	producer := batchingHandle(server).Topic("events").Batching().
		MaxRecords(4).Linger(time.Millisecond).Build()
	const senders, each = 8, 25

	var sending sync.WaitGroup
	for sender := range senders {
		sending.Add(1)
		go func(sender int) {
			defer sending.Done()
			for index := range each {
				body := []byte{byte(sender), byte(index)}
				if err := producer.Send(
					context.Background(), body, nil); err != nil {
					t.Errorf("send: %v", err)
					return
				}
			}
		}(sender)
	}
	sending.Wait()
	if err := producer.Close(context.Background()); err != nil {
		t.Fatalf("close: %v", err)
	}

	seen := 0
	for _, batch := range server.appended() {
		seen += len(batch)
	}
	if seen != senders*each {
		t.Errorf("want every record appended once, got %d of %d",
			seen, senders*each)
	}
}
